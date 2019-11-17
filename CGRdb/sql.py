
insert_molecule = '''CREATE OR REPLACE FUNCTION "{schema}".cgrdb_insert_molecule_structure()
RETURNS TRIGGER
AS $$
from pickle import loads
# todo: check integrity

mfp = GD['cgrdb_mfp']
data = TD['new']
molecule = loads(data['structure'])
data['fingerprint'] = mfp._transform_bitset([molecule])[0]
data['signature'] = bytes(molecule)

return 'MODIFY'
$$ LANGUAGE plpython3u'''.replace('$', '$$')


insert_molecule_trigger = '''CREATE TRIGGER cgrdb_insert_molecule_structure
    BEFORE INSERT ON "{schema}"."MoleculeStructure" FOR EACH ROW
    EXECUTE PROCEDURE "{schema}".cgrdb_insert_molecule_structure()'''


setup_fingerprint = '''CREATE OR REPLACE FUNCTION "{schema}".cgrdb_setup_fingerprint(cfg json)
RETURNS VOID
AS $$
from CIMtools.preprocessing import FragmentorFingerprint
from json import loads

config = loads(cfg)
molecule = config.get('molecule', {})
reaction = config.get('reaction', {})
GD['cgrdb_mfp'] = FragmentorFingerprint(**molecule)
GD['cgrdb_rfp'] = FragmentorFingerprint(**reaction)

$$ LANGUAGE plpython3u'''.replace('$', '$$')


# return cache id and number of found molecules for given pickled molecule
search_similar_molecules = '''CREATE OR REPLACE FUNCTION
"{schema}".cgrdb_search_similar_molecules(data bytea, OUT id integer, OUT count integer)
AS $$
from pickle import loads
 
molecule = loads(data)
sg = bytes(molecule).hex()

# test for existing cache
found = plpy.execute('SELECT x.id, array_length(x.molecules, 1) as count '
                     'FROM "{schema}"."MoleculeSearchCache" x '
                     "WHERE x.operator = 'similar' AND x.signature = '\\\\x{sg}'::bytea".replace('{sg}', sg))
if found:
    return found[0]

# cache not found. lets start searching
fp = GD['cgrdb_mfp']._transform_bitset([molecule])[0]

plpy.execute('CREATE TEMPORARY TABLE cgrdb_query(m INTEGER, s INTEGER, t DOUBLE PRECISION) ON COMMIT DROP')
plpy.execute('INSERT INTO cgrdb_query(m, s, t) '
             'SELECT x.molecule, x.id, smlar(x.fingerprint, ARRAY{fp}) '
             'FROM "{schema}"."MoleculeStructure" x '
             'WHERE x.fingerprint % ARRAY{fp}'.replace('{fp}', str(fp)))
# check for empty results
if not plpy.execute('SELECT COUNT(*) FROM cgrdb_query')[0]['count']:
    return (0, 0)

# store found molecules to cache
found = plpy.execute('INSERT INTO '
             '"{schema}"."MoleculeSearchCache"(signature, operator, date, molecules, structures, tanimotos) '
             "SELECT '\\\\x{sg}'::bytea, 'similar', CURRENT_TIMESTAMP, array_agg(o.m), array_agg(o.s), array_agg(o.t) "
             'FROM ('
                 'SELECT h.m, h.s, h.t '
                 'FROM ('
                     'SELECT DISTINCT ON (f.m) m, f.s, f.t '
                     'FROM cgrdb_query f '
                     'ORDER BY f.m, f.t DESC'
                 ') h '
                 'ORDER BY h.t DESC'
             ') o '
             'ON CONFLICT DO NOTHING '
             'RETURNING id, array_length(molecules, 1) as count'.replace('{sg}', sg))
plpy.execute('DROP TABLE cgrdb_query')

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute('SELECT x.id, array_length(x.molecules, 1) as count '
                         'FROM "{schema}"."MoleculeSearchCache" x '
                         "WHERE x.operator = 'similar' AND x.signature = '\\\\x{sg}'::bytea".replace('{sg}', sg))[0]

return found
$$ LANGUAGE plpython3u'''.replace('$', '$$')
