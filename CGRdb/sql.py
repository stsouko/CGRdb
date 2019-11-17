
insert_molecule = '''CREATE OR REPLACE FUNCTION "{schema}".cgrdb_insert_molecule_structure()
RETURNS TRIGGER
AS $$
from pickle import loads

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
