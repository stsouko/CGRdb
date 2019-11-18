/*
#  Copyright 2019 Ramil Nugmanov <stsouko@live.ru>
#  This file is part of CGRdb.
#
#  CGRdb is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA 02110-1301, USA.
*/

CREATE OR REPLACE FUNCTION
"{schema}".cgrdb_search_substructure_molecules(data bytea, OUT id integer, OUT count integer)
AS $$
from CGRtools.containers import MoleculeContainer, QueryContainer
from pickle import loads

molecule = loads(data)
if isinstance(molecule, QueryContainer):
    screen = MoleculeContainer()  # convert query to molecules for screening
    for n, a in molecule.atoms():
        screen.add_atom(a.copy(), _map=n, charge=a.charge, is_radical=a.is_radical)
    for n, m, b in molecule.bonds():
        screen.add_bond(n, m, b)
elif isinstance(molecule, MoleculeContainer):
    screen = molecule
else:
    raise TypeError('MoleculeContainer or QueryContainer required')

sg = bytes(molecule).hex()
# test for existing cache

get_cache = '''SELECT x.id, array_length(x.molecules, 1) as count
FROM "{schema}"."MoleculeSearchCache" x
WHERE x.operator = 'substructure' AND x.signature = '\\x{sg}'::bytea'''.replace('{sg}', sg)

found = plpy.execute(get_cache)
if found:
    return found[0]

# cache not found. lets start searching
fp = GD['cgrdb_mfp']._transform_bitset([screen])[0]

plpy.execute('CREATE TEMPORARY TABLE cgrdb_query(m integer, s integer, t double precision) ON COMMIT DROP')
plpy.execute('''INSERT INTO cgrdb_query(m, s, t)
SELECT x.molecule, x.id, smlar(x.fingerprint, ARRAY{fp})
FROM "{schema}"."MoleculeStructure" x
WHERE x.fingerprint @> ARRAY{fp}'''.replace('{fp}', str(fp)))

# check for empty results
if not plpy.execute('SELECT COUNT(*) FROM cgrdb_query')[0]['count']:
    # store empty cache
    found = plpy.execute('''INSERT INTO
"{schema}"."MoleculeSearchCache"(signature, operator, date, molecules, structures, tanimotos)
VALUES ('\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::integer[], ARRAY[]::integer[])
ON CONFLICT DO NOTHING
RETURNING id, 0 as count'''.replace('{sg}', sg))

    # concurrent process stored same query. just reuse it
    if not found:
        found = plpy.execute(get_cache)
    return found[0]

# get most similar structure for each molecule
mi, si, st = plpy.execute('''SELECT array_agg(o.m), array_agg(o.s), array_agg(o.t)
FROM (
    SELECT h.m, h.s, h.t
    FROM (
        SELECT DISTINCT ON (f.m) m, f.s, f.t
        FROM cgrdb_query f
        ORDER BY f.m, f.t DESC
    ) h
    ORDER BY h.t DESC
) o''')[0]

plpy.execute('DROP TABLE cgrdb_query')

# todo: filter structures

# store found molecules to cache
found = plpy.execute('''INSERT INTO
"{schema}"."MoleculeSearchCache"(signature, operator, date, molecules, structures, tanimotos)
VALUES ('\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY{mi}, ARRAY{si}, ARRAY{st})
ON CONFLICT DO NOTHING
RETURNING id, array_length(molecules, 1) as count'''.format(sg=sg, mi=mi, si=si, st=st))

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute(get_cache)

return found[0]
$$ LANGUAGE plpython3u
