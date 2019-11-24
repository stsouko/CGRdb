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
from compress_pickle import loads

molecule = loads(data, compression='gzip')
if isinstance(molecule, QueryContainer):
    screen = MoleculeContainer()  # convert query to molecules for screening
    for n, a in molecule.atoms():
        screen.add_atom(a.copy(), _map=n, charge=a.charge, is_radical=a.is_radical)
    for n, m, b in molecule.bonds():
        screen.add_bond(n, m, b)
elif isinstance(molecule, MoleculeContainer):
    screen = molecule
else:
    raise plpy.DataException('MoleculeContainer or QueryContainer required')

sg = bytes(molecule).hex()
# test for existing cache

get_cache = f'''SELECT x.id, array_length(x.molecules, 1) as count
FROM "{schema}"."MoleculeSearchCache" x
WHERE x.operator = 'substructure' AND x.signature = '\\x{sg}'::bytea'''

found = plpy.execute(get_cache)
if found:
    return found[0]

# cache not found. lets start searching
fp = GD['cgrdb_mfp'].transform_bitset([screen])[0]

plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
SELECT x.molecule AS m, x.id AS s, smlar(x.fingerprint, ARRAY{fp}::integer[]) AS t
FROM "{schema}"."MoleculeStructure" x
WHERE x.fingerprint @> ARRAY{fp}::integer[]''')

# check for empty results
if not plpy.execute('SELECT COUNT(*) FROM cgrdb_query')[0]['count']:
    # store empty cache
    found = plpy.execute(f'''INSERT INTO
"{schema}"."MoleculeSearchCache"(signature, operator, date, molecules, tanimotos)
VALUES ('\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::real[])
ON CONFLICT DO NOTHING
RETURNING id, 0 as count''')

    # concurrent process stored same query. just reuse it
    if not found:
        found = plpy.execute(get_cache)
    return found[0]

# get most similar structure for each molecule
get_data = '''SELECT h.m, h.t, s.structure as d
FROM (
    SELECT DISTINCT ON (f.m) m, f.s, f.t
    FROM cgrdb_query f
    ORDER BY f.m, f.t DESC
) h LEFT JOIN "{schema}"."MoleculeStructure" s ON h.s = s.id
ORDER BY h.t DESC'''
mis, sts = [], []
for row in plpy.cursor(get_data):
    if molecule <= loads(row['d'], compression='gzip'):
        mis.append(row['m'])
        sts.append(row['t'])

plpy.execute('DROP TABLE cgrdb_query')

# store found molecules to cache
found = plpy.execute(f'''INSERT INTO
"{schema}"."MoleculeSearchCache"(signature, operator, date, molecules, tanimotos)
VALUES ('\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY{mis}::integer[], ARRAY{sts}::real[])
ON CONFLICT DO NOTHING
RETURNING id, array_length(molecules, 1) as count''')

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute(get_cache)

return found[0]
$$ LANGUAGE plpython3u
