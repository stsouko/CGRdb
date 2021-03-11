/*
#  Copyright 2019-2021 Ramil Nugmanov <nougmanoff@protonmail.com>
#  This file is part of CGRdb.
#
#  CGRdb is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, see <https://www.gnu.org/licenses/>.
*/

CREATE OR REPLACE FUNCTION
"{schema}".cgrdb_search_similar_molecules(data bytea, OUT id integer, OUT count integer)
AS $$
from CGRtools.containers import MoleculeContainer
from pickle import loads

molecule = loads(data)
if not isinstance(molecule, MoleculeContainer):
    raise plpy.spiexceptions.DataException('MoleculeContainer required')

sg = bytes(molecule).hex()

get_cache = f'''SELECT x.id, array_length(x.molecules, 1) count
FROM "{schema}"."MoleculeSearchCache" x
WHERE x.operator = 'similar' AND x.signature = '\\x{sg}'::bytea'''

# test for existing cache
found = plpy.execute(get_cache)
if found:
    return found[0]

# cache not found. lets start searching
fp = GD['cgrdb_mfp'].transform_bitset([molecule])[0]

if GD['index']:  # use index search
    from requests import post
    found = post(f"{GD['index']}/similarity/molecule", json=fp).json()
    if found:  # create cgrdb_query temp table
        plpy.execute('DROP TABLE IF EXISTS cgrdb_query')
        if isinstance(found[0], int):  # need to calculate tanimoto
            plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
SELECT x.molecule m,
       icount(x.fingerprint & ARRAY{fp}::integer[])::float / icount(x.fingerprint | ARRAY{fp}::integer[])::float t
FROM "{schema}"."MoleculeStructure" x
WHERE x.id = ANY(ARRAY{found}::integer[])''')
        else:  # tanimoto exists
            plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
SELECT x.molecule m, f.t
FROM "{schema}"."MoleculeStructure" x,
     (VALUES {', '.join(f'({s}::integer, {t:.2f}::float)' for s, t in found)}) AS f (s, t)
WHERE x.id = f.s''')
else:  # sequential search
    plpy.execute('DROP TABLE IF EXISTS cgrdb_query')
    plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
SELECT c.m, c.t
FROM (
    SELECT x.molecule m,
           icount(x.fingerprint & ARRAY{fp}::integer[])::float / icount(x.fingerprint | ARRAY{fp}::integer[])::float t
    FROM "{schema}"."MoleculeStructure" x
) c
WHERE c.t > 0.5''')
    # check for empty results
    found = plpy.execute('SELECT COUNT(*) FROM cgrdb_query')[0]['count']

if not found:
    # store empty cache
    found = plpy.execute(f'''INSERT INTO
"{schema}"."MoleculeSearchCache"(signature, operator, date, molecules, tanimotos)
VALUES ('\\x{sg}'::bytea, 'similar', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::real[])
ON CONFLICT DO NOTHING
RETURNING id, 0 count''')

    # concurrent process stored same query. just reuse it
    if not found:
        found = plpy.execute(get_cache)
    return found[0]

# store found molecules to cache
found = plpy.execute(f'''INSERT INTO
"{schema}"."MoleculeSearchCache"(signature, operator, date, molecules, tanimotos)
SELECT '\\x{sg}'::bytea, 'similar', CURRENT_TIMESTAMP, array_agg(o.m), array_agg(o.t)
FROM (
    SELECT h.m, h.t
    FROM (
        SELECT DISTINCT ON (f.m) f.m, f.t
        FROM cgrdb_query f
        ORDER BY f.m, f.t DESC
    ) h
    ORDER BY h.t DESC
) o
ON CONFLICT DO NOTHING
RETURNING id, array_length(molecules, 1) count''')

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute(get_cache)

return found[0]
$$ LANGUAGE plpython3u
