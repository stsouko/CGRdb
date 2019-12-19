/*
#  Copyright 2019 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2019 Adelia Fatykhova <adelik21979@gmail.com>
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
"{schema}".cgrdb_search_similar_fingerprint_molecules(fingerprint integer[], OUT id integer, OUT count integer)
AS $$

sg = hash(frozenset(fingerprint))

get_cache = f'''SELECT x.id, array_length(x.molecules, 1) count
FROM "{schema}"."MoleculeSearchCache" x
WHERE x.operator = 'similar' AND x.signature = '\\x{sg}'::bytea'''

# test for existing cache
found = plpy.execute(get_cache)
if found:
    return found[0]

# cache not found. lets start searching

plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
SELECT x.molecule m, smlar(x.fingerprint, ARRAY{fingerprint}::integer[]) t
FROM "{schema}"."MoleculeStructure" x
WHERE x.fingerprint % ARRAY{fingerprint}::integer[]''')

# check for empty results
if not plpy.execute('SELECT COUNT(*) FROM cgrdb_query')[0]['count']:
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
        SELECT DISTINCT ON (f.m) m, f.t
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
