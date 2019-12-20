/*
#  Copyright 2019 Ramil Nugmanov <stsouko@live.ru>
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
"{schema}".cgrdb_search_reactions_by_molecule(data bytea, role integer, search integer, OUT id integer, OUT count integer)
AS $$
from CGRtools.containers import MoleculeContainer, QueryContainer
from compress_pickle import loads

if role == 0:  # role: 0 - any, 1 - reactant, 2 - product
    role_filter = ''
elif role == 1:
    role_filter = 'WHERE r.is_product = False'
elif role == 2:
    role_filter = 'WHERE r.is_product = True'
else:
    raise plpy.spiexceptions.DataException('role invalid')

if search  == 1:  # search: 1 - substructure, 2 - similar
    search_type = 'substructure'
elif search == 2:
    search_type = 'similar'
else:
    raise plpy.spiexceptions.DataException('search type invalid')

molecule = loads(data, compression='gzip')
if not isinstance(molecule, (MoleculeContainer, QueryContainer)):
    raise plpy.spiexceptions.DataException('MoleculeContainer or QueryContainer required')

sg = f'\\x{role}{role}{bytes(molecule).hex()}'

get_cache = f'''SELECT x.id, array_length(x.reactions, 1) count
FROM "{schema}"."ReactionSearchCache" x
WHERE x.operator = '{search_type}' AND x.signature = '{sg}'::bytea'''

# test for existing cache
found = plpy.execute(get_cache)
if found:
    return found[0]

# search molecules
found = plpy.execute(f'''SELECT * FROM "{schema}".cgrdb_search_{search_type}_molecules('\\x{data.hex()}'::bytea)''')[0]
# check for empty results
if not found['count']:
    # store empty cache
    found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
VALUES ('{sg}'::bytea, '{search_type}', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::real[])
ON CONFLICT DO NOTHING
RETURNING id, 0 count''')

    # concurrent process stored same query. just reuse it
    if not found:
        found = plpy.execute(get_cache)
    return found[0]

# store found molecules to cache
plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_filtered ON COMMIT DROP AS
SELECT h.r, h.t FROM (
    SELECT DISTINCT ON (r.reaction) r.reaction r, s.t
    FROM "{schema}"."MoleculeReaction" r
    JOIN
    (
        SELECT unnest(x.molecules) m, unnest(x.tanimotos) t
        FROM "{schema}"."MoleculeSearchCache" x
        WHERE x.id = {found['id']}
    ) s
    ON r.molecule = s.m
    {role_filter}
) h
ORDER BY h.t DESC''')

# check for empty results
if not plpy.execute('SELECT COUNT(*) FROM cgrdb_filtered')[0]['count']:
    # store empty cache
    found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
VALUES ('{sg}'::bytea, '{search_type}', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::real[])
ON CONFLICT DO NOTHING
RETURNING id, 0 count''')

    # concurrent process stored same query. just reuse it
    if not found:
        found = plpy.execute(get_cache)
    return found[0]

# save found results
found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
SELECT '{sg}'::bytea, '{search_type}', CURRENT_TIMESTAMP, array_agg(o.r), array_agg(o.t)
FROM cgrdb_filtered o
ON CONFLICT DO NOTHING
RETURNING id, array_length(reactions, 1) count''')

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute(get_cache)

return found[0]
$$ LANGUAGE plpython3u
