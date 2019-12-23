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
"{schema}".cgrdb_search_mappingless_substructure_reactions(data bytea, OUT id integer, OUT count integer)
AS $$
from CGRtools.containers import ReactionContainer
from compress_pickle import loads, dumps
from itertools import chain

reaction = loads(data, compression='gzip')
if not isinstance(reaction, ReactionContainer):
    raise plpy.spiexceptions.DataException('ReactionContainer required')

sg = bytes(reaction).hex()

get_cache = f'''SELECT x.id, array_length(x.reactions, 1) count
FROM "{schema}"."ReactionSearchCache" x
WHERE x.operator = 'substructure' AND x.signature = '\\x{sg}'::bytea'''

# test for existing cache
found = plpy.execute(get_cache)
if found:
    return found[0]

# search molecules
molecules = []  # cached molecules
for m in chain(reaction.reactants, reaction.products):
    m = dumps(m, compression='gzip').hex()
    found = plpy.execute(f'''SELECT * FROM "{schema}".cgrdb_search_substructure_molecules('\\x{m}'::bytea)''')[0]
    # check for empty results
    if not found['count']:
        # store empty cache
        found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
VALUES ('{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::real[])
ON CONFLICT DO NOTHING
RETURNING id, 0 count''')

        # concurrent process stored same query. just reuse it
        if not found:
            found = plpy.execute(get_cache)
        return found[0]
    molecules.append(found['id'])

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
SELECT '{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, array_agg(o.r), array_agg(o.t)
FROM cgrdb_filtered o
ON CONFLICT DO NOTHING
RETURNING id, array_length(reactions, 1) count''')

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute(get_cache)

return found[0]
$$ LANGUAGE plpython3u
