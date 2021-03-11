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
"{schema}".cgrdb_search_mappingless_substructure_reactions(data bytea, OUT id integer, OUT count integer)
AS $$
from CGRtools.containers import ReactionContainer
from itertools import chain, repeat
from pickle import loads, dumps

reaction = loads(data)
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
    m = dumps(m).hex()
    found = plpy.execute(f'''SELECT * FROM "{schema}".cgrdb_search_substructure_molecules('\\x{m}'::bytea)''')[0]
    # check for empty results
    if not found['count']:
        # store empty cache
        found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
VALUES ('\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::real[])
ON CONFLICT DO NOTHING
RETURNING id, 0 count''')

        # concurrent process stored same query. just reuse it
        if not found:
            found = plpy.execute(get_cache)
        return found[0]
    molecules.append(found['id'])

# find reactions for each molecules
for (n, m), is_p in zip(enumerate(molecules), chain(repeat(False, len(reaction.reactants)), repeat(True))):
    plpy.execute(f'DROP TABLE IF EXISTS cgrdb_filtered{n}')
    plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_filtered{n} ON COMMIT DROP AS
SELECT DISTINCT ON (r.reaction) r.reaction r, s.t
FROM "{schema}"."MoleculeReaction" r
JOIN
(
    SELECT unnest(x.molecules) m, unnest(x.tanimotos) t
    FROM "{schema}"."MoleculeSearchCache" x
    WHERE x.id = {m}
) s
ON r.molecule = s.m
WHERE r.is_product = {is_p}''')

    # check for empty results
    if not plpy.execute(f'SELECT COUNT(*) FROM cgrdb_filtered{n}')[0]['count']:
        # store empty cache
        found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
VALUES ('\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::real[])
ON CONFLICT DO NOTHING
RETURNING id, 0 count''')

        # concurrent process stored same query. just reuse it
        if not found:
            found = plpy.execute(get_cache)
        return found[0]

# save found results
t_sum = ' + '.join(f'o{n}.t' for n in range(len(molecules)))
joins = ' '.join(f'JOIN cgrdb_filtered{n} o{n} ON o0.r = o{n}.r' for n in range(1, len(molecules)))

found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
SELECT '\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, array_agg(o.r), array_agg(o.t)
FROM (
    SELECT o0.r, ({t_sum}) / {len(molecules)} t
    FROM cgrdb_filtered0 o0 {joins}
    ORDER BY t DESC
) o
ON CONFLICT DO NOTHING
RETURNING id, array_length(reactions, 1) count''')

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute(get_cache)

return found[0]
$$ LANGUAGE plpython3u
