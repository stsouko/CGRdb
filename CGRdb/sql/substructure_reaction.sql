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
"{schema}".cgrdb_search_substructure_reactions(data bytea, OUT id integer, OUT count integer)
AS $$
from CGRtools.containers import ReactionContainer
from collections import defaultdict
from compress_pickle import loads

reaction = loads(data, compression='gzip')
if not isinstance(reaction, ReactionContainer):
    raise plpy.DataException('ReactionContainer required')

cgr = ~reaction
sg = bytes(cgr).hex()
# test for existing cache

get_cache = f'''SELECT x.id, array_length(x.reactions, 1) as count
FROM "{schema}"."ReactionSearchCache" x
WHERE x.operator = 'substructure' AND x.signature = '\\x{sg}'::bytea'''

found = plpy.execute(get_cache)
if found:
    return found[0]

# cache not found. lets start searching
fp = GD['cgrdb_rfp'].transform_bitset([cgr])[0]

plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
SELECT x.reaction AS r, x.structures AS s, smlar(x.fingerprint, ARRAY{fp}::integer[]) AS t
FROM "{schema}"."ReactionIndex" x
WHERE x.fingerprint @> ARRAY{fp}::integer[]''')

# check for empty results
if not plpy.execute('SELECT COUNT(*) FROM cgrdb_query')[0]['count']:
    # store empty cache
    found = plpy.execute(f'''INSERT INTO
    "{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
    VALUES ('\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::real[])
    ON CONFLICT DO NOTHING
    RETURNING id, 0 AS count''')

    # concurrent process stored same query. just reuse it
    if not found:
        found = plpy.execute(get_cache)
    return found[0]

# filter by unique reactions
plpy.execute('''CREATE TEMPORARY TABLE cgrdb_filtered ON COMMIT DROP AS
SELECT h.r, h.s, h.t
FROM (
    SELECT DISTINCT ON (f.r) r, f.s, f.t
    FROM cgrdb_query f
    ORDER BY f.r, f.t DESC
) h
ORDER BY h.t DESC''')

get_ms = '''SELECT f.r, array_agg(ms.molecule) as m, array_agg(ms.id) as s, array_agg(ms.structure) as d
FROM cgrdb_filtered f LEFT JOIN "{schema}"."MoleculeStructure" ms ON ms.id = ANY(f.s)
GROUP BY f.r'''

get_mp = '''SELECT f.r, array_agg(mr.molecule) as m, array_agg(mr.mapping) as d
FROM cgrdb_filtered f LEFT JOIN "{schema}"."MoleculeReaction" as mr ON f.r = mr.reaction
GROUP BY f.r'''

cache_size = GD.get('cache_size', 100)
s_cache = {}
s_order = []
ris, rts = [], []
for ms_row, mp_row in zip(plpy.cursor(get_ms), plpy.cursor(get_mp)):
    ri = ms_row['r']
    m2s = defaultdict(list)

    for si, s in zip(ms_row['s'], ms_row['d']):  # load MoleculeContainer's into cache
        if si not in s_cache:
            s_cache[si] = loads(s, compression='gzip')
            if len(s_order) > cache_size:
                del s_cache[s_order.pop(0)]
            s_order.append(si)
    for mi, si in zip(ms_row['m'], ms_row['s']):
        m2s[mi].append(s_cache[si])


# store found molecules to cache
found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
VALUES ('\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY{ris}::integer[], ARRAY{rts}::real[])
ON CONFLICT DO NOTHING
RETURNING id, array_length(reactions, 1) as count''')

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute(get_cache)

return found[0]
$$ LANGUAGE plpython3u
