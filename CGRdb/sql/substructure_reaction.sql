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
"{schema}".cgrdb_search_substructure_reactions(data bytea, OUT id integer, OUT count integer)
AS $$
from CGRtools.containers import ReactionContainer
from collections import defaultdict
from compress_pickle import loads
from functools import lru_cache
from json import loads as json_loads
from itertools import product

reaction = loads(data, compression='lzma')
if not isinstance(reaction, ReactionContainer):
    raise plpy.spiexceptions.DataException('ReactionContainer required')

cgr = ~reaction
sg = bytes(cgr).hex()
# test for existing cache

get_cache = f'''SELECT x.id, array_length(x.reactions, 1) count
FROM "{schema}"."ReactionSearchCache" x
WHERE x.operator = 'substructure' AND x.signature = '\\x{sg}'::bytea'''

found = plpy.execute(get_cache)
if found:
    return found[0]

# cache not found. lets start searching
fp = GD['cgrdb_rfp'].transform_bitset([cgr])[0]

plpy.execute('DROP TABLE IF EXISTS cgrdb_query')
plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
SELECT x.reaction r, x.structures s, icount(x.fingerprint & ARRAY{fp}::integer[])::float / icount(x.fingerprint | ARRAY{fp}::integer[])::float t
FROM "{schema}"."ReactionIndex" x
WHERE x.fingerprint @> ARRAY{fp}::integer[]''')

# check for empty results
if not plpy.execute('SELECT COUNT(*) FROM cgrdb_query')[0]['count']:
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

# filter by unique reactions
plpy.execute('DROP TABLE IF EXISTS cgrdb_filtered')
plpy.execute('''CREATE TEMPORARY TABLE cgrdb_filtered ON COMMIT DROP AS
SELECT h.r, h.s, h.t
FROM (
    SELECT DISTINCT ON (f.r) f.r, f.s, f.t
    FROM cgrdb_query f
    ORDER BY f.r, f.t DESC
) h
ORDER BY h.t DESC''')

get_ms = '''SELECT array_agg(ms.molecule) m, array_agg(ms.id) s, array_agg(ms.structure) d
FROM cgrdb_filtered f JOIN "{schema}"."MoleculeStructure" ms ON ms.id = ANY(f.s)
GROUP BY f.r'''

get_mp = '''SELECT array_agg(mr.molecule) m, array_agg(mr.mapping) d, array_agg(mr.is_product) p
FROM cgrdb_filtered f JOIN "{schema}"."MoleculeReaction" mr ON f.r = mr.reaction
GROUP BY f.r'''

get_rt = 'SELECT f.r, f.t FROM cgrdb_filtered f'

cache = lru_cache(GD['cache_size'])(lambda x: loads(s, compression='lzma'))
ris, rts = [], []
for ms_row, mp_row, rt_row in zip(plpy.cursor(get_ms), plpy.cursor(get_mp), plpy.cursor(get_rt)):
    m2s = defaultdict(list)  # load structures of molecules
    for mi, si, s in zip(ms_row['m'], ms_row['s'], ms_row['d']):
        m2s[mi].append(cache(si))

    structures = []
    lr = 0
    for mi, mp, is_p in zip(mp_row['m'], mp_row['d'], mp_row['p']):
        if mp:
            mp = dict(json_loads(mp))
            ms = [x.remap(mp, copy=True) for x in m2s[mi]]
        else:
            ms = m2s[mi]
        if is_p:
            structures.append(ms)
        else:
            lr += 1
            structures.insert(0, ms)

    if any(cgr <= ~ReactionContainer(ms[:lr], ms[lr:]) for ms in product(*structures)):
        ris.append(rt_row['r'])
        rts.append(rt_row['t'])

# store found molecules to cache
found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
VALUES ('\\x{sg}'::bytea, 'substructure', CURRENT_TIMESTAMP, ARRAY{ris}::integer[], ARRAY{rts}::real[])
ON CONFLICT DO NOTHING
RETURNING id, array_length(reactions, 1) count''')

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute(get_cache)

return found[0]
$$ LANGUAGE plpython3u
