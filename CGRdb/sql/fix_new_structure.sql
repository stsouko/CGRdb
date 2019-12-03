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
"{schema}".cgrdb_fix_new_structure(molecule integer, structure integer)
RETURNS VOID
AS $$
from CGRtools.containers import ReactionContainer
from collections import defaultdict
from compress_pickle import loads
from functools import lru_cache
from json import loads as json_loads
from itertools import product


# cache not found. lets start searching
fp = GD['cgrdb_rfp'].transform_bitset([cgr])[0]

get_mp = f'''SELECT x.reaction as r, array_agg(x.molecule) AS m, array_agg(x.mapping) AS d, array_agg(x.is_product) AS p
FROM "{schema}"."MoleculeReaction" AS x
WHERE x.reaction IN (
    SELECT y.reaction
    FROM "{schema}"."MoleculeReaction" y
    WHERE y.molecule = {molecule}
)
GROUP BY x.reaction'''

get_ms = f'''SELECT x.molecule AS m, array_agg(x.id) AS s, array_agg(x.structure) AS d
FROM "cgrdb"."MoleculeStructure" x
WHERE x.molecule IN (
    SELECT z.molecule
    FROM "{schema}"."MoleculeReaction" y, "cgrdb"."MoleculeReaction" z
    WHERE y.molecule = {molecule} AND y.reaction = z.reaction
)
GROUP BY x.molecule'''

cache_size = GD['cache_size']
cache = lru_cache(cache_size)(lambda x: loads(s, compression='gzip'))
ris, rts = [], []
for ms_row, mp_row, rt_row in zip(plpy.cursor(get_ms), plpy.cursor(get_mp), plpy.cursor(get_rt)):
    m2s = defaultdict(list)  # load structures of molecules
    for mi, si, s in zip(ms_row['m'], ms_row['s'], ms_row['d']):
        m2s[mi].append(cache(si))

    rct = []
    prd = []
    for mi, mp, is_p in zip(mp_row['m'], mp_row['d'], mp_row['p']):
        ms = [x.remap(dict(json_loads(mp)), copy=True) for x in m2s[mi]] if mp else m2s[mi]
        if is_p:
            prd.append(ms)
        else:
            rct.append(ms)

    lr = len(rct)
    if any(cgr <= ~ReactionContainer(ms[:lr], ms[lr:]) for ms in product(*rct, *prd)):
        ris.append(rt_row['r'])
        rts.append(rt_row['t'])

$$ LANGUAGE plpython3u
