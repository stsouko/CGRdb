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

get_mp = f'''SELECT x.reaction r, array_agg(x.molecule) m, array_agg(x.mapping) d, array_agg(x.is_product) p
FROM "{schema}"."MoleculeReaction" x
WHERE x.reaction IN (
    SELECT y.reaction
    FROM "{schema}"."MoleculeReaction" y
    WHERE y.molecule = {molecule}
)
GROUP BY x.reaction'''

get_ms = f'''SELECT mr.reaction r, array_agg(x.molecule) m, array_agg(x.id) s, array_agg(x.structure) d
FROM "{schema}"."MoleculeStructure" x JOIN (
    SELECT DISTINCT ON (y.reaction, y.molecule) y.reaction, y.molecule
    FROM "{schema}"."MoleculeReaction" y
    WHERE y.reaction IN (
        SELECT z.reaction
        FROM "{schema}"."MoleculeReaction" z
        WHERE z.molecule = {molecule}
    )
) mr ON x.molecule = mr.molecule
GROUP BY mr.reaction'''

cache_size = GD['cache_size']
cache = lru_cache(cache_size)(lambda x: loads(s, compression='gzip'))

for ms_row, mp_row in zip(plpy.cursor(get_ms), plpy.cursor(get_mp)):
    m2s = defaultdict(list)  # load structures of molecules
    for mi, si, s in zip(ms_row['m'], ms_row['s'], ms_row['d']):
        m2s[mi].append((si, cache(si)))

    structures = []
    lr = 0
    for mi, mp, is_p in zip(mp_row['m'], mp_row['d'], mp_row['p']):
        if mp:
            mp = dict(json_loads(mp))
            ms = [(si, s.remap(mp, copy=True)) for si, s in m2s[mi]]
        else:
            ms = m2s[mi]
        structures.append(ms)
        if not is_p:
            lr += 1

    s = [(structure, cache(structure))]
    for n, mi in enumerate(mp_row['m']):
        if mi == molecule:
            tmp = structures.copy()
            tmp[n] = s
            for ms in product(*tmp):
                r = ReactionContainer(ms[:lr], ms[lr:])

# cache not found. lets start searching
fp = GD['cgrdb_rfp'].transform_bitset([cgr])[0]
$$ LANGUAGE plpython3u
