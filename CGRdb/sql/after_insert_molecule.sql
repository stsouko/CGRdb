/*
#  Copyright 2019 Ramil Nugmanov <nougmanoff@protonmail.com>
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
"{schema}".cgrdb_after_insert_molecule_structure()
RETURNS TRIGGER
AS $$
from CGRtools.containers import ReactionContainer
from collections import defaultdict
from compress_pickle import loads
from functools import lru_cache
from json import loads as json_loads
from itertools import product

data = TD['new']
if data['is_canonic']:
    return

rfp = GD['cgrdb_rfp']
cache_size = GD['cache_size']
molecule = data['molecule']
structure = data['id']

get_mp = f'''SELECT x.reaction r, array_agg(x.id) i, array_agg(x.molecule) m, array_agg(x.mapping) d, array_agg(x.is_product) p
FROM "{schema}"."MoleculeReaction" x
WHERE x.reaction IN (
    SELECT y.reaction
    FROM "{schema}"."MoleculeReaction" y
    WHERE y.molecule = {molecule}
)
GROUP BY x.reaction'''

get_ms = f'''SELECT array_agg(x.molecule) m, array_agg(x.id) s, array_agg(x.structure) d
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

cache = lru_cache(cache_size)(lambda x: loads(s, compression='gzip'))
for ms_row, mp_row in zip(plpy.cursor(get_ms), plpy.cursor(get_mp)):
    m2s = defaultdict(list)  # load structures of molecules
    for mi, si, s in zip(ms_row['m'], ms_row['s'], ms_row['d']):
        m2s[mi].append((si, cache(si)))

    structures = []
    replacement = {}
    lr = 0
    for mri, mi, mp, is_p in zip(mp_row['i'], mp_row['m'], mp_row['d'], mp_row['p']):
        if mp:
            mp = dict(json_loads(mp))
            ms = [(si, s.remap(mp, copy=True)) for si, s in m2s[mi]]
        else:
            ms = m2s[mi]
        if mi == molecule:  # remapped version of structure
            replacement[mri] = [(si, s) for si, s in ms if si == structure]
        if is_p:
            structures.append((ms, mri))
        else:
            lr += 1
            structures.insert(0, (ms, mri))

    cgrs = {}
    for mri, mi in zip(mp_row['i'], mp_row['m']):
        if mi == molecule:
            tmp = [replacement[i] if i == mri else ms for ms, i in structures]
            for r in product(*tmp):
                cgrs[~ReactionContainer([s for _, s in r[:lr]], [s for _, s in r[lr:]])] = list({si for si, _ in r})
    fps = rfp.transform_bitset(list(cgrs))
    ri = mp_row['r']
    plpy.execute('INSERT INTO "{schema}"."ReactionIndex" (reaction, signature, fingerprint, structures) VALUES %s' %
                 ', '.join(f"({ri}, '\\x{bytes(s).hex()}'::bytea, ARRAY{fp}::integer[], ARRAY{si}::integer[])"
                           for (s, si), fp in zip(cgrs.items(), fps)))
$$ LANGUAGE plpython3u
