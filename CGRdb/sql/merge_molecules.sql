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
"{schema}".cgrdb_merge_molecules(source integer, target integer, mapping json)
RETURNS VOID
AS $$
from CGRtools.containers import ReactionContainer
from collections import defaultdict
from functools import lru_cache
from json import loads as json_loads
from itertools import product
from pickle import dumps, loads

rfp = GD['cgrdb_rfp']
cache_size = GD['cache_size']

# load structures
s_structures = []
s_ids = []
t_structures = []
t_ids = []

q = f'''SELECT x.id, x.molecule, x.structure FROM "{schema}"."MoleculeStructure" x
WHERE x.molecule in ({source}, {target})'''
for x in plpy.execute(q):
    s = loads(x['structure'])
    if x['molecule'] == source:
        s_structures.append(s)
        s_ids.append(x['id'])
    else:
        t_structures.append(s)
        t_ids.append(x['id'])

# prepare and check mapping and structures compatibility
mp = dict(json_loads(mapping))  # source to target mapping
s = s_structures[0]
t = t_structures[0]

if set(mp) - set(s):
    raise plpy.spiexceptions.DataException('mapping invalid')
elif len(mp) != len(s):
    mp = {n: mp.get(n, n) for n in s}
if len(set(mp.values())) != len(mp) or {n: a.atomic_number for n, a in t.atoms()} != \
        {mp[n]: a.atomic_number for n, a in s.atoms()}:
    raise plpy.spiexceptions.DataException('mapping invalid or structures not compatible')

# source structures remapping
for s, si in zip(s_structures, s_ids):
    s.remap(mp)
    plpy.execute(f'''UPDATE "{schema}"."MoleculeStructure"
SET structure = '\\x{dumps(s).hex()}'::bytea, is_canonic = False WHERE id = {si}''')

# source reactions remapping
rmp = {t: s for s, t in mp.items()}  # target to source mapping
nmp = [[t, s] for t, s in rmp.items() if t != s]
nmp = f"'{nmp}'" if nmp else 'NULL'  # minified NULL-mapping
for x in plpy.cursor(f'SELECT x.id, x.mapping FROM "{schema}"."MoleculeReaction" x WHERE x.molecule = {source}'):
    mp = x['mapping']
    if mp:
        mp = dict(json_loads(mp))
        mp = [[t, r] for t, r in ((t, mp.get(s, s)) for t, s in rmp.items()) if t != r]
        mp = f"'{mp}'" if mp else 'NULL'
    else:
        mp = nmp
    plpy.execute(f'UPDATE "{schema}"."MoleculeReaction" SET mapping = {mp} WHERE id = {x["id"]}')

# index update
cache = lru_cache(cache_size)(lambda x: loads(s))
for molecule, update_s, update_si in ((source, t_structures, t_ids), (target, s_structures, s_ids)):
    get_mp = f'''SELECT x.reaction r, array_agg(x.id) i, array_agg(x.molecule) m, array_agg(x.mapping) d, array_agg(x.is_product) p
    FROM "{schema}"."MoleculeReaction" x
    WHERE x.reaction IN (
        SELECT y.reaction
        FROM "{schema}"."MoleculeReaction" y
        WHERE y.molecule = {molecule}
    )
    GROUP BY x.reaction ORDER BY x.reaction'''

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
    GROUP BY mr.reaction ORDER BY mr.reaction'''

    update = list(zip(update_si, update_s))
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
                if mi == molecule:
                    tmp = [(si, s.remap(mp, copy=True)) for si, s in update]
                    ms.extend(tmp)
                    replacement[mri] = tmp
            else:
                ms = m2s[mi].copy()
                if mi == molecule:
                    ms.extend(update)
                    replacement[mri] = update
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

# move structures
for si in s_ids:
    plpy.execute(f'UPDATE "{schema}"."MoleculeStructure" SET molecule = {target} WHERE id = {si}')
# move reactions
plpy.execute(f'UPDATE "{schema}"."MoleculeReaction" SET molecule = {target} WHERE molecule = {source}')
# delete molecule
plpy.execute(f'DELETE FROM "{schema}"."Molecule" WHERE id = {source}')

$$ LANGUAGE plpython3u
