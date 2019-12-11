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
"{schema}".cgrdb_merge_molecules(source integer, target integer, mapping json)
RETURNS VOID
AS $$
from compress_pickle import dumps, loads
from json import loads as json_loads

# load structures
s_structures = []
s_ids = []
t_structures = []
q = f'''SELECT x.id, x.molecule, x.structure FROM "{schema}"."MoleculeStructure" x
WHERE x.molecule in ({source}, {target})'''
for x in plpy.execute(q):
    s = loads(x['structure'], compression='gzip')
    if x['molecule'] == source:
        s_structures.append(s)
        s_ids.append(x['id'])
    else:
        t_structures.append(s)

mp = dict(json_loads(mapping))  # source to target mapping
s0 = s_structures[0]
t0 = t_structures[0]
# checks of mapping and structures compatibility
if set(mp) - set(s0):
    raise plpy.spiexceptions.DataException('mapping invalid')
elif len(mp) != len(s0):
    mp = {k: mp.get(k, k) for k in s0}
if len(set(mp.values())) != len(mp) or {n: a.atomic_number for n, a in t0.atoms()} != \
        {mp[n]: a.atomic_number for n, a in s0.atoms()}:
    raise plpy.spiexceptions.DataException('mapping invalid or structures not compatible')
rmp = {v: k for k, v in mp.items()}  # target to source mapping
nmp = [[k, v] for k, v in rmp.items() if k != v]
nmp = f"'{nmp}'" if nmp else 'NULL'  # minified NULL-mapping

# structures remapping
for s, n in zip(s_structures, s_ids):
    s.remap(mp)
    plpy.execute(f'''UPDATE "{schema}"."MoleculeStructure"
SET structure = '\\x{dumps(s, compression='gzip').hex()}'::bytea WHERE id = {n}''')

# source reactions remapping
r_mapping = []
for x in plpy.cursor(f'SELECT x.id, x.mapping FROM "{schema}"."MoleculeReaction" x WHERE x.molecule = {source}'):
    xm = x['mapping']
    if xm:
        xm = dict(json_loads(xm))
        xm = [[k, v] for k, v in ((k, xm.get(v, v)) for k, v in rmp.items()) if k != v]
        r_mapping.append((x['id'], f"'{xm}'" if xm else 'NULL'))
    else:
        r_mapping.append((x['id'], nmp))
for r, m in r_mapping:
    plpy.execute(f'UPDATE "{schema}"."MoleculeReaction" SET mapping = {m} WHERE id = {r}')

# source index update
$$ LANGUAGE plpython3u
