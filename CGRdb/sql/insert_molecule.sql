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


CREATE OR REPLACE FUNCTION "{schema}".cgrdb_insert_molecule_structure()
RETURNS TRIGGER
AS $$
from CGRtools.containers import MoleculeContainer
from compress_pickle import loads

mfp = GD['cgrdb_mfp']
data = TD['new']
molecule = loads(data['structure'], compression='lzma')
if not isinstance(molecule, MoleculeContainer):
    raise plpy.spiexceptions.DataException('MoleculeContainer required')

current = plpy.execute('SELECT x.id, x.structure FROM "{schema}"."MoleculeStructure" x '
                       f'WHERE x.molecule = {data["molecule"]} AND x.is_canonic')
if current:  # check for atom mapping
    s = loads(current[0]['structure'], compression='lzma')
    if {n: a.atomic_number for n, a in molecule.atoms()} != {n: a.atomic_number for n, a in s.atoms()}:
        raise plpy.spiexceptions.DataException('structure forms of molecule should has same mapping and atoms')
    data['is_canonic'] = False  # additional forms of structure should not be canonic
else:  # new structure should be canonic
    data['is_canonic'] = True

data['fingerprint'] = mfp.transform_bitset([molecule])[0]
data['signature'] = bytes(molecule)

return 'MODIFY'
$$ LANGUAGE plpython3u
