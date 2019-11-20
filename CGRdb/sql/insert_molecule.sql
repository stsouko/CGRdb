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


CREATE OR REPLACE FUNCTION "{schema}".cgrdb_insert_molecule_structure()
RETURNS TRIGGER
AS $$
from CGRtools.containers import MoleculeContainer
from compress_pickle import loads

mfp = GD['cgrdb_mfp']
data = TD['new']
molecule = loads(data['structure'], compression='gzip')
if not isinstance(molecule, MoleculeContainer):
    raise plpy.DataException('MoleculeContainer required')

current = plpy.execute('SELECT id, structure FROM "{schema}"."MoleculeStructure" '
                       'WHERE molecule = %d and is_canonic' % data['molecule'])
if current:  # check for atom mapping
    s = loads(current[0]['structure'], compression='gzip')
    if {n: a.atomic_number for n, a in molecule.atoms()} != {n: a.atomic_number for n, a in s.atoms()}:
        raise plpy.DataException('structure forms of molecule should has same mapping and atoms')
    elif data['is_canonic']:  # additional forms of structure should not be canonic
        data['is_canonic'] = False
elif not data['is_canonic']:  # new structure should be canonic
    data['is_canonic'] = True

data['fingerprint'] = mfp._transform_bitset([molecule])[0]
data['signature'] = bytes(molecule)

return 'MODIFY'
$$ LANGUAGE plpython3u
