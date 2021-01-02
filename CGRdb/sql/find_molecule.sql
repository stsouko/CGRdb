/*
#  Copyright 2020, 2021 Ramil Nugmanov <nougmanoff@protonmail.com>
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
"{schema}".cgrdb_search_structure_molecule(data bytea, OUT id integer)
AS $$
from CGRtools.containers import MoleculeContainer
from compress_pickle import loads

molecule = loads(data, compression='lzma')
if not isinstance(molecule, MoleculeContainer):
    raise plpy.spiexceptions.DataException('MoleculeContainer required')

sg = bytes(molecule).hex()

get_data = f'''SELECT x.id
FROM "{schema}"."MoleculeStructure" x
WHERE x.signature = '\\x{sg}'::bytea'''

found = plpy.execute(get_data)
if found:
    return found[0]['id']
return 0
$$ LANGUAGE plpython3u
