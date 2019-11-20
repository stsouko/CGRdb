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


CREATE OR REPLACE FUNCTION "{schema}".cgrdb_insert_reaction(reaction integer, data bytea)
RETURNS VOID
AS $$
from CGRtools.containers import ReactionContainer
from collections import defaultdict
from compress_pickle import loads
from itertools import chain

mfp = GD['cgrdb_rfp']
reaction = loads(data, compression='gzip')
if not isinstance(reaction, ReactionContainer):
    raise plpy.DataException('ReactionContainer required')

rs = bytes(reaction).hex()

if plpy.execute('''SELECT id FROM "{schema}"."ReactionIndex" WHERE signature = '\\x%s'::bytea''' % rs):
    raise plpy.UniqueViolation

signatures = {bytes(m): m for m in chain(reaction.reactants, reaction.products)}

'''SELECT x.id, x.molecule, x.signature
FROM test."MoleculeStructure" x
WHERE x.molecule IN (
    SELECT y.molecule
    FROM test."MoleculeStructure" y
    WHERE y.signature IN ('\xba'::bytea, '\xab'::bytea)
)'''


ms, s2ms = defaultdict(list), {}
$$ LANGUAGE plpython3u
