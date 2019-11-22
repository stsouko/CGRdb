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


CREATE OR REPLACE FUNCTION "{schema}".cgrdb_insert_reaction()
RETURNS TRIGGER
AS $$
from CGRtools.containers import ReactionContainer
from collections import defaultdict
from compress_pickle import dumps, loads
from itertools import chain

rfp = GD['cgrdb_rfp']
data = TD['new']
reaction = loads(data['structure'], compression='gzip')
if not isinstance(reaction, ReactionContainer):
    raise plpy.DataException('ReactionContainer required')

signatures = {bytes(m): m for m in chain(reaction.reactants, reaction.products)}

plpy.execute('SAVEPOINT molecules')

while True:
    # load existing in db molecules
    sg2s, sg2ms = {}, {}
    m2s = defaultdict(list)
    load = '''SELECT x.id, x.molecule, x.signature, x.structure
    FROM "{schema}".MoleculeStructure" x
    WHERE x.molecule IN (
        SELECT y.molecule
        FROM "{schema}"."MoleculeStructure" y
        WHERE y.signature IN (%s)
    )''' % ', '.join("'\\x%s'::bytea" % x for x in signatures)
    for row in plpy.cursor(load):
        sg2s[row['signature']] = s = loads(row['structure'], compression='gzip')
        sg2ms[row['signature']] = row['id']
        m2s[row['molecule']].append(s)

    # find new molecules and store in db
    new = {sg: m for sg, m in signatures.items() if sg not in sg2s}
    if new:
        mis = [m['id'] for m in plpy.execute('INSERT INTO "{schema}"."Molecule" (id) VALUES %s RETURNING id' % \
               ', '.join(['(DEFAULT)'] * len(new)))]
        insert = 'INSERT INTO "{schema}"."MoleculeStructure" (structure, molecule, is_canonic) VALUES %s RETURNING id' % \
                 ', '.join("('\\x%s'::bytea, %d, True)" % (dumps(s, compression='gzip'), m)
                           for m, s in zip(mis, new.values()))
        try:
            plpy.execute(insert)
        except plpy.SPIError:  # molecules inserting failed
            plpy.execute('ROLLBACK TO SAVEPOINT molecules')
            continue
        plpy.execute('COMMIT')  # save molecules
        for m, s in zip(mis, new.values()):
            m2s[m].append(s)
            sg2m[bytes(s)] = m
    # generate combinations and store indexes in db

return 'MODIFY'
$$ LANGUAGE plpython3u
