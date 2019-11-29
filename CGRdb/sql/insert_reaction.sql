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
from itertools import chain, product, repeat

rfp = GD['cgrdb_rfp']
data = TD['new']
reaction = loads(data['structure'], compression='gzip')
if not isinstance(reaction, ReactionContainer):
    raise plpy.DataException('ReactionContainer required')
elif not reaction.reactants or not reaction.products:
    raise ValueError('empty ReactionContainer')

while True:
    # load existing in db molecules
    sg2m, sg2c = {}, {}
    m2ms = defaultdict(list)  # Molecule to MoleculeStructure mapping
    m2c = defaultdict(list)  # Molecule to MoleculeContainer mapping
    load = '''SELECT x.id, x.molecule, x.signature, x.structure
    FROM "{schema}"."MoleculeStructure" x
    WHERE x.molecule IN (
        SELECT y.molecule
        FROM "{schema}"."MoleculeStructure" y
        WHERE y.signature IN (%s)
    )''' % ', '.join(f"'\\x{bytes(c).hex()}'::bytea" for c in chain(reaction.reactants, reaction.products))
    for row in plpy.execute(load):
        sg = row['signature']
        sg2m[sg] = mi = row['molecule']
        sg2c[sg] = c = loads(row['structure'], compression='gzip')  # structure with mapping as in db
        m2c[mi].append(c)
        m2ms[mi].append(row['id'])

    # find new molecules and store in db
    new = {}
    for c in chain(reaction.reactants, reaction.products):
        sg = bytes(c)
        if sg not in sg2m and sg not in new:  # add only first molecules of duplicates
            new[sg] = c
    if new:
        try:
            with plpy.subtransaction():
                mis = [x['id'] for x in plpy.execute('INSERT INTO "{schema}"."Molecule" (id) VALUES %s RETURNING id' % \
                       ', '.join(['(DEFAULT)'] * len(new)))]
                insert = 'INSERT INTO "{schema}"."MoleculeStructure" (structure, molecule, is_canonic) VALUES %s RETURNING id' % \
                         ', '.join(f"('\\x{dumps(s, compression='gzip').hex()}'::bytea, {m}, True)"
                                   for m, s in zip(mis, new.values()))
                sis = [x['id'] for x in plpy.execute(insert)]
        except plpy.SPIError:
            continue

        for mi, si, (sg, c) in zip(mis, sis, new.items()):
            sg2m[sg] = mi
            sg2c[sg] = c
            m2c[mi].append(c)
            m2ms[mi].append(si)
    break

# prepare all combinations of reaction
mapping = []
plain_reaction = []
duplicates = []
for c, is_p in chain(zip(reaction.reactants, repeat(False)), zip(reaction.products, repeat(True))):
    sg = bytes(c)
    mi = sg2m[sg]
    if sg in new and sg not in duplicates:
        plain_reaction.append([(c, m2ms[mi][0])])
        mapping.append((mi, is_p, 'NULL'))
        duplicates.append(sg)
    else:
        mp = next(sg2c[sg].get_mapping(c, automorphism_filter=False))
        plain_reaction.append([(m.remap(mp, copy=True), si) for m, si in zip(m2c[mi], m2ms[mi])])
        mp = [[k, v] for k, v in mp.items() if k != v]
        mapping.append((mi, is_p, mp and f"'{mp}'" or 'NULL'))

lr = len(reaction.reactants)
cgrs = []
sis = []
sgs = []
for r in product(*plain_reaction):
    c = ~ReactionContainer([c for c, _ in r[:lr]], [c for c, _ in r[lr:]])
    cgrs.append(c)
    sis.append(list({si for _, si in r}))
    sgs.append(bytes(c).hex())  # preload signature
fps = rfp.transform_bitset(cgrs)

# store in db
ri = plpy.execute('INSERT INTO "{schema}"."ReactionRecord" DEFAULT VALUES RETURNING id')[0]['id']
plpy.execute('INSERT INTO "{schema}"."ReactionIndex" (reaction, signature, fingerprint, structures) VALUES %s' %
             ', '.join(f"({ri}, '\\x{sg}'::bytea, ARRAY{fp}::integer[], ARRAY{si}::integer[])"
                       for sg, fp, si in zip(sgs, fps, sis)))

plpy.execute('INSERT INTO "{schema}"."MoleculeReaction" (reaction, molecule, is_product, mapping) VALUES %s' %
             ', '.join(f"({ri}, {mi}, {is_p}, {mp})" for mi, is_p, mp in mapping))
data['id'] = ri

return 'MODIFY'
$$ LANGUAGE plpython3u
