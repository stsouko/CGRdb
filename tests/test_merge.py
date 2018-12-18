# -*- coding: utf-8 -*-
#
#  Copyright 2018 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2018 Adelia Fatykhova <adelik21979@gmail.com>
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
#
import pytest
from pony.orm import db_session, count
from tests.preparer import DBPrepare
from .config import bromnitrobenzene, non_existent_nbb, wrong_map_nbb, exist_nbb, nitrobrombenzene, user
from CGRtools.files.SDFrw import SDFread


class TestMerge(DBPrepare):
    @pytest.mark.parametrize(("rdf", "expected", "ri"),
                             [('CGRdb/tests/data/simmetric_reactions.rdf', [{2, 3}, 5, {1, 2, 3}], 2),
                              ('CGRdb/tests/data/non_simm_reactions.rdf', [{2, 3}, 6, {1, 2, 3}], 2),
                              ('CGRdb/tests/data/simm_mixed_unique.rdf', [{1, 2}, 3, {3}], 1),
                              ('CGRdb/tests/data/non_simm_mixed_unique.rdf', [{1, 2}, 4, {3}], 1),
                              ('CGRdb/tests/data/left_doubles.rdf', [{2, 3}, 5, {3}], 3),
                              ('CGRdb/tests/data/non_sim_ left_doubles.rdf', [{2, 3}, 6, {3}], 3)])
    @db_session
    def test_main(self, db, rdf, expected, ri):
        self.populate(db, rdf)
        m2, m3 = db.Molecule[2], db.Molecule[3]
        db.ReactionClass(id=1, name='1st', reactions=db.Reaction[2])
        db.ReactionClass(id=2, name='2nd', reactions=db.Reaction[2])
        db.ReactionClass(id=3, name='3rd', reactions=db.Reaction[1])
        db.ReactionConditions({"T": 300, "P": 10}, db.Reaction[1], user)
        db.ReactionConditions({"T": 298, "P": 1}, db.Reaction[2], user)
        db.ReactionConditions({"T": 273, "P": 15}, db.Reaction[2], user)
        db.MoleculeProperties({"p": 1.719, "Melting point": 40 - 43}, m2, user)
        db.MoleculeProperties({"p": 1.719, "Melting point": 40 - 43}, m3, user)
        db.MoleculeClass(id=1, name="1st", molecules=m2)
        db.MoleculeClass(id=2, name="2nd", molecules=m3)
        m3.merge_molecules(bromnitrobenzene)
        molecules, reactions = db.Molecule, db.Reaction
        assert reactions.select().count() == 2
        assert molecules.select().count() == 6
        assert {r.id for r in reactions.select()} == expected[0]
        assert m3._structures.count() == 2
        assert {cls.id for cls in m3.classes} == {1, 2}
        assert {cls.id for cls in db.Reaction[ri].classes} == expected[2]
        assert len(m3.properties) == 2
        assert count(m3._structures.reaction_indexes) == expected[1]

    @pytest.mark.parametrize(("structure", "error_message"),
                             [(non_existent_nbb, 'structure not found'),
                              (wrong_map_nbb, 'structure has invalid mapping'),
                              (exist_nbb, 'structure already exists in current Molecule')])
    @db_session
    def test_errors(self, db, structure, error_message):
        self.populate(db, 'CGRdb/tests/data/simmetric_reactions.rdf')
        with pytest.raises(AssertionError) as ex:
            db.Molecule[3].merge_molecules(structure)
        assert ''.join(ex.value.args) == error_message

    @pytest.mark.parametrize(("rdf", "expected"),
                             [('CGRdb/tests/data/simmetric_reactions.rdf', 14),
                              ('CGRdb/tests/data/non_simm_reactions.rdf', 20)])
    @db_session
    def test_combo(self, db, rdf, expected):
        self.populate(db, rdf)
        m2, m3 = db.Molecule[2], db.Molecule[3]
        nbt = SDFread('CGRdb/tests/data/nbt.sdf', remap=False).read()[0]
        m3.new_structure(nitrobrombenzene)
        m2.new_structure(nbt)
        m3.merge_molecules(bromnitrobenzene)
        assert count(m3._structures.reaction_indexes) == expected
        assert not db.Molecule.select(lambda m: m.id == 2)
        assert db.Molecule.select().count() == 6
        assert m3._structures.count() == 4
        assert db.Reaction.select().count() == 2
        assert {r.id for r in db.Reaction.select()} == {2, 3}
