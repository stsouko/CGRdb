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
from .config import catechol, bromnitrobenzene, wrong_map_nbb, methyl_catechol, exist_nbb, third_nbb, nitrobrombenzene
from pony.orm import db_session, count
from .preparer import DBPrepare


class TestNewStructure(DBPrepare):
    @pytest.mark.parametrize(("rdf", "structure"),
                             [('CGRdb/tests/data/simmetric_reactions.rdf', catechol),
                              ('CGRdb/tests/data/non_sim.rdf', methyl_catechol)])
    @db_session
    def test_main(self, db, rdf, structure):
        self.populate(db, rdf)
        m1 = db.Molecule[1]
        m1.new_structure(structure)
        assert count(m1._structures) == 2
        assert count(m1._structures.reaction_indexes) == 6

    @pytest.mark.parametrize(("structure", "error_message"),
                             [(bromnitrobenzene, 'structure already exists in another Molecule'),
                              (exist_nbb, 'structure already canonical in current Molecule'),
                              (wrong_map_nbb, 'structure has invalid mapping')])
    @db_session
    def test_errors(self, db, structure, error_message):
        self.populate(db, 'CGRdb/tests/data/simmetric_reactions.rdf')
        with pytest.raises(AssertionError) as ex:
            db.Molecule[3].new_structure(structure)
        assert ''.join(ex.value.args) == error_message

    @pytest.mark.parametrize(("rdf", "expected"), [('CGRdb/tests/data/simmetric_reactions.rdf', (7, 12)),
                                                   ('CGRdb/tests/data/non_simm_reactions.rdf', (8, 15))])
    @db_session
    def test_more_structures(self, db, rdf, expected):
        self.populate(db, rdf)
        m3 = db.Molecule[3]
        m3.new_structure(nitrobrombenzene)
        assert count(m3._structures.reaction_indexes) == expected[0]
        m3.new_structure(third_nbb)
        assert count(m3._structures.reaction_indexes) == expected[1]
