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
from pony.orm import db_session, flush
from .preparer import DBPrepare
from .config import only_remap, only_update, update_remap, the_same, user


class TestUpdate(DBPrepare):

    @db_session
    def test_exist(self, db):
        self.populate(db, 'CGRdb/tests/data/db_for_reaction_management.rdf')
        with pytest.raises(AssertionError) as ex:
            db.Reaction[1].update_structure(the_same)
        assert ''.join(ex.value.args) == 'structure already exists'

    @pytest.mark.parametrize("structure", [only_remap, only_update, update_remap])
    @db_session
    def test_update(self, db, structure):
        self.populate(db, 'CGRdb/tests/data/db_for_reaction_management.rdf')
        db.ReactionClass(id=1, name='1st', reactions=db.Reaction[1])
        db.ReactionClass(id=2, name='2nd', reactions=db.Reaction[2])
        db.ReactionClass(id=3, name='3rd', reactions=db.Reaction[1])
        new = db.Reaction[1].update_structure(structure)
        assert {cls.id for cls in new.classes} == {1, 3}
        assert not db.Reaction.select(lambda r: r.id == 1)

    @db_session
    def test_merge(self, db):
        self.populate(db, 'CGRdb/tests/data/db_for_reaction_management.rdf')
        db.ReactionClass(id=1, name='1st', reactions=db.Reaction[2])
        db.ReactionClass(id=2, name='2nd', reactions=db.Reaction[2])
        db.ReactionClass(id=3, name='3rd', reactions=db.Reaction[1])
        db.ReactionConditions({"T": 300, "P": 10}, db.Reaction[1], user)
        db.ReactionConditions({"T": 298, "P": 1}, db.Reaction[2], user)
        db.ReactionConditions({"T": 273, "P": 15}, db.Reaction[2], user)
        new = db.Reaction[2].merge_reactions(the_same)
        flush()
        assert {cls.id for cls in new.classes} == {1, 2, 3}
        assert {c.id for c in new.conditions} == {1, 2, 3}
        assert not db.Reaction.select(lambda r: r.id == 2)
