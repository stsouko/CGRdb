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


def mixin_factory(db):
    class UpdateMixin:
        def update_structure(self, structure):
            """
            Update reaction structure in db.

            If this structure does not already exist in db, just create new reaction
            (necessary indexes are created automatically), move conditions and classes
            of current reaction to new reaction and delete incorrect reaction from db.

            Use when mapping or some molecules in reaction is wrong.

            :param structure: CGRtools ReactionContainer
            :return: updated reaction entity
            """
            assert not self.structure_exists(structure), 'structure already exists'

            reaction = db.Reaction(structure, self.user)
            for rc in self.conditions:
                rc.reaction = reaction
            for cls in self.classes:
                reaction.classes.add(cls)
            self.delete()
            return reaction

        def merge_reactions(self):
            pass
            # todo: implement

    return UpdateMixin


__all__ = [mixin_factory.__name__]
