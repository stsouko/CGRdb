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


class ManageReaction:
    def update_structure(self, structure, user=None):
        """
        Update reaction structure by creating a new reaction, moving
        all necessary data and removing incorrect reaction from db.
        Works only if this structure does not already exist in db.

        Use when mapping or some molecules in reaction is wrong.

        :param structure: CGRtools ReactionContainer
        :param user: user entity
        :return: updated reaction entity
        """
        if self.structure_exists(structure):
            raise ValueError('structure already exists')

        reaction = type(self)(structure, self.user if user is None else user)
        return self.__move_reaction_data(reaction)

    def merge_reactions(self, structure):
        """
        Merge metadata of two reaction into one.
        Use when its necessary to fix some wrong reaction structure,
        but correct reaction structure already exists in db. Move data
        of current reaction to existing reaction and remove it from db.

        :param structure: CGRtools ReactionContainer
        :return: merged reaction entity
        """

        reaction = self.find_structure(structure)
        return self.__move_reaction_data(reaction)

    def __move_reaction_data(self, reaction):
        """
        Move conditions and classes of current reaction
        to new reaction and delete incorrect reaction from db.

        :param reaction: reaction entity
        :return: new reaction entity
        """
        for c in self.metadata:
            c.structure = reaction
        for cls in self.classes:
            reaction.classes.add(cls)
        self.delete()
        return reaction


__all__ = ['ManageReaction']
