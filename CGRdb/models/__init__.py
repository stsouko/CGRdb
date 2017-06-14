# -*- coding: utf-8 -*-
#
#  Copyright 2017 Ramil Nugmanov <stsouko@live.ru>
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


class UserADHOCMeta(type):
    def __getitem__(cls, item):
        return cls(item)


class UserADHOC(metaclass=UserADHOCMeta):
    def __init__(self, uid):
        self.id = uid


def load_tables(db, schema, user_entity=None):
    if not user_entity:  # User Entity ADHOC.
        user_entity = UserADHOC

    from .molecule import load_tables as molecule_load
    from .reaction import load_tables as reaction_load
    from .data import load_tables as data_load

    molecule_load(db, schema, user_entity)
    reaction_load(db, schema, user_entity)
    data_load(db, schema, user_entity)

    return db.Molecule, db.Reaction
