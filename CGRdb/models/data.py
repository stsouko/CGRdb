# -*- coding: utf-8 -*-
#
#  Copyright 2017, 2018 Ramil Nugmanov <stsouko@live.ru>
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
from datetime import datetime
from pony.orm import PrimaryKey, Required, Set, Json
from .user import mixin_factory as um


def load_tables(db, schema, user_entity, debug=False):
    class MoleculeProperties(db.Entity, um(user_entity)):
        _table_ = '%s_properties' % schema if debug else (schema, 'properties')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime, default=datetime.utcnow)
        user_id = Required(int, column='user')
        data = Required(Json)
        structure = Required('Molecule')

        def __init__(self, data, structure, user):
            db.Entity.__init__(self, user_id=user.id, structure=structure, data=data)

    class ReactionConditions(db.Entity, um(user_entity)):
        _table_ = '%s_conditions' % schema if debug else (schema, 'conditions')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime, default=datetime.utcnow)
        user_id = Required(int, column='user')
        data = Required(Json)
        structure = Required('Reaction')

        def __init__(self, data, structure, user):
            db.Entity.__init__(self, user_id=user.id, structure=structure, data=data)

    class MoleculeClass(db.Entity):
        _table_ = '%s_molecule_class' % schema if debug else (schema, 'molecule_class')
        id = PrimaryKey(int, auto=True)
        name = Required(str)
        _type = Required(int, default=0, column='type')
        structures = Set('Molecule', table='%s_molecule_molecule_class' % schema if debug else
                                           (schema, 'molecule_molecule_class'))

    class ReactionClass(db.Entity):
        _table_ = '%s_reaction_class' % schema if debug else (schema, 'reaction_class')
        id = PrimaryKey(int, auto=True)
        name = Required(str)
        _type = Required(int, default=0, column='type')
        structures = Set('Reaction', table='%s_reaction_reaction_class' % schema if debug else
                                           (schema, 'reaction_reaction_class'))

    return MoleculeProperties, ReactionConditions, MoleculeClass, ReactionClass


__all__ = [load_tables.__name__]
