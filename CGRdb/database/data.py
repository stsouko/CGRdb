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
from LazyPony import LazyEntityMeta, DoubleLink
from pony.orm import PrimaryKey, Required, Set, Json


class MoleculeProperties(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    date = Required(datetime, default=datetime.utcnow)
    user = DoubleLink(Required('User', reverse='molecule_properties'), Set('MoleculeProperties'))
    structure = DoubleLink(Required('Molecule', reverse='metadata'), Set('MoleculeProperties'))
    data = Required(Json)


class ReactionConditions(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    date = Required(datetime, default=datetime.utcnow)
    user = DoubleLink(Required('User', reverse='reaction_conditions'), Set('ReactionConditions'))
    structure = DoubleLink(Required('Reaction', reverse='metadata'), Set('ReactionConditions'))
    data = Required(Json)


class MoleculeClass(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    _type = Required(int, default=0, column='type')
    structures = DoubleLink(Set('Molecule', table='Molecule_MoleculeClass', reverse='classes'), Set('MoleculeClass'))


class ReactionClass(metaclass=LazyEntityMeta, database='CGRdb'):
    id = PrimaryKey(int, auto=True)
    name = Required(str)
    type = Required(int, default=0)
    structures = DoubleLink(Set('Reaction', table='Reaction_ReactionClass', reverse='classes'), Set('ReactionClass'))


__all__ = ['MoleculeProperties', 'ReactionConditions', 'MoleculeClass', 'ReactionClass']
