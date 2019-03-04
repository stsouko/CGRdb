# -*- coding: utf-8 -*-
#
#  Copyright 2018, 2019 Ramil Nugmanov <stsouko@live.ru>
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
from LazyPony import LazyEntityMeta
from pony.orm import PrimaryKey, Required, Json


class Config(metaclass=LazyEntityMeta, database='CGRdb_config'):
    _table_ = 'cgr_db_config'
    id = PrimaryKey(int, auto=True)
    name = Required(str, unique=True)
    config = Required(Json, index=False, optimistic=False)
    version = Required(str)


__all__ = ['Config']
