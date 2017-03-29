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
from pony.orm import Database
from .config import DB_DATA_LIST
from .models import load_tables


def load_schemas():
    data_db = {}
    for schema in DB_DATA_LIST:
        x = Database()
        data_db[schema] = x
    return data_db


def load_databases(user_entity=None):
    data_tables = {}
    data_db = load_schemas()
    for schema in DB_DATA_LIST:
        data_tables[schema] = load_tables(data_db[schema], schema, user_entity)
    return data_tables


def init():
    from pony.orm import sql_debug
    from .config import DEBUG, DB_PASS, DB_HOST, DB_USER, DB_NAME

    data_db = load_schemas()

    if DEBUG:
        sql_debug(True)
        for x in data_db.values():
            x.bind('sqlite', 'database.sqlite')
            x.generate_mapping(create_tables=True)
    else:
        for x in data_db.values():
            x.bind('postgres', user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)
            x.generate_mapping()
