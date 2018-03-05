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
from pony.orm import Database, sql_debug
from .config import DB_DATA_LIST, DEBUG, DB_PASS, DB_HOST, DB_USER, DB_NAME
from .models import load_tables


class Loader:
    __schemas = {}
    __databases = {}

    @classmethod
    def load_schemas(cls, user_entity=None):
        if not cls.__schemas:
            if DEBUG:
                sql_debug(True)

            for schema in DB_DATA_LIST:
                x = Database()
                cls.__schemas[schema] = x
                cls.__databases[schema] = load_tables(x, schema, user_entity)
                if DEBUG:
                    x.bind('sqlite', 'database.sqlite')
                else:
                    x.bind('postgres', user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME)

                x.generate_mapping(create_tables=False)

    @classmethod
    def list_databases(cls):
        return cls.__databases

    @classmethod
    def get_database(cls, name):
        return cls.__databases[name]

    @classmethod
    def get_schema(cls, name):
        return cls.__schemas[name]
