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
from os import getenv
from pathlib import Path
from sys import path


env = getenv('CGR_DB')
if env:
    cfg = Path(env)
    if cfg.is_dir() and (cfg / 'config.py').is_file() and str(cfg) not in path:
        path.append(str(cfg))


class Loader:
    """
    loader of schemas based on common config.

    use this for equally configured bases.
    for custom bases use .models.load_tables factory.
    """
    __schemas = {}
    __databases = {}

    @classmethod
    def load_schemas(cls, user_entity=None):
        if not cls.__schemas:
            try:
                from config import (DB_DATA_LIST, DB_PASS, DB_HOST, DB_USER, DB_NAME, DB_PORT, DATA_ISOTOPE,
                                    DATA_STEREO, DATA_EXTRALABELS, FRAGMENTOR_VERSION, FRAGMENT_TYPE_MOL,
                                    FRAGMENT_MIN_MOL, FRAGMENT_MAX_MOL, FRAGMENT_TYPE_CGR, FRAGMENT_MIN_CGR,
                                    FRAGMENT_MAX_CGR, FRAGMENT_DYNBOND_CGR, WORKPATH, FP_SIZE, FP_ACTIVE_BITS, FP_COUNT)
            except ImportError:
                print('install config.py correctly')
                return

            from .models import load_tables

            for schema in DB_DATA_LIST:
                m, r, *_, db = load_tables(schema, FRAGMENTOR_VERSION, FRAGMENT_TYPE_MOL, FRAGMENT_MIN_MOL,
                                           FRAGMENT_MAX_MOL, FRAGMENT_TYPE_CGR, FRAGMENT_MIN_CGR, FRAGMENT_MAX_CGR,
                                           FRAGMENT_DYNBOND_CGR, FP_SIZE, FP_ACTIVE_BITS, FP_COUNT, WORKPATH,
                                           user_entity, DATA_ISOTOPE, DATA_STEREO, DATA_EXTRALABELS)
                cls.__schemas[schema] = db
                cls.__databases[schema] = m, r

                db.bind('postgres', user=DB_USER, password=DB_PASS, host=DB_HOST, database=DB_NAME, port=DB_PORT)
                db.generate_mapping(create_tables=False)

    @classmethod
    def list_databases(cls):
        return cls.__databases

    @classmethod
    def get_database(cls, name):
        return cls.__databases[name]

    @classmethod
    def get_schema(cls, name):
        return cls.__schemas[name]
