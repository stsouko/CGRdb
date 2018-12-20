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
from LazyPony import LazyEntityMeta
from os import getenv
from pathlib import Path
from pkg_resources import get_distribution
from pony.orm import db_session, Database
from sys import path
from .database import *


#major_version = '.'.join(get_distribution('CGRdb').version.split('.')[:-1])
major_version = '3.0'


env = getenv('CGR_DB')
if env:
    cfg = Path(env)
    if cfg.is_dir() and (cfg / 'config.py').is_file() and str(cfg) not in path:
        path.append(str(cfg))


class Loader:
    def __init__(self, user=None, password=None, host=None, database=None, port=5432, workpath='/tmp'):
        """
        load all schemas from db with compatible version

        :param user: if None then used from config
        :param password: if None then used from config
        :param host: if None then used from config
        :param database: if None then used from config
        :param port: if None then used from config
        """
        if user is None or password is None or host is None or database is None or port is None or workpath is None:
            try:
                from config import DB_PASS, DB_HOST, DB_USER, DB_NAME, DB_PORT, WORKPATH
            except ImportError:
                raise ImportError('install config.py correctly')

        if user is None:
            user = DB_USER
        if password is None:
            password = DB_PASS
        if host is None:
            host = DB_HOST
        if database is None:
            database = DB_NAME
        if port is None:
            port = DB_PORT
        if workpath is None:
            workpath = WORKPATH

        db_config = Database()
        LazyEntityMeta.attach(db_config, database='CGRdb_config')
        db_config.bind('postgres', user=user, password=password, host=host, database=database, port=port)
        db_config.generate_mapping()

        self.__schemas = {}

        with db_session:
            config = db_config.Config.select(lambda x: x.version == major_version)[:]

        for c in config:
            db = Database()
            LazyEntityMeta.attach(db, c.name, 'CGRdb')

            db.Molecule._fragmentor_workpath = db.Reaction._fragmentor_workpath = workpath
            for k, v in c.config.get('molecule', {}).items():
                setattr(db.Molecule, f'_{k}', v)
            for k, v in c.config.get('reaction', {}).items():
                setattr(db.Reaction, f'_{k}', v)

            db.bind('postgres', user=user, password=password, host=host, database=database, port=port)
            db.generate_mapping()
            self.__schemas[c.name] = db

    def __iter__(self):
        return iter(self.__schemas)

    def __getitem__(self, item):
        return self.__schemas[item]

    def __contains__(self, item):
        return item in self.__schemas

    def keys(self):
        return self.__schemas.keys()

    def values(self):
        return self.__schemas.values()

    def items(self):
        return self.__schemas.items()
