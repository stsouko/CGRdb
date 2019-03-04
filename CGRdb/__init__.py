# -*- coding: utf-8 -*-
#
#  Copyright 2017-2019 Ramil Nugmanov <stsouko@live.ru>
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
from importlib import import_module
from LazyPony import LazyEntityMeta
from os import getenv
from pathlib import Path
from pkg_resources import get_distribution, DistributionNotFound, VersionConflict
from pony.orm import db_session, Database
from sys import path
from .database import *


env = getenv('CGR_DB')
if env:
    cfg = Path(env)
    if cfg.is_dir() and (cfg / 'config.py').is_file() and str(cfg) not in path:
        path.append(str(cfg))


def load_schema(schema, user=None, password=None, host=None, database=None, port=5432, workpath='/tmp'):
    """
    load all schemas from db with compatible version

    :param schema: schema name for loading
    :param user: if None then used from config
    :param password: if None then used from config
    :param host: if None then used from config
    :param database: if None then used from config
    :param port: if None then used from config
    :param workpath: directory for temp files
    """
    if user is None or password is None or host is None or database is None or port is None or workpath is None:
        try:
            from config import DB_PASS, DB_HOST, DB_USER, DB_NAME, DB_PORT, WORKPATH
        except ImportError:
            raise ImportError('install config.py correctly')

        user = DB_USER
        password = DB_PASS
        host = DB_HOST
        database = DB_NAME
        port = DB_PORT
        if workpath is None:
            workpath = WORKPATH

    db_config = Database()
    LazyEntityMeta.attach(db_config, database='CGRdb_config')
    db_config.bind('postgres', user=user, password=password, host=host, database=database, port=port)
    db_config.generate_mapping()

    with db_session:
        major_version = '.'.join(get_distribution('CGRdb').version.split('.')[:-1])
        config = db_config.Config.get(name=schema, version=major_version)
    if not config:
        raise KeyError('schema not exists')
    config = config.config

    for p in config['packages']:
        try:
            p = get_distribution(p)
            import_module(p.project_name)
        except (DistributionNotFound, VersionConflict):
            raise ImportError(f'packages not installed or has invalid versions: {p}')

    db = Database()
    LazyEntityMeta.attach(db, schema, 'CGRdb')

    db.Molecule._fragmentor_workpath = db.Reaction._fragmentor_workpath = workpath
    for k, v in config.get('molecule', {}).items():
        setattr(db.Molecule, f'_{k}', v)
    for k, v in config.get('reaction', {}).items():
        setattr(db.Reaction, f'_{k}', v)

    db.bind('postgres', user=user, password=password, host=host, database=database, port=port)
    db.generate_mapping()
    return db


__all__ = ['load_schema']
