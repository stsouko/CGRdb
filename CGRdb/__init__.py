# -*- coding: utf-8 -*-
#
#  Copyright 2017-2020 Ramil Nugmanov <nougmanoff@protonmail.com>
#  This file is part of CGRdb.
#
#  CGRdb is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, see <https://www.gnu.org/licenses/>.
#
from importlib import import_module
from json import dumps
from LazyPony import LazyEntityMeta
from pkg_resources import get_distribution, DistributionNotFound, VersionConflict
from pony.orm import db_session, Database
from .database import *


def load_schema(schema, *args, **kwargs):
    """
    Load schema from db with compatible version

    :param schema: schema name for loading
    """
    db_config = Database()
    LazyEntityMeta.attach(db_config, database='CGRdb_config')
    db_config.bind('postgres', *args, **kwargs)
    db_config.generate_mapping()

    with db_session:
        major_version = '.'.join(get_distribution('CGRdb').version.split('.')[:-1])
        config = db_config.Config.get(name=schema, version=major_version)
    if not config:
        raise KeyError('schema not exists')
    config = config.config

    for p in config.get('packages', ()):
        try:
            p = get_distribution(p)
            import_module(p.project_name)
        except (DistributionNotFound, VersionConflict):
            raise ImportError(f'packages not installed or has invalid versions: {p}')

    db = Database()
    LazyEntityMeta.attach(db, schema, 'CGRdb')

    db.bind('postgres', *args, **kwargs)
    db.generate_mapping()

    with db_session:
        db.execute(f'SELECT "{schema}".cgrdb_init_session(\'{dumps(config)}\')')
    return db


__all__ = ['load_schema', 'Molecule', 'Reaction']
