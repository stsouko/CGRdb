# -*- coding: utf-8 -*-
#
#  Copyright 2021 Ramil Nugmanov <nougmanoff@protonmail.com>
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
from LazyPony import LazyEntityMeta
from pkg_resources import get_distribution, DistributionNotFound, VersionConflict
from pony.orm import db_session, Database


def clean_core(args):
    version = get_distribution('CGRdb').parsed_version
    major_version = f'{version.major}.{version.minor}'
    schema = args.name

    db_config = Database()
    LazyEntityMeta.attach(db_config, database='CGRdb_config')
    db_config.bind('postgres', **args.connection)
    db_config.generate_mapping()

    with db_session:
        config = db_config.Config.get(name=schema, version=major_version)
    if not config:
        raise KeyError('schema not exists or version incompatible')
    config = config.config

    for p in config['packages']:
        try:
            p = get_distribution(p)
            import_module(p.project_name)
        except (DistributionNotFound, VersionConflict):
            raise ImportError(f'packages not installed or has invalid versions: {p}')

    db = Database()
    LazyEntityMeta.attach(db, schema, 'CGRdb')
    db.bind('postgres', **args.connection)
    db.generate_mapping()

    with db_session:
        db.execute(f'TRUNCATE TABLE "{schema}"."MoleculeSearchCache", '
                   f'"{schema}"."ReactionSearchCache" RESTART IDENTITY')
