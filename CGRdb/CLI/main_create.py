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
from json import load
from LazyPony import LazyEntityMeta
from pkg_resources import get_distribution, resource_string
from pony.orm import db_session, Database, sql_debug


#major_version = '.'.join(get_distribution('CGRdb').version.split('.')[:-1])
major_version = '3.0'


def create_core(args):
    schema = args.name
    config = args.config and load(args.config) or {}
    sql_debug(True)
    db_config = Database()
    LazyEntityMeta.attach(db_config, database='CGRdb_config')
    db_config.bind('postgres', user=args.user, password=args.password, host=args.host, database=args.base,
                   port=args.port)
    db_config.generate_mapping()

    db = Database()
    LazyEntityMeta.attach(db, schema, 'CGRdb')
    db.bind('postgres', user=args.user, password=args.password, host=args.host, database=args.base, port=args.port)
    db.generate_mapping(create_tables=True)

    with db_session:
        db.execute('CREATE EXTENSION IF NOT EXISTS smlar')
        db.execute('CREATE EXTENSION IF NOT EXISTS intarray')
        db.execute('CREATE EXTENSION IF NOT EXISTS pg_cron')

    with db_session:
        db.execute(f'CREATE INDEX idx_smlar_molecule_structure ON "{schema}"."MoleculeStructure" USING '
                   'GIST (bit_array _int4_sml_ops)')
        db.execute(f'CREATE INDEX idx_smlar_reaction_index ON "{schema}"."ReactionIndex" USING '
                   'GIST (bit_array _int4_sml_ops)')
        db.execute(f'CREATE INDEX idx_subst_molecule_structure ON "{schema}"."MoleculeStructure" USING '
                   'GIN (bit_array gin__int_ops)')
        db.execute(f'CREATE INDEX idx_subst_reaction_index ON "{schema}"."ReactionIndex" USING '
                   'GIN (bit_array gin__int_ops)')

    # with db_session:
    #     db.execute(resource_string(__package__, 'create.sql').format(schema).replace('$', '$$'))

    with db_session:
        db_config.Config(name=schema, config=config, version=major_version)
