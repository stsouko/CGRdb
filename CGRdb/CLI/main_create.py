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
from pony.orm import db_session
from ..models import load_tables, load_config


def create_core(args):
    schema = args.name
    config = args.config and load(args.config) or {}

    tables = load_tables(schema, workpath='.', **config)(user=args.user, password=args.password, host=args.host,
                                                         database=args.base, port=args.port, create_tables=True)
    db_conf = load_config()(user=args.user, password=args.password, host=args.host, database=args.base, port=args.port)

    fix_tables(tables, schema)
    with db_session:
        db_conf.Config(name=schema, config=config)


def fix_tables(db, schema):
    with db_session:
        db.execute('CREATE EXTENSION IF NOT EXISTS smlar')
        db.execute('CREATE EXTENSION IF NOT EXISTS intarray')

    with db_session:
        db.execute(f'ALTER TABLE {schema}.reaction_index ADD bit_array INT[] NOT NULL')
        db.execute(f'ALTER TABLE {schema}.molecule_structure ADD bit_array INT[] NOT NULL')

    with db_session:
        db.execute(f'CREATE INDEX idx_smlar_molecule_structure ON {schema}.molecule_structure USING '
                   'GIST (bit_array _int4_sml_ops)')

        db.execute(f'CREATE INDEX idx_smlar_reaction_index ON {schema}.reaction_index USING '
                   'GIST (bit_array _int4_sml_ops)')

        db.execute(f'CREATE INDEX idx_subst_molecule_structure ON {schema}.molecule_structure USING '
                   'GIN (bit_array gin__int_ops)')

        db.execute(f'CREATE INDEX idx_subst_reaction_index ON {schema}.reaction_index USING '
                   'GIN (bit_array gin__int_ops)')
