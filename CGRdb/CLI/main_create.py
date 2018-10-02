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


def create_core(**kwargs):
    if any(kwargs[x] is None for x in ('user', 'pass', 'host', 'base', 'port')):
        try:
            from config import DB_PASS, DB_HOST, DB_USER, DB_NAME, DB_PORT
        except ImportError:
            print('set all keys or install config.py correctly')
            return

    schema = kwargs['name']
    user = DB_USER if kwargs['user'] is None else kwargs['user']
    pswd = DB_PASS if kwargs['pass'] is None else kwargs['pass']
    host = DB_HOST if kwargs['host'] is None else kwargs['host']
    base = DB_NAME if kwargs['base'] is None else kwargs['base']
    port = DB_PORT if kwargs['port'] is None else kwargs['port']
    config = kwargs['config'] and load(kwargs['config']) or {}

    tables = load_tables(schema, workpath='.', **config)(user=user, password=pswd, host=host, database=base, port=port,
                                                         create_tables=True)
    db_conf = load_config()(user=user, password=pswd, host=host, database=base, port=port)

    fix_tables(tables, schema)
    with db_session:
        db_conf.Config(name=schema, config=config)


def fix_tables(db, schema):
    with db_session:
        db.execute('CREATE EXTENSION IF NOT EXISTS smlar')
        db.execute('CREATE EXTENSION IF NOT EXISTS intarray')
        db.execute(f'CREATE OR REPLACE FUNCTION {schema}.json2int_arr_tr()\n'
                   'RETURNS trigger AS\n'
                   '$$BODY$$\n'
                   'BEGIN\n'
                   '    NEW.bit_array = ARRAY(SELECT jsonb_array_elements_text(NEW.bit_list));\n'
                   '    RETURN NEW;\n'
                   'END\n'
                   '$$BODY$$\n'
                   'LANGUAGE plpgsql')

    with db_session:
        db.execute(f'ALTER TABLE {schema}.reaction_index ADD bit_array INT[] NOT NULL')
        db.execute(f'ALTER TABLE {schema}.molecule_structure ADD bit_array INT[] NOT NULL')

        db.execute('CREATE TRIGGER list_to_array\n'
                   'BEFORE INSERT OR UPDATE\n'
                   f'ON {schema}.molecule_structure\n'
                   'FOR EACH ROW\n'
                   f'EXECUTE PROCEDURE {schema}.json2int_arr_tr()')

        db.execute('CREATE TRIGGER list_to_array\n'
                   'BEFORE INSERT OR UPDATE\n'
                   f'ON {schema}.reaction_index\n'
                   'FOR EACH ROW\n'
                   f'EXECUTE PROCEDURE {schema}.json2int_arr_tr()')

    with db_session:
        db.execute(f'CREATE INDEX idx_smlar_molecule_structure ON {schema}.molecule_structure USING '
                   'GIST (bit_array _int4_sml_ops)')

        db.execute(f'CREATE INDEX idx_smlar_reaction_index ON {schema}.reaction_index USING '
                   'GIST (bit_array _int4_sml_ops)')

        db.execute(f'CREATE INDEX idx_subst_molecule_structure ON {schema}.molecule_structure USING '
                   'GIN (bit_array gin__int_ops)')

        db.execute(f'CREATE INDEX idx_subst_reaction_index ON {schema}.reaction_index USING '
                   'GIN (bit_array gin__int_ops)')
