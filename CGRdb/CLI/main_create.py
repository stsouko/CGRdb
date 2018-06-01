#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
#
#  Copyright 2017, 2018 Ramil Nugmanov <stsouko@live.ru>
#  This file is part of CGRdb.
#
#  CGRdb 
#  is free software; you can redistribute it and/or modify
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
from pony.orm import Database, sql_debug, db_session
from ..config import DEBUG, DB_PASS, DB_HOST, DB_USER, DB_NAME
from ..models import load_tables


def create_core(**kwargs):
    schema = kwargs['name']
    user = DB_USER if kwargs['user'] is None else kwargs['user']
    password = DB_PASS if kwargs['pass'] is None else kwargs['pass']

    x = Database()
    load_tables(x, schema, None)

    if DEBUG:
        sql_debug(True)
        x.bind('sqlite', 'database.sqlite')
        x.generate_mapping(create_tables=True)
    else:
        x.bind('postgres', user=user, password=password, host=DB_HOST, database=DB_NAME)
        x.generate_mapping(create_tables=True)

        with db_session:
            ext_smlar = 'CREATE EXTENSION IF NOT EXISTS smlar'
            ext_intarr = 'CREATE EXTENSION IF NOT EXISTS intarray'

            json2int_arr = 'CREATE OR REPLACE FUNCTION {0}.json2int_arr_tr()\n' \
                           'RETURNS trigger AS\n' \
                           '$BODY$\n' \
                           'BEGIN\n' \
                           '    NEW.bit_array = ARRAY(SELECT jsonb_array_elements_text(NEW.bit_list));\n' \
                           '    RETURN NEW;\n' \
                           'END\n' \
                           '$BODY$\n' \
                           'LANGUAGE plpgsql'.format(schema)

            x.execute(ext_smlar)
            x.execute(ext_intarr)
            x.execute(json2int_arr)

        with db_session:
            array_ri = 'ALTER TABLE {0}.reaction_index ADD bit_array INT[] NOT NULL'.format(schema)
            array_ms = 'ALTER TABLE {0}.molecule_structure ADD bit_array INT[] NOT NULL'.format(schema)

            trigger_ms = 'CREATE TRIGGER list_to_array\n' \
                         'BEFORE INSERT OR UPDATE\n' \
                         'ON {0}.molecule_structure\n' \
                         'FOR EACH ROW\n' \
                         'EXECUTE PROCEDURE {0}.json2int_arr_tr()'.format(schema)

            trigger_ri = 'CREATE TRIGGER list_to_array\n' \
                         'BEFORE INSERT OR UPDATE\n' \
                         'ON {0}.reaction_index\n' \
                         'FOR EACH ROW\n' \
                         'EXECUTE PROCEDURE {0}.json2int_arr_tr()'.format(schema)

            x.execute(array_ri)
            x.execute(array_ms)
            x.execute(trigger_ms)
            x.execute(trigger_ri)

        with db_session:
            smlar_ms = 'CREATE INDEX idx_smlar_molecule_structure ON {0}.molecule_structure USING ' \
                       'GIST (bit_array _int4_sml_ops)'.format(schema)

            smlar_ri = 'CREATE INDEX idx_smlar_reaction_index ON {0}.reaction_index USING ' \
                       'GIST (bit_array _int4_sml_ops)'.format(schema)

            subst_ms = 'CREATE INDEX idx_subst_molecule_structure ON {0}.molecule_structure USING ' \
                       'GIN (bit_array gin__int_ops)'.format(schema)

            subst_ri = 'CREATE INDEX idx_subst_reaction_index ON {0}.reaction_index USING ' \
                       'GIN (bit_array gin__int_ops)'.format(schema)

            x.execute(smlar_ms)
            x.execute(smlar_ri)
            x.execute(subst_ms)
            x.execute(subst_ri)
