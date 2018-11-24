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

    with db_session:
        db.execute(f'CREATE TYPE {schema}.molecule_structure_result AS '
                   '(molecule_arr int[], id_arr int[], t_arr real[])')
        db.execute(
            f'CREATE OR REPLACE FUNCTION {schema}.get_molecules_func_arr(structure text, search_operator text, signature bytea, name_of_schema text)\n'
            f'  RETURNS setof "{schema}"."molecule_structure_result" AS $$\n'
            'DECLARE\n'
            '  sql_op             text;\n'
            '  result_raw_num     BIGINT;\n'
            'BEGIN\n'
            '  IF search_operator = \'similar\'\n'
            '  THEN\n'
            '    sql_op = \'%%\';\n'
            '  ELSE\n'
            '    sql_op = \'@>\';\n'
            '  END IF;\n'
            '    --saving results matching query into temporary table "help_temp_m_s_table"\n'
            '    EXECUTE FORMAT(\'CREATE TEMP TABLE help_temp_m_s_table ON COMMIT DROP AS\n'
            '      SELECT "x"."molecule", "x"."id", smlar("x".bit_array :: int [], \'\'%1$s\'\' :: int [], \'\'N.i / (N.a + N.b - N.i)\'\') AS t\n'
            '      FROM "%2$s"."molecule_structure" "x"\n'
            '      WHERE "x".bit_array :: int [] %3$s \'\'%1$s\'\' :: int []\', structure, name_of_schema, sql_op);\n'
            '    --saving non-duplicate results with unique molecule structures and max Tanimoto index into temporary table "temp_m_s_table" in sorted by Tanimoto index order\n'
            '    EXECUTE \'CREATE TEMP TABLE temp_m_s_table ON COMMIT DROP AS SELECT * FROM (\n'
            '      SELECT t1.id, t1.molecule, t1.t\n'
            '      FROM (SELECT * FROM help_temp_m_s_table) t1\n'
            '             JOIN (SELECT molecule, max(t) AS t FROM help_temp_m_s_table GROUP BY molecule) t2\n'
            '               ON t1.molecule = t2.molecule AND t1.t = t2.t) j ORDER BY t DESC\';\n'
            '    EXECUTE \'SELECT COUNT(*) FROM temp_m_s_table\'\n'
            '    INTO result_raw_num;\n'
            '    IF result_raw_num >= 1000\n'
            '    THEN\n'
            '      --saving results in "molecule_structure_save" table as arrays\n'
            '      EXECUTE FORMAT(\'INSERT INTO %s.molecule_structure_save(signature, molecules, structures, tanimotos, date, operator)\n'
            '      VALUES (\'\'%s\'\',\n'
            '              (SELECT array_agg(molecule) FROM temp_m_s_table),\n'
            '              (SELECT array_agg(id) FROM temp_m_s_table),\n'
            '              (SELECT array_agg(t) FROM temp_m_s_table),\n'
            '              CURRENT_TIMESTAMP,\n'
            '              \'\'%s\'\');\', name_of_schema, signature, search_operator);\n'
            '    ELSE\n'
            '      --returning all found results\n'
            '      return query execute format(\n'
            '          \'SELECT array_agg(molecule) molecule_arr, array_agg(id) id_arr, array_agg(t) t_arr FROM temp_m_s_table\');\n'
            '    END IF;\n'
            '    DROP TABLE IF EXISTS temp_m_s_table;\n'
            '    DROP TABLE IF EXISTS help_temp_m_s_table;\n'
            'END\n'
            '$$\n'
            'LANGUAGE plpgsql;\n')
