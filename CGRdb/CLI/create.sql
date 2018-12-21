CREATE TYPE "{}".molecules_result AS (molecules int[], molecule_structures int[], tanimotos real[]);
CREATE TYPE "{}".reactions_result AS (reactions int[], reaction_indexes int[], tanimotos real[]);


SELECT cron.schedule('0 3 * * *', $$
                                      DELETE FROM "{}"."MoleculeSearchCache"
                                          WHERE date < CURRENT_TIMESTAMP - INTERVAL '1 day'
                                  $$);
SELECT cron.schedule('0 3 * * *', $$
                                      DELETE FROM "{}"."ReactionSearchCache"
                                          WHERE date < CURRENT_TIMESTAMP - INTERVAL '1 day'
                                  $$);


CREATE OR REPLACE FUNCTION "{}".get_molecules_func_arr(structure int[], search_operator text, signature bytea)
RETURNS SETOF "{schema}"."molecule_structure_result" AS
$$
    DECLARE
        sql_op             text;
        result_raw_num     BIGINT;
    BEGIN
        IF search_operator = 'similar'
        THEN
            sql_op = '%';
        ELSE
            sql_op = '@>';
        END IF;

        --saving results matching query into temporary table "temp_molecules_table"
        EXECUTE 'CREATE TEMP TABLE temp_molecules_table ON COMMIT DROP AS ' ||
                'SELECT x.molecule, x.id, ' ||
                       'smlar(x.bit_array, $1, ''N.i / (N.a + N.b - N.i)'') AS t ' ||
                'FROM "{}"."MoleculeStructure" x WHERE x.bit_array $2 $1'
        USING structure, sql_op;

                --saving non-duplicate results with unique molecule structures and max Tanimoto index
                --into temporary table "temp_m_s_table" in sorted by Tanimoto index order
                EXECUTE 'CREATE TEMP TABLE temp_m_s_table ON COMMIT DROP AS SELECT * FROM (
                    SELECT t1.id, t1.molecule, t1.t
                    FROM (SELECT * FROM temp_molecules_table) t1
                        JOIN (SELECT molecule, max(t) AS t FROM temp_molecules_table GROUP BY molecule) t2
                        ON t1.molecule = t2.molecule AND t1.t = t2.t) j ORDER BY t DESC';
                EXECUTE 'SELECT COUNT(*) FROM temp_m_s_table'
                INTO result_raw_num;
                IF result_raw_num >= 1000
                THEN
                    --saving results in "molecule_structure_save" table as arrays
                   EXECUTE FORMAT('INSERT INTO
                    "{schema}".MoleculeSearchCache(signature, molecules, structures, tanimotos, date, operator)
                    VALUES (''%s'',
                        (SELECT array_agg(molecule) FROM temp_m_s_table),
                        (SELECT array_agg(id) FROM temp_m_s_table),
                        (SELECT array_agg(t) FROM temp_m_s_table),
                        CURRENT_TIMESTAMP,
                        ''%s'');', signature, search_operator);
                    return query execute format(
                        'SELECT ARRAY[]::INT[], ARRAY[]::INT[], ARRAY[]::REAL[]');
                ELSE
                    IF result_raw_num = 0
                    THEN
                    return query execute format(
                        'SELECT ARRAY[]::INT[], ARRAY[]::INT[], ARRAY[]::REAL[]');
                    ELSE
                        --returning all found results
                        return query execute format(
                            'SELECT array_agg(molecule) molecule_arr, array_agg(id) id_arr, array_agg(t) t_arr FROM temp_m_s_table');
                    END IF;
                END IF;
                DROP TABLE IF EXISTS temp_m_s_table;
                DROP TABLE IF EXISTS temp_molecules_table;
            END
$$
LANGUAGE plpgsql;

 db.execute(
            f'CREATE OR REPLACE FUNCTION "{schema}".get_reactions(structure text, '
            '    search_operator text, signature bytea)'
            f'RETURNS setof "{schema}"."reactions_result" AS\n'
            '$$$$\n'
            'DECLARE\n'
            '    sql_op         text;\n'
            '    result_raw_num BIGINT;\n'
            'BEGIN\n'
            "    IF search_operator = 'similar'\n"
            '    THEN\n'
            "       sql_op = '%';\n"
            '    ELSE\n'
            "        sql_op = '@>';\n"
            '    END IF;\n'
            "    EXECUTE FORMAT('CREATE TEMP TABLE temp_reactions_table ON COMMIT DROP AS\n"
            '        SELECT "x"."reaction", smlar(x.bit_array::int[], '
            "            ''%s''::int[], ''N.i / (N.a + N.b - N.i)'') as t, "
            '            "x"."id"\n'
            f'        FROM "{schema}"."ReactionIndex" "x"\n'
            "        WHERE x.bit_array::int[] %2$$s ''%1$$s''::int[]', structure, sql_op);\n"
            "    EXECUTE 'SELECT COUNT(*) FROM temp_reactions_table' INTO result_raw_num;\n"
            '    IF result_raw_num >= 1000\n'
            '    THEN\n'
            '        --saving results in "reactions_save" table as arrays\n'
            "        EXECUTE FORMAT('INSERT INTO "
            f'            "{schema}".ReactionSearchCache(signature, reactions, reaction_indexes, '
            f'                tanimotos, date, operator)\n'
            "            VALUES (''%s'',\n"
            '                (SELECT array_agg(reaction) FROM temp_reactions_table),\n'
            '                (SELECT array_agg(id) FROM temp_reactions_table),\n'
            '                (SELECT array_agg(t) FROM temp_reactions_table),\n'
            '                CURRENT_TIMESTAMP,\n'
            "                ''%s'');', signature, search_operator);\n"
            '        return query execute format(\n'
            "          'SELECT ARRAY[]::INT[], ARRAY[]::INT[], ARRAY[]::REAL[]');\n"
            '    ELSE\n'
            '        IF result_raw_num = 0'
            '        THEN'
            '        return query execute format(\n'
            "            'SELECT ARRAY[]::INT[], ARRAY[]::INT[], ARRAY[]::REAL[]');"
            '        ELSE'
            '            --returning all found results\n'
            '            return query execute format(\n'
            "                'SELECT array_agg(reaction) reactions, array_agg(id) reaction_indexes, "
            "                    array_agg(t) tanimotos FROM temp_reactions_table');\n"
            "        END IF;"
            '    END IF;\n'
            '    DROP TABLE IF EXISTS temp_reactions_table;\n'
            'END\n'
            '$$$$\n'
            'LANGUAGE plpgsql;\n')
