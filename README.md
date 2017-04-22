**CGRdb** - DataBase Management system for chemical data

INSTALL
=======

    pip install -U git+https://github.com/stsouko/CGRdb.git@master#egg=MWUI --process-dependency-links --allow-all-external

POSTGRES SETUP
======
build smlar extension:

    git clone git://sigaev.ru/smlar.git
    cd smlar
    sudo su
    export USE_PGXS=1
    make

install by checkinstall:

    checkinstall -D
    
or

    make install

in postgres load extension:

    CREATE extension smlar;

pony.orm currently can't operate with arrays. jsonb instead used. but smlar work with arrays.
AD-HOC: define pg function and trigger for convert json lists into arrays on the fly.
    
    CREATE OR REPLACE FUNCTION json_int2(_js jsonb)
    RETURNS INT2[] AS
    $func$
    SELECT ARRAY(SELECT jsonb_array_elements_text(_js)::INT2)
    $func$
    LANGUAGE sql IMMUTABLE;

    CREATE OR REPLACE FUNCTION trigger_json_int2()
    RETURNS trigger AS
    $func$
    BEGIN
        NEW.bit_array = json_int2(NEW.bit_list);
        RETURN NEW;
    END
    $func$
    LANGUAGE plpgsql;

modify tables.

    ALTER TABLE schema.reaction_index ADD bit_array INT2[] NOT NULL;
    ALTER TABLE schema.molecule_structure ADD bit_array INT2[] NOT NULL;

set trigger to tables reaction_index and molecule_structure. replace schema with correct name

    CREATE TRIGGER trg_ja_schema_molecule_structure
    BEFORE INSERT OR UPDATE
    ON schema.molecule_structure
    FOR EACH ROW
    EXECUTE PROCEDURE trigger_json_int2();
    
    CREATE TRIGGER trg_ja_schema_reaction_index
    BEFORE INSERT OR UPDATE
    ON schema.reaction_index
    FOR EACH ROW
    EXECUTE PROCEDURE trigger_json_int2();

create index on array with similarity extension

    CREATE INDEX idx_gist_schema_molecule_structure ON schema.molecule_structure USING gist (bit_array _int2_sml_ops);
    CREATE INDEX idx_gist_schema_reaction_index ON schema.reaction_index USING gist (bit_array _int2_sml_ops);
