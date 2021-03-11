CGRdb
=====

Chemical cartridge for reactions and molecules.

INSTALL
-------

Stable version

    pip install CGRdb

Stable version with index support

    pip install CGRdb[index]

DEV version

    pip install -U git+https://github.com/stsouko/CGRdb.git@master#egg=CGRdb

INDEX GENERATION
----------------

Index stored in RAM and implemented as http service. Index can be hosted on a different machine.

    cgrdb index -c '{"host": "localhost", "password": "your password", "user": "postgres"}'
        -n 'schema_name'
        -p '{"check_threshold": null or positive float in range 0-1,
             "threshold": float in range 0-1 (approximate treshold for MinHashLSH),
             "num_perm": MinHashLSH parameter,
             "n_workers": number of workers for index creation,
             "chunk_size": 10000}'
        -d path/to/index.dump

default parameters : check_threshold=.7, threshold=.6, num_perm=64, n_workers=1, chunk_size=10000

INDEX LOADING
-------------

    cgrdb daemon -p '{parameters of aiohttp run_app}' -d path/to/index.dump

For each schema separate daemons should be used.

SETUP
-----

### initialize CGRdb \[after postgres setup only\] \[need only once\]

    cgrdb init -c '{"host": "localhost", "password": "your password", "user": "postgres"}'

Note:  connection config is JSON string with arguments acceptable by psycopg2

### create database

    cgrdb create -c '{"host": "localhost", "password": "your password", "user": "postgres"}'
        -n 'schema_name'
        -f 'path/to/cartridge.parameters. see example config.json}'

Note: database admin rights required (postgres user by default)  
Note: schema 'schema_name' will be dropped if exists and not proper CGRdb schema.

POSTGRES SETUP (Ubuntu example)
-------------------------------

install  postgresql (required version 10 or newer):

    sudo apt install postgresql postgresql-plpython3-10

edit user: 

    sudo -u postgres psql
    ALTER USER postgres WITH PASSWORD 'your password';
    \q

uncomment and change next line in `/etc/postgresql/10/main/postgresql.conf`

    deadlock_timeout = 10s

restart postgres

    sudo systemctl restart postgresql

install `CGRtools`, `StructureFingerprint` into system or virtual environment accessible for postgres user.

    sudo pip3 install CGRtools StructureFingerprint

Note: virtual environment should contain in bin directory activate_this.py script.

COPYRIGHT
---------

2017-2021 Ramil Nugmanov <nougmanoff@protonmail.com>

CONTRIBUTORS
------------

* Adelia Fatykhova <adelik21979@gmail.com>
* Salavat Zabirov <zab.sal42@gmail.com>
