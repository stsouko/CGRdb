CGRdb
=====

Chemical cartridge for reactions and molecules.

INSTALL
-------

Stable version

    pip install CGRdb

DEV version

    pip install -U git+https://github.com/stsouko/CGRdb.git@master#egg=CGRdb

SETUP
-----

initialize CGRdb \[after postgres setup only\] \[need only once\]

    cgrdb init -p 'your password' # use -h argument for help printing

create database \[required empty schema 'schema_name' in db\]

    cgrdb create -p 'your password' -n 'schema_name' # use -h argument for help printing

POSTGRES SETUP (Ubuntu 18.04 example)
-------------------------------------

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

install `CGRtools`, `StructureFingerprint` and `compress-pickle` into system or virtual environment accessible for postgres user.

    sudo pip3 install compress-pickle 
    sudo pip3 install git+https://github.com/cimm-kzn/CGRtools.git@master#egg=CGRtools
    sudo pip3 install git+https://github.com/dcloudf/MorganFingerprint.git@master#egg=StructureFingerprint

COPYRIGHT
---------

2017-2021 Ramil Nugmanov <nougmanoff@protonmail.com>

CONTRIBUTORS
------------

* Adelia Fatykhova <adelik21979@gmail.com>
* Salavat Zabirov <zab.sal42@gmail.com>
