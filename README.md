CGRdb
=====

Chemical cartridge for reactions and molecules.

INSTALL
-------

Stable version

    pip install CGRdb[postgres]

or

    pip install CGRdb[postgres_cffi]

DEV version

    pip install -U git+https://github.com/stsouko/CGRdb.git@master#egg=CGRdb[postgres]

or

    pip install -U git+https://github.com/stsouko/CGRdb.git@master#egg=CGRdb[postgres_cffi]

SETUP
-----

initialize CGRdb \[after postgres setup only\] \[need only once\]

    cgrdb init -p 'your password' # use -h argument for help printing

create database \[required empty schema 'schema_name' in db\]

    cgrdb create -p 'your password' -n 'schema_name' # use -h argument for help printing

POSTGRES SETUP (Ubuntu 18.04 example)
-------------------------------------

install  postgresql (required version 10):

    sudo apt install postgresql postgresql-server-dev-10 postgresql-plpython3-10

edit user: 

    sudo -u postgres psql

and type:

    ALTER USER postgres WITH PASSWORD 'your password';
    \q

build patched smlar extension:

    git clone https://github.com/stsouko/smlar.git
    cd smlar
    sudo su
    export USE_PGXS=1
    make

install by checkinstall:

    sudo apt install checkinstall
    checkinstall -D

or

    make install

uncomment and change next line in `/etc/postgresql/10/main/postgresql.conf`

    deadlock_timeout = 10s

add line into `/etc/postgresql/10/main/environment`

    PATH = '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'

restart postgres

    sudo systemctl restart postgresql

install `CGRtools`, `CIMtools` and `compress-pickle` into system or virtual environment accessible for postgres user.

    sudo pip3 install compress-pickle 
    sudo pip3 install git+https://github.com/cimm-kzn/CGRtools.git@master#egg=CGRtools
    sudo pip3 install git+https://github.com/stsouko/CIMtools.git@master#egg=CIMtools

COPYRIGHT
---------

2017-2020 Ramil Nugmanov <nougmanoff@protonmail.com>

CONTRIBUTORS
------------

* Adelia Fatykhova <adelik21979@gmail.com>
* Salavat Zabirov <zab.sal42@gmail.com>
