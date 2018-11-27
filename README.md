**CGRdb** - DataBase Management system for chemical data

INSTALL
=======

    pip install -U git+https://github.com/stsouko/CGRdb.git@master#egg=MWUI --process-dependency-links

or

    pip install CGRdb

POSTGRES SETUP
==============
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

Install and set up pg_cron extension, following the [instruction](https://github.com/citusdata/pg_cron/blob/master/README.md#installing-pg_cron)

CONFIGURE
=========
optional

add to environment key CGR_DB with path/to/config.py directory
edit config.py_example and put it to config directory by name config.py
