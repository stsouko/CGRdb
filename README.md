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
