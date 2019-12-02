**CGRdb** - DataBase Management system for chemical data

INSTALL
=======

    pip install -U git+https://github.com/stsouko/CGRdb.git@master#egg=CGRdb

or

    pip install CGRdb

initialize CGRdb \[after postgres setup only\]

    cgrdb init -p 'your password' # use -h argument for help printing

POSTGRES SETUP (Ubuntu 18.04 example)
=====================================
install  postgresql:

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

restart postgres

    sudo systemctl restart postgresql

install `CGRtools`, `CIMtools` and `compress-pickle` into system

    sudo pip3 install compress-pickle 
    sudo pip3 install git+https://github.com/cimm-kzn/CGRtools.git@master#egg=CGRtools
    sudo pip3 install git+https://github.com/stsouko/CIMtools.git@master#egg=CIMtools
