# -*- coding: utf-8 -*-
#
#  Copyright 2018 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2018 Adelia Fatykhova <adelik21979@gmail.com>
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
import pytest
from pony.orm import db_session, Database
from CGRdb.models import load_tables
from CGRtools.files.RDFrw import RDFread
from .config import user


class DBPrepare:

    @pytest.fixture()
    def db(self):
        db = Database()
        load_tables(db, 'cgrdb', None)

        db.bind('postgres', user='postgres', password='123', host=None, database=None)
        with db_session:
            db.execute('drop SCHEMA cgrdb CASCADE')
            db.execute('CREATE SCHEMA cgrdb')

        db.generate_mapping(create_tables=True)

        return db

    @staticmethod
    def populate(db, rdf):
        for r in RDFread(rdf).read():
            db.Reaction(r, user)
