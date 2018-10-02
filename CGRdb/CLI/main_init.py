# -*- coding: utf-8 -*-
#
#  Copyright 2018 Ramil Nugmanov <stsouko@live.ru>
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
from pony.orm import db_session, Database


def init_core(**kwargs):
    if any(kwargs[x] is None for x in ('user', 'pass', 'host', 'base')):
        try:
            from config import DB_PASS, DB_HOST, DB_USER, DB_NAME, DB_PORT
        except ImportError:
            print('set all keys or install config.py correctly')
            return

    user = DB_USER if kwargs['user'] is None else kwargs['user']
    pswd = DB_PASS if kwargs['pass'] is None else kwargs['pass']
    host = DB_HOST if kwargs['host'] is None else kwargs['host']
    base = DB_NAME if kwargs['base'] is None else kwargs['base']
    port = DB_PORT if kwargs['port'] is None else kwargs['port']

    db = Database()
    db.bind('postgres', user=user, password=pswd, host=host, database=base, port=port)

    with db_session:
        db.execute('CREATE TABLE IF NOT EXISTS cgr_db_config\n'
                   '(\n'
                   '    id serial PRIMARY KEY NOT NULL,\n'
                   '    name text NOT NULL,\n'
                   '    config json NOT NULL,\n'
                   '    version text NOT NULL\n'
                   ');\n'
                   'CREATE UNIQUE INDEX IF NOT EXISTS cgr_db_config_name ON cgr_db_config (name);')
