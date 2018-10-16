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


def init_core(args):
    db = Database()
    db.bind('postgres', user=args.user, password=args.password, host=args.host, database=args.base, port=args.port)

    with db_session:
        db.execute(f'CREATE TABLE IF NOT EXISTS {args.name}.cgr_db_config\n'
                   '(\n'
                   '    id serial PRIMARY KEY NOT NULL,\n'
                   '    name text NOT NULL,\n'
                   '    config json NOT NULL,\n'
                   '    version text NOT NULL\n'
                   ');\n'
                   'CREATE UNIQUE INDEX IF NOT EXISTS cgr_db_config_name ON cgr_db_config (name);')
