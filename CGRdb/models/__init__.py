# -*- coding: utf-8 -*-
#
#  Copyright 2017, 2018 Ramil Nugmanov <stsouko@live.ru>
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
from functools import wraps
from pony.orm import Database, Required, Json, PrimaryKey
from .data import load_tables as data_load
from .molecule import load_tables as molecule_load
from .reaction import load_tables as reaction_load
from .user import UserADHOC
from ..version import major_version


def post_binder(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        db = f(*args, **kwargs)

        def bind(create_tables=False, **bind_kwargs):
            db.bind('postgres', **bind_kwargs)
            db.generate_mapping(create_tables=create_tables)
            return db
        return bind
    return wrap


@post_binder
def load_tables(schema, workpath='.', fragmentor_version=None, fragment_type_mol=3, fragment_min_mol=2,
                fragment_max_mol=6, fragment_type_cgr=3, fragment_min_cgr=2, fragment_max_cgr=6, fragment_dynbond_cgr=1,
                fp_size=12, fp_active_bits=2, fp_count=4, user_entity=None, isotope=False, stereo=False,
                extralabels=False):
    """

    NOTE! never change default settings!
    """

    if user_entity is None:  # User Entity ADHOC.
        user_entity = UserADHOC

    db = Database()

    molecule_load(db, schema, user_entity, fragmentor_version, fragment_type_mol, fragment_min_mol,
                  fragment_max_mol, fp_size, fp_active_bits, fp_count, workpath, isotope, stereo, extralabels)
    reaction_load(db, schema, user_entity, fragmentor_version, fragment_type_cgr, fragment_min_cgr, fragment_max_cgr,
                  fragment_dynbond_cgr, fp_size, fp_active_bits, fp_count, workpath, isotope, stereo, extralabels)
    data_load(db, schema, user_entity)
    return db


@post_binder
def load_config():
    db = Database()

    class Config(db.Entity):
        _table_ = 'cgr_db_config'
        id = PrimaryKey(int, auto=True)
        name = Required(str)
        config = Required(Json)
        version = Required(str, default=major_version)

    return db
