#!/usr/bin/env python3.4
# -*- coding: utf-8 -*-
#
#  Copyright 2017 Ramil Nugmanov <stsouko@live.ru>
#  This file is part of predictor.
#
#  predictor 
#  is free software; you can redistribute it and/or modify
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
from pony.orm import commit, left_join, db_session, select, flush
from CGRtools.files.SDFrw import SDFread, SDFwrite
from CGRtools.files.RDFrw import RDFread, RDFwrite
from CGRtools.FEAR import FEAR
from CGRtools.CGRreactor import CGRreactor
from CGRtools.CGRcore import CGRcore
from io import StringIO
import subprocess as sp
from CGRtools.utils.cxcalc import stereo


cgr_core = CGRcore()


def bind_db(path):
    db.bind('sqlite', path)
    db.generate_mapping()


def to_rdf(structures):
    with StringIO() as f:
        for x in structures if isinstance(structures, list) else [structures]:
            RDFwrite(f).write(x)
        return f.getvalue()


def to_sdf(structures):
    with StringIO() as f:
        for x in structures if isinstance(structures, list) else [structures]:
            SDFwrite(f).write(x)
        return f.getvalue()


def view_sdf(structures):
    sp.call(['/home/stsouko/ChemAxon/JChem/bin/mview', to_sdf(structures).encode()])


def view_rdf(structures):
    sp.call(['/home/stsouko/ChemAxon/JChem/bin/mview', to_rdf(structures).encode()])


def get_reaction_fear(structure):
    return db.Reaction.get_fear(structure)


def get_mapless_fear(structure):
    return db.Reaction.get_mapless_fear(structure)
