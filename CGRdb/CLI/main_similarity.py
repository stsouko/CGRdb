# -*- coding: utf-8 -*-
#
#  Copyright 2017 Boris Sattarov <brois475@gmail.com>
#  Copyright 2017 Ramil Nugmanov <stsouko@live.ru>
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
from CGRtools.files import RDFread, RDFwrite, SDFread, SDFwrite
from pony.orm import db_session
from .. import Loader


def similarity_search_core(**kwargs):
    Loader.load_schemas()
    Molecule, Reaction = Loader.get_database(kwargs['database'])

    if kwargs['reaction']:
        output = RDFwrite(kwargs['output'])
        queries = RDFread(kwargs['input'])
        source = Reaction
    else:
        output = SDFwrite(kwargs['output'])
        queries = SDFread(kwargs['input'])
        source = Molecule

    for n, r in enumerate(queries, start=1):
        with db_session:
            for f in source.find_similar(r, number=kwargs['number']):
                f.structure.meta['query_structure_number'] = n
                output.write(f.structure)
