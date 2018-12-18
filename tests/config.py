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
from CGRtools.files import SDFread, RDFread
from CGRdb.models.user import UserADHOC


user = UserADHOC(0)
only_remap, the_same, update_remap, only_update = RDFread('CGRdb/tests/data/test_reactions.rdf').read()[:4]
wrong_map_nbb, bromnitrobenzene, catechol, non_existent_nbb, exist_nbb, methyl_catechol,\
nitrobrombenzene, third_nbb = SDFread('CGRdb/tests/data/Molecules.sdf', remap=False).read()[:8]
