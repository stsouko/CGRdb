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
from CGRtools.reactor import CGRreactor


def mixin_factory(isotope=False, stereo=False):
    class GraphMatcher:
        __cgr_reactor = CGRreactor(isotope=isotope, stereo=stereo)

        @classmethod
        def get_matcher(cls, g, h):
            return cls.__cgr_reactor.get_cgr_matcher(g, h)

        @classmethod
        def match_structures(cls, g, h):
            return next(cls.get_matcher(g, h).isomorphisms_iter())

    return GraphMatcher


__all__ = [mixin_factory.__name__]
