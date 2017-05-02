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
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType
from importlib.util import find_spec
from ..config import DB_DATA_LIST
from ..version import version
from .main_populate import populate_core
from .main_similarity import similarity_search_core
from .main_structure import structure_search_core
from .main_substructure import substructure_search_core


def search_common(parser):
    parser.add_argument("--input", "-i", type=FileType(), help="SDF/RDF input file")
    parser.add_argument("--output", "-o", type=FileType('w'), help="SDF/RDF file with found objects")
    parser.add_argument("--reaction", '-rs', action='store_true', help='Reactions search. by default Molecules')
    parser.add_argument("--database", '-db', type=str, default=DB_DATA_LIST[0], choices=DB_DATA_LIST,
                        help='Database name for search')


def similar_common(parser):
    parser.add_argument("--number", "-n", type=int, default=10, help='Number of objects, that you want to get')


def structure_search(subparsers):
    parser = subparsers.add_parser('structure', help='Molecules/Reactions structure search.',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    search_common(parser)

    parser.add_argument("--enclosure", '-en', action='store_true',
                        help='Use this if you want to use advanced non-hash reaction search ')

    parser.set_defaults(func=structure_search_core)


def substructure_search(subparsers):
    parser = subparsers.add_parser('substructure',
                                   help='Molecules/Reactions substructure search. This one searches objects with '
                                        'certain fragment in their structure',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    search_common(parser)
    similar_common(parser)

    parser.set_defaults(func=substructure_search_core)


def similarity_search(subparsers):
    parser = subparsers.add_parser('similar',
                                   help='Molecules/Reactions similarity search. This one searches similar objects, '
                                        'using fingerprints and Tanimoto index',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    search_common(parser)
    similar_common(parser)

    parser.set_defaults(func=similarity_search_core)


def populate(subparsers):
    parser = subparsers.add_parser('populate', help='This utility fills database with new entities',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--input', '-i', default='input.rdf', type=FileType(), help='RDF inputfile')
    parser.add_argument('--parser', '-p', default='reaxys', choices=['reaxys'], type=str, help='Data Format')
    parser.add_argument('--chunk', '-c', default=100, type=int, help='Chunks size')
    parser.add_argument('--user', '-u', default=1, type=int, help='User id')
    parser.add_argument("--database", '-db', type=str, default=DB_DATA_LIST[0], choices=DB_DATA_LIST,
                        help='Database name for populate')
    parser.set_defaults(func=populate_core)


def argparser():
    parser = ArgumentParser(description="CGRdb", epilog="(c) Dr. Ramil Nugmanov", prog='cgrdb')
    parser.add_argument("--version", "-v", action="version", version=version(), default=False)
    subparsers = parser.add_subparsers(title='subcommands', description='available utilities')

    structure_search(subparsers)
    substructure_search(subparsers)
    similarity_search(subparsers)

    populate(subparsers)

    if find_spec('argcomplete'):
        from argcomplete import autocomplete
        autocomplete(parser)

    return parser
