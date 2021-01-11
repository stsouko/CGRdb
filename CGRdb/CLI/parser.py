# -*- coding: utf-8 -*-
#
#  Copyright 2017 Boris Sattarov <brois475@gmail.com>
#  Copyright 2017-2021 Ramil Nugmanov <nougmanoff@protonmail.com>
#  This file is part of CGRdb.
#
#  CGRdb is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, see <https://www.gnu.org/licenses/>.
#
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType
from importlib.util import find_spec
from json import loads
from .main_clean import clean_core
from .main_create import create_core
from .main_daemon import daemon_core
from .main_index import index_core
from .main_init import init_core
from .main_update import update_core


def init_db(subparsers):
    parser = subparsers.add_parser('init', help='initialize postgres db for cartridge using',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--connection', '-c', default='{}', type=loads, help='db connection params. see pony db.bind')
    parser.set_defaults(func=init_core)


def create_db(subparsers):
    parser = subparsers.add_parser('create', help='create new db',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--connection', '-c', default='{}', type=loads, help='db connection params. see pony db.bind')
    parser.add_argument('--name', '-n', help='schema name', required=True)
    parser.add_argument('--config', '-f', default=None, type=FileType(), help='database config in JSON format')
    parser.set_defaults(func=create_core)


def create_index(subparsers):
    parser = subparsers.add_parser('index', help='create substructure and similarity index',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--connection', '-c', default='{}', type=loads, help='db connection params. see pony db.bind')
    parser.add_argument('--name', '-n', help='schema name', required=True)
    parser.add_argument('--params', '-p', default='{}', type=loads, help='indexation params')
    parser.add_argument('--data', '-d', type=FileType(mode='wb'), required=True, help='dump of index')
    parser.set_defaults(func=index_core)


def update_db(subparsers):
    parser = subparsers.add_parser('update', help='update sql functions in db',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--connection', '-c', default='{}', type=loads, help='db connection params. see pony db.bind')
    parser.add_argument('--name', '-n', help='schema name', required=True)
    parser.set_defaults(func=update_core)


def clean_cache(subparsers):
    parser = subparsers.add_parser('clean', help='clean cache table in db',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--connection', '-c', default='{}', type=loads, help='db connection params. see pony db.bind')
    parser.add_argument('--name', '-n', help='schema name', required=True)
    parser.set_defaults(func=clean_core)


def run_daemon(subparsers):
    parser = subparsers.add_parser('daemon', help='index daemon',
                                   formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('--params', '-p', default='{}', type=loads, help='aiohttp run_app params')
    parser.add_argument('--data', '-d', type=FileType(mode='rb'), required=True, help='dump of index')
    parser.set_defaults(func=daemon_core)


def argparser():
    parser = ArgumentParser(description="CGRdb", epilog="(c) Dr. Ramil Nugmanov", prog='cgrdb')
    subparsers = parser.add_subparsers(title='subcommands', description='available utilities')

    create_db(subparsers)
    init_db(subparsers)
    create_index(subparsers)
    update_db(subparsers)
    clean_cache(subparsers)
    run_daemon(subparsers)

    if find_spec('argcomplete'):
        from argcomplete import autocomplete
        autocomplete(parser)

    return parser
