# -*- coding: utf-8 -*-
#
#  Copyright 2015-2017 Ramil Nugmanov <stsouko@live.ru>
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
from pathlib import Path
from os.path import expanduser  # python 3.4 ad-hoc
from sys import stderr
from traceback import format_exc


DEBUG = False

DB_USER = None
DB_PASS = None
DB_HOST = None
DB_NAME = None
DB_DATA = None

FP_SIZE = 12
FP_ACTIVE_BITS = 2
FRAGMENTOR_VERSION = None
FRAGMENT_TYPE_CGR = 3
FRAGMENT_MIN_CGR = 2
FRAGMENT_MAX_CGR = 6
FRAGMENT_DYNBOND_CGR = 1
FRAGMENT_TYPE_MOL = 3
FRAGMENT_MIN_MOL = 2
FRAGMENT_MAX_MOL = 6
DATA_ISOTOPE = False
DATA_STEREO = False
WORKPATH = '.'


config_list = ('DB_USER', 'DB_PASS', 'DB_HOST', 'DB_NAME', 'DB_MAIN', 'DB_PRED', 'DB_DATA',
               'FP_SIZE', 'FP_ACTIVE_BITS', 'FRAGMENTOR_VERSION', 'DATA_ISOTOPE', 'DATA_STEREO',
               'FRAGMENT_TYPE_CGR', 'FRAGMENT_MIN_CGR', 'FRAGMENT_MAX_CGR', 'FRAGMENT_DYNBOND_CGR',
               'FRAGMENT_TYPE_MOL', 'FRAGMENT_MIN_MOL', 'FRAGMENT_MAX_MOL', 'WORKPATH')

config_load_list = ['DEBUG']
config_load_list.extend(config_list)

config_dirs = [x / '.CGRdb.ini' for x in (Path(__file__).parent, Path(expanduser('~')), Path('/etc'))]

if not any(x.exists() for x in config_dirs):
    with config_dirs[1].open('w') as f:
        f.write('\n'.join('%s = %s' % (x, y or '') for x, y in globals().items() if x in config_list))

with next(x for x in config_dirs if x.exists()).open() as f:
    for n, line in enumerate(f, start=1):
        try:
            line = line.strip()
            if line and not line.startswith('#'):
                k, v = line.split('=')
                k = k.rstrip()
                v = v.lstrip()
                if k in config_load_list:
                    globals()[k] = int(v) if v.isdigit() else v == 'True' if v in ('True', 'False', '') else v
        except ValueError:
            print('line %d\n\n%s\n consist errors: %s' % (n, line, format_exc()), file=stderr)

DB_DATA_LIST = DB_DATA.split() if DB_DATA else []
