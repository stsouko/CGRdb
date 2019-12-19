#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  Copyright 2017-2019 Ramil Nugmanov <stsouko@live.ru>
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
from os import listdir
from pathlib import Path
from setuptools import setup, find_packages


version = '4.0.0'


setup(
    name='CGRdb',
    version=version,
    packages=find_packages(),
    url='https://github.com/stsouko/CGRdb',
    license='AGPLv3',
    author='Dr. Ramil Nugmanov',
    author_email='stsouko@live.ru',
    python_requires='>=3.6.0',
    entry_points={'console_scripts': ['cgrdb=CGRdb.CLI:launcher']},
    install_requires=['CGRtools>=4.0.14,<4.1', 'CIMtools>=4.0.1,<4.1', 'LazyPony>=0.2.1,<0.3',
                      'CachedMethods>=0.1.4,<0.2', 'pony>=0.7.11,<0.8'],
    extras_require={'postgres':  ['psycopg2-binary'],
                    'postgres_cffi':  ['cffi', 'psycopg2cffi'],
                    'autocomplete': ['argcomplete']},
    package_data={'CGRdb.sql': [x for x in listdir(Path(__file__).parent / 'CGRdb' / 'sql') if x.endswith('.sql')]},
    long_description=(Path(__file__).parent / 'README.md').open().read(),
    classifiers=['Environment :: Plugins',
                 'Intended Audience :: Science/Research',
                 'Intended Audience :: Developers',
                 'Topic :: Scientific/Engineering :: Chemistry',
                 'Topic :: Software Development :: Libraries :: Python Modules',
                 'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 3',
                 'Programming Language :: Python :: 3.6',
                 ]
)
