#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  Copyright 2017-2019 Ramil Nugmanov <stsouko@live.ru>
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
from setuptools import setup, find_packages


version = '3.1.1'


setup(
    name='CGRdb',
    version=version,
    packages=find_packages(),
    url='https://github.com/stsouko/CGRdb',
    license='AGPLv3',
    author='Dr. Ramil Nugmanov',
    author_email='stsouko@live.ru',
    python_requires='>=3.7.0',
    entry_points={'console_scripts': ['cgrdb=CGRdb.CLI:launcher']},
    install_requires=['CGRtools>=3.1.2,<3.2', 'CIMtools>=3.1.0,<3.2', 'LazyPony>=0.2,<0.3',
                      'CachedMethods>=0.1.0,<0.2', 'pony'],
    extras_require={'postgres':  ['psycopg2-binary'],
                    'postgres_cffi':  ['cffi', 'psycopg2cffi'],
                    'autocomplete': ['argcomplete']},
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
                 'Programming Language :: Python :: 3.7',
                 ]
)
