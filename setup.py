#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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
from CGRdb.version import version
from os.path import join, dirname
from setuptools import setup, find_packages

setup(
    name='CGRdb',
    version=version(),
    packages=find_packages(),
    url='https://github.com/stsouko/predictor',
    license='AGPLv3',
    author='Dr. Ramil Nugmanov',
    author_email='stsouko@live.ru',
    description='CGRdb',
    entry_points={'console_scripts': ['cgrdb=CGRdb.CLI:launcher']},
    install_requires=['networkx>=2.0.dev', 'CGRtools>=2.6', 'CIMtools>=1.2', 'bitstring', 'pony'],
    extras_require={'postgres':  ['cffi', 'psycopg2cffi'],
                    'autocomplete': ['argcomplete']},
    dependency_links=['git+https://github.com/networkx/networkx.git@master#egg=networkx-2.0.dev',
                      'git+https://github.com/stsouko/CGRtools.git@2.6#egg=CGRtools-2.6',
                      'git+https://github.com/stsouko/MODtools.git@1.2#egg=CIMtools-1.2'],
    long_description=open(join(dirname(__file__), 'README.md')).read(),
    keywords='CGRdb database search similarity chemistry',
    classifiers=['Environment :: Web Environment',
                 'Intended Audience :: Science/Research',
                 'Intended Audience :: Developers',
                 'Topic :: Scientific/Engineering :: Chemistry',
                 'Topic :: Software Development :: Libraries :: Python Modules',
                 'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python',
                 'Programming Language :: Python :: 3',
                 'Programming Language :: Python :: 3.4',
                 'Programming Language :: Python :: 3.5',
                 ]
)
