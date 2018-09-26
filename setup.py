#!/usr/bin/env python3
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
from CGRdb.version import version
from pathlib import Path
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
    install_requires=['CGRtools>=2.8.28,<2.9', 'CIMtools>=1.4.6,<1.5', 'bitstring', 'pony'],
    extras_require={'postgres':  ['psycopg2'],
                    'postgres_cffi':  ['cffi', 'psycopg2cffi'],
                    'autocomplete': ['argcomplete']},
    long_description=(Path(__file__).parent / 'README.md').open().read(),
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
                 'Programming Language :: Python :: 3.7',
                 ],
    command_options={'build_sphinx': {'project': ('setup.py', 'CGRdb'),
                                      'version': ('setup.py', version()), 'source_dir': ('setup.py', 'doc'),
                                      'build_dir':  ('setup.py', 'build/doc'),
                                      'all_files': ('setup.py', True),
                                      'copyright': ('setup.py', 'Dr. Ramil Nugmanov <stsouko@live.ru>')},
                     'easy_install': {'allow_hosts': ('setup.py', 'github.com, pypi.python.org')}}
)
