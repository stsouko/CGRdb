# -*- coding: utf-8 -*-
#
#  Copyright 2020, 2021 Ramil Nugmanov <nougmanoff@protonmail.com>
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
from importlib import import_module
from LazyPony import LazyEntityMeta
from pickle import dump
from pkg_resources import get_distribution, DistributionNotFound, VersionConflict
from pony.orm import db_session, Database


def index_core(args):
    from ..index import SimilarityIndex, SubstructureIndex

    major_version = '.'.join(get_distribution('CGRdb').version.split('.')[:-1])
    schema = args.name

    db_config = Database()
    LazyEntityMeta.attach(db_config, database='CGRdb_config')
    db_config.bind('postgres', **args.connection)
    db_config.generate_mapping()

    with db_session:
        config = db_config.Config.get(name=schema, version=major_version)
    if not config:
        raise KeyError('schema not exists or version incompatible')
    config = config.config

    for p in config['packages']:
        try:
            p = get_distribution(p)
            import_module(p.project_name)
        except (DistributionNotFound, VersionConflict):
            raise ImportError(f'packages not installed or has invalid versions: {p}')

    db = Database()
    LazyEntityMeta.attach(db, schema, 'CGRdb')
    db.bind('postgres', **args.connection)
    db.generate_mapping()

    if 'check_threshold' in args.params:
        sort_by_tanimoto = args.params['check_threshold'] is not None
    else:
        sort_by_tanimoto = True

    with db_session:
        substructure_molecule = SubstructureIndex(
                db.execute(f'SELECT id, fingerprint FROM "{schema}"."MoleculeStructure"'), False)
    with db_session:
        similarity_molecule = SimilarityIndex(
                db.execute(f'SELECT id, fingerprint FROM "{schema}"."MoleculeStructure"'), **args.params)
    if sort_by_tanimoto:  # pairing fingerprints for memory saving
        substructure_molecule._fingerprints = similarity_molecule._fingerprints
    with db_session:
        substructure_reaction = SubstructureIndex(db.execute(f'SELECT id, fingerprint FROM "{schema}"."ReactionIndex"'),
                                                  False)
    with db_session:
        similarity_reaction = SimilarityIndex(
                db.execute(f'SELECT id, fingerprint FROM "{schema}"."ReactionIndex"'), **args.params)
    if sort_by_tanimoto:  # pairing fingerprints for memory saving
        substructure_reaction._fingerprints = similarity_reaction._fingerprints

    dump((substructure_molecule, substructure_reaction, similarity_molecule, similarity_reaction), args.data)
