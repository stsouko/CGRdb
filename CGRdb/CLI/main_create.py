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
from importlib import import_module
from json import load
from LazyPony import LazyEntityMeta
from pkg_resources import get_distribution
from pony.orm import db_session, Database
from ..sql import *


def create_core(args):
    major_version = '.'.join(get_distribution('CGRdb').version.split('.')[:-1])
    schema = args.name
    config = args.config and load(args.config) or {}
    if 'packages' not in config:  # by default CGRdbUser package used for User entity
        config['packages'] = []
    for p in config['packages']:  # check availability of extra packages
        p = get_distribution(p)
        import_module(p.project_name)

    db_config = Database()
    LazyEntityMeta.attach(db_config, database='CGRdb_config')
    db_config.bind('postgres', user=args.user, password=args.password, host=args.host, database=args.base,
                   port=args.port)
    db_config.generate_mapping()

    with db_session:
        if db_config.Config.exists(name=schema):
            raise KeyError('schema already exists')

    db = Database()
    LazyEntityMeta.attach(db, schema, 'CGRdb')
    db.bind('postgres', user=args.user, password=args.password, host=args.host, database=args.base, port=args.port)
    db.generate_mapping(create_tables=True)

    with db_session:
        db.execute('CREATE EXTENSION IF NOT EXISTS smlar')
        db.execute('CREATE EXTENSION IF NOT EXISTS intarray')
        db.execute('CREATE EXTENSION IF NOT EXISTS pg_cron')
        db.execute(f'ALTER TABLE "{schema}"."Reaction" DROP COLUMN structure')
        db.execute(f'ALTER TABLE "{schema}"."Reaction" RENAME TO "ReactionRecord"')
        db.execute(f'CREATE VIEW "{schema}"."Reaction" AS SELECT id, NULL::bytea as structure'
                   f' FROM "{schema}"."ReactionRecord"')

        db.execute(f'CREATE INDEX idx_moleculestructure__smlar ON "{schema}"."MoleculeStructure" USING '
                   'GIST (fingerprint _int4_sml_ops)')
        db.execute(f'CREATE INDEX idx_moleculestructure__subst ON "{schema}"."MoleculeStructure" USING '
                   'GIN (fingerprint gin__int_ops)')
        db.execute(f'CREATE INDEX idx_reactionindex__smlar ON "{schema}"."ReactionIndex" USING '
                   'GIST (fingerprint _int4_sml_ops)')
        db.execute(f'CREATE INDEX idx_reactionindex__subst ON "{schema}"."ReactionIndex" USING '
                   'GIN (fingerprint gin__int_ops)')

        db.execute(f"SELECT cron.schedule('0 3 * * *', $$$$"
                   f'DELETE FROM "{schema}"."MoleculeSearchCache" '
                   "WHERE date < CURRENT_TIMESTAMP - INTERVAL '1 day' $$$$)")
        db.execute(f"SELECT cron.schedule('0 3 * * *', $$$$"
                   f'DELETE FROM "{schema}"."ReactionSearchCache" '
                   "WHERE date < CURRENT_TIMESTAMP - INTERVAL '1 day' $$$$)")

        db.execute(setup_fingerprint.replace('{schema}', schema))
        db.execute(insert_molecule.replace('{schema}', schema))
        db.execute(insert_molecule_trigger.replace('{schema}', schema))
        db.execute(insert_reaction.replace('{schema}', schema))
        db.execute(insert_reaction_trigger.replace('{schema}', schema))
        db.execute(search_similar_molecules.replace('{schema}', schema))
        db.execute(search_substructure_molecule.replace('{schema}', schema))
        db.execute(search_similar_reactions.replace('{schema}', schema))
        db.execute(search_substructure_reaction.replace('{schema}', schema))

    with db_session:
        db_config.Config(name=schema, config=config, version=major_version)
