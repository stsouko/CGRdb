# -*- coding: utf-8 -*-
#
#  Copyright 2019 Ramil Nugmanov <stsouko@live.ru>
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
from io import TextIOWrapper
from pkg_resources import resource_stream


insert_molecule_trigger = '''CREATE TRIGGER cgrdb_insert_molecule_structure
    BEFORE INSERT ON "{schema}"."MoleculeStructure" FOR EACH ROW
    EXECUTE PROCEDURE "{schema}".cgrdb_insert_molecule_structure()'''

insert_reaction_trigger = '''CREATE TRIGGER cgrdb_insert_reaction
    INSTEAD OF INSERT ON "{schema}"."Reaction" FOR EACH ROW
    EXECUTE PROCEDURE "{schema}".cgrdb_insert_reaction()'''

setup_fingerprint = '''CREATE OR REPLACE FUNCTION "{schema}".cgrdb_setup_fingerprint(cfg json)
RETURNS VOID
AS $$
from CIMtools.preprocessing import FragmentorFingerprint
from json import loads

config = loads(cfg)
molecule = config.get('molecule', {})
reaction = config.get('reaction', {})
GD['cgrdb_mfp'] = FragmentorFingerprint(**molecule)
GD['cgrdb_rfp'] = FragmentorFingerprint(**reaction)

$$ LANGUAGE plpython3u'''.replace('$', '$$')

delete_molecule = '''CREATE OR REPLACE FUNCTION "{schema}".cgrdb_delete_molecule_structure()
RETURNS TRIGGER
AS $$
BEGIN
    DELETE FROM "{schema}"."ReactionIndex" ri WHERE OLD.id = ANY(ri.structures);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql'''.replace('$', '$$')

delete_molecule_trigger = '''CREATE TRIGGER cgrdb_delete_molecule_structure
    AFTER DELETE ON "{schema}"."MoleculeStructure" FOR EACH ROW
    EXECUTE PROCEDURE "{schema}".cgrdb_delete_molecule_structure()'''


def load_sql(file):
    return ''.join(x for x in TextIOWrapper(resource_stream('CGRdb.sql', file))
                   if not x.startswith(('#', '/*', '*/', '\n'))).replace('$', '$$')


insert_molecule = load_sql('insert_molecule.sql')
insert_reaction = load_sql('insert_reaction.sql')
search_substructure_molecule = load_sql('substructure_molecule.sql')
search_substructure_reaction = load_sql('substructure_reaction.sql')
search_similar_molecules = load_sql('similar_molecule.sql')
search_similar_reactions = load_sql('similar_reaction.sql')
