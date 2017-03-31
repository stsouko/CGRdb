# -*- coding: utf-8 -*-
#
#  Copyright 2017 Boris Sattarov <brois475@gmail.com>
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
from CGRtools.files.RDFrw import RDFread, RDFwrite, ReactionContainer
from CGRtools.files.SDFrw import SDFread, SDFwrite, MoleculeContainer
from pony.orm import db_session
from networkx.readwrite import json_graph
from .. import init, load_databases


def similarity_search_reactions_core(**kwargs):
    init()
    Molecule, Reaction, Conditions = load_databases()[kwargs['']] # придумать аргумент команд лайн
    outputdata = RDFwrite(kwargs['output'])
    reactions = RDFread(kwargs['input'])
    num = kwargs['number']
    rebuild = kwargs['rebuild']
    with db_session():
        x = TreeIndex(Reactions, reindex=rebuild)
        for reaction_container in reactions:
            print(reaction_container)
            a,b = TreeIndex.get_similar(x, reaction_container, num)
            print(a)
            print(b)
            for i in b:
                react_cont = i.structure
                react_cont.__class__ = ReactionContainer
                outputdata.write(react_cont)

def similarity_search_molecules_core(**kwargs):
    init()
    Molecule, Reaction, Conditions = load_databases()[kwargs['']]  # придумать аргумент команд лайн
    molecules = SDFread(kwargs['input'])
    outputdata = SDFwrite(kwargs['output'])
    num = kwargs['number']
    rebuild = kwargs['rebuild']
    with db_session():
        x = TreeIndex(Molecules, reindex=rebuild)
        for molecule_container in molecules:
            a,b = TreeIndex.get_similar(x, molecule_container,num)
            print(a)
            print(b)
            for i in b:
                mol_cont = json_graph.node_link_graph(i.data)
                mol_cont.__class__ = MoleculeContainer
                outputdata.write(mol_cont)

