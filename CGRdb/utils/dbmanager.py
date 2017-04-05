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


class MoleculeManager(object):
    def update_structure(self, structure, user):
        """
        update structure representation. atom mapping should be equal to self.
        :param structure: Molecule container
        :param user: user entity
        :return: True if updated. False if conflict found.
        """
        new_hash = {k: v['element'] for k, v in structure.nodes(data=True)}
        old_hash = {k: v['element'] for k, v in self.structure_raw.nodes(data=True)}
        if new_hash != old_hash:
            raise Exception('Structure or mapping not match')

        fear_string = self.get_fear(structure)
        exists = Molecule.get(fear=fear_string)
        if not exists:
            m = Molecule(structure, user, fear_string=fear_string)
            for mr in self.last_edition.reactions:
                ''' replace current last molecule edition in all reactions.
                '''
                mr.molecule = m
                mr.reaction.refresh_fear_fingerprint()

            self.last_edition.last = False
            m.parent = self.parent or self
            self.__last_edition = m
            return True

        ''' this code not optimal. but this procedure is rare if db correctly standardized before population.
        '''
        ex_parent = exists.parent or exists
        if ex_parent != (self.parent or self) and not any((x.target.parent or x.target) == ex_parent
                                                          for x in self.merge_target):
            ''' if exists structure not already in merge list
            '''
            mapping = self.match_structures(structure, exists.structure_raw)
            MoleculeMerge(target=exists, source=self,
                          mapping=[(k, v) for k, v in mapping.items() if k != v] or None)

        return False

    def merge_molecule(self, molecule):
        m = Molecule[molecule]
        mm = MoleculeMerge.get(target=m, source=self)
        if not mm:
            return False
        ''' replace self in reactions to last edition of mergable molecule.
        '''
        mmap = dict(mm.mapping or [])
        mapping = [(n, mmap.get(n, n)) for n in self.structure_raw.nodes()]
        for mr in self.last_edition.reactions:
            rmap = dict(mr.mapping or [])
            mr.mapping = [(k, v) for k, v in ((v, rmap.get(k, k)) for k, v in mapping) if k != v] or None
            mr.molecule = m.last_edition
            mr.reaction.refresh_fear_fingerprint()

        ''' remap self'''
        if self.parent:
            tmp = [self.parent] + list(self.parent.children)
        else:
            tmp = [self] + list(self.children)

        for x in tmp:
            x.data = node_link_data(relabel_nodes(x.structure_raw, mmap))

        ''' set self.parent to molecule chain
        '''
        if m.parent:
            tmp = [m.parent] + list(m.parent.children)
        else:
            tmp = [m] + list(m.children)

        for x in tmp:
            x.parent = self.parent or self

        self.last_edition.last = False
        self.__last_edition = m.last_edition
        mm.delete()
        return True
