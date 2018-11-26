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
from CGRtools.containers import ReactionContainer, MergedReaction
from CGRtools.preparer import CGRpreparer
from collections import OrderedDict, defaultdict
from datetime import datetime
from itertools import count, product
from pony.orm import PrimaryKey, Required, Optional, Set, Json, select, IntArray
from .user import mixin_factory as um
from ..management.reaction import mixin_factory as rmm
from ..search.fingerprints import reaction_mixin_factory as rfp
from ..search.graph_matcher import mixin_factory as gmm
from ..search.reaction import mixin_factory as rsm


def load_tables(db, schema, user_entity, fragmentor_version, fragment_type, fragment_min, fragment_max,
                fragment_dynbond, fp_size, fp_active_bits, fp_count, workpath='.',
                isotope=False, stereo=False, extralabels=False):

    FingerprintsReaction, FingerprintsIndex = rfp(fragmentor_version, fragment_type, fragment_min, fragment_max,
                                                  fragment_dynbond, fp_size, fp_active_bits, fp_count, workpath)

    class Reaction(db.Entity, FingerprintsReaction, gmm(isotope, stereo, extralabels), rsm(db), um(user_entity),
                   rmm(db)):
        _table_ = (schema, 'reaction')
        id = PrimaryKey(int, auto=True)
        date = Required(datetime, default=datetime.utcnow)
        user_id = Required(int, column='user')
        molecules = Set('MoleculeReaction')
        reaction_indexes = Set('ReactionIndex')
        metadata = Set('ReactionConditions')
        classes = Set('ReactionClass')
        special = Optional(Json)

        __cgr_core = CGRpreparer()

        def __init__(self, structure, user, metadata=None, special=None, fingerprints=None, cgr_signatures=None,
                     signatures=None, cgrs=None, reagents_signatures=None, products_signatures=None):
            """
            storing reaction in DB.
            :param structure: CGRtools ReactionContainer
            :param user: user entity
            :param metadata: list of Json serializable Data (expected list of dicts)
            :param special: Json serializable Data (expected dict)
            :param fingerprints: reaction fingerprints for all existing in db molecules structures combinations.
             for example: reaction A + B -> C. A has 2 structure in db, B - 3 and C - 1. number of combinations = 6.
             order of fingerprints have to be same as in product(A[], B[], C[]) where X[] is ordered by id list of
             structures of molecules.
            :param cgr_signatures: signatures strings of reaction CGR. see fingerprints for details
            :param signatures: unique signature strings of reaction. see fingerprints for details
            :param cgrs: list of all possible CGRs of reaction. see fingerprints for details
            :param reagents_signatures: signatures of structure reagents in same order as reagents molecules
            :param products_signatures: see reagents_signatures
            """
            batch, combos = self.__prepare_molecules_batch(structure, user, reagents_signatures=reagents_signatures,
                                                           products_signatures=products_signatures)

            reagents_len = len(structure.reagents)
            combos, signatures, cgr_signatures, fingerprints, structures, cgrs = \
                self._prepare_reaction_sf(combos, reagents_len, cgrs, signatures, cgr_signatures, fingerprints, True)

            db.Entity.__init__(self, user_id=user.id)

            for m, is_p, mapping in (batch[x] for x in sorted(batch)):
                MoleculeReaction(self, m, is_product=is_p, mapping=mapping)

            self._create_reaction_indexes(combos, fingerprints, cgr_signatures, signatures)

            for c, r, cc in zip(combos, structures, cgrs):
                if self.__cached_structure is None:
                    if all(x[1].last for x in c):
                        self.__cached_structure = r
                        self.__cached_cgr = cc
                else:
                    break

            if metadata:
                for c in metadata:
                    db.ReactionConditions(c, self, user)

            if special:
                self.special = special

        def _create_reaction_indexes(self, combos, fingerprints, cgr_signatures, signatures):
            for c, fp, cs, s in zip(combos, fingerprints, cgr_signatures, signatures):
                ReactionIndex(self, {x[1] for x in c}, fp, cs, s)

        @classmethod
        def _prepare_reaction_sf(cls, combinations, reagents_len, cgrs=None, signatures=None, cgr_signatures=None,
                                 fingerprints=None, get_structure_cgr=False):
            """
            prepare index data for structures with filtering of automorphic structures
            """
            structures = [ReactionContainer(reagents=[s[0] for s in x[:reagents_len]],
                                            products=[s[0] for s in x[reagents_len:]]) for x in combinations]
            combo_len = len(structures)

            if signatures is None or len(signatures) != combo_len:
                signatures, merged = [], []
                for x in structures:
                    s, ms = cls.get_signature(x, get_merged=True)
                    signatures.append(s)
                    merged.append(ms)
            else:
                merged = None

            if cgr_signatures is None or len(cgr_signatures) != combo_len:
                cgr_signatures, cgrs = [], []
                for x in merged or structures:
                    s, c = cls.get_cgr_signature(x, get_cgr=True)
                    cgr_signatures.append(s)
                    cgrs.append(c)
            elif cgrs is None or len(cgrs) != combo_len:
                cgrs = [cls.get_cgr(x) for x in merged or structures]

            if fingerprints is None or len(fingerprints) != combo_len:
                fingerprints = cls.get_fingerprints(cgrs, bit_array=False)

            clean_signatures, clean_cgr_signatures, clean_fingerprints, clean_combinations = [], [], [], []
            clean_cgrs, clean_structures = [], []
            for cc, s, cs, f, c, r in zip(combinations, signatures, cgr_signatures, fingerprints, cgrs, structures):
                if cs not in clean_cgr_signatures:
                    clean_signatures.append(s)
                    clean_cgr_signatures.append(cs)
                    clean_fingerprints.append(f)
                    clean_cgrs.append(c)
                    clean_structures.append(r)
                    clean_combinations.append(cc)

            if get_structure_cgr:
                return clean_combinations, clean_signatures, clean_cgr_signatures, clean_fingerprints, \
                       clean_structures, clean_cgrs
            return clean_combinations, clean_signatures, clean_cgr_signatures, clean_fingerprints

        @classmethod
        def __prepare_molecules_batch(cls, structure, user, reagents_signatures=None, products_signatures=None):
            new_mols, batch = OrderedDict(), {}
            signatures_set, structure_signatures = cls.__prepare_molecules_signatures(structure, reagents_signatures,
                                                                                      products_signatures)
            molecule_structures, signature_molecule_structure = cls.__preload_molecules(signatures_set)

            m_count, all_combos = count(), {}
            for i, is_p in (('reagents', False), ('products', True)):
                for x, s in zip(structure[i], structure_signatures[i]):
                    ms = signature_molecule_structure.get(s)
                    n = next(m_count)
                    if ms:
                        mapping = cls.match_structures(ms.structure, x)
                        batch[n] = (ms.molecule, is_p, mapping)

                        tmp = [(x, ms)]
                        if ms.molecule in molecule_structures:
                            tmp.extend((y.structure.remap(mapping, copy=True), y)
                                       for y in molecule_structures[ms.molecule])

                        all_combos[n] = sorted(tmp, key=lambda k: k[1].id)
                    else:
                        new_mols[n] = (x, is_p, s)

            if new_mols:
                s_list, x_list = [], []
                for x, _, s in new_mols.values():
                    if s not in s_list:
                        s_list.append(s)
                        x_list.append(x)

                signature_fingerprint = dict(zip(s_list, db.Molecule.get_fingerprints(x_list, bit_array=False)))
                dups = {}
                for n, (x, is_p, s) in new_mols.items():
                    if s not in dups:
                        m = db.Molecule(x, user, signature=s, fingerprint=signature_fingerprint[s])
                        dups[s] = m
                        mapping = None
                    else:
                        m = dups[s]
                        mapping = cls.match_structures(m.structure, x)

                    batch[n] = (m, is_p, mapping)
                    all_combos[n] = [(x, m.last_edition)]

            return batch, list(product(*(all_combos[x] for x in sorted(all_combos))))

        @staticmethod
        def __prepare_molecules_signatures(structure, reagents_signatures=None, products_signatures=None):
            if not (reagents_signatures and products_signatures and
                    len(reagents_signatures) == len(structure.reagents) and
                    len(products_signatures) == len(structure.products)):
                reagents_signatures = [db.Molecule.get_signature(x) for x in structure.reagents]
                products_signatures = [db.Molecule.get_signature(x) for x in structure.products]

            signatures_set = set(reagents_signatures + products_signatures)
            structure_signatures = dict(reagents=reagents_signatures, products=products_signatures)
            return signatures_set, structure_signatures

        @staticmethod
        def __preload_molecules(signatures_set):
            # preload molecules entities. pony caching it.
            list(select(x.molecule for x in db.MoleculeStructure if x.signature in signatures_set))
            # preload all molecules structures entities
            molecule_structures, signature_molecule_structure = defaultdict(list), {}
            for ms in select(x for x in db.MoleculeStructure if x.molecule in
                             select(y.molecule for y in db.MoleculeStructure if y.signature in signatures_set)):
                # NEED PR
                # select(y for x in db.MoleculeStructure if x.signature in signatures_set
                #        for y in db.MoleculeStructure if y.molecule == x.molecule)
                if ms.signature in signatures_set:
                    signature_molecule_structure[ms.signature] = ms
                else:
                    molecule_structures[ms.molecule].append(ms)
            return dict(molecule_structures), signature_molecule_structure

        @classmethod
        def get_cgr(cls, structure):
            return cls.__cgr_core.condense(structure)

        @classmethod
        def merge_mols(cls, structure):
            return cls.__cgr_core.merge_mols(structure)

        @classmethod
        def get_cgr_signature(cls, structure, get_cgr=False):
            cgr = cls.get_cgr(structure)
            signature = cgr.get_signature_hash(isotope=isotope, stereo=stereo, hybridization=extralabels,
                                               neighbors=extralabels)
            return (signature, cgr) if get_cgr else signature

        @classmethod
        def get_signature(cls, structure, get_merged=False):
            merged = structure if isinstance(structure, MergedReaction) else cls.merge_mols(structure)
            signature = merged.get_signature_hash(isotope=isotope, stereo=stereo, hybridization=extralabels,
                                                  neighbors=extralabels)
            return (signature, merged) if get_merged else signature

        @property
        def cgr(self):
            if self.__cached_cgr is None:
                self.__cached_cgr = self.get_cgr(self.structure)
            return self.__cached_cgr

        @property
        def cgrs_raw(self):
            if self.__cached_cgrs_raw is None:
                self.__cached_cgrs_raw = [self.get_cgr(x) for x in self.structures_raw]
            return self.__cached_cgrs_raw

        @property
        def structure(self):
            if self.__cached_structure is None:
                mrs = list(self.molecules.order_by(lambda x: x.id))
                mss = {x.molecule.id: x for x in
                       select(ms for ms in db.MoleculeStructure for mr in MoleculeReaction
                              if ms.molecule == mr.molecule and mr.reaction == self and ms.last)}

                r = ReactionContainer()
                for mr in mrs:
                    ms = mss[mr.molecule.id]
                    r['products' if mr.is_product else 'reagents'].append(
                        ms.structure.remap(mr.mapping, copy=True) if mr.mapping else ms.structure)
                self.__cached_structure = r
            return self.__cached_structure

        @property
        def structures_raw(self):
            assert self.__cached_structures_raw is not None, 'available in entities from queries results only'
            return self.__cached_structures_raw

        @structure.setter
        def structure(self, structure):
            self.__cached_structure = structure

        @structures_raw.setter
        def structures_raw(self, structures):
            self.__cached_structures_raw = structures

        def add_metadata(self, data, user):
            return db.ReactionConditions(data, self, user)

        __cached_structure = __cached_cgr = __cached_structures_raw = __cached_cgrs_raw = None

    class MoleculeReaction(db.Entity):
        """ molecule to reaction mapping data and role (reagent, reactant, product)
        """
        _table_ = (schema, 'molecule_reaction')
        id = PrimaryKey(int, auto=True)
        reaction = Required('Reaction')
        molecule = Required('Molecule')
        is_product = Required(bool, default=False)
        _mapping = Optional(Json, column='mapping')

        def __init__(self, reaction, molecule, is_product=False, mapping=None):
            mapping = mapping and self.compressed_mapping(mapping)
            db.Entity.__init__(self, reaction=reaction, molecule=molecule, is_product=is_product, _mapping=mapping)

        @property
        def mapping(self):
            if self.__cached_mapping is None:
                self.__cached_mapping = dict(self._mapping) if self._mapping else {}
            return self.__cached_mapping

        @mapping.setter
        def mapping(self, mapping):
            self._mapping = self.compressed_mapping(mapping)
            self.__cached_mapping = None

        @staticmethod
        def compressed_mapping(mapping):
            return [(k, v) for k, v in mapping.items() if k != v] or None

        __cached_mapping = None

    class ReactionIndex(db.Entity, FingerprintsIndex):
        _table_ = (schema, 'reaction_index')
        id = PrimaryKey(int, auto=True)
        reaction = Required('Reaction')
        structures = Set('MoleculeStructure', table=(schema, 'reaction_index_structure'))

        cgr_signature = Required(bytes, unique=True)
        signature = Required(bytes)
        bit_array = Required(IntArray, optimistic=False)

        def __init__(self, reaction, structures, fingerprint, cgr_signature, signature):
            bs = self.get_bits_list(fingerprint)

            db.Entity.__init__(self, reaction=reaction, cgr_signature=cgr_signature, signature=signature, bit_array=bs)
            for m in set(structures):
                self.structures.add(m)

    return Reaction


__all__ = [load_tables.__name__]
