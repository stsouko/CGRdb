# -*- coding: utf-8 -*-
#
#  Copyright 2016, 2017 Ramil Nugmanov <stsouko@live.ru>
#  Copyright 2016 Svetlana Musaeva <sveta_musaeva.95@mail.ru>
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
import sys
from itertools import zip_longest, count
from pony.orm import db_session
from CGRtools.files.RDFrw import RDFread
from ..utils.reaxys_data import Parser as ReaxysParser
from ..models import UserADHOC
from .. import Loader


class NoneParser:
    @staticmethod
    def parse(_):
        return dict(rxd=None)


parsers = dict(reaxys=ReaxysParser, none=NoneParser)


def populate_core(**kwargs):
    Loader.load_schemas()
    Molecule, Reaction = Loader.get_database(kwargs['database'])

    inputdata = RDFread(kwargs['input'])
    data_parser = parsers[kwargs['parser']]()

    raw_data = count()
    clean_data = count()
    added_data = count()
    upd_data = count()

    for nums, chunk in enumerate(zip_longest(*[inputdata] * kwargs['chunk']), start=1):
        print("chunk: %d" % nums, file=sys.stderr)

        cleaned = []
        molecules = []
        lrms = []
        for r in chunk:
            if r is None:
                break

            rnum = next(raw_data)
            try:
                ml_fear_str, mr = Reaction.get_mapless_fear(r, get_merged=True)
                fear_str, cgr = Reaction.get_fear(mr, is_merged=True, get_cgr=True)
                rms = dict(substrats=[], products=[])

                for i in ('substrats', 'products'):
                    for m in r[i]:
                        ms = Molecule.get_fear(m)
                        rms[i].append(ms)
                        molecules.append((m, ms, rnum))

                lrms.append(rms)
                cleaned.append((r, fear_str, ml_fear_str, rnum, cgr))
                next(clean_data)
            except:
                pass

        rfps = Reaction.get_fingerprints([x for *_, x in cleaned], is_cgr=True, bit_array=False)
        mfps = Molecule.get_fingerprints([m for m, *_ in molecules], bit_array=False)

        with db_session:
            user = UserADHOC[kwargs['user']]
            fuck_opt = []
            for (m, ms, rnum), mf in zip(molecules, mfps):
                mol_db = Molecule.find_structure(ms, is_fear=True)
                if not mol_db:
                    Molecule(m, user, fingerprint=mf, fear=ms)
                elif mol_db.structures.count() > 1:
                    fuck_opt.append(rnum)

            for (r, rs, ml_fear, rnum, cgr), r_fp, rms in zip(cleaned, rfps, lrms):
                reaction = Reaction.find_structure(rs, is_fear=True)
                meta = data_parser.parse(r['meta'])

                if not reaction:
                    next(added_data)
                    if rnum in fuck_opt:  # if molecules has multiple forms don't use precomputed fields. мне влом.
                        Reaction(r, user, conditions=meta.pop('rxd'), special=meta,
                                 substrats_fears=rms['substrats'], products_fears=rms['products'])
                    else:
                        Reaction(r, user, conditions=meta.pop('rxd'), special=meta,
                                 fingerprints=[r_fp], fears=[rs], cgrs=[cgr], mapless_fears=[ml_fear],
                                 substrats_fears=rms['substrats'], products_fears=rms['products'])

                else:
                    next(upd_data)
                    for c in meta['rxd'] or []:
                        reaction.add_conditions(c, user)

    print('Data processed\nRaw: %d, Clean: %d, Added: %d, Updated: %d' % next(zip(raw_data, clean_data,
                                                                                  added_data, upd_data)))
