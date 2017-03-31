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
from CGRtools.CGRcore import CGRcore
from ..utils.reaxys_data import Parser as ReaxysParser
from .. import Loader


cgr_core = CGRcore()
parsers = dict(reaxys=ReaxysParser)


def populate_core(**kwargs):
    Loader.load_schemas()
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

            next(raw_data)
            try:
                rs, cgr = Reaction.get_fear(r, get_cgr=True)
                rms = dict(substrats=[], products=[])
                merged = cgr_core.merge_mols(r)
                ml_fear = '%s>>%s' % (Molecule.get_fear(merged['substrats']), Molecule.get_fear(merged['products']))
                for i in ('substrats', 'products'):
                    for m in r[i]:
                        ms = Molecule.get_fear(m)
                        rms[i].append(ms)
                        molecules.append((m, ms))

                lrms.append(rms)
                cleaned.append((r, rs, ml_fear, cgr))
                next(clean_data)
            except:
                pass

        rfps = Reaction.get_fingerprints([x for *_, x in cleaned], is_cgr=True)
        mfps = Molecule.get_fingerprints([m for m, _ in molecules])

        with db_session:
            user = User[1]
            for_analyse = []

            for (m, ms), mf in zip(molecules, mfps):
                if not Molecule.exists(fear=ms):
                    Molecule(m, user, fingerprint=mf, fear_string=ms)

            for (r, rs, ml_fear, cgr), r_fp, rms in zip(cleaned, rfps, lrms):
                reaction = Reaction.get(fear=rs)
                meta = data_parser.parse(r['meta'])
                media = set()
                if not reaction:
                    next(added_data)
                    reaction = Reaction(r, user, special=dict(rx_id=meta['rx_id']),
                                        fingerprint=r_fp, fear_string=rs, cgr=cgr, mapless_fear_string=ml_fear,
                                        substrats_fears=rms['substrats'], products_fears=rms['products'])
                    for c in meta['rxd']:
                        Conditions(user=user, data=c, reaction=reaction)
                        media.update(c['media'])
                    for_analyse.append(reaction)
                else:
                    next(upd_data)
                    for c in meta['rxd']:
                        if not Conditions.exists(data=c, reaction=reaction):
                            Conditions(user=user, data=c, reaction=reaction)
                            media.update(c['media'])

                for m in media:
                    if not RawMedia.exists(name=m):
                        RawMedia(name=m)

    print('Data processed\nRaw: %d, Clean: %d, Added: %d, Updated: %d' % next(zip(raw_data, clean_data,
                                                                                  added_data, upd_data)))
