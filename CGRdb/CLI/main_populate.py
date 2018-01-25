# -*- coding: utf-8 -*-
#
#  Copyright 2016-2018 Ramil Nugmanov <stsouko@live.ru>
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
from CGRtools.files.RDFrw import RDFread
from itertools import zip_longest, count
from multiprocess import Queue, Process
from pony.orm import db_session
from sys import stderr
from time import sleep
from .. import Loader
from ..models import UserADHOC
from ..utils.reaxys_data import Parser as ReaxysParser


class NoneParser:
    @staticmethod
    def parse(_):
        return dict(rxd=None)


parsers = dict(reaxys=ReaxysParser, none=NoneParser)


def calc(chunk, database, parser):
    Molecule, Reaction = Loader.get_database(database)
    data_parser = parsers[parser]()

    raw_data = count()
    clean_data = count()
    cleaned, molecules, lrms = [], [], []

    for r in chunk:
        if r is None:
            break

        rnum = next(raw_data)
        try:
            ml_fear_str, mr = Reaction.get_signature(r, get_merged=True)
            fear_str, cgr = Reaction.get_cgr_signature(mr, get_cgr=True)
        except Exception as e:
            print(e, file=stderr)
        else:
            rms = dict(reagents=[], products=[])
            for i in ('reagents', 'products'):
                for m in r[i]:
                    ms = Molecule.get_signature(m)
                    rms[i].append(ms)
                    molecules.append((m, ms, rnum))

            lrms.append(rms)
            cleaned.append((r, fear_str, ml_fear_str, rnum, cgr))
            next(clean_data)

    if cleaned:
        rfps = Reaction.get_fingerprints([x for *_, x in cleaned], bit_array=False)
        mfps = Molecule.get_fingerprints([x for x, *_ in molecules], bit_array=False)

        mol_data = [(m, ms, mf, rnum) for (m, ms, rnum), mf in zip(molecules, mfps)]
        rxn_data = [(r, rs, ml_fear, rnum, cgr, r_fp, rms, data_parser.parse(r['meta'])) for
                    (r, rs, ml_fear, rnum, cgr), r_fp, rms in zip(cleaned, rfps, lrms)]

        return next(raw_data), next(clean_data), mol_data, rxn_data


def populate(res, database, user):
    added_data = count()
    upd_data = count()
    Molecule, Reaction = Loader.get_database(database)

    raw_num, clean_num, mol_data, rxn_data = res

    with db_session:
        user = UserADHOC[user]
        fuck_opt = []
        for m, ms, mf, rnum in mol_data:
            mol_db = Molecule.find_structure(ms)
            if not mol_db:
                Molecule(m, user, fingerprint=mf, signature=ms)
            elif mol_db._structures.count() > 1:
                fuck_opt.append(rnum)

        for r, rs, ml_fear, rnum, cgr, r_fp, rms, meta in rxn_data:
            reaction = Reaction.find_structure(rs)

            if not reaction:
                next(added_data)
                if rnum in fuck_opt:  # if molecules has multiple forms don't use precomputed fields. мне влом.
                    Reaction(r, user, conditions=meta.pop('rxd'), special=meta,
                             reagents_signatures=rms['reagents'], products_signatures=rms['products'])
                else:
                    Reaction(r, user, conditions=meta.pop('rxd'), special=meta,
                             fingerprints=[r_fp], cgr_signatures=[rs], cgrs=[cgr], signatures=[ml_fear],
                             reagents_signatures=rms['reagents'], products_signatures=rms['products'])

            else:
                next(upd_data)
                for c in meta['rxd'] or []:
                    reaction.add_conditions(c, user)
                    # Tell child processes to stop

    return next(added_data), next(upd_data), raw_num, clean_num


def worker(input_queue, output_queue):
    for args in iter(input_queue.get, 'STOP'):
        result = calc(*args[1:])
        output_queue.put((args[0], result))


def populate_core(**kwargs):
    Loader.load_schemas()
    raw_data, clean_data, added_data, upd_data = 0, 0, 0, 0

    task_queue, done_queue = Queue(), Queue()
    processes = [Process(target=worker, args=(task_queue, done_queue)) for _ in range(kwargs['n_jobs'])]
    for p in processes:
        p.start()

    print('workers started\nunordered preprocessed chunks:', file=stderr)
    inputdata = RDFread(kwargs['input'])
    for nums, chunk in enumerate(zip_longest(*[inputdata] * kwargs['chunk']), start=1):
        task_queue.put([nums, chunk, kwargs['database'], kwargs['parser']])
        if task_queue.qsize() < 2 * kwargs['n_jobs'] and done_queue.qsize() == 0:
            continue

        res = done_queue.get()
        print("chunk: %d" % res[0], file=stderr)
        if res[1]:
            added_num, upd_num, raw_num, clean_num = populate(res[1], kwargs['database'], kwargs['user'])
            raw_data += raw_num
            clean_data += clean_num
            added_data += added_num
            upd_data += upd_num
    else:
        for i in range(kwargs['n_jobs']):
            task_queue.put('STOP')
        print('STOP sent', file=stderr)

        while task_queue.qsize():
            sleep(2)
        print('Process done', file=stderr)

        while done_queue.qsize():
            res = done_queue.get()
            print("chunk: %d" % res[0], file=stderr)
            if res[1]:
                added_num, upd_num, raw_num, clean_num = populate(res[1], kwargs['database'], kwargs['user'])
                raw_data += raw_num
                clean_data += clean_num
                added_data += added_num
                upd_data += upd_num

    print('Data processed\nRaw: %d, Clean: %d, Added: %d, Updated: %d' % (raw_data, clean_data, added_data, upd_data),
          file=stderr)
