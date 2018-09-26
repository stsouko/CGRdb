# -*- coding: utf-8 -*-
#
#  Copyright 2018 Dinar Batyrshin <batyrshin-dinar@mail.ru>
#  Copyright 2018 Ramil Nugmanov <stsouko@live.ru>
#  This file is part of Reaxys API wrapper.
#
#  Reaxys API wrapper is free software; you can redistribute it and/or modify
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
from abc import ABC, abstractmethod
from CGRtools.containers import MoleculeContainer, ReactionContainer
from CGRtools.files import SDFwrite, RDFwrite, SDFread, RDFread
from CGRtools.files.MRVrw import xml_dict
from io import StringIO
from lxml.etree import XML
from math import ceil
from typing import List
from requests import Session
from requests_futures.sessions import FuturesSession
from warnings import warn


class ReaxysAPI:
    _url = 'https://www.reaxys.com/reaxys/api'

    def __init__(self, caller, username, password):
        self.__caller = caller
        self.__session, self.__sessionid = self.__connect(username, password, caller)

    def __connect(self, username, password, caller):
        payload = '<?xml version="1.0"?>\n' \
                  '<xf>\n' \
                  '  <request caller="{}">\n' \
                  '    <statement command="connect" username="{}" password="{}"/>\n' \
                  '  </request>\n' \
                  '</xf>'.format(caller, username, password)

        session = Session()
        root = XML(session.post(self._url, payload).content)
        if next(root.getiterator('status')).text == 'OK':
            return session, next(root.getiterator('sessionid')).text

        raise Exception(next(root.getiterator('message')).text.split(';', 1)[0])

    def search(self, query, context=None, dbname='RX', no_coresult=False, worker=False, **kwargs):
        options = []
        if no_coresult:
            options.append('NO_CORESULT')
        if worker:
            options.append('WORKER')

        if isinstance(query, MoleculeContainer):
            query = self.__mol2query(query, kwargs)
            if context == 'R':
                options.extend(self.__options(kwargs))
            elif context is None:
                context = 'S'
            else:
                assert context != 'S', 'Invalid context value'
        elif isinstance(query, ReactionContainer):
            context = 'R'
            query = self.__rxn2query(query, kwargs)
        elif not isinstance(query, str):
            raise Exception('Invalid query')
        else:
            assert context in ('S', 'R'), 'Invalid context value'

        payload = '<?xml version="1.0" encoding="UTF-8"?>\n' \
                  '<xf>\n' \
                  '  <request caller="{}" sessionid="{}">\n' \
                  '    <statement command="select"/>\n' \
                  '    <select_list><select_item/></select_list>\n' \
                  '    <from_clause dbname="{}" context="{}"/>\n' \
                  '    <where_clause>{}</where_clause>\n' \
                  '    <order_by_clause></order_by_clause>\n' \
                  '    <options>{}</options>\n' \
                  '  </request>\n' \
                  '</xf>'.format(self.__caller, self.__sessionid, dbname, context, query, ','.join(options))
        root = XML(self.__session.post(self._url, payload).text.encode())
        resultname = next(root.getiterator('resultname')).text
        resultsize = int(next(root.getiterator('resultsize')).text)
        if resultsize:
            return (Structure if context == 'S' else Reaction)(self.__session, self.__sessionid, self.__caller,
                                                               resultname, resultsize, self._url)

    def __options(self, kw):  # check options
        xx = []
        for k, v in kw.items():
            if k in self._sel_mol:
                if xx:
                    raise Exception('Only one of [{}] acceptable'.format(', '.join(self._sel_mol)))
                xx.append(k)
        return xx

    @staticmethod
    def __add_query(dictionary, kw):
        xx = []
        for k, v in kw.items():
            if k in dictionary:
                if dictionary[k] is None:
                    if v:
                        xx.append(k)
                else:
                    a, b = dictionary[k]
                    if a <= v <= b:
                        xx.append('{}={}'.format(k, v))
                    else:
                        raise Exception('Invalid value for {}'.format(k))
        return xx

    def __mol2query(self, mol, kw):
        xx = self.__add_query(self._mol_acc, kw)
        with StringIO() as b, SDFwrite(b) as w:
            w.write(mol)
            txt = b.getvalue()
        return "structure('{}', '{}')".format(txt, ','.join(xx))

    def __rxn2query(self, rxn, kw):
        xx = self.__add_query(self._rxn_acc, kw)
        with StringIO() as b, RDFwrite(b) as w:
            w.write(rxn)
            txt = b.getvalue()
        return "structure('{}', '{}')".format(txt, ','.join(xx))

    def __del__(self):
        payload = '<?xml version="1.0"?>\n' \
                  '<xf>\n' \
                  '  <request caller="{}">\n' \
                  '    <statement command="disconnect" sessionid="{}"/>\n' \
                  '  </request>\n' \
                  '</xf>'.format(self.__caller, self.__sessionid)
        self.__session.post(self._url, payload)

    _sel_mol = ('starting_material', 'product', 'reagent', 'catalyst', 'solvent', 'reagent_or_catalyst')

    _mol_acc = {'exact': None, 'substructure': None, 'sub_hetereo': None,
                'isotopes': None, 'tautomers': None, 'similarity': (1, 99), 'stereo_absolute': None,
                'stereo_relative': None, 'separate_fragments': None, 'salts': None,
                'no_extra_rings': None, 'charges': None, 'radicals': None, 'mixtures': None, 'markush': None,
                'atoms': (1, 999), 'fragments': (1, 999), 'rings': (1, 999), 'align': None}

    _rxn_acc = {'ignore_mappings': None, 'all_reactions': None, **_mol_acc}


class Results(ABC):
    def __init__(self, session, sessionid, caller, resultname, resultsize, url):
        self.__session = session
        self.__sessionid = sessionid
        self.__caller = caller
        self.__result_name = resultname
        self.__result_size = resultsize
        self.__url = url
        self.__si = ''.join('<select_item>{}</select_item>\n'.format(xx) for xx in self._fetch_keys)

    def __str__(self):
        return '{}/{}/<{}>'.format(type(self), self.__result_size, self.__result_name)

    def __len__(self):
        return self.__result_size

    def __iter__(self):
        if self.__gen is None:
            self.__gen = self.__fetch(1, self.__result_size)
        return self.__gen

    def __next__(self):
        return next(iter(self))

    def seek(self, offset):
        if 0 <= offset < self.__result_size:
            self.__gen = self.__fetch(offset + 1, self.__result_size)
        else:
            raise IndexError('invalid offset')

    def __getitem__(self, item):
        if isinstance(item, slice):
            start, stop, step = item.indices(self.__result_size)
            if start >= stop:
                return []
            req = list(self.__fetch(start + 1, stop))
            if stop - start != len(req):
                warn('reaxys returned invalid number of records', ResourceWarning)
            if step > 1:
                return req[::step]
            return req

        elif isinstance(item, int):
            if item >= self.__result_size or item < -self.__result_size:
                raise IndexError('list index out of range')
            if item < 0:
                item += self.__result_size
            item += 1
            return next(self.__fetch(item, item))
        else:
            raise TypeError('indices must be integers or slices')

    def __fetch(self, first_item, last_item):
        payloads = self.__payload_generator(first_item, last_item)
        futures = []

        with FuturesSession(session=self.__session, max_workers=10) as session:
            for payload in payloads:
                futures.append(session.post(self.__url, payload, background_callback=self._parser))
                if len(futures) == 30:
                    break

            while futures:
                yield from futures.pop(0).result().data

                payload = next(payloads, None)
                if payload:
                    futures.append(session.post(self.__url, payload, background_callback=self._parser))

    def __payload_generator(self, first_item, last_item):
        if last_item == first_item:
            yield self.__format_payload(first_item, first_item)
        else:
            num = int(ceil((last_item - first_item) / 50))
            for i in range(num):
                first = i * 50 + first_item
                last = (i + 1) * 50 - 1 + first_item
                if last > last_item:
                    last = last_item
                yield self.__format_payload(first, last)

    def __format_payload(self, first, last):
        return '<?xml version="1.0" encoding="UTF-8"?>\n' \
              '<xf>\n' \
              '  <request caller="{}" sessionid="{}">\n' \
              '    <statement command="select"/>\n' \
              '    <select_list>{}</select_list>\n' \
              '    <from_clause resultname="{}" first_item="{}" last_item="{}"></from_clause>\n' \
              '    <order_by_clause></order_by_clause>\n' \
              '    <group_by_clause></group_by_clause>\n' \
              '    <options>{}</options>\n' \
              '  </request>\n' \
              '</xf>'.format(self.__caller, self.__sessionid, self.__si, self.__result_name, first, last,
                             self._options_keys)

    __gen = None

    @abstractmethod
    def _parser(self, session, response) -> List:
        pass

    @property
    @abstractmethod
    def _fetch_keys(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def _options_keys(self) -> str:
        pass


class Structure(Results):
    pass


class Reaction(Results):
    _fetch_keys = ('RXD(1,50)', 'RY', 'RX')
    _options_keys = 'ISSUE_RXN=true,ISSUE_RCT=false,OMIT_MAPS=false'

    @classmethod
    def _parser(cls, session, response):
        root = XML(response.content)
        res = xml_dict(next(root.getiterator('reactions')), ['sup', 'sub', 'i', 'b'])['reaction']
        if isinstance(res, list):
            response.data = data = []
            for x in res:
                try:
                    data.append(cls.__parser(x))
                except (KeyError, StopIteration) as e:
                    print(e)
        else:
            response.data = [cls.__parser(res)]

    @classmethod
    def __parser(cls, data):
        ry = cls.__ry_parser(data['RY']['RY.STR']['$'])
        rxd = data['RXD']
        rxd = [cls.__rxd_parser(x) for x in rxd] if isinstance(rxd, list) else [cls.__rxd_parser(rxd)]
        ry.meta['reaxys_data'] = rxd
        ry.meta['reaxys_id'] = data['RX']['RX.ID']['$']
        return ry

    @staticmethod
    def __ry_parser(data):
        with StringIO(data) as f, RDFread(f) as r:
            return next(r)

    @classmethod
    def __rxd_parser(cls, data):
        res = {}
        rxd = data['RXDS01']
        if 'RXD.SNR' in data and data['RXD.SNR']['$'] != '1':
            if not isinstance(rxd, list):
                raise Exception('invalid data')
            res['stages'] = [cls.__rxds_parser(x) for x in rxd]
        else:
            if data['RXD.STP']['$'] != '1':
                if 'RXD.MTEXT' not in data:
                    res['steps'] = []
                else:
                    mtxt = data['RXD.MTEXT']
                    res['steps'] = [x['$'] for x in mtxt] if isinstance(mtxt, list) else [mtxt['$']]
            res['stages'] = [cls.__rxds_parser(rxd)]

        if 'citations' in data:
            cit = data['citations']['citation']
            res['citations'] = [cls.__cit_parser(x) for x in cit] if isinstance(cit, list) else [cls.__cit_parser(cit)]
        return res

    @staticmethod
    def __cit_parser(data):
        res = {}
        cit = data['CIT']

        if cit['CIT.DT']['$'] == 'Article':
            if 'CIT.AU' in cit:
                res['author'] = cit['CIT.AU']['$']
            art = cit['CIT01']
            if 'CIT.JT' in art:
                res['journal'] = art['CIT.JT']['$']
            if 'CIT.VL' in art:
                res['volume'] = art['CIT.VL']['$']
            if 'CIT.PY' in art:
                res['year'] = art['CIT.PY']['$']
            if 'CIT.PAG' in art:
                res['pages'] = art['CIT.PAG']['$']
            if 'CIT.DOI' in art:
                res['doi'] = art['CIT.DOI']['$']
        elif cit['CIT.DT']['$'] == 'Patent':
            if 'CIT.PA' in cit:
                res['assignee'] = cit['CIT.PA']['$']
            pat = cit['CIT02']
            if 'CIT.PCC' in pat:
                res['country_code'] = pat['CIT.PCC']['$']
            if 'CIT.PN' in pat:
                res['patent_number'] = pat['CIT.PN']['$']
            if 'CIT.PPY' in pat:
                res['year'] = pat['CIT.PPY']['$']
        return res

    @staticmethod
    def __unhighlight(data):
        if 'hi' in data:
            return data['hi']['$']
        return data['$']

    @classmethod
    def __media_parser(cls, data, key):
        if isinstance(data, list):
            return [cls.__unhighlight(x[key]) for x in data if key in x]
        elif key in data:
            return [cls.__unhighlight(data[key])]

    @classmethod
    def __rxds_parser(cls, data):
        res = {}
        if 'RXD.TXT' in data:
            res['text'] = data['RXD.TXT']['$']
        if 'RXD.T' in data:
            res['temperature'] = cls.__unhighlight(data['RXD.T'])
        if 'RXD.P' in data:
            res['pressure'] = cls.__unhighlight(data['RXD.P'])
        if 'RXD.YD' in data:
            res['yield'] = cls.__unhighlight(data['RXD.YD'])
        if 'RXD.TIM' in data:
            res['time'] = cls.__unhighlight(data['RXD.TIM'])
        if 'RXD02' in data:
            val = cls.__media_parser(data['RXD02'], 'RXD.SRCT')
            if val:
                res['stage_reactants'] = val
        if 'RXD03' in data:
            val = cls.__media_parser(data['RXD03'], 'RXD.RGT')
            if val:
                res['reactants'] = val
        if 'RXD05' in data:
            val = cls.__media_parser(data['RXD05'], 'RXD.SOL')
            if val:
                res['solvents'] = val
        if 'RXD04' in data:
            val = cls.__media_parser(data['RXD04'], 'RXD.CAT')
            if val:
                res['catalysts'] = val

        if __debug__ and 'RXD01' in data:  # for debug
            print(data)
        return res
