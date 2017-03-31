# -*- coding: utf-8 -*-
#
#  Copyright 2016 Ramil Nugmanov <stsouko@live.ru>
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
from collections import defaultdict


class Parser(object):
    def __init__(self):

        self.__fields = dict(COND='conditions', COM='comment', YD='product_yield',
                             TIM='time', STP='steps', T='temperature', P='pressure', TXT='description',
                             citation='citation', RGT=None, CAT=None, SOL=None)

    def parse(self, meta):
        rxds = defaultdict(dict)
        cleaned = dict(rx_id=int(meta['ROOT:RX_ID']))

        for meta_key, meta_value in meta.items():
            meta_value = meta_value.replace("+\n", "")
            *presection, section = meta_key.split(':')
            if presection and section in self.__fields and presection[-1].startswith('RXD('):
                if section in ('CAT', 'SOL', 'RGT'):
                    rxds[presection[-1]].setdefault('media', []).extend(meta_value.split('|'))
                else:
                    rxds[presection[-1]][self.__fields[section]] = meta_value

        cleaned['rxd'] = []
        for x in rxds.values():
            x['media'] = sorted(x.get('media', []))
            cleaned['rxd'].append(x)

        return cleaned
