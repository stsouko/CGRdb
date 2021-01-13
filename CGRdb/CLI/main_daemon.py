# -*- coding: utf-8 -*-
#
#  Copyright 2021 Ramil Nugmanov <nougmanoff@protonmail.com>
#  This file is part of CGRdb.
#
#  CGRdb is free software; you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with this program; if not, see <https://www.gnu.org/licenses/>.
#
from pickle import load


def daemon_core(args):
    from aiohttp.web import json_response, post, run_app, Application

    substructure_molecule, substructure_reaction, similarity_molecule, similarity_reaction = load(args.data)

    async def search(request):
        _type = request.match_info.get('type')
        target = request.match_info.get('target')
        nonlocal substructure_molecule
        nonlocal substructure_reaction
        nonlocal similarity_molecule
        nonlocal similarity_reaction

        index = locals()[f'{_type}_{target}']

        fingerprint = await request.json()
        found = index.search(fingerprint)
        return json_response(found)

    app = Application()
    app.add_routes([post('/{type}/{target}', search)])
    run_app(app, **args.params)
