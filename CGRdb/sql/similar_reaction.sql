/*
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
*/

CREATE OR REPLACE FUNCTION
"{schema}".cgrdb_search_similar_reactions(data bytea, OUT id integer, OUT count integer)
AS $$
from CGRtools.containers import ReactionContainer
from compress_pickle import loads

reaction = loads(data, compression='gzip')
if not isinstance(reaction, ReactionContainer):
    raise plpy.spiexceptions.DataException('ReactionContainer required')

cgr = ~reaction
sg = bytes(cgr).hex()

get_cache = f'''SELECT x.id, array_length(x.reactions, 1) count
FROM "{schema}"."ReactionSearchCache" x
WHERE x.operator = 'similar' AND x.signature = '\\x{sg}'::bytea'''

# test for existing cache
found = plpy.execute(get_cache)
if found:
    return found[0]

# cache not found. lets start searching
fp = GD['cgrdb_rfp'].transform_bitset([cgr])[0]

plpy.execute(f'''CREATE TEMPORARY TABLE cgrdb_query ON COMMIT DROP AS
SELECT x.reaction r, smlar(x.fingerprint, ARRAY{fp}::integer[]) t
FROM "{schema}"."ReactionIndex" x
WHERE x.fingerprint % ARRAY{fp}::integer[]''')

# check for empty results
if not plpy.execute('SELECT COUNT(*) FROM cgrdb_query')[0]['count']:
    # store empty cache
    found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
VALUES ('\\x{sg}'::bytea, 'similar', CURRENT_TIMESTAMP, ARRAY[]::integer[], ARRAY[]::real[])
ON CONFLICT DO NOTHING
RETURNING id, 0 count''')

    # concurrent process stored same query. just reuse it
    if not found:
        found = plpy.execute(get_cache)
    return found[0]

# store found molecules to cache
found = plpy.execute(f'''INSERT INTO
"{schema}"."ReactionSearchCache"(signature, operator, date, reactions, tanimotos)
SELECT '\\x{sg}'::bytea, 'similar', CURRENT_TIMESTAMP, array_agg(o.r), array_agg(o.t)
FROM (
    SELECT h.r, h.t
    FROM (
        SELECT DISTINCT ON (f.r) r, f.t
        FROM cgrdb_query f
        ORDER BY f.r, f.t DESC
    ) h
    ORDER BY h.t DESC
) o
ON CONFLICT DO NOTHING
RETURNING id, array_length(reactions, 1) count''')

# concurrent process stored same query. just reuse it
if not found:
    found = plpy.execute(get_cache)

return found[0]
$$ LANGUAGE plpython3u
