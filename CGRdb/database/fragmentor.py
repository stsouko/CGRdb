

def dfs(graph, start, path_deep):
    if path_deep < 2:
        raise ValueError
    stack = [(node, 1) for node in graph[start]]
    path = [start]
    while stack:
        point, deep = stack.pop()
        if len(path) > deep:
            path = path[:deep]
        path.append(point)
        yield path
        deep += 1
        if deep > path_deep:
            continue
        neib = [(node, deep) for node in set(graph[point]).difference(path)]
        stack.extend(neib)


def fragmentor(molecule, deep):
    atoms = molecule._atoms
    bonds = molecule._bonds

    array = []
    for atom in bonds:
        way = dfs(bonds, atom, deep)
        for path in way:
            chain = [int(atoms[path[0]])]
            for n, m in zip(path, path[1:]):
                chain.append(int(bonds[n][m]))
                chain.append(int(atoms[m]))
            array.append(chain)
    return array

