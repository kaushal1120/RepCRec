
def get_up_sites(sites):
    up_sites = [x for x in sites if x.up]
    return up_sites


def get_down_sites(sites):
    down_sites = [x for x in sites if not x.up]
    return down_sites


def node_in_cycle(start_node, graph):

    # implemnts DFS
    # https://stackoverflow.com/questions/43430309/depth-first-search-dfs-code-in-python
    def path_to_start(graph, node, seen):

        seen.append(node)
        # print('seen', node, seen, graph[node])
        for n in graph[node]:
            if n == start_node:
                return True
            if n not in seen:
                return path_to_start(graph, n, seen)
        return False

    res = path_to_start(graph, start_node, [])
    # print('DONE', start_node, res, graph)
    # print(res)
    return res


# Utility to check if variable is replicated beyond this site
def is_replicated_variable(variable):
    if int(variable[1:]) % 2 == 0:
        return True
    return False
