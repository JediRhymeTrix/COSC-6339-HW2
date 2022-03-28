import networkx as nx


class Validation:
    def __init__(self, edge_list, k):
        # edge_list = the edge list table E(i, j, v) where i, j are 2 different vertices and v is the distance
        # between i, j k = the maximal size clique threshold
        G = nx.Graph()
        G.add_weighted_edges_from(edge_list)
        self.G = G
        # self.cl = sql_cliques
        self.nodes = G.nodes
        self.edges = G.edges
        self.k = k

    def get_subcliques(self, clique):
        # algorithm for finding subcliques from a complete graph/clique
        # taken from: geeksforgeeks.org/combinations-in-python-without-using-itertools/
        n = len(clique)
        indices = list(range(self.k))
        yield tuple(clique[i] for i in indices)
        while True:
            for i in reversed(range(self.k)):
                if indices[i] != i + n - self.k:
                    break
            else:
                return
            indices[i] += 1
            for j in range(i + 1, self.k):
                indices[j] = indices[j - 1] + 1
            yield tuple(clique[i] for i in indices)

    def check_lists(self, list1, list2):
        # checks if lists containing cliques are equal
        len1 = len(list1)
        len2 = len(list2)
        if len1 != len2:
            return False
        return sorted([sorted(i) for i in list1]) == sorted([sorted(i) for i in list2])

    def test_find_cliques(self, sql_cliques):
        # compares the cliques found using networkx and sql
        # sql_cliques is the list of cliques of size k found using the sql implementation
        cliques = list(nx.find_cliques(self.G))
        clique_k = []
        for clique in cliques:
            if len(clique) == self.k:
                clique_k.append(clique)
            elif len(clique) > self.k:
                # a clique > k means that it is a complete graph containing C(len(clique), k) cliques of size k
                sub_cliques = self.get_subcliques(clique)
                clique_k.extend(sub_cliques)
        return self.check_lists(clique_k, sql_cliques)

    def get_nodes(self):
        return self.nodes

    def get_edges(self):
        return self.edges
