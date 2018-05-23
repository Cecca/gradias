#!/usr/bin/env python

import numpy.random as npr
import networkx as nx
import sys
import math


class RunningStat:
    """From Donald Knuth's Art of Computer Programming, Vol 2, page 232, 3rd edition"""

    def __init__(self):
        self.cnt = 0
        self.old_mean = 0.0
        self.new_mean = 0.0
        self.old_sigma = 0.0
        self.new_sigma = 0.0
        self.min = sys.maxint
        self.max = 0

    def push(self, num):
        self.cnt += 1
        
        self.max = max(self.max, num)
        self.min = min(self.min, num)

        if self.cnt == 1:
            self.old_mean = float(num)
            self.new_mean = float(num)
            self.old_sigma = 0.0

        else:
            self.new_mean = self.old_mean + (num - self.old_mean) / self.cnt
            self.new_sigma = self.old_sigma + (num - self.old_mean) * (num - self.new_mean)

            self.old_mean = self.new_mean
            self.old_sigma = self.new_sigma

    def mean(self):
        return self.new_mean

    def variance(self):
        if self.cnt > 1:
            return self.new_sigma / (self.cnt - 1)
        else:
            return 0.0

    def standard_deviation(self):
        return math.sqrt(self.variance())

def estimate(graph, runs):
    it = 0
    max_id = max(graph.nodes())
    it_since_last_update = 0
    seen = set()

    est = {'src': -1, 'dst': -1, 'distance': 0, 'edges': 0}

    src = npr.randint(max_id)

    while not graph.has_node(src):
        src = npr.randint(max_id)

    while it_since_last_update < runs:
        seen.add(src)
        pred, dists = nx.dijkstra_predecessor_and_distance(graph, src)
        dst, dist = max(dists.items(), key=lambda x: x[1])
        if dist > est['distance']:
            est['src'] = src
            est['dst'] = dst
            est['distance'] = dist
            edges = 0
            x = dst
            while x != src:
                edges += 1
                x = pred[x][0]
            est['edges'] = edges
            it_since_last_update = 0
        print "{} | {} -- {} : {} (cur estimate {} standing {})".format(
            it, src, dst, dist, est, it_since_last_update)
        it += 1
        src = npr.randint(max_id) if dst in seen else dst
        it_since_last_update += 1
    return est


def degree_info(graph):
    deg_stat = RunningStat()
    for _, deg in graph.degree_iter():
        deg_stat.push(deg)
    return deg_stat


def weight_info(graph):
    weight_stat = RunningStat()
    for u, v in graph.edges_iter():
        weight = graph.get_edge_data(u, v)["weight"]
        weight_stat.push(weight)
    return weight_stat


if __name__ == "__main__":
    data = sys.argv[1]
    runs = int(sys.argv[2])
    print "*** Loading", data
    G = nx.read_edgelist(data, nodetype=int, data=(('weight', int), ))
    print "Loaded graph with {} nodes and {} edges".format(
        G.number_of_nodes(), G.number_of_edges())

    est = estimate(G, runs)
    weights = weight_info(G)
    degs = degree_info(G)

    print "="*80
    print """* Graph {}
  - connected {}
  - directed {}
  - nodes {}
  - edges {}
  - weights
    - min {}
    - max {}
    - mean {}
    - variance {}
    - standard deviation {}
  - degree
    - min {}
    - max {}
    - mean {}
    - variance {}
    - std deviation {}
  - Diameter {}
  - Hop-diameter {}
  - Source {}
  - Destination {}""".format(
      data,
      nx.is_connected(G),
      nx.is_directed(G),
      G.number_of_nodes(),
      G.number_of_edges(),
      weights.min,
      weights.max,
      weights.mean(),
      weights.variance(),
      weights.standard_deviation(),
      degs.min,
      degs.max,
      degs.mean(),
      degs.variance(),
      degs.standard_deviation(),
      est['distance'],
      est['edges'],
      est['src'],
      est['dst'])
