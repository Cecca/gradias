#!/usr/bin/env python

import numpy as np
import argparse
import ast
import math
import sys


def load_graph(path):
    graph = []
    with open(path, 'r') as in_file:
        for line in in_file.readlines():
            tokens = line.split()
            src = tokens[0]
            for dest in tokens[1:]:
                if int(src) < int(dest):
                    yield (src, dest)


def load_weighted_graph(path):
    graph = []
    with open(path, 'r') as in_file:
        for line in in_file.readlines():
            tokens = line.split()
            src = int(tokens[0])
            dest = int(tokens[1])
            if src < dest:
                yield (src, dest)


def write_graph(graph, path):
    with open(path, 'w') as out:
        for src, dst, weight in graph:
            out.write('{} {} {}\n'.format(src, dst, weight))
            out.write('{} {} {}\n'.format(dst, src, weight))


def add_random_weights(graph, distribution, **kwargs):
    for src, dst in graph:
        weight = int(math.ceil(distribution(**kwargs)))
        yield (src, dst, weight)


def perturb(prob=0.25, scale=1024):
    if np.random.uniform() < prob:
        return scale
    else:
        return 1


def normal(**kwargs):
    return abs(int(np.random.normal(**kwargs))) + 1


def constant(scale=1):
    return scale


def get_distribution(name):
    if name == 'constant':
        return constant
    if name == 'normal':
        return normal
    if name == 'perturb':
        return perturb
    return getattr(np.random, name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--input', metavar='FILE', required=True)
    parser.add_argument('-o', '--output', metavar='FILE', required=True)
    parser.add_argument('-d', '--distribution', metavar='DISTRIBUTION', required=True)
    parser.add_argument('-a', '--args', metavar='DICT', default='{}')
    parser.add_argument('--reweight', action='store_true')

    args = parser.parse_args()
    print args

    distribution_args = ast.literal_eval(args.args)
    distribution = get_distribution(args.distribution)
    if args.reweight:
        print 'Loading input (weighted) graph:', args.input
        input_graph = load_weighted_graph(args.input)
    else:
        print 'Loading input graph', args.input
        input_graph = load_graph(args.input)
    output_graph = add_random_weights(input_graph, distribution, **distribution_args)
    write_graph(output_graph, args.output)
