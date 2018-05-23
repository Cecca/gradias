#!/usr/bin/env python

##                         gradias.py
##                         ==========
##
## This is a little script to help running spark-graph without having
## to deal with the long spark-submit command line every time.
##

import sys
import os.path
import os
import subprocess

base_path = os.getenv('SPARK_DATA_BASE_PATH', 'file:///mnt/gluster/datasets/graphs')
properties_file = os.getenv('SPARK_PROPERTIES_FILE', 'default.conf')
spark_jar = os.getenv('GRADIAS_JAR', 'gradias.jar')
extra_options = os.getenv('GRADIAS_JAVA_OPTIONS', '')

def get_dataset_path(cmd_args):
    data_name = cmd_args[1 + cmd_args.index('-i')]
    if not os.path.isfile(data_name):
        data_name = os.path.join(base_path, data_name)
    return data_name

def get_dataset_name(dataset_path):
    basename = os.path.basename(dataset_path)
    return basename

def build_driver_options(cmd_args):
    command_name = cmd_args[0]
    data_name = get_dataset_name(get_dataset_path(cmd_args))
    return '-Dexperiment.name={} -Dexperiment.category={} '.format(
        data_name, command_name) + extra_options

def build_cmdline(cmd_args, **kwargs):
    driver_options = kwargs['driver_options'] if 'driver_options' in kwargs else build_driver_options(cmd_args)
    submit_options = kwargs['submit_options'] if 'submit_options' in kwargs else []
    dataset_path = get_dataset_path(cmd_args)
    pre_data = cmd_args[:cmd_args.index('-i')]
    post_data = cmd_args[cmd_args.index('-i')+2:]
    sg_jar = kwargs['jar'] if 'jar' in kwargs else spark_jar
    args = pre_data + ['-i', dataset_path] + post_data
    cmdline = ['spark-submit', '--properties-file', properties_file,
               '--driver-java-options', driver_options,
               '--class', 'it.unipd.dei.gradias.Tool'
               ] + submit_options + [sg_jar] + args
    return cmdline

def exec_command(cmd_args, **kwargs):
    """:cmd_args: command line arguments for spark-graph, either as a list
    of tokens or as a string"""
    if type(cmd_args) == list:
        subprocess.call( build_cmdline(cmd_args, **kwargs) )
    elif type(cmd_args) == str:
        subprocess.call( build_cmdline(cmd_args.split(), **kwargs) )
    else:
        raise TypeError("exec_command needs either a string or a list")

def exec_all(commands, **kwargs):
    for cmd in commands:
        exec_command(cmd, **kwargs)

def fun(**kwargs):
    print kwargs

if __name__ == '__main__':
    cmd_args = sys.argv[1:]
    if '--' in cmd_args:
        submit_options = cmd_args[:cmd_args.index('--')]
        prog_options = cmd_args[cmd_args.index('--')+1:]
    else:
        submit_options = []
        prog_options = cmd_args
    exec_command(prog_options, submit_options=submit_options)
