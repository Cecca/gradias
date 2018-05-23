#!/usr/bin/env python
###############################################################################
##
## Performance Monitor
## ===================
##
## This script is used to launch a set of jobs to monitor the performance of
## the implementation of the various algorithms
##
###############################################################################

if __name__ == '__main__':

    import sys
    import gradias

    import datetime

    exp_date = datetime.datetime.now().isoformat()

    experiments = ["cluster -i livejournal -t 0.001 --diameter-algorithm Distributed",
                   "bfs -i livejournal",
                   "hadi -i livejournal"]

    jar = sys.argv[1]
    opts = '-Dexperiment.name={} -Dexperiment.category={}'.format(
        exp_date, 'performance-monitor')

    for run in range(3):
        gradias.exec_all(experiments, jar=jar, driver_options=opts)
