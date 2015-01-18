#!/bin/bash

set -e -u

nohup ./benchmarks.sh > benchmarks.out 2> benchmarks.err < /dev/null &
