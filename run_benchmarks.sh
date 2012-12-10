#!/bin/bash

#
# Optional time-consuming Benchmarks
#

julia test/perf/datavec.jl
julia test/perf/io.jl
julia test/perf/datastreams.jl
