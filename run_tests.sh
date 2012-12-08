#!/bin/bash

#
# Correctness Tests
#

julia test/data.jl
julia test/dataframe.jl
julia test/operators.jl
julia test/io.jl
# julia test/formula.jl
julia test/datastream.jl

#
# Optional time-consuming Benchmarks
#

# julia test/perf/datavec.jl
