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
julia test/datamatrix.jl
