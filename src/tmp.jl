load("DataFrames")

using DataFrames

fn = "/Users/johnmyleswhite/julia/extras/bitarray.jl"
include(fn)

DataVec([1, 2, 3], bitpack([false, false, false]))

DataVec([1, 2, 3], [false, false, false])
