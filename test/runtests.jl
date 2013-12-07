#
# Correctness Tests
#

using Base.Test
using DataFrames

my_tests = ["extras.jl",
            "data.jl",
            "index.jl",
            "dataframe.jl",
            "operators.jl",
            "io.jl",
            # "formula.jl",
            # "datastream.jl",
            "constructors.jl",
            "indexing.jl",
            # "indexedvector.jl",
            "RDA.jl",
            "sort.jl",
            "iteration.jl",
            #"test/colfuncs.jl",
            "duplicates.jl"]

println("Running tests:")

for my_test in my_tests
    println(" * $(my_test)")
    include(my_test)
end
