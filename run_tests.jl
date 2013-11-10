#
# Correctness Tests
#

using Base.Test
using DataFrames

my_tests = ["test/extras.jl",
            "test/data.jl",
            "test/index.jl",
            "test/dataframe.jl",
            "test/operators.jl",
            "test/io.jl",
            # "test/formula.jl",
            # "test/datastream.jl",
            "test/constructors.jl",
            "test/indexing.jl",
            # "test/indexedvector.jl",
            "test/sort.jl",
            "test/iteration.jl",
            "test/colfuncs.jl",
            "test/duplicates.jl"]

println("Running tests:")

for my_test in my_tests
    println(" * $(my_test)")
    include(my_test)
end
