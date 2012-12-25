#
# Correctness Tests
#

require("extras/test.jl")

load("DataFrames")
using DataFrames

my_tests = ["test/data.jl",
            "test/dataframe.jl",
            "test/operators.jl",
            "test/io.jl",
            # "test/formula.jl",
            "test/datastream.jl",
            "test/datamatrix.jl",
            "test/constructors.jl",
            "test/abstractarray.jl"]

println("Running tests:")

for my_test in my_tests
    println(" * $(my_test)")
    include(my_test)
end
