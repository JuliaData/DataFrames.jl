#
# Correctness Tests
#

# TODO: Load DataFrames just once for all tests

tests = ["test/data.jl",
         "test/dataframe.jl",
         "test/operators.jl",
         "test/io.jl",
         # "test/formula.jl",
         "test/datastream.jl",
         "test/datamatrix.jl",
         "test/constructors.jl"]

println("Running tests:")

for test in tests
    println(" * $(test)")
    run(`julia $(test)`)
end
