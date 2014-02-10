#
# Correctness Tests
#

fatalerrors = length(ARGS) > 0 && ARGS[1] == "-f"
anyerrors = false

using Base.Test
using DataFrames

my_tests = ["data.jl",
            "index.jl",
            "dataframe.jl",
            "io.jl",
            # "formula.jl",
            "constructors.jl",
            # "indexing.jl",
            "RDA.jl",
            "sort.jl",
            "grouping.jl",
            "join.jl",
            "vcat.jl",
            "iteration.jl",
            "duplicates.jl",
            "show.jl"]

println("Running tests:")

for my_test in my_tests
    try
        include(my_test)
        println("\t\033[1m\033[32mPASSED\033[0m: $(my_test)")
    catch
        anyerrors = true
        println("\t\033[1m\033[31mFAILED\033[0m: $(my_test)")
        if fatalerrors
            rethrow()
        end
    end
end

if anyerrors
    throw("Tests failed")
end

stdin = Pkg.dir("DataFrames", "test", "stdin.sh")
run(`bash $stdin`)
