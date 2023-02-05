#
# Correctness Tests
#

fatalerrors = length(ARGS) > 0 && ARGS[1] == "-f"
quiet = length(ARGS) > 0 && ARGS[1] == "-q"
anyerrors = false

using DataFrames, Dates, Test, Random, InlineStrings

if Threads.nthreads() < 2
    @warn("Running tests with only one thread: correctness of parallel operations is not checked")
else
    @info("Running tests with $(Threads.nthreads()) threads")
end

ambiguities_vec = Test.detect_ambiguities(DataFrames, recursive=true)
if !isempty(ambiguities_vec)
    @error "Method ambiguities:"
    display(ambiguities_vec)
    throw(AssertionError("method dispatch ambiguities found"))
end

unbound_args_vec = Test.detect_unbound_args(DataFrames, recursive=true)
if !isempty(unbound_args_vec)
    @error "Unbound type parameters:"
    display(unbound_args_vec)
    throw(AssertionError("unbound type parameters found"))
end

my_tests = ["utils.jl",
            "cat.jl",
            "data.jl",
            "index.jl",
            "dataframe.jl",
            "insertion.jl",
            "select.jl",
            "reshape.jl",
            "dataframerow.jl",
            "io.jl",
            "constructors.jl",
            "conversions.jl",
            "sort.jl",
            "grouping.jl",
            "subset.jl",
            "join.jl",
            "iteration.jl",
            "duplicates.jl",
            "show.jl",
            "subdataframe.jl",
            "subdataframe_mutation.jl",
            "tables.jl",
            "tabletraits.jl",
            "indexing.jl",
            "broadcasting.jl",
            "string.jl",
            "multithreading.jl",
            "metadata.jl",
            "deprecated.jl"]

println("Running tests:")

for my_test in my_tests
    try
        include(my_test)
        println("\t\033[1m\033[32mPASSED\033[0m: $(my_test)")
    catch e
        global anyerrors = true
        println("\t\033[1m\033[31mFAILED\033[0m: $(my_test)")
        if fatalerrors
            rethrow(e)
        elseif !quiet
            showerror(stdout, e, backtrace())
            println()
        end
    end
end

if anyerrors
    throw("Tests failed")
end
