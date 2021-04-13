#!/usr/bin/env julia
#
# Run _join_ benchmarks
#
# Usage: julia runtests.jl [--single-process] <llen> <rlen> <join_type>
#     --single-process      Runs all tests in a single julia process instead of
#                           spawning multiple julia subprocesses
#     <llen>                The length of the left dataframe
#     <rlen>                The length of the right dataframe
#     <join_type>           One of 'inner', 'left', 'right', 'outer', 'semi', 'anti'
#
# Note: Either provide all of <llen>, <rlen>, <join_type> arguments or leave blank
#       to run all test cases (see `run_all_tests` below)


using DataFrames
include("join_performance.jl")

# re-enable "cat" when
# https://github.com/JuliaData/CategoricalArrays.jl/pull/327
# and
# https://github.com/JuliaData/CategoricalArrays.jl/pull/331
# are merged and tagged

const file_loc = joinpath(dirname(@__FILE__), "join_performance.jl")

function run_all_tests(single_process)
    cases = [
        (100000, 50000000, "inner"),
        (5000000, 10000000, "inner"),
        (100000, 50000000, "left"),
        (5000000, 10000000, "left"),
        (100000, 50000000, "right"),
        (5000000, 10000000, "right"),
        (100000, 50000000, "outer"),
        (5000000, 10000000, "outer"),
        (100000, 50000000, "semi"),
        (5000000, 10000000, "semi"),
        (100000, 50000000, "anti"),
        (5000000, 10000000, "anti")
    ]

    for (llen, rlen, join_type) in cases
        run_test(single_process, llen, rlen, join_type)
    end
end

function run_test(single_process, llen, rlen, join_type)
    for
        arr_type in ["str", "int", "pool"], # "cat"],
        dup_mode in ["uniq", "dup", "manydup"],
        sort_mode in ["sort", "rand"],
        num_keys in ["1", "2"]
        dup_mode == "uniq" && arr_type in ["pool", "cat"] && continue

        if single_process
            @info "Running test: $llen $rlen $arr_type $dup_mode $sort_mode $num_keys $join_type"
            flush(stdout)

            t = run_bench(llen, rlen, arr_type, dup_mode, sort_mode, num_keys, join_type)

            @info "Timing: $t seconds"
            flush(stdout)
        else
            run(`julia $file_loc $llen $rlen $arr_type $dup_mode $sort_mode $num_keys $join_type`)
        end
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    path = pathof(DataFrames)
    @info "DataFrames.jl at $path"

    # do some argument processing
    args = copy(ARGS)
    single_process = false
    pos = []
    while length(args) > 0
        arg = popfirst!(args)
        if arg == "--single-process"
            @info "Using single Julia process"
            global single_process = true
        elseif startswith(arg, "--")
            @error("Unknown option $arg")
            exit(1)
        else
            push!(pos, arg)
        end
    end

    @assert length(args) in [0, 3] "Wrong positional arguments. Expect [llen, rlen, join_type], got $args."

    if length(pos) == 0
        @info "Running all tests"
        run_all_tests(single_process)
    else
        llen, rlen, join_type = pos
        llen = parse(Int, llen)
        rlen = parse(Int, rlen)
        run_test(single_process, llen, rlen, join_type)
    end
end
