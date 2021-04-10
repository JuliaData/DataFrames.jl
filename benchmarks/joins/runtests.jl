using DataFrames
include("join_performance.jl")

# re-enable "cat" when
# https://github.com/JuliaData/CategoricalArrays.jl/pull/327
# and
# https://github.com/JuliaData/CategoricalArrays.jl/pull/331
# are merged and tagged

function run_all_tests()
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

    println("llen,rlen,arr_type,dup_mode,sort_mode,num_keys,join_type,run_time")
    flush(stdout)

    for (llen, rlen, join_type) in cases,
        arr_type in ["str", "int", "pool"], # "cat"],
        dup_mode in ["uniq", "dup", "manydup"],
        sort_mode in ["sort", "rand"],
        num_keys in ["1", "2"]
        dup_mode == "uniq" && arr_type in ["pool", "cat"] && continue

        @info "running test:  $llen $rlen $arr_type $dup_mode $sort_mode $num_keys $join_type"
        t = run_bench(llen, rlen, arr_type, dup_mode, sort_mode, num_keys, join_type)
        @info "timing: $t seconds"

        println("$llen,$rlen,$arr_type,$dup_mode,$sort_mode,$num_keys,$join_type,$t")
        flush(stdout)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    path = pathof(DataFrames)
    @info "DataFrames.jl at $path"
    run_all_tests()
end
