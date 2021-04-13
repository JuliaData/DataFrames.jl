using CategoricalArrays
using DataFrames
using PooledArrays
using Random

fullgc() = (GC.gc(true); GC.gc(true); GC.gc(true); GC.gc(true))


function run_bench(llen, rlen, arr_type, dup_mode, sort_mode, num_keys, join_type)


    @assert llen > 1000
    @assert rlen > 2000

    @assert arr_type in ["int", "pool", "cat", "str"]
    @assert dup_mode in ["uniq", "dup", "manydup"]
    @assert sort_mode in ["sort", "rand"]
    @assert num_keys in ["1", "2"]
    @assert join_type in ["inner", "left", "right", "outer", "semi", "anti"]

    pad = maximum(length.(string.((llen, rlen))))

    if arr_type == "int"
        if dup_mode == "uniq"
            col1 = [1:llen;]
            col2 = [1:rlen;]
        elseif dup_mode == "dup"
            col1 = repeat(1:llen ÷ 2, inner=2)
            col2 = repeat(1:rlen ÷ 2, inner=2)
        else
            @assert dup_mode == "manydup"
            col1 = repeat(1:llen ÷ 20, inner=20)
            col2 = repeat(1:rlen ÷ 20, inner=20)
        end
    elseif arr_type == "pool"
        if dup_mode == "dup"
            col1 = PooledArray(repeat(string.(1:llen ÷ 2, pad=pad), inner=2))
            col2 = PooledArray(repeat(string.(1:rlen ÷ 2, pad=pad), inner=2))
        else
            @assert dup_mode == "manydup"
            col1 = PooledArray(repeat(string.(1:llen ÷ 20, pad=pad), inner=20))
            col2 = PooledArray(repeat(string.(1:rlen ÷ 20, pad=pad), inner=20))
        end
    elseif arr_type == "cat"
        if dup_mode == "dup"
            col1 = categorical(repeat(string.(1:llen ÷ 2, pad=pad), inner=2))
            col2 = categorical(repeat(string.(1:rlen ÷ 2, pad=pad), inner=2))
        else
            @assert dup_mode == "manydup"
            col1 = categorical(repeat(string.(1:llen ÷ 20, pad=pad), inner=20))
            col2 = categorical(repeat(string.(1:rlen ÷ 20, pad=pad), inner=20))
        end
    else
        @assert arr_type == "str"
        if dup_mode == "uniq"
            col1 = string.(1:llen, pad=pad)
            col2 = string.(1:rlen, pad=pad)
        elseif dup_mode == "dup"
            col1 = repeat(string.(1:llen ÷ 2, pad=pad), inner=2)
            col2 = repeat(string.(1:rlen ÷ 2, pad=pad), inner=2)
        else
            @assert dup_mode == "manydup"
            col1 = repeat(string.(1:llen ÷ 20, pad=pad), inner=20)
            col2 = repeat(string.(1:rlen ÷ 20, pad=pad), inner=20)
        end
    end

    Random.seed!(1234)

    if sort_mode == "rand"
        shuffle!(col1)
        shuffle!(col2)
    else
        @assert sort_mode == "sort"
    end

    joinfun = Dict("inner" => innerjoin, "left" => leftjoin,
                    "right" => rightjoin, "outer" => outerjoin,
                    "semi" => semijoin, "anti" => antijoin)[join_type]

    if num_keys == "1"
        df1 = DataFrame(id1=col1)
        df2 = DataFrame(id1=col2)
        joinfun(df1[1:1000, :], df2[1:2000, :], on=:id1)
        joinfun(df2[1:2000, :], df1[1:1000, :], on=:id1)
        fullgc()
        t1 = @elapsed joinfun(df1, df2, on=:id1)
        fullgc()
        t2 = @elapsed joinfun(df2, df1, on=:id1)
    else
        @assert num_keys == "2"
        df1 = DataFrame(id1=col1, id2=col1)
        df2 = DataFrame(id1=col2, id2=col2)
        joinfun(df1[1:1000, :], df2[1:2000, :], on=[:id1, :id2])
        joinfun(df2[1:2000, :], df1[1:1000, :], on=[:id1, :id2])
        fullgc()
        t1 = @elapsed joinfun(df1, df2, on=[:id1, :id2])
        fullgc()
        t2 = @elapsed joinfun(df2, df1, on=[:id1, :id2])
    end

    return (t1 + t2) / 2.0
end


if abspath(PROGRAM_FILE) == @__FILE__
    @info "$ARGS"

    llen = parse(Int, ARGS[1])
    rlen = parse(Int, ARGS[2])
    t = run_bench(llen, rlen, ARGS[3], ARGS[4], ARGS[5], ARGS[6], ARGS[7])
    @info("Timing: $t sec")
end