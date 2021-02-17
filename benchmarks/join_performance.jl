using CategoricalArrays
using DataFrames
using PooledArrays
using Random

fullgc() = (GC.gc(true); GC.gc(true); GC.gc(true); GC.gc(true))

@assert length(ARGS) == 7
@assert ARGS[3] in ["int", "pool", "cat", "str"]
@assert ARGS[4] in ["uniq", "dup", "manydup"]
@assert ARGS[5] in ["sort", "rand"]
@assert ARGS[6] in ["1", "2"]
@assert ARGS[7] in ["inner", "left", "right", "outer"]

@info ARGS

llen = parse(Int, ARGS[1])
rlen = parse(Int, ARGS[2])
@assert llen > 1000
@assert rlen > 2000

pad = maximum(length.(string.((llen, rlen))))

if ARGS[3] == "int"
    if ARGS[4] == "uniq"
        col1 = [1:llen;]
        col2 = [1:rlen;]
    elseif ARGS[4] == "dup"
        col1 = repeat(1:llen ÷ 2, inner=2)
        col2 = repeat(1:rlen ÷ 2, inner=2)
    else
        @assert ARGS[4] == "manydup"
        col1 = repeat(1:llen ÷ 20, inner=20)
        col2 = repeat(1:rlen ÷ 20, inner=20)
    end
elseif ARGS[3] == "pool"
    if ARGS[4] == "dup"
        col1 = PooledArray(repeat(string.(1:llen ÷ 2, pad=pad), inner=2))
        col2 = PooledArray(repeat(string.(1:rlen ÷ 2, pad=pad), inner=2))
    else
        @assert ARGS[4] == "manydup"
        col1 = PooledArray(repeat(string.(1:llen ÷ 20, pad=pad), inner=20))
        col2 = PooledArray(repeat(string.(1:rlen ÷ 20, pad=pad), inner=20))
    end
elseif ARGS[3] == "cat"
    if ARGS[4] == "dup"
        col1 = categorical(repeat(string.(1:llen ÷ 2, pad=pad), inner=2))
        col2 = categorical(repeat(string.(1:rlen ÷ 2, pad=pad), inner=2))
    else
        @assert ARGS[4] == "manydup"
        col1 = categorical(repeat(string.(1:llen ÷ 20, pad=pad), inner=20))
        col2 = categorical(repeat(string.(1:rlen ÷ 20, pad=pad), inner=20))
    end
else
    @assert ARGS[3] == "str"
    if ARGS[4] == "uniq"
        col1 = string.(1:llen, pad=pad)
        col2 = string.(1:rlen, pad=pad)
    elseif ARGS[4] == "dup"
        col1 = repeat(string.(1:llen ÷ 2, pad=pad), inner=2)
        col2 = repeat(string.(1:rlen ÷ 2, pad=pad), inner=2)
    else
        @assert ARGS[4] == "manydup"
        col1 = repeat(string.(1:llen ÷ 20, pad=pad), inner=20)
        col2 = repeat(string.(1:rlen ÷ 20, pad=pad), inner=20)
    end
end

Random.seed!(1234)

if ARGS[5] == "rand"
    shuffle!(col1)
    shuffle!(col2)
else
    @assert ARGS[5] == "sort"
end

const joinfun = Dict("inner" => innerjoin, "left" => leftjoin,
                     "right" => rightjoin, "outer" => outerjoin)[ARGS[7]]

if ARGS[6] == "1"
    df1 = DataFrame(id1 = col1)
    df2 = DataFrame(id1 = col2)
    joinfun(df1[1:1000, :], df2[1:2000, :], on=:id1)
    joinfun(df2[1:2000, :], df1[1:1000, :], on=:id1)
    fullgc()
    @time joinfun(df1, df2, on=:id1)
    fullgc()
    @time joinfun(df2, df1, on=:id1)
else
    @assert ARGS[6] == "2"
    df1 = DataFrame(id1 = col1, id2 = col1)
    df2 = DataFrame(id1 = col2, id2 = col2)
    joinfun(df1[1:1000, :], df2[1:2000, :], on=[:id1, :id2])
    joinfun(df2[1:2000, :], df1[1:1000, :], on=[:id1, :id2])
    fullgc()
    @time joinfun(df1, df2, on=[:id1, :id2])
    fullgc()
    @time joinfun(df2, df1, on=[:id1, :id2])
end
