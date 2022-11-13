module TestGrouping

using Test, DataFrames, Random, Statistics, PooledArrays, CategoricalArrays, DataAPI,
    Combinatorics, Unitful
const ≅ = isequal
const ≇ = !isequal

"""Check if passed data frames are `isequal` and have the same element types of columns"""
isequal_typed(df1::AbstractDataFrame, df2::AbstractDataFrame) =
    isequal(df1, df2) && eltype.(eachcol(df1)) == eltype.(eachcol(df2))

"""Check if passed data frames are `isequal` and have the same types of columns"""
isequal_coltyped(df1::AbstractDataFrame, df2::AbstractDataFrame) =
    isequal(df1, df2) && typeof.(eachcol(df1)) == typeof.(eachcol(df2))

"""Check that groups in gd are equal to provided data frames, ignoring order"""
function isequal_unordered(gd::GroupedDataFrame,
                            dfs::AbstractVector{<:AbstractDataFrame})
    n = length(gd)
    @assert n == length(dfs)
    remaining = Set(1:n)
    for i in 1:n
        for j in remaining
            if gd[i] ≅ dfs[j]
                pop!(remaining, j)
                break
            end
        end
    end
    isempty(remaining) || error("gd is not equal to provided groups")
end

"""Helper to set the order of values in the pool and add unused values"""
function _levels!(x::PooledArray, levels::AbstractVector)
    res = similar(x)
    copyto!(res, levels)
    copyto!(res, x)
end
_levels!(x::CategoricalArray, levels::AbstractVector) = levels!(x, levels)

function validate_gdf(ogd::GroupedDataFrame)
    # To return original object to test when indices have not been computed
    gd = deepcopy(ogd)

    @assert allunique(gd.cols)
    @assert issubset(gd.cols, propertynames(parent(gd)))

    g = sort!(unique(gd.groups))
    if length(gd) > 0
        @assert 0 <= g[1] <= 1
        @assert g == g[1]:g[end]
        @assert gd.ngroups == g[end]
        @assert length(gd.starts) == length(gd.ends) == g[end]
    else
        @assert gd.ngroups == 0
        @assert length(gd.starts) == length(gd.ends) == 0
    end
    @assert isperm(gd.idx)
    @assert length(gd.idx) == length(gd.groups) == nrow(parent(gd))

    # checking that groups field is consistent with other fields
    # (since == and isequal do not use it)
    # and that idx is increasing per group
    new_groups = zeros(Int, length(gd.groups))
    for idx in eachindex(gd.starts)
        subidx = gd.idx[gd.starts[idx]:gd.ends[idx]]
        @assert issorted(subidx)
        new_groups[subidx] .= idx
    end
    @assert new_groups == gd.groups

    if length(gd) > 0
        se = sort!(collect(zip(gd.starts, gd.ends)))

        # correct start-end range
        @assert se[1][1] > 0
        @assert se[end][2] == length(gd.idx)

        # correct start-end relations
        for i in eachindex(se)
            @assert se[i][1] <= se[i][2]
            if i > 1
                # the blocks returned by groupby must be continuous
                @assert se[i-1][2] + 1 == se[i][1]
            end
        end

        # all grouping keys must be equal within a group
        for (s, e) in zip(gd.starts, gd.ends)
            firstkeys = gd.parent[gd.idx[s], gd.cols]
            @assert all(j -> gd.parent[gd.idx[j], gd.cols] ≅ firstkeys, s:e)
        end
        # all groups have different grouping keys
        @test allunique(eachrow(gd.parent[gd.idx[gd.starts], gd.cols]))
    end
    return ogd
end

function groupby_checked(df::AbstractDataFrame, keys, args...; kwargs...)
    ogd = groupby(df, keys, args...; kwargs...)
    validate_gdf(ogd)
    return ogd
end

@testset "parent" begin
    df = DataFrame(a=[1, 1, 2, 2], b=[5, 6, 7, 8])
    gd = groupby_checked(df, :a)
    @test parent(gd) === df
    @test_throws ArgumentError identity.(gd)
end

@testset "consistency" begin
    df = DataFrame(a=[1, 1, 2, 2], b=[5, 6, 7, 8], c=1:4)
    push!(df.c, 5)
    @test_throws AssertionError groupby_checked(df, :a)

    df = DataFrame(a=[1, 1, 2, 2], b=[5, 6, 7, 8], c=1:4)
    push!(DataFrames._columns(df), df[:, :a])
    @test_throws AssertionError groupby_checked(df, :a)
end

@testset "accepted columns" begin
    df = DataFrame(A=[1, 1, 1, 2, 2, 2], B=[1, 2, 1, 2, 1, 2], C=1:6)
    @test groupby_checked(df, [1, 2]) == groupby_checked(df, 1:2) ==
          groupby_checked(df, [:A, :B]) == groupby_checked(df, ["A", "B"])
    @test groupby_checked(df, [2, 1]) == groupby_checked(df, 2:-1:1) ==
          groupby_checked(df, [:B, :A]) == groupby_checked(df, ["B", "A"])
    @test_throws BoundsError groupby_checked(df, 0)
    @test_throws BoundsError groupby_checked(df, 10)
    @test_throws ArgumentError groupby_checked(df, :Z)
    @test_throws ArgumentError groupby_checked(df, "Z")
end

@testset "grouping with missings" begin
    xv = ["A", missing, "B", "B", "A", "B", "A", "A"]
    yv = ["B", "A", "A", missing, "A", missing, "A", "A"]
    xvars = (xv,
             categorical(xv),
             levels!(categorical(xv), ["A", "B", "X"]),
             levels!(categorical(xv), ["X", "B", "A"]),
             _levels!(PooledArray(xv), ["A", "B", missing]),
             _levels!(PooledArray(xv), ["B", "A", missing, "X"]),
             _levels!(PooledArray(xv), [missing, "X", "A", "B"]))
    yvars = (yv,
             categorical(yv),
             levels!(categorical(yv), ["A", "B", "X"]),
             levels!(categorical(yv), ["B", "X", "A"]),
             _levels!(PooledArray(yv), ["A", "B", missing]),
             _levels!(PooledArray(yv), [missing, "A", "B", "X"]),
             _levels!(PooledArray(yv), ["B", "A", "X", missing]))
    for x in xvars, y in yvars
        df = DataFrame(Key1=x, Key2=y, Value=1:8)

        @testset "sort=false, skipmissing=false" begin
            gd = groupby_checked(df, :Key1)
            @test length(gd) == 3
            @test isequal_unordered(gd, [
                    DataFrame(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                    DataFrame(Key1="B", Key2=["A", missing, missing], Value=[3, 4, 6]),
                    DataFrame(Key1=missing, Key2="A", Value=2)
                ])

            gd = groupby_checked(df, [:Key1, :Key2])
            @test length(gd) == 5
            @test isequal_unordered(gd, [
                    DataFrame(Key1="A", Key2="A", Value=[5, 7, 8]),
                    DataFrame(Key1="A", Key2="B", Value=1),
                    DataFrame(Key1="B", Key2="A", Value=3),
                    DataFrame(Key1="B", Key2=missing, Value=[4, 6]),
                    DataFrame(Key1=missing, Key2="A", Value=2)
                ])
        end

        @testset "sort=false, skipmissing=true" begin
            gd = groupby_checked(df, :Key1, skipmissing=true)
            @test length(gd) == 2
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                DataFrame(Key1="B", Key2=["A", missing, missing], Value=[3, 4, 6])
            ])

            gd = groupby_checked(df, [:Key1, :Key2], skipmissing=true)
            @test length(gd) == 3
            @test isequal_unordered(gd, [
                    DataFrame(Key1="A", Key2="A", Value=[5, 7, 8]),
                    DataFrame(Key1="A", Key2="B", Value=1),
                    DataFrame(Key1="B", Key2="A", Value=3),
                ])
        end

        @testset "sort=true, skipmissing=false" begin
            gd = groupby_checked(df, :Key1, sort=true)
            @test length(gd) == 3
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                DataFrame(Key1="B", Key2=["A", missing, missing], Value=[3, 4, 6]),
                DataFrame(Key1=missing, Key2="A", Value=2)
            ])
            @test issorted(vcat(gd...), :Key1)

            gd = groupby_checked(df, [:Key1, :Key2], sort=true)
            @test length(gd) == 5
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2="A", Value=[5, 7, 8]),
                DataFrame(Key1="A", Key2="B", Value=1),
                DataFrame(Key1="B", Key2="A", Value=3),
                DataFrame(Key1="B", Key2=missing, Value=[4, 6]),
                DataFrame(Key1=missing, Key2="A", Value=2)
            ])
            @test issorted(vcat(gd...), [:Key1, :Key2])
        end

        @testset "sort=true, skipmissing=true" begin
            gd = groupby_checked(df, :Key1, sort=true, skipmissing=true)
            @test length(gd) == 2
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                DataFrame(Key1="B", Key2=["A", missing, missing], Value=[3, 4, 6])
            ])
            @test issorted(vcat(gd...), :Key1)

            gd = groupby_checked(df, [:Key1, :Key2], sort=true, skipmissing=true)
            @test length(gd) == 3
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2="A", Value=[5, 7, 8]),
                DataFrame(Key1="A", Key2="B", Value=1),
                DataFrame(Key1="B", Key2="A", Value=3)
            ])
            @test issorted(vcat(gd...), [:Key1, :Key2])
        end
    end

    @test groupby_checked(DataFrame(x=[missing]), :x).groups ==
        groupby_checked(DataFrame(x=Union{Int, Missing}[missing]), :x).groups ==
        groupby_checked(DataFrame(x=Union{String, Missing}[missing]), :x).groups ==
        groupby_checked(DataFrame(x=Any[missing]), :x).groups == [1]
    @test isempty(groupby_checked(DataFrame(x=[missing]), :x, skipmissing=true))
    @test isempty(groupby_checked(DataFrame(x=Union{Int, Missing}[missing]),
                                  :x, skipmissing=true))
    @test isempty(groupby_checked(DataFrame(x=Union{String, Missing}[missing]),
                                  :x, skipmissing=true))
    @test isempty(groupby_checked(DataFrame(x=Any[missing]), :x, skipmissing=true))
end

@testset "grouping arrays that allow missing without missings" begin
    xv = ["A", "B", "B", "B", "A", "B", "A", "A"]
    yv = ["B", "A", "A", "B", "A", "B", "A", "A"]
    xvars = (xv,
             categorical(xv),
             levels!(categorical(xv), ["A", "B", "X"]),
             levels!(categorical(xv), ["X", "B", "A"]),
             _levels!(PooledArray(xv), ["A", "B"]),
             _levels!(PooledArray(xv), ["B", "A", "X"]),
             _levels!(PooledArray(xv), ["X", "A", "B"]))
    yvars = (yv,
             categorical(yv),
             levels!(categorical(yv), ["A", "B", "X"]),
             levels!(categorical(yv), ["B", "X", "A"]),
             _levels!(PooledArray(yv), ["A", "B"]),
             _levels!(PooledArray(yv), ["A", "B", "X"]),
             _levels!(PooledArray(yv), ["B", "A", "X"]))
    for x in xvars, y in yvars,
        fx in (identity, allowmissing),
        fy in (identity, allowmissing)
        df = DataFrame(Key1=fx(x), Key2=fy(y), Value=1:8)

        @testset "sort=false, skipmissing=false" begin
            gd = groupby_checked(df, :Key1)
            @test length(gd) == 2
            @test isequal_unordered(gd, [
                    DataFrame(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                    DataFrame(Key1="B", Key2=["A", "A", "B", "B"], Value=[2, 3, 4, 6]),
                ])

            gd = groupby_checked(df, [:Key1, :Key2])
            @test length(gd) == 4
            @test isequal_unordered(gd, [
                    DataFrame(Key1="A", Key2="A", Value=[5, 7, 8]),
                    DataFrame(Key1="A", Key2="B", Value=1),
                    DataFrame(Key1="B", Key2="A", Value=[2, 3]),
                    DataFrame(Key1="B", Key2="B", Value=[4, 6])
                ])
        end

        @testset "sort=false, skipmissing=true" begin
            gd = groupby_checked(df, :Key1, skipmissing=true)
            @test length(gd) == 2
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                DataFrame(Key1="B", Key2=["A", "A", "B", "B"], Value=[2, 3, 4, 6])
            ])

            gd = groupby_checked(df, [:Key1, :Key2], skipmissing=true)
            @test length(gd) == 4
            @test isequal_unordered(gd, [
                    DataFrame(Key1="A", Key2="A", Value=[5, 7, 8]),
                    DataFrame(Key1="A", Key2="B", Value=1),
                    DataFrame(Key1="B", Key2="A", Value=[2, 3]),
                    DataFrame(Key1="B", Key2="B", Value=[4, 6])
                ])
        end

        @testset "sort=true, skipmissing=false" begin
            gd = groupby_checked(df, :Key1, sort=true)
            @test length(gd) == 2
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                DataFrame(Key1="B", Key2=["A", "A", "B", "B"], Value=[2, 3, 4, 6]),
            ])
            @test issorted(vcat(gd...), :Key1)

            gd = groupby_checked(df, [:Key1, :Key2], sort=true)
            @test length(gd) == 4
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2="A", Value=[5, 7, 8]),
                DataFrame(Key1="A", Key2="B", Value=1),
                DataFrame(Key1="B", Key2="A", Value=[2, 3]),
                DataFrame(Key1="B", Key2="B", Value=[4, 6]),
            ])
            @test issorted(vcat(gd...), [:Key1, :Key2])
        end

        @testset "sort=true, skipmissing=true" begin
            gd = groupby_checked(df, :Key1, sort=true, skipmissing=true)
            @test length(gd) == 2
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2=["B", "A", "A", "A"], Value=[1, 5, 7, 8]),
                DataFrame(Key1="B", Key2=["A", "A", "B", "B"], Value=[2, 3, 4, 6])
            ])
            @test issorted(vcat(gd...), :Key1)

            gd = groupby_checked(df, [:Key1, :Key2], sort=true, skipmissing=true)
            @test length(gd) == 4
            @test isequal_unordered(gd, [
                DataFrame(Key1="A", Key2="A", Value=[5, 7, 8]),
                DataFrame(Key1="A", Key2="B", Value=1),
                DataFrame(Key1="B", Key2="A", Value=[2, 3]),
                DataFrame(Key1="B", Key2="B", Value=[4, 6])
            ])
            @test issorted(vcat(gd...), [:Key1, :Key2])
        end
    end
end

@testset "grouping refarray with fallback" begin
    # The high number of categories compared to the number of rows triggers the use
    # of the fallback grouping method
    for x in ([3, 1, 2], [3, 1, missing])
        df = DataFrame(x=categorical(x, levels=10000:-1:1),
                       x2=categorical(x, levels=3:-1:1),
                       y=[1, 2, 3])
        for skipmissing in (true, false)
            @test groupby(df, :x, sort=true, skipmissing=skipmissing) ≅
                groupby(df, :x, sort=true, skipmissing=skipmissing)
            @test isequal_unordered(groupby(df, :x, skipmissing=skipmissing),
                                    collect(AbstractDataFrame, groupby(df, :x, skipmissing=skipmissing)))
        end
    end
end

@testset "grouping on integer columns" begin
    Random.seed!(6)

    # Check optimized approach based on refpool method
    for sm in (false, true),
        S in (Int, Float64),
        T in (Int, Float64),
        df in (DataFrame(x=rand(1:10, 1000),
                         y=rand(-3:10, 1000), z=rand(1000)),
               DataFrame(x=rand([1:10; missing], 1000),
                         y=rand([1:10; missing], 1000), z=rand(1000)),
               DataFrame(x=rand([1:10; missing], 1000),
                         y=rand(-3:10, 1000), z=rand(1000)))
        df.x = convert.(Union{S, Missing}, df.x)
        df.y = convert.(Union{T, Missing}, df.y)
        df.x2 = passmissing(string).(df.x)
        df.y2 = passmissing(string).(df.y)
        gd = groupby_checked(df, :x, skipmissing=sm)
        @test issorted(combine(gd, :x)) # Test that optimized method is used
        @test isequal_unordered(gd, [groupby_checked(df, :x2, skipmissing=sm)...])
        gd = groupby_checked(df, [:x, :y], skipmissing=sm)
        @test issorted(combine(gd, :x, :y)) # Test that optimized method is used
        @test isequal_unordered(gd, [groupby_checked(df, [:x2, :y2], skipmissing=sm)...])
    end
    for sm in (false, true),
        v in (typemin(Int), typemax(Int) - 11),
        df in (DataFrame(x=rand((1:10) .+ v, 1000),
                         y=rand(-3:10, 1000), z=rand(1000)),
               DataFrame(x=rand([1:10; missing] .+ v, 1000),
                         y=rand([1:10; missing], 1000), z=rand(1000)),
               DataFrame(x=rand([1:10; missing] .+ v, 1000),
                         y=rand(-3:10, 1000), z=rand(1000)))
        df.x = allowmissing(df.x)
        df.y = allowmissing(df.y)
        df.x2 = passmissing(string).(df.x)
        df.y2 = passmissing(string).(df.y)
        gd = groupby_checked(df, :x, skipmissing=sm)
        @test issorted(combine(gd, :x)) # Test that optimized method is used
        @test isequal_unordered(gd, [groupby_checked(df, :x2, skipmissing=sm)...])
        gd = groupby_checked(df, [:x, :y], skipmissing=sm)
        @test issorted(combine(gd, :x, :y)) # Test that optimized method is used
        @test isequal_unordered(gd, [groupby_checked(df, [:x2, :y2], skipmissing=sm)...])
    end

    # Check fallback to hash table method when range is too wide
    for sm in (false, true),
        S in (Int, Float64),
        T in (Int, Float64),
        df in (DataFrame(x=rand(1:100_000, 100),
                         y=rand(-50:110_000, 100), z=rand(100)),
               DataFrame(x=rand([1:100_000; missing], 100),
                         y=rand([-50:110_000; missing], 100), z=rand(100)),
               DataFrame(x=rand([1:100_000; missing], 100),
                         y=rand(-50:110_000, 100), z=rand(100)))
        df.x = convert.(Union{S, Missing}, df.x)
        df.y = convert.(Union{T, Missing}, df.y)
        df.x2 = passmissing(string).(df.x)
        df.y2 = passmissing(string).(df.y)
        gd = groupby_checked(df, :x, skipmissing=sm)
        @test !issorted(combine(gd, :x)) # Test that optimized method is not used
        @test isequal_unordered(gd, [groupby_checked(df, :x2, skipmissing=sm)...])
        gd = groupby_checked(df, [:x, :y], skipmissing=sm)
        @test !issorted(combine(gd, :x, :y)) # Test that optimized method is not used
        @test isequal_unordered(gd, [groupby_checked(df, [:x2, :y2], skipmissing=sm)...])
    end

    @test isempty(groupby_checked(DataFrame(x=Int[]), :x))
    @test isempty(groupby_checked(DataFrame(x=Union{}[]), :x))
    @test isempty(groupby_checked(DataFrame(x=Union{Int, Missing}[]), :x))
    @test groupby_checked(DataFrame(x=Union{Int, Missing}[missing]), :x) ≅
        groupby_checked(DataFrame(x=Union{String, Missing}[missing]), :x) ≅
        groupby_checked(DataFrame(x=[missing]), :x)
    @test isempty(groupby_checked(DataFrame(x=Union{Int, Missing}[missing]),
                                  skipmissing=true, :x))
    @test isempty(groupby_checked(DataFrame(x=[missing]), skipmissing=true, :x))

    # Check Int overflow
    groups = rand(1:3, 100)
    for i in (0, 1, 2, 10), j in (0, 1, 2, 10),
        v in (big(0), missing)
        @test groupby_checked(DataFrame(x=[big(typemax(Int)) + i, v,
                                           big(typemin(Int)) - j][groups]), :x) ≅
            groupby_checked(DataFrame(x=Any[big(typemax(Int)) + i, v,
                                            big(typemin(Int)) - j][groups]), :x)
    end
    # Corner cases where overflow could happen due to additional missing values group
    for i in (0, 1, 2), j in (0, 1, 2),
        v in (0, missing)
        @test groupby_checked(DataFrame(x=[typemax(Int) - i, v,
                                           typemin(Int) + j][groups]), :x) ≅
            groupby_checked(DataFrame(x=Any[typemax(Int) - i, v,
                                            typemin(Int) + j][groups]), :x)
        @test groupby_checked(DataFrame(x=[typemax(Int) ÷ 2 - i, v,
                                           typemin(Int) ÷ 2 - j][groups]), :x) ≅
            groupby_checked(DataFrame(x=Any[typemax(Int) ÷ 2 - i, v,
                                            typemin(Int) ÷ 2 - j][groups]), :x)
    end
    for i in (0, 1, -1, 2, -2, 10, -10)
        @test groupby_checked(DataFrame(x=fill(big(typemax(Int)) + i, 100)), :x).groups ==
            fill(1, 100)
    end

    # Check special case of Bool
    for sm in (false, true),
        df in (DataFrame(x=rand(Bool, 1000), y=rand(1000)),
               DataFrame(x=rand([true, false, missing], 1000), y=rand(1000)))
        df.x2 = passmissing(string).(df.x)
        gd = groupby_checked(df, :x, skipmissing=sm)
        @test issorted(combine(gd, :x)) # Test that optimized method is used
        @test isequal_unordered(gd, [groupby_checked(df, :x2, skipmissing=sm)...])
    end
end

@testset "grouping with three keys" begin
    # We need many rows so that optimized CategoricalArray method is used
    xv = rand(["A", "B", missing], 100)
    yv = rand(["A", "B", missing], 100)
    zv = rand(["A", "B", missing], 100)
    xvars = (xv,
             categorical(xv),
             levels!(categorical(xv), ["A", "B", "X"]),
             levels!(categorical(xv), ["X", "B", "A"]),
             _levels!(PooledArray(xv), ["A", "B", missing]),
             _levels!(PooledArray(xv), ["B", "A", missing, "X"]),
             _levels!(PooledArray(xv), [missing, "X", "A", "B"]))
    yvars = (yv,
             categorical(yv),
             levels!(categorical(yv), ["A", "B", "X"]),
             levels!(categorical(yv), ["B", "X", "A"]),
             _levels!(PooledArray(yv), ["A", "B", missing]),
             _levels!(PooledArray(yv), [missing, "A", "B", "X"]),
             _levels!(PooledArray(yv), ["B", "A", "X", missing]))
    zvars = (zv,
             categorical(zv),
             levels!(categorical(zv), ["B", "A"]),
             levels!(categorical(zv), ["X", "A", "B"]),
             _levels!(PooledArray(zv), ["A", missing, "B"]),
             _levels!(PooledArray(zv), ["B", missing, "A", "X"]),
             _levels!(PooledArray(zv), ["X", "A", missing, "B"]))
    for x in xvars, y in yvars, z in zvars
        df = DataFrame(Key1=x, Key2=y, Key3=z, Value=string.(1:100))
        dfb = mapcols(Vector{Union{String, Missing}}, df)

        gd = groupby_checked(df, [:Key1, :Key2, :Key3], sort=true)
        dfs = [groupby_checked(dfb, [:Key1, :Key2, :Key3], sort=true)...]
        @test isequal_unordered(gd, dfs)
        @test issorted(vcat(gd...), [:Key1, :Key2, :Key3])
        gd = groupby_checked(df, [:Key1, :Key2, :Key3], sort=true, skipmissing=true)
        dfs = [groupby_checked(dfb, [:Key1, :Key2, :Key3], sort=true, skipmissing=true)...]
        @test isequal_unordered(gd, dfs)
        @test issorted(vcat(gd...), [:Key1, :Key2, :Key3])
    end
end

@testset "grouping with hash collisions" begin
    # Hash collisions are almost certain on 32-bit
    df = DataFrame(A=1:2_000_000)
    gd = groupby_checked(df, :A)
    @test isequal_typed(DataFrame(df), df)
end

@testset "iteration protocol" begin
    gd = groupby_checked(DataFrame(A=[:A, :A, :B, :B], B=1:4), :A)
    @test IndexStyle(gd) == IndexLinear()
    @test IndexStyle(typeof(gd)) == IndexLinear()
    count = 0
    for v in gd
        count += 1
        @test v ≅ gd[count]
    end
    @test count == length(gd)
end

@testset "type stability of index fields" begin
    gd = groupby_checked(DataFrame(A=[:A, :A, :B, :B], B=1:4), :A)
    idx(gd::GroupedDataFrame) = gd.idx
    starts(gd::GroupedDataFrame) = gd.starts
    ends(gd::GroupedDataFrame) = gd.ends
    @inferred idx(gd) == getfield(gd, :idx)
    @inferred starts(gd) == getfield(gd, :starts)
    @inferred ends(gd) == getfield(gd, :ends)
end

@testset "Array-like getindex" begin
    df = DataFrame(a=repeat([1, 2, 3, 4], outer=[2]),
                   b=1:8)
    gd = groupby_checked(df, :a)

    # Invalid
    @test_throws ArgumentError gd[true]
    @test_throws ArgumentError gd[[1, 2, 1]]  # Duplicate
    @test_throws ArgumentError gd["a"]
    @test_throws ArgumentError gd[1, 1]

    # Single integer
    @test gd[1] isa SubDataFrame
    @test gd[1] == view(df, [1, 5], :)
    @test_throws BoundsError gd[5]

    # first, last, lastindex
    @test first(gd) == gd[1]
    @test first(gd, 1) == gd[1:1]
    @test first(gd, 2) == gd[1:2]
    @test first(gd, 0) == gd[1:0]
    @test_throws ArgumentError first(gd, -1)
    @test last(gd) == gd[4]
    @test last(gd, 1) == gd[4:4]
    @test last(gd, 2) == gd[3:4]
    @test last(gd, 0) == gd[1:0]
    @test_throws ArgumentError last(gd, -1)
    @test lastindex(gd) == 4
    @test gd[end] == gd[4]

    # Boolean array
    idx2 = [false, true, false, false]
    gd2 = gd[idx2]
    @test length(gd2) == 1
    @test gd2[1] == gd[2]
    @test_throws BoundsError gd[[true, false]]
    @test gd2.groups == [0, 1, 0, 0, 0, 1, 0, 0]
    @test gd2.starts == [3]
    @test gd2.ends == [4]
    @test gd2.idx == gd.idx
    @test gd[BitArray(idx2)] ≅ gd2
    @test gd[1:2][false:true] ≅ gd[[2]]  # AbstractArray{Bool}

    # Colon
    gd3 = gd[:]
    @test gd3 isa GroupedDataFrame
    @test length(gd3) == 4
    @test gd3 == gd
    for i in 1:4
        @test gd3[i] == gd[i]
    end

    # Integer array
    idx4 = [2, 1]
    gd4 = gd[idx4]
    @test gd4 isa GroupedDataFrame
    @test length(gd4) == 2
    for (i, j) in enumerate(idx4)
        @test gd4[i] == gd[j]
    end
    @test gd4.groups == [2, 1, 0, 0, 2, 1, 0, 0]
    @test gd4.starts == [3, 1]
    @test gd4.ends == [4, 2]
    @test gd4.idx == gd.idx

    # Infer eltype
    @test gd[Array{Any}(idx4)] ≅ gd4
    # Mixed (non-Bool) integer types should work
    @test gd[Any[idx4[1], Unsigned(idx4[2])]] ≅ gd4
    @test_throws ArgumentError gd[Any[2, true]]

    # Out-of-bounds
    @test_throws BoundsError gd[1:5]
    @test_throws BoundsError gd[0]
end

@testset "== and isequal" begin
    df1 = DataFrame(a=repeat([1, 2, 3, 4], outer=[2]),
                    b=1:8)
    df2 = DataFrame(a=repeat([1, 2, 3, 4], outer=[2]),
                    b=[1:7;missing])
    gd1 = groupby_checked(df1, :a)
    gd2 = groupby_checked(df2, :a)
    @test gd1 == gd1
    @test isequal(gd1, gd1)
    @test ismissing(gd1 == gd2)
    @test !isequal(gd1, gd2)
    @test ismissing(gd2 == gd2)
    @test isequal(gd2, gd2)
    df1.c = df1.a
    df2.c = df2.a
    @test gd1 != groupby_checked(df2, :c)
    df2[7, :b] = 10
    @test gd1 != gd2
    df3 = DataFrame(a=repeat([1, 2, 3, missing], outer=[2]),
                    b=1:8)
    df4 = DataFrame(a=repeat([1, 2, 3, missing], outer=[2]),
                    b=[1:7;missing])
    gd3 = groupby_checked(df3, :a)
    gd4 = groupby_checked(df4, :a)
    @test ismissing(gd3 == gd4)
    @test !isequal(gd3, gd4)
    gd3 = groupby_checked(df3, :a, skipmissing = true)
    gd4 = groupby_checked(df4, :a, skipmissing = true)
    @test gd3 == gd4
    @test isequal(gd3, gd4)
end

@testset "show" begin
    function capture_stdout(f::Function)
        oldstdout = stdout
        rd, wr = redirect_stdout()
        f()
        str = String(readavailable(rd))
        redirect_stdout(oldstdout)
        size = displaysize(rd)
        close(rd)
        close(wr)
        str, size
    end

    df = DataFrame(A=Int64[1:4;], B=["x\"", "∀ε>0: x+ε>x", "z\$", "A\nC"],
                   C=Float32[1.0, 2.0, 3.0, 4.0])
    gd = groupby_checked(df, :A)
    io = IOContext(IOBuffer(), :limit=>true)
    show(io, gd)
    str = String(take!(io.io))
    summary_str = summary(gd)
    @test summary_str == "GroupedDataFrame with 4 groups based on key: A"
    @test str == """
        $summary_str
        First Group (1 row): A = 1
         Row │ A      B       C
             │ Int64  String  Float32
        ─────┼────────────────────────
           1 │     1  x"          1.0
        ⋮
        Last Group (1 row): A = 4
         Row │ A      B       C
             │ Int64  String  Float32
        ─────┼────────────────────────
           1 │     4  A\\nC        4.0"""
    show(io, gd, allgroups=true)
    str = String(take!(io.io))
    @test str == """
        $summary_str
        Group 1 (1 row): A = 1
         Row │ A      B       C
             │ Int64  String  Float32
        ─────┼────────────────────────
           1 │     1  x\"          1.0
        Group 2 (1 row): A = 2
         Row │ A      B            C
             │ Int64  String       Float32
        ─────┼─────────────────────────────
           1 │     2  ∀ε>0: x+ε>x      2.0
        Group 3 (1 row): A = 3
         Row │ A      B       C
             │ Int64  String  Float32
        ─────┼────────────────────────
           1 │     3  z\$          3.0
        Group 4 (1 row): A = 4
         Row │ A      B       C
             │ Int64  String  Float32
        ─────┼────────────────────────
           1 │     4  A\\nC        4.0"""

    # Test two-argument show
    str1, dsize = capture_stdout() do
        show(gd)
    end
    io = IOContext(IOBuffer(), :limit=>true, :displaysize=>dsize)
    show(io, gd)
    str2 = String(take!(io.io))
    @test str1 == str2

    # Test error when invalid keyword arguments are passed in text backend.
    @test_throws ArgumentError show(stdout, gd, max_column_width="100px")

    str = sprint(show, "text/html", gd)
    @test str == "<p>" *
                 "<b>GroupedDataFrame with 4 groups based on key: A</b>" *
                 "</p>" *
                 "<div>" *
                 "<div style = \"float: left;\">" *
                 "<span>First Group (1 row): A = 1</span>" *
                 "</div>" *
                 "<div style = \"clear: both;\">" *
                 "</div>" *
                 "</div>" *
                 "<div class = \"data-frame\" style = \"overflow-x: scroll;\">" *
                 "<table class = \"data-frame\" style = \"margin-bottom: 6px;\">" *
                 "<thead>" *
                 "<tr class = \"header\">" *
                 "<th class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">Row</th>" *
                 "<th style = \"text-align: left;\">A</th>" *
                 "<th style = \"text-align: left;\">B</th>" *
                 "<th style = \"text-align: left;\">C</th>" *
                 "</tr>" *
                 "<tr class = \"subheader headerLastRow\">" *
                 "<th class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">" *
                 "</th>" *
                 "<th title = \"Int64\" style = \"text-align: left;\">Int64</th>" *
                 "<th title = \"String\" style = \"text-align: left;\">String</th>" *
                 "<th title = \"Float32\" style = \"text-align: left;\">Float32</th>" *
                 "</tr>" *
                 "</thead>" *
                 "<tbody>" *
                 "<tr>" *
                 "<td class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">1</td>" *
                 "<td style = \"text-align: right;\">1</td>" *
                 "<td style = \"text-align: left;\">x&quot;</td>" *
                 "<td style = \"text-align: right;\">1.0</td>" *
                 "</tr>" *
                 "</tbody>" *
                 "</table>" *
                 "</div>" *
                 "<p>&vellip;</p>" *
                 "<div>" *
                 "<div style = \"float: left;\">" *
                 "<span>Last Group (1 row): A = 4</span>" *
                 "</div>" *
                 "<div style = \"clear: both;\">" *
                 "</div>" *
                 "</div>" *
                 "<div class = \"data-frame\" style = \"overflow-x: scroll;\">" *
                 "<table class = \"data-frame\" style = \"margin-bottom: 6px;\">" *
                 "<thead>" *
                 "<tr class = \"header\">" *
                 "<th class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">Row</th>" *
                 "<th style = \"text-align: left;\">A</th>" *
                 "<th style = \"text-align: left;\">B</th>" *
                 "<th style = \"text-align: left;\">C</th>" *
                 "</tr>" *
                 "<tr class = \"subheader headerLastRow\">" *
                 "<th class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">" *
                 "</th>" *
                 "<th title = \"Int64\" style = \"text-align: left;\">Int64</th>" *
                 "<th title = \"String\" style = \"text-align: left;\">String</th>" *
                 "<th title = \"Float32\" style = \"text-align: left;\">Float32</th>" *
                 "</tr>" *
                 "</thead>" *
                 "<tbody>" *
                 "<tr>" *
                 "<td class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">1</td>" *
                 "<td style = \"text-align: right;\">4</td>" *
                 "<td style = \"text-align: left;\">A\\nC</td>" *
                 "<td style = \"text-align: right;\">4.0</td>" *
                 "</tr>" *
                 "</tbody>" *
                 "</table>" *
                 "</div>"

    @test sprint(show, "text/latex", gd) == """
        GroupedDataFrame with 4 groups based on key: A

        First Group (1 row): A = 1

        \\begin{tabular}{r|ccc}
        \t& A & B & C\\\\
        \t\\hline
        \t& Int64 & String & Float32\\\\
        \t\\hline
        \t1 & 1 & x" & 1.0 \\\\
        \\end{tabular}

        \$\\dots\$

        Last Group (1 row): A = 4

        \\begin{tabular}{r|ccc}
        \t& A & B & C\\\\
        \t\\hline
        \t& Int64 & String & Float32\\\\
        \t\\hline
        \t1 & 4 & A\\textbackslash{}nC & 4.0 \\\\
        \\end{tabular}
        """

    gd = groupby_checked(DataFrame(a=[Symbol("&")], b=["&"]), [1, 2])
    summary_str = summary(gd)
    @test summary_str == "GroupedDataFrame with 1 group based on keys: a, b"
    @test sprint(show, gd) === """
        $summary_str
        Group 1 (1 row): a = :&, b = "&"
         Row │ a       b
             │ Symbol  String
        ─────┼────────────────
           1 │ &       &"""

    str = sprint(show, "text/html", gd)
    @test str == "<p>" *
                 "<b>GroupedDataFrame with 1 group based on keys: a, b</b>" *
                 "</p>" *
                 "<div>" *
                 "<div style = \"float: left;\">" *
                 "<span>First Group (1 row): a = :&amp;, b = &quot;&amp;&quot;</span>" *
                 "</div>" *
                 "<div style = \"clear: both;\">" *
                 "</div>" *
                 "</div>" *
                 "<div class = \"data-frame\" style = \"overflow-x: scroll;\">" *
                 "<table class = \"data-frame\" style = \"margin-bottom: 6px;\">" *
                 "<thead>" *
                 "<tr class = \"header\">" *
                 "<th class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">Row</th>" *
                 "<th style = \"text-align: left;\">a</th>" *
                 "<th style = \"text-align: left;\">b</th>" *
                 "</tr>" *
                 "<tr class = \"subheader headerLastRow\">" *
                 "<th class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">" *
                 "</th>" *
                 "<th title = \"Symbol\" style = \"text-align: left;\">Symbol</th>" *
                 "<th title = \"String\" style = \"text-align: left;\">String</th>" *
                 "</tr>" *
                 "</thead>" *
                 "<tbody>" *
                 "<tr>" *
                 "<td class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">1</td>" *
                 "<td style = \"text-align: left;\">&amp;</td>" *
                 "<td style = \"text-align: left;\">&amp;</td>" *
                 "</tr>" *
                 "</tbody>" *
                 "</table>" *
                 "</div>"

    @test sprint(show, "text/latex", gd) == """
        $summary_str

        First Group (1 row): a = :\\&, b = "\\&"

        \\begin{tabular}{r|cc}
        \t& a & b\\\\
        \t\\hline
        \t& Symbol & String\\\\
        \t\\hline
        \t1 & \\& & \\& \\\\
        \\end{tabular}
        """

        gd = groupby_checked(DataFrame(a=[1, 2], b=[1.0, 2.0]), :a)
        @test sprint(show, "text/csv", gd) == """
        "a","b"
        1,1.0
        2,2.0
        """
        @test sprint(show, "text/tab-separated-values", gd) == """
        "a"\t"b"
        1\t1.0
        2\t2.0
        """
end

@testset "DataFrame" begin
    dfx = DataFrame(A=[missing, :A, :B, :A, :B, missing], B=1:6)

    for df in [dfx, view(dfx, :, :)]
        gd = groupby_checked(df, :A)
        @test sort(DataFrame(gd), :B) ≅ sort(df, :B)
        @test eltype.(eachcol(DataFrame(gd))) == [Union{Missing, Symbol}, Int]

        gd2 = gd[[3, 2]]
        @test isequal_typed(DataFrame(gd2), df[[3, 5, 2, 4], :])

        gd = groupby_checked(df, :A, skipmissing=true)
        @test sort(DataFrame(gd), :B) ==
              sort(dropmissing(df, disallowmissing=false), :B)
        @test eltype.(eachcol(DataFrame(gd))) == [Union{Missing, Symbol}, Int]

        gd2 = gd[[2, 1]]
        @test isequal_typed(DataFrame(gd2), df[[3, 5, 2, 4], :])

        @test_throws ArgumentError DataFrame(gd, copycols=false)
    end

    df = DataFrame(a=Int[], b=[], c=Union{Missing, String}[])
    gd = groupby_checked(df, :a)
    @test size(DataFrame(gd)) == size(df)
    @test eltype.(eachcol(DataFrame(gd))) == [Int, Any, Union{Missing, String}]

    dfv = view(dfx, 1:0, :)
    gd = groupby_checked(dfv, :A)
    @test size(DataFrame(gd)) == size(dfv)
    @test eltype.(eachcol(DataFrame(gd))) == [Union{Missing, Symbol}, Int]
end

@testset "groupindices, groupcols, and valuecols" begin
    df = DataFrame(A=[missing, :A, :B, :A, :B, missing], B=1:6)
    gd = groupby_checked(df, :A)
    @inferred groupindices(gd)
    @test groupindices(gd) == [1, 2, 3, 2, 3, 1]
    @test groupcols(gd) == [:A]
    @test valuecols(gd) == [:B]
    gd2 = gd[[3, 2]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 2, 1, 2, 1, missing]
    @test groupcols(gd2) == [:A]
    @test valuecols(gd2) == [:B]

    gd = groupby_checked(df, :A, skipmissing=true)
    @inferred groupindices(gd)
    @test groupindices(gd) ≅ [missing, 1, 2, 1, 2, missing]
    @test groupcols(gd) == [:A]
    @test valuecols(gd) == [:B]
    gd2 = gd[[2, 1]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 2, 1, 2, 1, missing]
    @test groupcols(gd2) == [:A]
    @test valuecols(gd2) == [:B]

    df2 = DataFrame(A=vcat(df.A, df.A), B=repeat([:X, :Y], inner=6), C=1:12)

    gd = groupby_checked(df2, [:A, :B])
    @inferred groupindices(gd)
    @test groupindices(gd) == [1, 2, 3, 2, 3, 1, 4, 5, 6, 5, 6, 4]
    @test groupcols(gd) == [:A, :B]
    @test valuecols(gd) == [:C]
    gd2 = gd[[3, 2, 5]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 2, 1, 2, 1, missing, missing, 3, missing, 3, missing, missing]
    @test groupcols(gd2) == [:A, :B]
    @test valuecols(gd) == [:C]

    gd = groupby_checked(df2, [:A, :B], skipmissing=true)
    @inferred groupindices(gd)
    @test groupindices(gd) ≅ [missing, 1, 2, 1, 2, missing, missing, 3, 4, 3, 4, missing]
    @test groupcols(gd) == [:A, :B]
    @test valuecols(gd) == [:C]
    gd2 = gd[[4, 2, 1]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 3, 2, 3, 2, missing, missing, missing, 1, missing, 1, missing]
    @test groupcols(gd2) == [:A, :B]
    @test valuecols(gd) == [:C]
end

@testset "non standard cols arguments" begin
    df = DataFrame(x1=Int64[1, 2, 2], x2=Int64[1, 1, 2], y=Int64[1, 2, 3])
    gdf = groupby_checked(df, r"x")
    @test groupcols(gdf) == [:x1, :x2]
    @test valuecols(gdf) == [:y]
    @test groupindices(gdf) == [1, 2, 3]

    gdf = groupby_checked(df, Not(r"x"))
    @test groupcols(gdf) == [:y]
    @test valuecols(gdf) == [:x1, :x2]
    @test groupindices(gdf) == [1, 2, 3]

    gdf = groupby_checked(df, [])
    @test groupcols(gdf) == []
    @test valuecols(gdf) == [:x1, :x2, :y]
    @test groupindices(gdf) == [1, 1, 1]

    gdf = groupby_checked(df, r"z")
    @test groupcols(gdf) == []
    @test valuecols(gdf) == [:x1, :x2, :y]
    @test groupindices(gdf) == [1, 1, 1]

    @test combine(groupby_checked(df, []),
                  :x1 => sum => :a, :x2=>length => :b) == DataFrame(a=5, b=3)

    gdf = groupby_checked(df, [])
    @test isequal_typed(gdf[1], df)
    @test_throws BoundsError gdf[2]
    @test gdf[:] == gdf
    @test gdf[1:1] == gdf

    @test validate_gdf(combine(gdf, nrow => :x1, ungroup=false)) ==
          groupby_checked(DataFrame(x1=3), [])
    @test validate_gdf(combine(gdf, :x2 => identity => :x2_identity, ungroup=false)) ==
          groupby_checked(DataFrame(x2_identity=[1, 1, 2]), [])
    @test isequal_typed(DataFrame(gdf), df)

    # work around automatic trimming of trailing whitespace in most editors
    @test sprint(show, groupby_checked(df, [])) ==
        "GroupedDataFrame with 1 group based on key: \n" *
        "Group 1 (3 rows): \n" *
        """ Row │ x1     x2     y
             │ Int64  Int64  Int64
        ─────┼─────────────────────
           1 │     1      1      1
           2 │     2      1      2
           3 │     2      2      3"""

    df = DataFrame(a=[1, 1, 2, 2, 2], b=1:5)
    gd = groupby_checked(df, :a)
    @test size(combine(gd)) == (0, 1)
    @test names(combine(gd)) == ["a"]
end

@testset "GroupedDataFrame dictionary interface" begin
    df = DataFrame(a=repeat([:A, :B, missing], outer=4), b=repeat(1:2, inner=6), c=1:12)
    gd = groupby_checked(df, [:a, :b])

    @test gd[1] == DataFrame(a=[:A, :A], b=[1, 1], c=[1, 4])

    @test gd[Any[Dict("a" => :A, "b" => 1)]] == gd[[Dict("a" => :A, "b" => 1)]] ==
          gd[[(a=:A, b=1)]]
    @test haskey(gd, Dict("a" => :A, "b" => 1))

    @test map(NamedTuple, keys(gd)) ≅
        [(a=:A, b=1), (a=:B, b=1), (a=missing, b=1), (a=:A, b=2), (a=:B, b=2), (a=missing, b=2)]

    @test collect(pairs(gd)) ≅ map(Pair, keys(gd), gd)

    for (i, key) in enumerate(keys(gd))
        # Plain key
        @test gd[key] ≅ gd[i]
        # Named tuple
        @test gd[NamedTuple(key)] ≅ gd[i]
        # Plain tuple
        @test gd[Tuple(key)] ≅ gd[i]
        # Dict with `Symbol` keys
        @test gd[Dict(key)] ≅ gd[i]
        # Dict with string keys
        @test gd[Dict([String(k) => v for (k, v) in pairs(key)]...)] ≅ gd[i]
        # Dict with AbstractString keys
        @test gd[Dict([Test.GenericString(String(k)) => v for (k, v)  in pairs(key)]...)] ≅ gd[i]
        # Out of order Dict
        @test gd[Dict([k => v for (k, v) in Iterators.reverse(pairs(key))]...)] ≅ gd[i]
        # AbstractDict
        @test gd[Test.GenericDict(Dict(key))] ≅ gd[i]
    end

    # Equivalent value of different type
    @test gd[(a=:A, b=1.0)] ≅ gd[1]

    @test get(gd, (a=:A, b=1), nothing) ≅ gd[1]
    @test get(gd, (a=:A, b=3), nothing) === nothing
    @test get(gd, (:A, 1), nothing) ≅ gd[1]
    @test get(gd, (:A, 3), nothing) === nothing
    @test get(gd, first(keys(gd)), gd) ≅ gd[1]
    @test get(gd, Dict("a" => :A, "b" => 1), nothing) ≅ gd[1]
    @test get(gd, Dict(:a => :A, :b => 1), nothing) ≅ gd[1]
    @test get(gd, Dict(:b => 1, :a => :A), nothing) ≅ gd[1]
    @test get(gd, Dict(:a => :A, :b => 3), nothing) === nothing

    # Wrong values
    @test_throws KeyError gd[(a=:A, b=3)]
    @test_throws KeyError gd[(:A, 3)]
    @test_throws KeyError gd[(a=:A, b="1")]
    @test_throws KeyError gd[Dict(:a => :A, :b => "1")]
    # Wrong length
    @test_throws KeyError gd[(a=:A,)]
    @test_throws KeyError gd[(:A,)]
    @test_throws KeyError gd[(a=:A, b=1, c=1)]
    @test_throws KeyError gd[(:A, 1, 1)]
    @test_throws KeyError gd[Dict(:a => :A, :b => 1, :c => 2)]
    # Out of order
    @test_throws KeyError gd[(b=1, a=:A)]
    @test_throws KeyError gd[(1, :A)]
    # Empty
    @test_throws KeyError gd[()]
    @test_throws KeyError gd[NamedTuple()]
    @test_throws KeyError gd[Dict{String, Any}()]

    # Bad Dict types
    @test_throws ArgumentError gd[Dict()]
    @test_throws ArgumentError gd[Dict(1 => :A, 2 => 1)]
end

@testset "GroupKey and GroupKeys" begin
    df = DataFrame(a=repeat([:A, :B, missing], outer=4),
                   b=repeat([:X, :Y], inner=6), c=1:12)
    cols = [:a, :b]
    gd = groupby_checked(df, cols)
    gdkeys = keys(gd)

    expected = [(a=:A, b=:X), (a=:B, b=:X), (a=missing, b=:X),
                (a=:A, b=:Y), (a=:B, b=:Y), (a=missing, b=:Y)]

    # Check AbstractVector behavior
    @test IndexStyle(gdkeys) === IndexLinear()
    @test length(gdkeys) == length(expected)
    @test size(gdkeys) == size(expected)
    @test eltype(gdkeys) == DataFrames.GroupKey{typeof(gd)}
    @test_throws BoundsError gdkeys[0]
    @test_throws BoundsError gdkeys[length(gdkeys) + 1]

    # Test each key
    cnt = 0
    for (i, key) in enumerate(gdkeys)
        cnt += 1
        nt = expected[i]

        # Check iteration vs indexing of GroupKeys
        @test key ≅ gdkeys[i]

        @test Base.IteratorEltype(key) == Base.EltypeUnknown()

        # Basic methods
        @test parent(key) === gd
        @test length(key) == length(cols)
        @test propertynames(key) == cols
        @test keys(key) == cols
        @test propertynames(key) == cols
        @test propertynames(key, true) == cols
        @test values(key) ≅ values(nt)

        # (Named)Tuple conversion
        @test Tuple(key) ≅ values(nt)
        @test NamedTuple(key) ≅ nt
        @test copy(key) ≅ nt

        # other conversions
        @test Vector(key) ≅ collect(nt)
        @test eltype(Vector(key)) === eltype([v for v in key])
        @test Array(key) ≅ collect(nt)
        @test eltype(Array(key)) === eltype([v for v in key])
        @test Vector{Any}(key) ≅ collect(nt)
        @test eltype(Vector{Any}(key)) === Any
        @test Array{Any}(key) ≅ collect(nt)
        @test eltype(Array{Any}(key)) === Any

        # Iteration
        @test collect(key) ≅ collect(nt)
        @test eltype(collect(key)) == eltype([v for v in key])

        @test_throws ArgumentError identity.(key)

        # Integer/symbol indexing, getproperty of key
        for (j, n) in enumerate(cols)
            @test key[j] ≅ nt[j]
            @test key[n] ≅ nt[j]
            @test getproperty(key, n) ≅ nt[j]
        end

        # Out-of-bounds integer index
        @test_throws BoundsError key[0]
        @test_throws BoundsError key[length(key) + 1]

        # Invalid key/property of key
        @test_throws KeyError key[:foo]
        @test_throws ArgumentError key.foo

        # Using key to index GroupedDataFrame
        @test gd[key] ≅ gd[i]
    end

    # Make sure we actually iterated over all of them
    @test cnt == length(gd)

    # Indexing using another GroupedDataFrame instance should fail
    gd2 = groupby_checked(df, cols, skipmissing=true)
    gd3 = groupby_checked(df, cols, skipmissing=true)
    @test gd2 == gd3  # Use GDF's without missing so they compare equal
    @test_throws ErrorException gd3[first(keys(gd2))]

    # Key equality
    @test collect(keys(gd)) ≅ gdkeys  # These are new instances
    @test all(Ref(gdkeys[1]) .≇ gdkeys[2:end])  # Keys should not be equal to each other
    @test all(collect(keys(gd2)) .≅ keys(gd3))  # Same values but different (but equal) parent

    # Printing of GroupKey
    df = DataFrame(a=repeat([:foo, :bar, :baz], outer=[4]),
                   b=repeat(1:2, outer=[6]),
                   c=1:12)

    gd = groupby_checked(df, [:a, :b])

    gk = keys(gd)
    @test map(repr, gk) == [
        "GroupKey: (a = :foo, b = 1)",
        "GroupKey: (a = :bar, b = 2)",
        "GroupKey: (a = :baz, b = 1)",
        "GroupKey: (a = :foo, b = 2)",
        "GroupKey: (a = :bar, b = 1)",
        "GroupKey: (a = :baz, b = 2)",
    ]


    @test (:foo, 1) in gk
    @test !((:foo, -1) in gk)
    @test (a=:foo, b=1) in gk
    @test gk[1] in gk
    @test 1 in gk
    @test !(0 in gk)
    @test big(1) in gk
    @test !(true in gk)
    @test_throws ArgumentError keys(groupby(DataFrame(x=1), :x))[1] in gk
end

@testset "GroupedDataFrame indexing with array of keys" begin
    df_ref = DataFrame(a=repeat([:A, :B, missing], outer=4),
                       b=repeat(1:2, inner=6), c = 1:12)
    Random.seed!(1234)
    for df in [df_ref, df_ref[randperm(nrow(df_ref)), :]], grpcols = [[:a, :b], :a, :b],
        dosort in (true, false, nothing), doskipmissing in (true, false)

        gd = groupby_checked(df, grpcols, sort=dosort, skipmissing=doskipmissing)

        ints = unique(min.(length(gd), [4, 6, 2, 1]))
        gd2 = gd[ints]
        gkeys = keys(gd)[ints]

        # Test with GroupKeys, Tuples, and NamedTuples
        for converter in [identity, Tuple, NamedTuple, Dict]
            a = converter.(gkeys)
            @test gd[a] ≅ gd2

            # Infer eltype
            @test gd[Array{Any}(a)] ≅ gd2

            # Duplicate keys
            a2 = converter.(keys(gd)[[1, 2, 1]])
            @test_throws ArgumentError gd[a2]
        end
    end
end

@testset "InvertedIndex with GroupedDataFrame" begin
    df = DataFrame(a=repeat([:A, :B, missing], outer=4),
                   b=repeat(1:2, inner=6), c=1:12)
    gd = groupby_checked(df, [:a, :b])

    # Inverted scalar index
    skip_i = 3
    skip_key = keys(gd)[skip_i]
    expected = gd[[i != skip_i for i in 1:length(gd)]]
    expected_inv = gd[[skip_i]]

    for skip in [skip_i, skip_key, Tuple(skip_key), NamedTuple(skip_key)]
        @test gd[Not(skip)] ≅ expected
        # Nested
        @test gd[Not(Not(skip))] ≅ expected_inv
    end

    @test_throws ArgumentError gd[Not(true)]  # Bool <: Integer, but should fail

    # Inverted array index
    skipped = [3, 5, 2]
    skipped_bool = [i ∈ skipped for i in 1:length(gd)]
    skipped_keys = keys(gd)[skipped]
    expected2 = gd[.!skipped_bool]
    expected2_inv = gd[skipped_bool]

    for skip in [skipped, skipped_keys, Tuple.(skipped_keys), NamedTuple.(skipped_keys)]
        @test gd[Not(skip)] ≅ expected2
        # Infer eltype
        @test gd[Not(Array{Any}(skip))] ≅ expected2
        # Nested
        @test gd[Not(Not(skip))] ≅ expected2_inv
        @test gd[Not(Not(Array{Any}(skip)))] ≅ expected2_inv
    end

    # Mixed integer arrays
    @test gd[Not(Any[Unsigned(skipped[1]), skipped[2:end]...])] ≅ expected2
    @test_throws ArgumentError gd[Not(Any[2, true])]

    # Boolean array
    @test gd[Not(skipped_bool)] ≅ expected2
    @test gd[Not(Not(skipped_bool))] ≅ expected2_inv
    @test gd[1:2][Not(false:true)] ≅ gd[[1]]  # Not{AbstractArray{Bool}}

    # Inverted colon
    @test gd[Not(:)] ≅ gd[Int[]]
    @test gd[Not(Not(:))] ≅ gd
end

@testset "GroupedDataFrame array index homogeneity" begin
    df = DataFrame(a=repeat([:A, :B, missing], outer=4),
                   b=repeat(1:2, inner=6), c=1:12)
    gd = groupby_checked(df, [:a, :b])

    # All scalar index types
    idxsets = [1:length(gd), keys(gd), Tuple.(keys(gd)), NamedTuple.(keys(gd))]

    # Mixing index types should fail
    for (i, idxset1) in enumerate(idxsets)
        idx1 = idxset1[1]
        for (j, idxset2) in enumerate(idxsets)
            i == j && continue

            idx2 = idxset2[2]

            # With Any eltype
            a = Any[idx1, idx2]
            @test_throws ArgumentError gd[a]
            @test_throws ArgumentError gd[Not(a)]

            # Most specific applicable eltype, which is <: GroupKeyTypes
            T = Union{typeof(idx1), typeof(idx2)}
            a2 = T[idx1, idx2]
            @test_throws ArgumentError gd[a2]
            @test_throws ArgumentError gd[Not(a2)]
        end
    end
end

@testset "Parent DataFrame names changed" begin
    df = DataFrame(a=repeat([:A, :B, missing], outer=4), b=repeat([:X, :Y], inner=6), c=1:12)
    gd = groupby_checked(df, [:a, :b])

    @test names(gd) == names(df)
    @test groupcols(gd) == [:a, :b]
    @test valuecols(gd) == [:c]
    @test map(NamedTuple, keys(gd)) ≅
        [(a=:A, b=:X), (a=:B, b=:X), (a=missing, b=:X), (a=:A, b=:Y), (a=:B, b=:Y), (a=missing, b=:Y)]
    @test gd[(a=:A, b=:X)] ≅ gd[1]
    @test gd[keys(gd)[1]] ≅ gd[1]
    @test NamedTuple(keys(gd)[1]) == (a=:A, b=:X)
    @test keys(gd)[1].a == :A

    rename!(df, [:d, :e, :f])

    @test names(gd) == names(df)
    @test_throws ErrorException groupcols(gd)
    @test_throws ErrorException valuecols(gd)
    @test_throws ArgumentError map(NamedTuple, keys(gd))
end

@testset "haskey for GroupKey" begin
    gdf = groupby_checked(DataFrame(a=1, b=2, c=3), [:a, :b])
    k = keys(gdf)[1]
    @test !haskey(k, 0)
    @test haskey(k, 1)
    @test haskey(k, 2)
    @test !haskey(k, 3)
    @test haskey(k, :a)
    @test haskey(k, :b)
    @test !haskey(k, :c)
    @test !haskey(k, :d)

    @test !haskey(gdf, 0)
    @test haskey(gdf, 1)
    @test !haskey(gdf, 2)
    @test_throws ArgumentError haskey(gdf, true)

    @test haskey(gdf, k)
    @test_throws ArgumentError haskey(gdf, keys(groupby_checked(DataFrame(a=1, b=2, c=3), [:a, :b]))[1])
    @test_throws BoundsError haskey(gdf, DataFrames.GroupKey(gdf, 0))
    @test_throws BoundsError haskey(gdf, DataFrames.GroupKey(gdf, 2))
    @test haskey(gdf, (1, 2))
    @test !haskey(gdf, (1, 3))
    @test_throws ArgumentError haskey(gdf, (1, 2, 3))
    @test haskey(gdf, (a=1, b=2))
    @test !haskey(gdf, (a=1, b=3))
    @test_throws ArgumentError haskey(gdf, (a=1, c=3))
    @test_throws ArgumentError haskey(gdf, (a=1, c=2))
    @test_throws ArgumentError haskey(gdf, (a=1, b=2, c=3))
end

@testset "filter" begin
    for df in (DataFrame(g1=[1, 3, 2, 1, 4, 1, 2, 5], x1=1:8,
                         g2=[1, 3, 2, 1, 4, 1, 2, 5], x2=1:8),
               view(DataFrame(g1=[1, 3, 2, 1, 4, 1, 2, 5, 4, 5], x1=1:10,
                              g2=[1, 3, 2, 1, 4, 1, 2, 5, 4, 5], x2=1:10, y=1:10),
                    1:8, Not(:y)))
        for gcols in (:g1, [:g1, :g2]), cutoff in (1, 0, 10),
            predicate in (x -> nrow(x) > cutoff,
                          1 => x -> length(x) > cutoff,
                          :x1 => x -> length(x) > cutoff,
                          "x1" => x -> length(x) > cutoff,
                          [1, 2] => (x1, x2) -> length(x1) > cutoff,
                          [:x1, :x2] => (x1, x2) -> length(x1) > cutoff,
                          ["x1", "x2"] => (x1, x2) -> length(x1) > cutoff,
                          r"x" => (x1, x2) -> length(x1) > cutoff,
                          AsTable(:x1) => x -> length(x.x1) > cutoff,
                          AsTable(r"x") => x -> length(x.x1) > cutoff)
            gdf1  = groupby_checked(df, gcols)
            gdf2 = @inferred filter(predicate, gdf1)
            if cutoff == 1
                @test getindex.(keys(gdf2), 1) == 1:2
            elseif cutoff == 0
                @test gdf1 == gdf2
            elseif cutoff == 10
                @test isempty(gdf2)
            end
            @test filter(predicate, gdf1, ungroup=true) == DataFrame(gdf2)
        end

        for ug in (true, false)
            @test_throws TypeError filter(x -> 1, groupby_checked(df, :g1), ungroup=ug)
            @test_throws TypeError filter(r"x" => (x...) -> 1, groupby_checked(df, :g1), ungroup=ug)
            @test_throws TypeError filter(AsTable(r"x") => (x...) -> 1, groupby_checked(df, :g1), ungroup=ug)

            @test_throws ArgumentError filter(r"y" => (x...) -> true, groupby_checked(df, :g1), ungroup=ug)
            @test_throws ArgumentError filter([] => (x...) -> true, groupby_checked(df, :g1), ungroup=ug)
            @test_throws ArgumentError filter(AsTable(r"y") => (x...) -> true, groupby_checked(df, :g1), ungroup=ug)
            @test_throws ArgumentError filter(AsTable([]) => (x...) -> true, groupby_checked(df, :g1), ungroup=ug)
        end
    end
end

@testset "hashing of pooled vectors" begin
    # test both hashrow calculation paths - the of pool length threshold is 50%
    for x in ([1:9; fill(1, 101)], [1:100;],
              [1:9; fill(missing, 101)], [1:99; missing])
        x1 = PooledArray(x);
        x2 = categorical(x);
        @test DataFrames.hashrows((x,), false) ==
              DataFrames.hashrows((x1,), false) ==
              DataFrames.hashrows((x2,), false)
        @test DataFrames.hashrows((x,), true) ==
              DataFrames.hashrows((x1,), true) ==
              DataFrames.hashrows((x2,), true)
    end
end

@testset "grouped data frame iteration" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)
    dfv = @view df[1:3, 1:3]
    gdf = groupby(df, :a)
    gdfv = groupby(dfv, :a)

    for x in (gdf, gdfv)
        @test collect(x)  == [v for v in x] == [x[i] for i in 1:3]
        @test reduce(vcat, x) == parent(x)
        @test mapreduce(v -> sum(Matrix(v)), +, x) == sum(Matrix(parent(x)))
    end
end

@testset "groupby multithreading" begin
    for x in (PooledArray(rand(1:10, 1_100_000)),
              PooledArray(rand([1:9; missing], 1_100_000))),
        y in (PooledArray(rand(["a", "b", "c", "d"], 1_100_000)),
              PooledArray(rand(["a"; "b"; "c"; missing], 1_100_000)))
        df = DataFrame(x=x, y=y)

        # Checks are done by groupby_checked
        @test length(groupby_checked(df, :x)) == 10
        @test length(groupby_checked(df, :x, skipmissing=true)) ==
            length(unique(skipmissing(x)))

        @test length(groupby_checked(df, [:x, :y])) == 40
        @test length(groupby_checked(df, [:x, :y], skipmissing=true)) ==
            length(unique(skipmissing(x))) * length(unique(skipmissing(y)))
    end
end

@testset "map on GroupedDataFrame" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)
    dfv = @view df[1:3, 1:3]
    gdf = groupby(df, :a)
    gdfv = groupby(dfv, :a)

    for x in (gdf, gdfv)
        @test_throws ArgumentError map(identity, x)
    end
end

@testset "grouping correctness with threading" begin
    function cmp_gdf(gdf1::GroupedDataFrame, gdf2::GroupedDataFrame)
        @test gdf1.ngroups == gdf2.ngroups
        @test gdf1.groups == gdf2.groups
        @test gdf1.starts == gdf2.starts
        @test gdf1.ends == gdf2.ends
        @test gdf1.idx == gdf2.idx
    end

    Random.seed!(1234)
    for levs in (100, 99_000), sz in (100_000, 1_100_000)
        df = DataFrame(x_int=rand(1:levs, sz))
        df.x_str = string.(df.x_int, pad=5)
        df.x_pool = PooledArray(df.x_str)
        g_str = groupby_checked(df, :x_str)
        g_pool = groupby_checked(df, :x_pool)
        cmp_gdf(g_str, g_pool)
        g_int = groupby_checked(df, :x_int, sort=true)
        g_str = groupby_checked(df, :x_str, sort=true)
        g_pool = groupby_checked(df, :x_pool, sort=true)
        cmp_gdf(g_int, g_pool)
        cmp_gdf(g_str, g_pool)

        df = df[reverse(1:nrow(df)), :]
        g_str = groupby_checked(df, :x_str, sort=true)
        g_pool = groupby_checked(df, :x_pool, sort=true)
        cmp_gdf(g_str, g_pool)

        df = DataFrame(x_int=[1:levs; rand(1:levs, sz)])
        df.x_str = string.(df.x_int, pad=5)
        df.x_pool = PooledArray(df.x_str)
        allowmissing!(df)
        df[rand(levs+1:sz, 10_000), :] .= missing
        g_str = groupby_checked(df, :x_str)
        g_pool = groupby_checked(df, :x_pool)
        cmp_gdf(g_str, g_pool)
        for sm in (false, true)
            g_str = groupby_checked(df, :x_str, skipmissing=sm)
            g_pool = groupby_checked(df, :x_pool, skipmissing=sm)
            cmp_gdf(g_str, g_pool)
            g_int = groupby_checked(df, :x_int, sort=true, skipmissing=sm)
            g_str = groupby_checked(df, :x_str, sort=true, skipmissing=sm)
            g_pool = groupby_checked(df, :x_pool, sort=true, skipmissing=sm)
            cmp_gdf(g_int, g_pool)
            cmp_gdf(g_str, g_pool)
        end
    end
end

@testset "grouping floats" begin
    @test length(groupby_checked(DataFrame(a=[0.0, -0.0]), :a)) == 2
    @test getindex.(keys(groupby_checked(DataFrame(a=[3.0, 2.0, 0.0]), :a)), 1) ==
          [0, 2, 3]
    @test getindex.(keys(groupby_checked(DataFrame(a=[3.0, 2.0, -0.0]), :a)), 1) ==
          [3, 2, 0]
end

# other tests of subset! are in subset.jl, but for these tests we need
# validate_gdf function defined in this testset
@testset "subset! tests" begin
    df = DataFrame(a=[1, 1, 2, 2], b=[1, 2, 3, 4]);
    gd = groupby_checked(df, :a);
    subset!(gd, :b => x -> x .> first(x))
    validate_gdf(gd)
    @test gd == groupby_checked(df, :a)
    @test sort!(unique(gd.groups)) == 1:length(gd)

    df = DataFrame(a=[1, 1, 2, 2], b=[1, 2, 3, 4]);
    gd = groupby_checked(df, :a)[[2, 1]];
    subset!(gd, :b => x -> x .> first(x))
    validate_gdf(gd)
    @test gd == groupby_checked(df, :a)[[2, 1]]
    @test sort!(unique(gd.groups)) == 1:length(gd)

    df = DataFrame(a=[1, 1, 2, 2], b=[1, 2, 3, 4]);
    gd = groupby_checked(df, :a);
    subset!(gd, :a => x -> x .== 1)
    validate_gdf(gd)
    @test sort!(unique(gd.groups)) == 1:length(gd)
    @test gd == groupby_checked(df, :a)

    df = DataFrame(a=[1, 1, 2, 2], b=[1, 2, 3, 4]);
    gd = groupby_checked(df, :a)[[2, 1]];
    subset!(gd, :b => x -> x .== 1)
    validate_gdf(gd)
    @test sort!(unique(gd.groups)) == 1:length(gd)
    @test gd == groupby_checked(df, :a)

    function issubsequence(v1, v2, l1, l2)
        l1 == 0 && return true
        l2 == 0 && return false
        return issubsequence(v1, v2, l1 - (v1[l1] == v2[l2]), l2 - 1)
    end

    Random.seed!(1234)
    for n in 1:10, j in 1:10, _ in 1:100
        df = DataFrame(a=rand(1:j, n))
        # need to sort to ensure grouping algorithm stability
        gd = groupby_checked(df, :a, sort=true)
        subset!(gd, :a => ByRow(x -> rand() < 0.5))
        validate_gdf(gd)
        @test sort!(unique(gd.groups)) == 1:length(gd)
        @test gd == groupby_checked(df, :a, sort=true)

        # below we do not have a well defined order so just validate gd
        df = DataFrame(a=rand(1:j, n))
        gd = groupby_checked(df, :a)
        subset!(gd, :a => ByRow(x -> rand() < 0.5))
        validate_gdf(gd)
        @test sort!(unique(gd.groups)) == 1:length(gd)

        df = DataFrame(a=rand(1:j, n))
        gd = groupby_checked(df, :a)
        p = randperm(length(gd))
        gd = gd[p]
        superseq = [first(x.a) for x in gd]
        subset!(gd, :a => ByRow(x -> rand() < 0.5))
        validate_gdf(gd)
        @test sort!(unique(gd.groups)) == 1:length(gd)
        subseq = [first(x.a) for x in gd]
        @test issubsequence(subseq, superseq, length(subseq), length(superseq))
    end
end

@testset "consistency check" begin
    df = DataFrame(a=1)
    gdf = groupby_checked(df, :a)
    push!(df, [2])
    @test_throws AssertionError gdf[1]
end

@testset "check sort keyword argument of groupby" begin
    df = DataFrame(id=[3, 1, 2])
    gd = groupby_checked(df, :id, sort=nothing)
    @test Vector.(keys(gd)) == [[1], [2], [3]]
    gd = groupby_checked(df, :id, sort=true)
    @test Vector.(keys(gd)) == [[1], [2], [3]]
    gd = groupby_checked(df, :id, sort=false)
    @test Vector.(keys(gd)) == [[3], [1], [2]]

    df = DataFrame(id=[300, 1, 2])
    gd = groupby_checked(df, :id, sort=nothing)
    @test Vector.(keys(gd)) == [[300], [1], [2]]
    gd = groupby_checked(df, :id, sort=true)
    @test Vector.(keys(gd)) == [[1], [2], [300]]
    gd = groupby_checked(df, :id, sort=false)
    @test Vector.(keys(gd)) == [[300], [1], [2]]

    df = DataFrame(id=["3", "1", "2"])
    gd = groupby_checked(df, :id, sort=nothing)
    @test Vector.(keys(gd)) == [["3"], ["1"], ["2"]]
    gd = groupby_checked(df, :id, sort=true)
    @test Vector.(keys(gd)) == [["1"], ["2"], ["3"]]
    gd = groupby_checked(df, :id, sort=false)
    @test Vector.(keys(gd)) == [["3"], ["1"], ["2"]]

    df.id = PooledArray(df.id)
    gd = groupby_checked(df, :id, sort=nothing)
    @test Vector.(keys(gd)) == [["3"], ["1"], ["2"]]
    gd = groupby_checked(df, :id, sort=true)
    @test Vector.(keys(gd)) == [["1"], ["2"], ["3"]]
    gd = groupby_checked(df, :id, sort=false)
    @test Vector.(keys(gd)) == [["3"], ["1"], ["2"]]

    # for PooledVector sort=true is not the same as sort=nothing
    df.id[1] = "300"
    df.id[3] = "200"
    gd = groupby_checked(df, :id, sort=nothing)
    @test Vector.(keys(gd)) == [["1"], ["300"], ["200"]]
    gd = groupby_checked(df, :id, sort=true)
    @test Vector.(keys(gd)) == [["1"], ["200"], ["300"]]
    gd = groupby_checked(df, :id, sort=false)
    @test Vector.(keys(gd)) == [["300"], ["1"], ["200"]]

    # for CategoricalVector sort=true is the same as sort=nothing
    df = DataFrame(id=categorical(["1", "2", "3"], ordered=true))
    levels!(df.id, ["2", "1", "3"])
    gd = groupby_checked(df, :id, sort=nothing)
    @test Vector.(keys(gd)) == [["2"], ["1"], ["3"]]
    gd = groupby_checked(df, :id, sort=true)
    @test Vector.(keys(gd)) == [["2"], ["1"], ["3"]]
    gd = groupby_checked(df, :id, sort=false)
    @test Vector.(keys(gd)) == [["1"], ["2"], ["3"]]

    df = DataFrame(id1=[2, 2, 1, 1], id2=[1, 2, 2, 1])
    gd = groupby_checked(df, [:id1, :id2], sort=nothing)
    @test Vector.(keys(gd)) == [[1, 1], [1, 2], [2, 1], [2, 2]]
    gd = groupby_checked(df, [:id1, :id2], sort=true)
    @test Vector.(keys(gd)) == [[1, 1], [1, 2], [2, 1], [2, 2]]
    gd = groupby_checked(df, [:id1, :id2], sort=false)
    @test Vector.(keys(gd)) == [[2, 1], [2, 2], [1, 2], [1, 1]]

    df = DataFrame(id1=[200, 200, 1, 1], id2=[1, 2, 2, 1])
    gd = groupby_checked(df, [:id1, :id2], sort=nothing)
    @test Vector.(keys(gd)) == [[200, 1], [200, 2], [1, 2], [1, 1]]
    gd = groupby_checked(df, [:id1, :id2], sort=true)
    @test Vector.(keys(gd)) == [[1, 1], [1, 2], [200, 1], [200, 2]]
    gd = groupby_checked(df, [:id1, :id2], sort=false)
    @test Vector.(keys(gd)) == [[200, 1], [200, 2], [1, 2], [1, 1]]

    df = DataFrame(id=[3, 1, missing, 2])
    gd = groupby_checked(df, :id, sort=nothing, skipmissing=true)
    @test Vector.(keys(gd)) == [[1], [2], [3]]
    gd = groupby_checked(df, :id, sort=true, skipmissing=true)
    @test Vector.(keys(gd)) == [[1], [2], [3]]
    gd = groupby_checked(df, :id, sort=false, skipmissing=true)
    @test Vector.(keys(gd)) == [[3], [1], [2]]
    gd = groupby_checked(df, :id, sort=nothing)
    @test Vector.(keys(gd)) ≅ [[1], [2], [3], [missing]]
    gd = groupby_checked(df, :id, sort=true)
    @test Vector.(keys(gd)) ≅ [[1], [2], [3], [missing]]
    gd = groupby_checked(df, :id, sort=false)
    @test Vector.(keys(gd)) ≅ [[3], [1], [missing], [2]]

    df = DataFrame(id=[300, missing, 1, 2])
    gd = groupby_checked(df, :id, sort=nothing, skipmissing=true)
    @test Vector.(keys(gd)) == [[300], [1], [2]]
    gd = groupby_checked(df, :id, sort=true, skipmissing=true)
    @test Vector.(keys(gd)) == [[1], [2], [300]]
    gd = groupby_checked(df, :id, sort=false, skipmissing=true)
    @test Vector.(keys(gd)) == [[300], [1], [2]]
    gd = groupby_checked(df, :id, sort=nothing)
    @test Vector.(keys(gd)) ≅ [[300], [missing], [1], [2]]
    gd = groupby_checked(df, :id, sort=true)
    @test Vector.(keys(gd)) ≅ [[1], [2], [300], [missing]]
    gd = groupby_checked(df, :id, sort=false)
    @test Vector.(keys(gd)) ≅ [[300], [missing], [1], [2]]
end

@testset "fillfirst! correctness tests" begin
    Random.seed!(1234)
    for len in 0:100
        for df in [DataFrame(id=rand(1:5, len)), DataFrame(id=rand(string.(1:5), len))]
            global gdf = groupby(df, :id);
            @assert getfield(gdf, :idx) === nothing
            x1 = fill(-1, length(gdf))
            DataFrames.fillfirst!(nothing, x1, 1:length(gdf.groups), gdf)
            @assert getfield(gdf, :idx) === nothing
            @test length(gdf.idx) >= 0
            @assert getfield(gdf, :idx) !== nothing
            x2 = fill(-1, length(gdf))
            DataFrames.fillfirst!(nothing, x2, 1:length(gdf.groups), gdf)
            @test x1 == x2
        end
    end
end

@testset "corner cases of indexing" begin
    df = DataFrame(id=1:4)
    gdf = groupby_checked(df, :id)
    @test_throws ArgumentError gdf[CartesianIndex(1)]
    @test_throws ArgumentError gdf[CartesianIndex(1, 1)]
    @test_throws ArgumentError gdf[[CartesianIndex(1)]]
    @test_throws ArgumentError gdf[[CartesianIndex(1, 1)]]
    @test_throws ArgumentError gdf[Any[CartesianIndex(1)]]
    @test_throws ArgumentError gdf[Any[CartesianIndex(1, 1)]]

    @test_throws ArgumentError gdf[Not(CartesianIndex(1))]
    @test_throws ArgumentError gdf[Not(CartesianIndex(1, 1))]
    @test_throws ArgumentError gdf[Not([CartesianIndex(1)])]
    @test_throws ArgumentError gdf[Not([CartesianIndex(1, 1)])]
    @test_throws ArgumentError gdf[Not(Any[CartesianIndex(1)])]
    @test_throws ArgumentError gdf[Not(Any[CartesianIndex(1, 1)])]

    @test_throws BoundsError gdf[[true]]
    @test_throws BoundsError gdf[Not([true])]
    @test_throws BoundsError gdf[trues(1)]
    @test_throws BoundsError gdf[Not(trues(1))]
    @test_throws BoundsError gdf[view([true], 1:1)]
    @test_throws BoundsError gdf[Not(view([true], 1:1))]
    @test_throws BoundsError gdf[[true true true true]]
    @test_throws ArgumentError gdf[Not([true true true true])]
end

include("groupby_operations.jl")

end # module

