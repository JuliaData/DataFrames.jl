module TestGrouping

using Test, DataFrames, Random, Statistics, PooledArrays
const ≅ = isequal

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

function groupby_checked(df::AbstractDataFrame, keys, args...; kwargs...)
    gd = groupby(df, keys, args...; kwargs...)

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
    end

    gd
end

@testset "parent" begin
    df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8])
    gd = groupby(df, :a)
    @test parent(gd) === df
end

@testset "accepted columns" begin
    df = DataFrame(A=[1,1,1,2,2,2], B=[1,2,1,2,1,2], C=1:6)
    @test groupby(df, [1,2]) == groupby(df, 1:2) == groupby(df, [:A, :B])
    @test groupby(df, [2,1]) == groupby(df, 2:-1:1) == groupby(df, [:B, :A])
end

@testset "by, groupby and map(::Function, ::GroupedDataFrame)" begin
    Random.seed!(1)
    df = DataFrame(a = repeat(Union{Int, Missing}[1, 3, 2, 4], outer=[2]),
                   b = repeat(Union{Int, Missing}[2, 1], outer=[4]),
                   c = repeat([0, 1], outer=[4]),
                   x = Vector{Union{Float64, Missing}}(randn(8)))

    f1(df) = DataFrame(xmax = maximum(df.x))
    f2(df) = (xmax = maximum(df.x),)
    f3(df) = maximum(df.x)
    f4(df) = [maximum(df.x), minimum(df.x)]
    f5(df) = reshape([maximum(df.x), minimum(df.x)], 2, 1)
    f6(df) = [maximum(df.x) minimum(df.x)]
    f7(df) = (x2 = df.x.^2,)
    f8(df) = DataFrame(x2 = df.x.^2)

    for cols in ([:a, :b], [:b, :a], [:a, :c], [:c, :a],
                 [1, 2], [2, 1], [1, 3], [3, 1],
                 [true, true, false, false], [true, false, true, false])
        colssym = names(df[cols])
        hcatdf = hcat(df[cols], df, makeunique=true)
        nms = names(hcatdf)
        res = unique(df[cols])
        res.xmax = [maximum(df[(df[colssym[1]] .== a) .& (df[colssym[2]] .== b), :x])
                    for (a, b) in zip(res[colssym[1]], res[colssym[2]])]
        res2 = unique(df[cols])[repeat(1:4, inner=2), :]
        res2.x1 = collect(Iterators.flatten(
            [[maximum(df[(df[colssym[1]] .== a) .& (df[colssym[2]] .== b), :x]),
              minimum(df[(df[colssym[1]] .== a) .& (df[colssym[2]] .== b), :x])]
             for (a, b) in zip(res[colssym[1]], res[colssym[2]])]))
        res3 = unique(df[cols])
        res3.x1 = [maximum(df[(df[colssym[1]] .== a) .& (df[colssym[2]] .== b), :x])
                   for (a, b) in zip(res[colssym[1]], res[colssym[2]])]
        res3.x2 = [minimum(df[(df[colssym[1]] .== a) .& (df[colssym[2]] .== b), :x])
                   for (a, b) in zip(res[colssym[1]], res[colssym[2]])]
        res4 = df[cols]
        res4.x2 = df.x.^2
        shcatdf = sort(hcatdf, colssym)
        sres = sort(res, colssym)
        sres2 = sort(res2, colssym)
        sres3 = sort(res3, colssym)
        sres4 = sort(res4, colssym)

        # by() without groups sorting
        @test sort(by(df, cols, identity), colssym) == shcatdf
        @test sort(by(df, cols, df -> df[1, :]), colssym) ==
            shcatdf[.!nonunique(shcatdf, colssym), :]
        @test by(df, cols, f1) == res
        @test by(df, cols, f2) == res
        @test rename(by(df, cols, f3), :x1 => :xmax) == res
        @test by(df, cols, f4) == res2
        @test by(df, cols, f5) == res2
        @test by(df, cols, f6) == res3
        @test sort(by(df, cols, f7), colssym) == sres4
        @test sort(by(df, cols, f8), colssym) == sres4

        # by() with groups sorting
        @test by(df, cols, identity, sort=true) == shcatdf
        @test by(df, cols, df -> df[1, :], sort=true) ==
            shcatdf[.!nonunique(shcatdf, colssym), :]
        @test by(df, cols, f1, sort=true) == sres
        @test by(df, cols, f2, sort=true) == sres
        @test rename(by(df, cols, f3, sort=true), :x1 => :xmax) == sres
        @test by(df, cols, f4, sort=true) == sres2
        @test by(df, cols, f5, sort=true) == sres2
        @test by(df, cols, f6, sort=true) == sres3
        @test by(df, cols, f7, sort=true) == sres4
        @test by(df, cols, f8, sort=true) == sres4

        @test by(df, [:a], f1) == by(df, :a, f1)
        @test by(df, [:a], f1, sort=true) == by(df, :a, f1, sort=true)

        # groupby() without groups sorting
        gd = groupby_checked(df, cols)
        @test names(parent(gd))[gd.cols] == colssym
        df_comb = combine(identity, gd)
        @test sort(df_comb, colssym) == shcatdf
        df_ref = DataFrame(gd)
        @test sort(hcat(df_ref[cols], df_ref, makeunique=true), colssym) == shcatdf
        @test df_ref.x == df_comb.x
        @test combine(f1, gd) == res
        @test combine(f2, gd) == res
        @test rename(combine(f3, gd), :x1 => :xmax) == res
        @test combine(f4, gd) == res2
        @test combine(f5, gd) == res2
        @test combine(f6, gd) == res3
        @test sort(combine(f7, gd), colssym) == sort(res4, colssym)
        @test sort(combine(f8, gd), colssym) == sort(res4, colssym)

        # groupby() with groups sorting
        gd = groupby_checked(df, cols, sort=true)
        @test names(parent(gd))[gd.cols] == colssym
        for i in 1:length(gd)
            @test all(gd[i][colssym[1]] .== sres[i, colssym[1]])
            @test all(gd[i][colssym[2]] .== sres[i, colssym[2]])
        end
        @test combine(identity, gd) == shcatdf
        df_ref = DataFrame(gd)
        @test hcat(df_ref[cols], df_ref, makeunique=true) == shcatdf
        @test combine(f1, gd) == sres
        @test combine(f2, gd) == sres
        @test rename(combine(f3, gd), :x1 => :xmax) == sres
        @test combine(f4, gd) == sres2
        @test combine(f5, gd) == sres2
        @test combine(f6, gd) == sres3
        @test combine(f7, gd) == sres4
        @test combine(f8, gd) == sres4

        # map() without and with groups sorting
        for sort in (false, true)
            gd = groupby_checked(df, cols, sort=sort)
            v = map(d -> d[[:x]], gd)
            @test length(gd) == length(v)
            nms = [colssym; :x]
            @test v[1] == gd[1][nms]
            @test v[1] == gd[1][nms] &&
                v[2] == gd[2][nms] &&
                v[3] == gd[3][nms] &&
                v[4] == gd[4][nms]
            @test names(parent(v))[v.cols] == colssym
            v = map(f1, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f1, df, cols, sort=sort)
            v = map(f2, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f2, df, cols, sort=sort)
            v = map(f3, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f3, df, cols, sort=sort)
            v = map(f4, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f4, df, cols, sort=sort)
            v = map(f5, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f5, df, cols, sort=sort)
            v = map(f5, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f5, df, cols, sort=sort)
            v = map(f6, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f6, df, cols, sort=sort)
            v = map(f7, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f7, df, cols, sort=sort)
            v = map(f8, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f8, df, cols, sort=sort)
        end
    end

    # test number of potential combinations higher than typemax(Int32)
    N = 2000
    df2 = DataFrame(v1 = levels!(categorical(rand(1:N, 100)), collect(1:N)),
                    v2 = levels!(categorical(rand(1:N, 100)), collect(1:N)),
                    v3 = levels!(categorical(rand(1:N, 100)), collect(1:N)))
    df2b = mapcols(Vector{Int}, df2)
    @test groupby_checked(df2, [:v1, :v2, :v3]) ==
        groupby_checked(df2b, [:v1, :v2, :v3])

    # grouping empty table
    @test groupby_checked(DataFrame(A=Int[]), :A).starts == Int[]
    # grouping single row
    @test groupby_checked(DataFrame(A=Int[1]), :A).starts == Int[1]

    # issue #960
    x = CategoricalArray(collect(1:20))
    df = DataFrame(v1=x, v2=x)
    groupby_checked(df, [:v1, :v2])

    df2 = by(e->1, DataFrame(x=Int64[]), :x)
    @test size(df2) == (0, 1)
    @test sum(df2[:x]) == 0

    # Check that reordering levels does not confuse groupby
    for df in (DataFrame(Key1 = CategoricalArray(["A", "A", "B", "B", "B", "A"]),
                         Key2 = CategoricalArray(["A", "B", "A", "B", "B", "A"]),
                         Value = 1:6),
                DataFrame(Key1 = PooledArray(["A", "A", "B", "B", "B", "A"]),
                          Key2 = PooledArray(["A", "B", "A", "B", "B", "A"]),
                          Value = 1:6))
        gd = groupby_checked(df, :Key1)
        @test length(gd) == 2
        @test gd[1] == DataFrame(Key1="A", Key2=["A", "B", "A"], Value=[1, 2, 6])
        @test gd[2] == DataFrame(Key1="B", Key2=["A", "B", "B"], Value=[3, 4, 5])
        gd = groupby_checked(df, [:Key1, :Key2])
        @test length(gd) == 4
        @test gd[1] == DataFrame(Key1="A", Key2="A", Value=[1, 6])
        @test gd[2] == DataFrame(Key1="A", Key2="B", Value=2)
        @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
        @test gd[4] == DataFrame(Key1="B", Key2="B", Value=[4, 5])
        # Reorder levels, add unused level
        _levels!(df.Key1, ["Z", "B", "A"])
        _levels!(df.Key2, ["Z", "B", "A"])
        gd = groupby_checked(df, :Key1)
        @test gd == groupby_checked(df, :Key1, skipmissing=true)
        @test length(gd) == 2
        if df.Key1 isa CategoricalVector
            @test gd[1] == DataFrame(Key1="B", Key2=["A", "B", "B"], Value=[3, 4, 5])
            @test gd[2] == DataFrame(Key1="A", Key2=["A", "B", "A"], Value=[1, 2, 6])
        else
            @test gd[1] == DataFrame(Key1="A", Key2=["A", "B", "A"], Value=[1, 2, 6])
            @test gd[2] == DataFrame(Key1="B", Key2=["A", "B", "B"], Value=[3, 4, 5])
        end
        gd = groupby_checked(df, [:Key1, :Key2])
        @test gd == groupby_checked(df, [:Key1, :Key2], skipmissing=true)
        @test length(gd) == 4
        if df.Key1 isa CategoricalVector
            @test gd[1] == DataFrame(Key1="B", Key2="B", Value=[4, 5])
            @test gd[2] == DataFrame(Key1="B", Key2="A", Value=3)
            @test gd[3] == DataFrame(Key1="A", Key2="B", Value=2)
            @test gd[4] == DataFrame(Key1="A", Key2="A", Value=[1, 6])
        else
            @test gd[1] == DataFrame(Key1="A", Key2="A", Value=[1, 6])
            @test gd[2] == DataFrame(Key1="A", Key2="B", Value=2)
            @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
            @test gd[4] == DataFrame(Key1="B", Key2="B", Value=[4, 5])
        end
        # Make first level unused too
        replace!(df.Key1, "A"=>"B")
        gd = groupby_checked(df, :Key1)
        @test length(gd) == 1
        @test gd[1] == DataFrame(Key1="B", Key2=["A", "B", "A", "B", "B", "A"], Value=1:6)
        gd = groupby_checked(df, [:Key1, :Key2])
        @test gd == groupby_checked(df, [:Key1, :Key2])
        @test length(gd) == 2
        if df.Key1 isa CategoricalVector
            @test gd[1] == DataFrame(Key1="B", Key2="B", Value=[2, 4, 5])
            @test gd[2] == DataFrame(Key1="B", Key2="A", Value=[1, 3, 6])
        else
            @test gd[1] == DataFrame(Key1="B", Key2="A", Value=[1, 3, 6])
            @test gd[2] == DataFrame(Key1="B", Key2="B", Value=[2, 4, 5])
        end
    end

    df = DataFrame(Key1 = CategoricalArray(["A", "A", "B", "B", "B", "A"]),
                    Key2 = CategoricalArray(["A", "B", "A", "B", "B", "A"]),
                    Value = 1:6)

    # Check that CategoricalArray column is preserved when returning a value...
    res = combine(d -> DataFrame(x=d[1, :Key2]), groupby_checked(df, :Key1))
    @test typeof(res.x) == typeof(df.Key2)
    res = combine(d -> (x=d[1, :Key2],), groupby_checked(df, :Key1))
    @test typeof(res.x) == typeof(df.Key2)
    # ...and when returning an array
    res = combine(d -> DataFrame(x=d[:Key1]), groupby_checked(df, :Key1))
    @test typeof(res.x) == typeof(df.Key1)

    # Check that CategoricalArray and String give a String...
    res = combine(d -> d.Key1 == ["A", "A"] ? DataFrame(x=d[1, :Key1]) : DataFrame(x="C"),
                  groupby_checked(df, :Key1))
    @test res.x isa Vector{String}
    res = combine(d -> d.Key1 == ["A", "A"] ? (x=d[1, :Key1],) : (x="C",),
                  groupby_checked(df, :Key1))
    @test res.x isa Vector{String}
    # ...even when CategoricalString comes second
    res = combine(d -> d.Key1 == ["B", "B"] ? DataFrame(x=d[1, :Key1]) : DataFrame(x="C"),
                  groupby_checked(df, :Key1))
    @test res.x isa Vector{String}
    res = combine(d -> d.Key1 == ["B", "B"] ? (x=d[1, :Key1],) : (x="C",),
                  groupby_checked(df, :Key1))
    @test res.x isa Vector{String}

    df = DataFrame(x = [1, 2, 3], y = [2, 3, 1])

    # Test function returning DataFrameRow
    res = by(d -> DataFrameRow(d, 1, :), df, :x)
    @test res == DataFrame(x=df.x, x_1=df.x, y=df.y)

    # Test function returning Tuple
    res = by(d -> (sum(d.y),), df, :x)
    @test res == DataFrame(x=df.x, x1=tuple.([2, 3, 1]))

    # Test with some groups returning empty data frames
    @test by(d -> d.x == [1] ? DataFrame(z=[]) : DataFrame(z=1), df, :x) ==
        DataFrame(x=[2, 3], z=[1, 1])
    v = map(d -> d.x == [1] ? DataFrame(z=[]) : DataFrame(z=1), groupby_checked(df, :x))
    @test length(v) == 2
    @test vcat(v[1], v[2]) == DataFrame(x=[2, 3], z=[1, 1])

    # Test that returning values of different types works with NamedTuple
    res = by(d -> d.x == [1] ? 1 : 2.0, df, :x)
    @test res.x1 isa Vector{Float64}
    @test res.x1 == [1, 2, 2]
    # Two columns need to be widened at different times
    res = by(d -> (a=d.x == [1] ? 1 : 2.0, b=d.x == [3] ? missing : "a"), df, :x)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String,Missing}}
    @test res.b ≅ ["a", "a", missing]
    # Corner case: two columns need to be widened at the same time
    res = by(d -> (a=d.x == [1] ? 1 : 2.0, b=d.x == [1] ? missing : "a"), df, :x)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String,Missing}}
    @test res.b ≅ [missing, "a", "a"]

    # Test that returning values of different types works with DataFrame
    res = by(d -> DataFrame(x1 = d.x == [1] ? 1 : 2.0), df, :x)
    @test res.x1 isa Vector{Float64}
    @test res.x1 == [1, 2, 2]
    # Two columns need to be widened at different times
    res = by(d -> DataFrame(a=d.x == [1] ? 1 : 2.0, b=d.x == [3] ? missing : "a"), df, :x)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String,Missing}}
    @test res.b ≅ ["a", "a", missing]
    # Corner case: two columns need to be widened at the same time
    res = by(d -> DataFrame(a=d.x == [1] ? 1 : 2.0, b=d.x == [1] ? missing : "a"), df, :x)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String,Missing}}
    @test res.b ≅ [missing, "a", "a"]

    # Test return values with columns in different orders
    @test by(d -> d.x == [1] ? (x1=1, x2=3) : (x2=2, x1=4), df, :x) ==
        DataFrame(x=1:3, x1=[1, 4, 4], x2=[3, 2, 2])
    @test by(d -> d.x == [1] ? DataFrame(x1=1, x2=3) : DataFrame(x2=2, x1=4), df, :x) ==
        DataFrame(x=1:3, x1=[1, 4, 4], x2=[3, 2, 2])

    # Test with NamedTuple with columns of incompatible lengths
    @test_throws DimensionMismatch by(d -> (x1=[1], x2=[3, 4]), df, :x)
    @test_throws DimensionMismatch by(d -> d.x == [1] ? (x1=[1], x2=[3]) :
                                                        (x1=[1], x2=[3, 4]), df, :x)

    # Test with incompatible return values
    @test_throws ArgumentError by(d -> d.x == [1] ? (x1=1,) : DataFrame(x1=1), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? DataFrame(x1=1) : (x1=1,), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? NamedTuple() : (x1=1), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? (x1=1) : NamedTuple(), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? 1 : DataFrame(x1=1), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? DataFrame(x1=1) : 1, df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? DataFrame() : DataFrame(x1=1), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? DataFrame(x1=1) : DataFrame(), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? (x1=1) : (x1=[1]), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? (x1=[1]) : (x1=1), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? 1 : [1], df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? [1] : 1, df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? (x1=1, x2=1) : (x1=[1], x2=1), df, :x)
    @test_throws ArgumentError by(d -> d.x == [1] ? (x1=[1], x2=1) : (x1=1, x2=1), df, :x)
    # Special case allowed due to how implementation works
    @test by(d -> d.x == [1] ? 1 : (x1=1), df, :x) == by(d -> 1, df, :x)

    # Test that columns names and types are respected for empty input
    df = DataFrame(x=Int[], y=String[])
    res = by(d -> 1, df, :x)
    @test size(res) == (0, 1)
    @test res.x isa Vector{Int}

    # Test with empty data frame
    df = DataFrame(x=[], y=[])
    gd = groupby_checked(df, :x)
    @test combine(df -> sum(df.x), gd) == DataFrame(x=[])
    res = map(df -> sum(df.x), gd)
    @test length(res) == 0
    @test res.parent == DataFrame(x=[])
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
        df = DataFrame(Key1 = x, Key2 = y, Value = 1:8)

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
        df = DataFrame(Key1 = x, Key2 = y, Key3 = z, Value = string.(1:100))
        dfb = mapcols(Vector{Union{String, Missing}}, df)

        gd = groupby_checked(df, [:Key1, :Key2, :Key3], sort=true)
        dfs = [groupby_checked(dfb, [:Key1, :Key2, :Key3], sort=true)...]
        @test isequal_unordered(gd, dfs)
        @test issorted(vcat(gd...), [:Key1, :Key2, :Key3])
        gd = groupby_checked(df, [:Key1, :Key2, :Key3], sort=true, skipmissing=true)
        dfs = [groupby_checked(dfb, [:Key1, :Key2, :Key3], sort=true, skipmissing=true)...]
        @test isequal_unordered(gd, dfs)
        @test issorted(vcat(gd...), [:Key1, :Key2, :Key3])

        # This is an implementation detail but it allows checking
        # that the optimized method is used
        if df.Key1 isa CategoricalVector &&
            df.Key2 isa CategoricalVector &&
            df.Key3 isa CategoricalVector
            @test groupby_checked(df, [:Key1, :Key2, :Key3], sort=true) ≅
                groupby_checked(df, [:Key1, :Key2, :Key3], sort=false)
            @test groupby_checked(df, [:Key1, :Key2, :Key3], sort=true, skipmissing=true) ≅
                groupby_checked(df, [:Key1, :Key2, :Key3], sort=false, skipmissing=true)
        end
    end
end

@testset "by, combine and map with pair interface" begin
    vexp = x -> exp.(x)
    Random.seed!(1)
    df = DataFrame(a = repeat([1, 3, 2, 4], outer=[2]),
                   b = repeat([2, 1], outer=[4]),
                   c = rand(Int, 8))

    # Only test that different by syntaxes work,
    # and rely on tests below for deeper checks
    @test by(:c => sum, df, :a) ==
        by(df, :a, :c => sum) ==
        by(df, :a, (:c => sum,)) ==
        by(df, :a, [:c => sum]) ==
        by(df, :a, c_sum = :c => sum) ==
        by(d -> (c_sum=sum(d.c),), df, :a)
        by(df, :a, d -> (c_sum=sum(d.c),))

    @test by(:c => vexp, df, :a) ==
        by(df, :a, :c => vexp) ==
        by(df, :a, (:c => vexp,)) ==
        by(df, :a, [:c => vexp]) ==
        by(df, :a, c_function = :c => vexp) ==
        by(d -> (c_function=vexp(d.c),), df, :a)
        by(df, :a, d -> (c_function=vexp(d.c),))

    @test by(df, :a, :b => sum, :c => sum) ==
        by(df, :a, (:b => sum, :c => sum,)) ==
        by(df, :a, [:b => sum, :c => sum]) ==
        by(df, :a, b_sum = :b => sum, c_sum = :c => sum) ==
        by(d -> (b_sum=sum(d.b), c_sum=sum(d.c)), df, :a)
        by(df, :a, d -> (b_sum=sum(d.b), c_sum=sum(d.c)))

    @test by(df, :a, :b => vexp, :c => identity) ==
        by(df, :a, (:b => vexp, :c => identity,)) ==
        by(df, :a, [:b => vexp, :c => identity]) ==
        by(df, :a, b_function = :b => vexp, c_identity = :c => identity) ==
        by(d -> (b_function=vexp(d.b), c_identity=identity(d.c)), df, :a)
        by(df, :a, d -> (b_function=vexp(d.b), c_identity=identity(d.c)))

    gd = groupby(df, :a)

    # Only test that different combine syntaxes work,
    # and rely on tests below for deeper checks
    @test combine(:c => sum, gd) ==
        combine(gd, :c => sum) ==
        combine(gd, (:c => sum,)) ==
        combine(gd, [:c => sum]) ==
        combine(gd, c_sum = :c => sum) ==
        combine(:c => x -> (c_sum=sum(x),), gd) ==
        combine(gd, :c => x -> (c_sum=sum(x),)) ==
        combine(d -> (c_sum=sum(d.c),), gd) ==
        combine(gd, d -> (c_sum=sum(d.c),))

    @test combine(:c => vexp, gd) ==
        combine(gd, :c => vexp) ==
        combine(gd, (:c => vexp,)) ==
        combine(gd, [:c => vexp]) ==
        combine(gd, c_function = :c => vexp) ==
        combine(:c => x -> (c_function=exp.(x),), gd) ==
        combine(gd, :c => x -> (c_function=exp.(x),)) ==
        combine(d -> (c_function=exp.(d.c),), gd)
        combine(gd, d -> (c_function=exp.(d.c),))

    @test combine(gd, :b => sum, :c => sum) ==
        combine(gd, (:b => sum, :c => sum,)) ==
        combine(gd, [:b => sum, :c => sum]) ==
        combine(gd, b_sum = :b => sum, c_sum = :c => sum) ==
        combine((:b, :c) => x -> (b_sum=sum(x.b), c_sum=sum(x.c)), gd) ==
        combine(gd, (:b, :c) => x -> (b_sum=sum(x.b), c_sum=sum(x.c))) ==
        combine(d -> (b_sum=sum(d.b), c_sum=sum(d.c)), gd) ==
        combine(gd, d -> (b_sum=sum(d.b), c_sum=sum(d.c)))

    @test combine(gd, :b => vexp, :c => identity) ==
        combine(gd, (:b => vexp, :c => identity,)) ==
        combine(gd, [:b => vexp, :c => identity]) ==
        combine(gd, b_function = :b => vexp, c_identity = :c => identity) ==
        combine((:b, :c) => x -> (b_function=vexp(x.b), c_identity=x.c), gd) ==
        combine(gd, (:b, :c) => x -> (b_function=vexp(x.b), c_identity=x.c)) ==
        combine(d -> (b_function=vexp(d.b), c_identity=d.c), gd) ==
        combine(gd, d -> (b_function=vexp(d.b), c_identity=d.c))

    for f in (map, combine)
        for col in (:c, 3)
            @test f(col => sum, gd) == f(d -> (c_sum=sum(d.c),), gd)
            @test f(col => x -> sum(x), gd) == f(d -> (c_function=sum(d.c),), gd)
            @test f(col => x -> (z=sum(x),), gd) == f(d -> (z=sum(d.c),), gd)
            @test f(col => x -> DataFrame(z=sum(x),), gd) == f(d -> (z=sum(d.c),), gd)
            @test f(col => identity, gd) == f(d -> (c_identity=d.c,), gd)
            @test f(col => x -> (z=x,), gd) == f(d -> (z=d.c,), gd)

            @test f((xyz = col => sum,), gd) ==
                f(d -> (xyz=sum(d.c),), gd)
            @test f((xyz = col => x -> sum(x),), gd) ==
                f(d -> (xyz=sum(d.c),), gd)
            @test f((xyz = col => x -> (sum(x),),), gd) ==
                f(d -> (xyz=(sum(d.c),),), gd)
            @test_throws ArgumentError f((xyz = col => x -> (z=sum(x),),), gd)
            @test_throws ArgumentError f((xyz = col => x -> DataFrame(z=sum(x),),), gd)
            @test_throws ArgumentError f((xyz = col => x -> (z=x,),), gd)
            @test_throws ArgumentError f(col => x -> (z=1, xzz=[1]), gd)

            for wrap in (vcat, tuple)
                @test f(wrap(col => sum), gd) ==
                    f(d -> (c_sum=sum(d.c),), gd)
                @test f(wrap(col => x -> sum(x)), gd) ==
                    f(d -> (c_function=sum(d.c),), gd)
                @test f(wrap(col => x -> (sum(x),)), gd) ==
                    f(d -> (c_function=(sum(d.c),),), gd)
                @test_throws ArgumentError f(wrap(col => x -> (z=sum(x),)), gd)
                @test_throws ArgumentError f(wrap(col => x -> DataFrame(z=sum(x),)), gd)
                @test_throws ArgumentError f(wrap(col => x -> (z=x,)), gd)
                @test_throws ArgumentError f(wrap(col => x -> (z=1, xzz=[1])), gd)
            end
        end
        for cols in ((:b, :c), [:b, :c], (2, 3), 2:3, [2, 3], [false, true, true])
            @test f(cols => x -> (y=exp.(x.b), z=x.c), gd) ==
                f(d -> (y=exp.(d.b), z=d.c), gd)
            @test f(cols => x -> [exp.(x.b) x.c], gd) ==
                f(d -> [exp.(d.b) d.c], gd)

            @test f((xyz = cols => x -> sum(x.b) + sum(x.c),), gd) ==
                f(d -> (xyz=sum(d.b) + sum(d.c),), gd)
            if eltype(cols) === Bool
                cols2 = [[false, true, false], [false, false, true]]
                @test_throws MethodError f((xyz = cols[1] => sum, xzz = cols2[2] => sum), gd)
                @test_throws MethodError f((xyz = cols[1] => sum, xzz = cols2[1] => sum), gd)
                @test_throws MethodError f((xyz = cols[1] => sum, xzz = cols2[2] => x -> first(x)), gd)
            else
                cols2 = cols
                @test f((xyz = cols2[1] => sum, xzz = cols2[2] => sum), gd) ==
                    f(d -> (xyz=sum(d.b), xzz=sum(d.c)), gd)
                @test f((xyz = cols2[1] => sum, xzz = cols2[1] => sum), gd) ==
                    f(d -> (xyz=sum(d.b), xzz=sum(d.b)), gd)
                @test f((xyz = cols2[1] => sum, xzz = cols2[2] => x -> first(x)), gd) ==
                    f(d -> (xyz=sum(d.b), xzz=first(d.c)), gd)
                @test_throws ArgumentError f((xyz = cols2[1] => vexp, xzz = cols2[2] => sum), gd)
            end

            @test_throws ArgumentError f(cols => x -> (y=exp.(x.b), z=sum(x.c)), gd)
            @test_throws ArgumentError f((xyz = cols2 => x -> DataFrame(y=exp.(x.b), z=sum(x.c)),), gd)
            @test_throws ArgumentError f((xyz = cols2 => x -> [exp.(x.b) x.c],), gd)

            for wrap in (vcat, tuple)
                @test f(wrap(cols => x -> sum(x.b) + sum(x.c)), gd) ==
                    f(d -> sum(d.b) + sum(d.c), gd)

                if eltype(cols) === Bool
                    cols2 = [[false, true, false], [false, false, true]]
                    @test f(wrap(cols2[1] => x -> sum(x.b), cols2[2] => x -> sum(x.c)), gd) ==
                        f(d -> (x1=sum(d.b), x2=sum(d.c)), gd)
                    @test f(wrap(cols2[1] => x -> sum(x.b), cols2[2] => x -> first(x.c)), gd) ==
                        f(d -> (x1=sum(d.b), x2=first(d.c)), gd)
                else
                    cols2 = cols
                    @test f(wrap(cols[1] => sum, cols[2] => sum), gd) ==
                        f(d -> (b_sum=sum(d.b), c_sum=sum(d.c)), gd)
                    @test f(wrap(cols[1] => sum, cols[2] => x -> first(x)), gd) ==
                        f(d -> (b_sum=sum(d.b), c_function=first(d.c)), gd)
                    @test_throws ArgumentError f(wrap(cols2[1] => vexp, cols2[2] => sum), gd)
                end

                @test_throws ArgumentError f(wrap(cols => x -> DataFrame(y=exp.(x.b), z=sum(x.c))), gd)
                @test_throws ArgumentError f(wrap(cols => x -> [exp.(x.b) x.c]), gd)
            end
        end
    end
end

struct TestType end
Base.isless(::TestType, ::Int) = true
Base.isless(::Int, ::TestType) = false
Base.isless(::TestType, ::TestType) = false

@testset "combine with aggregation functions" begin
    Random.seed!(1)
    df = DataFrame(a = rand(1:5, 20), x1 = rand(Int, 20), x2 = rand(Complex{Int}, 20))

    for f in (sum, prod, maximum, minimum, mean, var, std, first, last, length)
        gd = groupby_checked(df, :a)

        res = combine(gd, y = :x1 => f)
        expected = combine(gd, y = :x1 => x -> f(x))
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        for T in (Union{Missing, Int}, Union{Int, Int8},
                  Union{Missing, Int, Int8})
            df.x3 = Vector{T}(df.x1)
            gd = groupby_checked(df, :a)
            res = combine(gd, y = :x3 => f)
            expected = combine(gd, y = :x3 => x -> f(x))
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
        end

        f === length && continue

        df.x3 = allowmissing(df.x1)
        df.x3[1] = missing
        gd = groupby_checked(df, :a)
        res = combine(gd, y = :x3 => f)
        expected = combine(gd, y = :x3 => x -> f(x))
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
        res = combine(gd, y = :x3 => f∘skipmissing)
        expected = combine(gd, y = :x3 => x -> f(collect(skipmissing(x))))
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
    end
    # Test complex numbers
    for f in (sum, prod, mean, var, std, first, last, length)
        gd = groupby_checked(df, :a)

        res = combine(gd, y = :x2 => f)
        expected = combine(gd, y = :x2 => x -> f(x))
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
    end
    # Test CategoricalArray
    for f in (maximum, minimum, first, last, length),
        (T, m) in ((Int, false),
                   (Union{Missing, Int}, false), (Union{Missing, Int}, true))
        df.x3 = CategoricalVector{T}(df.x1)
        m && (df.x3[1] = missing)
        gd = groupby_checked(df, :a)
        res = combine(gd, y = :x3 => f)
        expected = combine(gd, y = :x3 => x -> f(x))
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        f === length && continue

        res = combine(gd, y = :x3 => f∘skipmissing)
        expected = combine(gd, y = :x3 => x -> f(collect(skipmissing(x))))
        @test res == expected
        @test typeof(res.y) == typeof(expected.y)
        if m
            gd[1].x3 = missing
            @test_throws ArgumentError combine(gd, y = :x3 => f∘skipmissing)
        end
    end
    @test combine(gd, y = :x1 => maximum, z = :x2 => sum) ==
        combine(gd, y = :x1 => x -> maximum(x), z = :x2 => x -> sum(x))
    # Test floating point corner cases
    df = DataFrame(a = [1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6],
                   x1 = [0.0, 1.0, 2.0, NaN, NaN, NaN, Inf, Inf, Inf, 1.0, NaN, 0.0, -0.0])

    for f in (sum, prod, maximum, minimum, mean, var, std, first, last, length),
        sort in (false, true),
        skip in (false, true)
        gd = groupby_checked(df, :a, sort=sort, skipmissing=skip)

        res = combine(gd, y = :x1 => f)
        expected = combine(gd, y = :x1 => x -> f(x))
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        f === length && continue

        df.x3 = allowmissing(df.x1)
        df.x3[1] = missing
        gd = groupby_checked(df, :a, sort=sort, skipmissing=skip)
        res = combine(gd, y = :x3 => f)
        expected = combine(gd, y = :x3 => x -> f(x))
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
        res = combine(gd, y = :x3 => f∘skipmissing)
        expected = combine(gd, y = :x3 => x -> f(collect(skipmissing(x))))
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
    end

    df = DataFrame(x = [1, 1, 2, 2], y = Any[1, 2.0, 3.0, 4.0])
    res = by(df, :x, z = :y => maximum)
    @test res.z isa Vector{Float64}
    @test res.z == by(df, :x, z = :y => x -> maximum(x)).z

    # Test maximum when no promotion rule exists
    df = DataFrame(x = [1, 1, 2, 2], y = [1, TestType(), TestType(), TestType()])
    gd = groupby_checked(df, :x)
    for f in (maximum, minimum)
        res = combine(gd, z = :y => maximum)
        @test res.z isa Vector{Any}
        @test res.z == by(df, :x, z = :y => x -> maximum(x)).z
    end
end

@testset "iteration protocol" begin
    gd = groupby_checked(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
    for v in gd
        @test size(v) == (2,2)
    end
end

@testset "getindex" begin
    df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                   b = 1:8)
    gd = groupby_checked(df, :a)
    @test gd[1] isa SubDataFrame
    @test gd[1] == view(df, [1, 5], :)
    @test_throws BoundsError gd[5]
    @test_throws ArgumentError gd[true]
    @test_throws ArgumentError gd[[1, 2, 1]]
    @test_throws MethodError gd["a"]
    gd2 = gd[[false, true, false, false]]
    @test length(gd2) == 1
    @test gd2[1] == gd[2]
    @test_throws BoundsError gd[[true, false]]
    @test gd2.groups == [0, 1, 0, 0, 0, 1, 0, 0]
    @test gd2.starts == [3]
    @test gd2.ends == [4]
    @test gd2.idx == gd.idx

    gd3 = gd[:]
    @test gd3 isa GroupedDataFrame
    @test length(gd3) == 4
    @test gd3 == gd
    for i in 1:4
        @test gd3[i] == gd[i]
    end
    gd4 = gd[[2,1]]
    @test gd4 isa GroupedDataFrame
    @test length(gd4) == 2
    for i in 1:2
        @test gd4[i] == gd[3-i]
    end
    @test_throws BoundsError gd[1:5]
    @test gd4.groups == [2, 1, 0, 0, 2, 1, 0, 0]
    @test gd4.starts == [3,1]
    @test gd4.ends == [4,2]
    @test gd4.idx == gd.idx
end

@testset "== and isequal" begin
    df1 = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                    b = 1:8)
    df2 = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                    b = [1:7;missing])
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
    df3 = DataFrame(a = repeat([1, 2, 3, missing], outer=[2]),
                    b = 1:8)
    df4 = DataFrame(a = repeat([1, 2, 3, missing], outer=[2]),
                    b = [1:7;missing])
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

    df = DataFrame(A = Int64[1:4;], B = ["x\"", "∀ε>0: x+ε>x", "z\$", "A\nC"],
                   C = Float32[1.0, 2.0, 3.0, 4.0])
    gd = groupby_checked(df, :A)
    io = IOContext(IOBuffer(), :limit=>true)
    show(io, gd)
    str = String(take!(io.io))
    @test str == """
    GroupedDataFrame with 4 groups based on key: A
    First Group (1 row): A = 1
    │ Row │ A     │ B      │ C       │
    │     │ Int64 │ String │ Float32 │
    ├─────┼───────┼────────┼─────────┤
    │ 1   │ 1     │ x"     │ 1.0     │
    ⋮
    Last Group (1 row): A = 4
    │ Row │ A     │ B      │ C       │
    │     │ Int64 │ String │ Float32 │
    ├─────┼───────┼────────┼─────────┤
    │ 1   │ 4     │ A\\nC   │ 4.0     │"""
    show(io, gd, allgroups=true)
    str = String(take!(io.io))
    @test str == """
    GroupedDataFrame with 4 groups based on key: A
    Group 1 (1 row): A = 1
    │ Row │ A     │ B      │ C       │
    │     │ Int64 │ String │ Float32 │
    ├─────┼───────┼────────┼─────────┤
    │ 1   │ 1     │ x\"     │ 1.0     │
    Group 2 (1 row): A = 2
    │ Row │ A     │ B           │ C       │
    │     │ Int64 │ String      │ Float32 │
    ├─────┼───────┼─────────────┼─────────┤
    │ 1   │ 2     │ ∀ε>0: x+ε>x │ 2.0     │
    Group 3 (1 row): A = 3
    │ Row │ A     │ B      │ C       │
    │     │ Int64 │ String │ Float32 │
    ├─────┼───────┼────────┼─────────┤
    │ 1   │ 3     │ z\$     │ 3.0     │
    Group 4 (1 row): A = 4
    │ Row │ A     │ B      │ C       │
    │     │ Int64 │ String │ Float32 │
    ├─────┼───────┼────────┼─────────┤
    │ 1   │ 4     │ A\\nC   │ 4.0     │"""

    # Test two-argument show
    str1, dsize = capture_stdout() do
        show(gd)
    end
    io = IOContext(IOBuffer(), :limit=>true, :displaysize=>dsize)
    show(io, gd)
    str2 = String(take!(io.io))
    @test str1 == str2


    @test sprint(show, "text/html", gd) ==
        "<p><b>GroupedDataFrame with 4 groups based on key: A</b></p>" *
        "<p><i>First Group (1 row): A = 1</i></p><table class=\"data-frame\">" *
        "<thead><tr><th></th><th>A</th><th>B</th><th>C</th></tr><tr><th></th>" *
        "<th>Int64</th><th>String</th><th>Float32</th></tr></thead>" *
        "<tbody><tr><th>1</th><td>1</td><td>x\"</td><td>1.0</td></tr></tbody>" *
        "</table><p>&vellip;</p><p><i>Last Group (1 row): A = 4</i></p>" *
        "<table class=\"data-frame\"><thead><tr><th></th><th>A</th><th>B</th><th>C</th></tr>" *
        "<tr><th></th><th>Int64</th><th>String</th><th>Float32</th></tr></thead>" *
        "<tbody><tr><th>1</th><td>4</td><td>A\\nC</td><td>4.0</td></tr></tbody></table>"

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

    gd = groupby(DataFrame(a=[Symbol("&")], b=["&"]), [1,2])
    @test sprint(show, gd) === """
        GroupedDataFrame with 1 group based on keys: a, b
        Group 1 (1 row): a = :&, b = "&"
        │ Row │ a      │ b      │
        │     │ Symbol │ String │
        ├─────┼────────┼────────┤
        │ 1   │ &      │ &      │"""

    @test sprint(show, "text/html", gd) ==
        "<p><b>GroupedDataFrame with 1 group based on keys: a, b</b></p><p><i>" *
        "First Group (1 row): a = :&amp;, b = \"&amp;\"</i></p>" *
        "<table class=\"data-frame\"><thead><tr><th></th><th>a</th><th>b</th></tr>" *
        "<tr><th></th><th>Symbol</th><th>String</th></tr></thead><tbody><tr><th>1</th>" *
        "<td>&amp;</td><td>&amp;</td></tr></tbody></table>"

    @test sprint(show, "text/latex", gd) == """
        GroupedDataFrame with 1 group based on keys: a, b

        First Group (1 row): a = :\\&, b = "\\&"

        \\begin{tabular}{r|cc}
        \t& a & b\\\\
        \t\\hline
        \t& Symbol & String\\\\
        \t\\hline
        \t1 & \\& & \\& \\\\
        \\end{tabular}
        """

        gd = groupby(DataFrame(a = [1,2], b = [1.0, 2.0]), :a)
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
    dfx = DataFrame(A = [missing, :A, :B, :A, :B, missing], B = 1:6)

    for df in [dfx, view(dfx, :, :)]
        gd = groupby_checked(df, :A)
        @test sort(DataFrame(gd), :B) ≅ sort(df, :B)
        @test eltypes(DataFrame(gd)) == [Union{Missing, Symbol}, Int]

        gd2 = gd[[3,2]]
        @test DataFrame(gd2) == df[[3,5,2,4], :]

        gd = groupby_checked(df, :A, skipmissing=true)
        @test sort(DataFrame(gd), :B) ==
              sort(dropmissing(df, disallowmissing=false), :B)
        @test eltypes(DataFrame(gd)) == [Union{Missing, Symbol}, Int]

        gd2 = gd[[2,1]]
        @test DataFrame(gd2) == df[[3,5,2,4], :]
    end

    df = DataFrame(a=Int[], b=[], c=Union{Missing, String}[])
    gd = groupby_checked(df, :a)
    @test size(DataFrame(gd)) == size(df)
    @test eltypes(DataFrame(gd)) == [Int, Any, Union{Missing, String}]

    dfv = view(dfx, 1:0, :)
    gd = groupby_checked(dfv, :A)
    @test size(DataFrame(gd)) == size(dfv)
    @test eltypes(DataFrame(gd)) == [Union{Missing, Symbol}, Int]
end

@testset "groupindices and groupvars" begin
    df = DataFrame(A = [missing, :A, :B, :A, :B, missing], B = 1:6)
    gd = groupby_checked(df, :A)
    @inferred groupindices(gd)
    @test groupindices(gd) == [1, 2, 3, 2, 3, 1]
    @test groupvars(gd) == [:A]
    gd2 = gd[[3,2]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 2, 1, 2, 1, missing]
    @test groupvars(gd2) == [:A]

    gd = groupby_checked(df, :A, skipmissing=true)
    @inferred groupindices(gd)
    @test groupindices(gd) ≅ [missing, 1, 2, 1, 2, missing]
    @test groupvars(gd) == [:A]
    gd2 = gd[[2,1]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 2, 1, 2, 1, missing]
    @test groupvars(gd2) == [:A]
end

end # module
