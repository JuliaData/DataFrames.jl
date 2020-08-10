module TestGrouping

using Test, DataFrames, Random, Statistics, PooledArrays
const ≅ = isequal

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
        @assert length(gd.starts) == length(gd.ends) == g[end]
    else
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
    df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8])
    gd = groupby_checked(df, :a)
    @test parent(gd) === df
    @test_throws ArgumentError identity.(gd)
end

@testset "consistency" begin
    df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(df.c, 5)
    @test_throws AssertionError groupby_checked(df, :a)

    df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(DataFrames._columns(df), df[:, :a])
    @test_throws AssertionError groupby_checked(df, :a)
end

@testset "accepted columns" begin
    df = DataFrame(A=[1,1,1,2,2,2], B=[1,2,1,2,1,2], C=1:6)
    @test groupby_checked(df, [1,2]) == groupby_checked(df, 1:2) ==
          groupby_checked(df, [:A, :B]) == groupby_checked(df, ["A", "B"])
    @test groupby_checked(df, [2,1]) == groupby_checked(df, 2:-1:1) ==
          groupby_checked(df, [:B, :A]) == groupby_checked(df, ["B", "A"])
    @test_throws BoundsError groupby_checked(df, 0)
    @test_throws BoundsError groupby_checked(df, 10)
    @test_throws ArgumentError groupby_checked(df, :Z)
    @test_throws ArgumentError groupby_checked(df, "Z")
end

@testset "groupby and combine(::Function, ::GroupedDataFrame)" begin
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
        colssym = propertynames(df[!, cols])
        hcatdf = hcat(df[!, cols], df[!, Not(cols)])
        nms = propertynames(hcatdf)
        res = unique(df[:, cols])
        res.xmax = [maximum(df[(df[!, colssym[1]] .== a) .& (df[!, colssym[2]] .== b), :x])
                    for (a, b) in zip(res[!, colssym[1]], res[!, colssym[2]])]
        res2 = unique(df[:, cols])[repeat(1:4, inner=2), :]
        res2.x1 = collect(Iterators.flatten(
            [[maximum(df[(df[!, colssym[1]] .== a) .& (df[!, colssym[2]] .== b), :x]),
              minimum(df[(df[!, colssym[1]] .== a) .& (df[!, colssym[2]] .== b), :x])]
             for (a, b) in zip(res[!, colssym[1]], res[!, colssym[2]])]))
        res3 = unique(df[:, cols])
        res3.x1 = [maximum(df[(df[!, colssym[1]] .== a) .& (df[!, colssym[2]] .== b), :x])
                   for (a, b) in zip(res[!, colssym[1]], res[!, colssym[2]])]
        res3.x2 = [minimum(df[(df[!, colssym[1]] .== a) .& (df[!, colssym[2]] .== b), :x])
                   for (a, b) in zip(res[!, colssym[1]], res[!, colssym[2]])]
        res4 = df[:, cols]
        res4.x2 = df.x.^2
        shcatdf = sort(hcatdf, colssym)
        sres = sort(res, colssym)
        sres2 = sort(res2, colssym)
        sres3 = sort(res3, colssym)
        sres4 = sort(res4, colssym)

        # groupby_checked() without groups sorting
        gd = groupby_checked(df, cols)
        @test names(parent(gd), gd.cols) == string.(colssym)
        df_comb = combine(identity, gd)
        @test sort(df_comb, colssym) == shcatdf
        @test sort(combine(df -> df[1, :], gd), colssym) ==
            shcatdf[.!nonunique(shcatdf, colssym), :]
        df_ref = DataFrame(gd)
        @test sort(hcat(df_ref[!, cols], df_ref[!, Not(cols)]), colssym) == shcatdf
        @test df_ref.x == df_comb.x
        @test combine(f1, gd) == res
        @test combine(f2, gd) == res
        @test rename(combine(f3, gd), :x1 => :xmax) == res
        @test combine(f4, gd) == res2
        @test combine(f5, gd) == res2
        @test combine(f6, gd) == res3
        @test sort(combine(f7, gd), colssym) == sort(res4, colssym)
        @test sort(combine(f8, gd), colssym) == sort(res4, colssym)

        # groupby_checked() with groups sorting
        gd = groupby_checked(df, cols, sort=true)
        @test names(parent(gd), gd.cols) == string.(colssym)
        for i in 1:length(gd)
            @test all(gd[i][!, colssym[1]] .== sres[i, colssym[1]])
            @test all(gd[i][!, colssym[2]] .== sres[i, colssym[2]])
        end
        @test combine(identity, gd) == shcatdf
        @test combine(df -> df[1, :], gd) ==
            shcatdf[.!nonunique(shcatdf, colssym), :]
        df_ref = DataFrame(gd)
        @test hcat(df_ref[!, cols], df_ref[!, Not(cols)]) == shcatdf
        @test combine(f1, gd) == sres
        @test combine(f2, gd) == sres
        @test rename(combine(f3, gd), :x1 => :xmax) == sres
        @test combine(f4, gd) == sres2
        @test combine(f5, gd) == sres2
        @test combine(f6, gd) == sres3
        @test combine(f7, gd) == sres4
        @test combine(f8, gd) == sres4

        # combine() with ungroup without and with groups sorting
        for dosort in (false, true)
            gd = groupby_checked(df, cols, sort=dosort)
            v = validate_gdf(combine(d -> d[:, [:x]], gd, ungroup=false))
            @test length(gd) == length(v)
            nms = [colssym; :x]
            @test v[1] == gd[1][:, nms]
            @test v[1] == gd[1][:, nms] && v[2] == gd[2][:, nms] &&
                v[3] == gd[3][:, nms] && v[4] == gd[4][:, nms]
            @test names(parent(v), v.cols) == string.(colssym)
            v = validate_gdf(combine(f1, gd, ungroup=false))
            @test extrema(v.groups) == extrema(gd.groups)
            @test vcat(v[1], v[2], v[3], v[4]) == combine(f1, gd)
            v = validate_gdf(combine(f2, gd, ungroup=false))
            @test extrema(v.groups) == extrema(gd.groups)
            @test vcat(v[1], v[2], v[3], v[4]) == combine(f2, gd)
            v = validate_gdf(combine(f3, gd, ungroup=false))
            @test extrema(v.groups) == extrema(gd.groups)
            @test vcat(v[1], v[2], v[3], v[4]) == combine(f3, gd)
            v = validate_gdf(combine(f4, gd, ungroup=false))
            @test extrema(v.groups) == extrema(gd.groups)
            @test vcat(v[1], v[2], v[3], v[4]) == combine(f4, gd)
            v = validate_gdf(combine(f5, gd, ungroup=false))
            @test extrema(v.groups) == extrema(gd.groups)
            @test vcat(v[1], v[2], v[3], v[4]) == combine(f5, gd)
            v = validate_gdf(combine(f5, gd, ungroup=false))
            @test extrema(v.groups) == extrema(gd.groups)
            @test vcat(v[1], v[2], v[3], v[4]) == combine(f5, gd)
            v = validate_gdf(combine(f6, gd, ungroup=false))
            @test extrema(v.groups) == extrema(gd.groups)
            @test vcat(v[1], v[2], v[3], v[4]) == combine(f6, gd)
            v = validate_gdf(combine(f7, gd, ungroup=false))
            @test extrema(v.groups) == extrema(gd.groups)
            @test vcat(v[1], v[2], v[3], v[4]) == combine(f7, gd)
            v = validate_gdf(combine(f8, gd, ungroup=false))
            @test extrema(v.groups) == extrema(gd.groups)
            @test vcat(v[1], v[2], v[3], v[4]) == combine(f8, gd)
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
    @test length(groupby_checked(DataFrame(A=Int[]), :A)) == 0
    # grouping single row
    @test length(groupby_checked(DataFrame(A=Int[1]), :A)) == 1

    # issue #960
    x = CategoricalArray(collect(1:20))
    df = DataFrame(v1=x, v2=x)
    groupby_checked(df, [:v1, :v2])

    df2 = combine(e -> "a", groupby_checked(DataFrame(x=Int64[]), :x))
    @test size(df2) == (0, 2)
    @test names(df2) == ["x", "x1"]
    @test eltype.(eachcol(df2)) == [Int64, String]
    @test combine(e -> "a", groupby_checked(DataFrame(x=Int64[]), :x), ungroup=false) ==
          groupby_checked(df2, :x)

    df2 = combine(groupby_checked(DataFrame(x=Int64[]), :x), :x => (e -> "a") => :x1)
    @test size(df2) == (0, 2)
    @test names(df2) == ["x", "x1"]
    @test eltype.(eachcol(df2)) == [Int64, String]
    @test combine(groupby_checked(DataFrame(x=Int64[]), :x), ungroup=false, :x => (e -> "a") => :x1) ==
          groupby_checked(df2, :x)

    df2 = combine(e -> "a", groupby_checked(DataFrame(x=[1, 2]), :x))
    @test df2 == DataFrame(x=[1,2], x1=["a", "a"])
    @test combine(e -> "a", groupby_checked(DataFrame(x=[1, 2]), :x), ungroup=false) ==
          groupby_checked(df2, :x)

    df2 = combine(groupby_checked(DataFrame(x=[1, 2]), :x), :x => (e -> "a") => :x1)
    @test df2 == DataFrame(x=[1,2], x1=["a", "a"])
    @test combine(groupby_checked(DataFrame(x=[1, 2]), :x), ungroup=false, :x => (e -> "a") => :x1) ==
          groupby_checked(df2, :x)

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
    gdf = groupby_checked(df, :Key1)
    # Check that CategoricalArray column is preserved when returning a value...
    res = combine(d -> DataFrame(x=d[1, :Key2]), gdf)
    @test typeof(res.x) == typeof(df.Key2)
    res = combine(d -> (x=d[1, :Key2],), gdf)
    @test typeof(res.x) == typeof(df.Key2)
    # ...and when returning an array
    res = combine(d -> DataFrame(x=d.Key1), gdf)
    @test typeof(res.x) == typeof(df.Key1)

    # Check that CategoricalArray and String give a String...
    res = combine(d -> d.Key1 == ["A", "A"] ? DataFrame(x=d[1, :Key1]) : DataFrame(x="C"), gdf)
    @test res.x isa Vector{String}
    res = combine(d -> d.Key1 == ["A", "A"] ? (x=d[1, :Key1],) : (x="C",), gdf)
    @test res.x isa Vector{String}
    # ...even when CategoricalValue comes second
    res = combine(d -> d.Key1 == ["B", "B"] ? DataFrame(x=d[1, :Key1]) : DataFrame(x="C"), gdf)
    @test res.x isa Vector{String}
    res = combine(d -> d.Key1 == ["B", "B"] ? (x=d[1, :Key1],) : (x="C",), gdf)
    @test res.x isa Vector{String}

    df = DataFrame(x = [1, 2, 3], y = [2, 3, 1])
    gdf = groupby_checked(df, :x)
    # Test function returning DataFrameRow
    res = combine(d -> DataFrameRow(d, 1, :), gdf)
    @test res == DataFrame(x=df.x, y=df.y)

    # Test function returning Tuple
    res = combine(d -> (sum(d.y),), gdf)
    @test res == DataFrame(x=df.x, x1=tuple.([2, 3, 1]))

    # Test with some groups returning empty data frames
    @test combine(d -> d.x == [1] ? DataFrame(z=[]) : DataFrame(z=1), gdf) ==
        DataFrame(x=[2, 3], z=[1, 1])
    v = validate_gdf(combine(d -> d.x == [1] ? DataFrame(z=[]) : DataFrame(z=1),
                             groupby_checked(df, :x), ungroup=false))
    @test length(v) == 2
    @test vcat(v[1], v[2]) == DataFrame(x=[2, 3], z=[1, 1])

    # Test that returning values of different types works with NamedTuple
    res = combine(d -> d.x == [1] ? 1 : 2.0, gdf)
    @test res.x1 isa Vector{Float64}
    @test res.x1 == [1, 2, 2]
    # Two columns need to be widened at different times
    res = combine(d -> (a=d.x == [1] ? 1 : 2.0, b=d.x == [3] ? missing : "a"), gdf)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String,Missing}}
    @test res.b ≅ ["a", "a", missing]
    # Corner case: two columns need to be widened at the same time
    res = combine(d -> (a=d.x == [1] ? 1 : 2.0, b=d.x == [1] ? missing : "a"), gdf)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String,Missing}}
    @test res.b ≅ [missing, "a", "a"]

    # Test that returning values of different types works with DataFrame
    res = combine(d -> DataFrame(x1 = d.x == [1] ? 1 : 2.0), gdf)
    @test res.x1 isa Vector{Float64}
    @test res.x1 == [1, 2, 2]
    # Two columns need to be widened at different times
    res = combine(d -> DataFrame(a=d.x == [1] ? 1 : 2.0, b=d.x == [3] ? missing : "a"), gdf)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String,Missing}}
    @test res.b ≅ ["a", "a", missing]
    # Corner case: two columns need to be widened at the same time
    res = combine(d -> DataFrame(a=d.x == [1] ? 1 : 2.0, b=d.x == [1] ? missing : "a"), gdf)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String,Missing}}
    @test res.b ≅ [missing, "a", "a"]

    # Test return values with columns in different orders
    @test combine(d -> d.x == [1] ? (x1=1, x2=3) : (x2=2, x1=4), gdf) ==
        DataFrame(x=1:3, x1=[1, 4, 4], x2=[3, 2, 2])
    @test combine(d -> d.x == [1] ? DataFrame(x1=1, x2=3) : DataFrame(x2=2, x1=4), gdf) ==
        DataFrame(x=1:3, x1=[1, 4, 4], x2=[3, 2, 2])

    # Test with NamedTuple with columns of incompatible lengths
    @test_throws DimensionMismatch combine(d -> (x1=[1], x2=[3, 4]), gdf)
    @test_throws DimensionMismatch combine(d -> d.x == [1] ? (x1=[1], x2=[3]) :
                                                        (x1=[1], x2=[3, 4]), gdf)

    # Test with incompatible return values
    @test_throws ArgumentError combine(d -> d.x == [1] ? (x1=1,) : DataFrame(x1=1), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? DataFrame(x1=1) : (x1=1,), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? NamedTuple() : (x1=1), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? (x1=1) : NamedTuple(), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? 1 : DataFrame(x1=1), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? DataFrame(x1=1) : 1, gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? (x1=1) : (x1=[1]), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? (x1=[1]) : (x1=1), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? 1 : [1], gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? [1] : 1, gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? (x1=1, x2=1) : (x1=[1], x2=1), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? (x1=[1], x2=1) : (x1=1, x2=1), gdf)
    # Special case allowed due to how implementation works
    @test combine(d -> d.x == [1] ? 1 : (x1=1), gdf) == combine(d -> 1, gdf)

    # Test that columns names and types are respected for empty input
    df = DataFrame(x=Int[], y=String[])
    res = combine(d -> 1, groupby_checked(df, :x))
    @test size(res) == (0, 2)
    @test res.x isa Vector{Int}
    @test res.x1 isa Vector{Int}

    # Test with empty data frame
    df = DataFrame(x=Int[], y=Int[])
    gd = groupby_checked(df, :x)
    @test isequal_typed(combine(df -> sum(df.x), gd), DataFrame(x=Int[], x1=Int[]))
    res = validate_gdf(combine(df -> sum(df.x), gd, ungroup=false))
    @test length(res) == 0
    @test eltype.(eachcol((res.parent))) == [Int, Int]

    # Test with zero groups in output
    df = DataFrame(A = [1, 2])
    gd = groupby_checked(df, :A)
    gd2 = validate_gdf(combine(d -> DataFrame(), gd, ungroup=false))
    @test length(gd2) == 0
    @test gd.cols == [:A]
    @test isempty(gd2.groups)
    @test isempty(gd2.idx)
    @test isempty(gd2.starts)
    @test isempty(gd2.ends)
    @test isequal_typed(parent(gd2), DataFrame(A=Int[]))

    gd2 = validate_gdf(combine(d -> DataFrame(X=Int[]), gd, ungroup=false))
    @test length(gd2) == 0
    @test gd.cols == [:A]
    @test isempty(gd2.groups)
    @test isempty(gd2.idx)
    @test isempty(gd2.starts)
    @test isempty(gd2.ends)
    @test isequal_typed(parent(gd2), DataFrame(A=Int[], X=Int[]))
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

@testset "grouping with hash collisions" begin
    # Hash collisions are almost certain on 32-bit
    df = DataFrame(A=1:2_000_000)
    gd = groupby_checked(df, :A)
    @test isequal_typed(DataFrame(df), df)
end

@testset "combine with pair interface" begin
    vexp = x -> exp.(x)
    Random.seed!(1)
    df = DataFrame(a = repeat([1, 3, 2, 4], outer=[2]),
                   b = repeat([2, 1], outer=[4]),
                   c = rand(Int, 8))

    gd = groupby_checked(df, :a)

    # Only test that different combine syntaxes work,
    # and rely on tests below for deeper checks
    @test combine(gd, :c => sum) ==
        combine(:c => sum, gd) ==
        combine(gd, :c => sum => :c_sum) ==
        combine(:c => sum => :c_sum, gd) ==
        combine(gd, [:c => sum]) ==
        combine(gd, [:c => sum => :c_sum]) ==
        combine(d -> (c_sum=sum(d.c),), gd)
    @test_throws MethodError combine(gd, d -> (c_sum=sum(d.c),))

    @test combine(gd, :c => vexp) ==
        combine(:c => vexp, gd) ==
        combine(gd, :c => vexp => :c_function) ==
        combine(:c => vexp => :c_function, gd) ==
        combine(:c => c -> (c_function = vexp(c),), gd) ==
        combine(gd, [:c => vexp]) ==
        combine(gd, [:c => vexp => :c_function]) ==
        combine(d -> (c_function=exp.(d.c),), gd)
    @test_throws ArgumentError combine(gd, :c => c -> (c_function = vexp(c),))
    @test_throws MethodError combine(gd, d -> (c_function=exp.(d.c),))

    @test combine(gd, :b => sum, :c => sum) ==
        combine(gd, :b => sum => :b_sum, :c => sum => :c_sum) ==
        combine(gd, [:b => sum, :c => sum]) ==
        combine(gd, [:b => sum => :b_sum, :c => sum => :c_sum]) ==
        combine(d -> (b_sum=sum(d.b), c_sum=sum(d.c)), gd)
    @test_throws MethodError combine(gd, d -> (b_sum=sum(d.b), c_sum=sum(d.c)))

    @test combine(gd, :b => vexp, :c => identity) ==
        combine(gd, :b => vexp => :b_function, :c => identity => :c_identity) ==
        combine(gd, [:b => vexp, :c => identity]) ==
        combine(gd, [:b => vexp => :b_function, :c => identity => :c_identity]) ==
        combine(d -> (b_function=vexp(d.b), c_identity=d.c), gd) ==
        combine([:b, :c] => (b, c) -> (b_function=vexp(b), c_identity=c), gd)
    @test_throws MethodError combine(gd, d -> (b_function=vexp(d.b), c_identity=d.c))
    @test_throws ArgumentError combine(gd, [:b, :c] => (b, c) -> (b_function=vexp(b), c_identity=c))

    @test combine(x -> extrema(x.c), gd) == combine(:c => (x -> extrema(x)) => :x1, gd)
    @test combine(x -> x.b+x.c, gd) == combine([:b,:c] => (+) => :x1, gd)
    @test combine(x -> (p=x.b, q=x.c), gd) ==
          combine([:b,:c] => (b,c) -> (p=b,q=c), gd)
    @test_throws MethodError combine(gd, x -> (p=x.b, q=x.c))
    @test_throws ArgumentError combine(gd, [:b,:c] => (b,c) -> (p=b,q=c))

    @test combine(x -> DataFrame(p=x.b, q=x.c), gd) ==
          combine([:b,:c] => (b,c) -> DataFrame(p=b,q=c), gd)
    @test_throws MethodError combine(gd, x -> DataFrame(p=x.b, q=x.c))
    @test_throws ArgumentError combine(gd, [:b,:c] => (b,c) -> DataFrame(p=b,q=c))

    @test combine(x -> [1 2; 3 4], gd) ==
          combine([:b,:c] => (b,c) -> [1 2; 3 4], gd)
    @test_throws MethodError combine(gd, x -> [1 2; 3 4])
    @test_throws ArgumentError combine(gd, [:b,:c] => (b,c) -> [1 2; 3 4])

    @test combine(nrow, gd) == combine(gd, nrow) == combine(gd, [nrow => :nrow]) ==
          combine(gd, 1 => length => :nrow)
    @test combine(nrow => :res, gd) == combine(gd, nrow => :res) ==
          combine(gd, [nrow => :res]) == combine(gd, 1 => length => :res)
    @test combine(gd, nrow => :res, nrow, [nrow => :res2]) ==
          combine(gd, 1 => length => :res, 1 => length => :nrow, 1 => length => :res2)
    @test_throws ArgumentError combine([:b,:c] => ((b,c) -> [1 2; 3 4]) => :xxx, gd)
    @test_throws ArgumentError combine(gd, [:b,:c] => ((b,c) -> [1 2; 3 4]) => :xxx)
    @test_throws ArgumentError combine(gd, nrow, nrow)
    @test_throws ArgumentError combine(gd, [nrow])

    for col in (:c, 3)
        @test combine(col => sum, gd) == combine(d -> (c_sum=sum(d.c),), gd)
        @test combine(col => x -> sum(x), gd) == combine(d -> (c_function=sum(d.c),), gd)
        @test combine(col => x -> (z=sum(x),), gd) == combine(d -> (z=sum(d.c),), gd)
        @test combine(col => x -> DataFrame(z=sum(x),), gd) == combine(d -> (z=sum(d.c),), gd)
        @test combine(col => identity, gd) == combine(d -> (c_identity=d.c,), gd)
        @test combine(col => x -> (z=x,), gd) == combine(d -> (z=d.c,), gd)

        @test combine(col => sum => :xyz, gd) ==
            combine(d -> (xyz=sum(d.c),), gd)
        @test combine(col => (x -> sum(x)) => :xyz, gd) ==
            combine(d -> (xyz=sum(d.c),), gd)
        @test combine(col => (x -> (sum(x),)) => :xyz, gd) ==
            combine(d -> (xyz=(sum(d.c),),), gd)
        @test combine(nrow, gd) == combine(d -> (nrow=length(d.c),), gd)
        @test combine(nrow => :res, gd) == combine(d -> (res=length(d.c),), gd)
        @test combine(col => sum => :res, gd) == combine(d -> (res=sum(d.c),), gd)
        @test combine(col => (x -> sum(x)) => :res, gd) == combine(d -> (res=sum(d.c),), gd)
        @test_throws ArgumentError combine(col => (x -> (z=sum(x),)) => :xyz, gd)
        @test_throws ArgumentError combine(col => (x -> DataFrame(z=sum(x),)) => :xyz, gd)
        @test_throws ArgumentError combine(col => (x -> (z=x,)) => :xyz, gd)
        @test_throws ArgumentError combine(col => x -> (z=1, xzz=[1]), gd)
    end
    for cols in ([:b, :c], 2:3, [2, 3], [false, true, true]), ungroup in (true, false)
        @test combine(cols => (b,c) -> (y=exp.(b), z=c), gd, ungroup=ungroup) ==
            combine(d -> (y=exp.(d.b), z=d.c), gd, ungroup=ungroup)
        @test combine(cols => (b,c) -> [exp.(b) c], gd, ungroup=ungroup) ==
            combine(d -> [exp.(d.b) d.c], gd, ungroup=ungroup)
        @test combine(cols => ((b,c) -> sum(b) + sum(c)) => :xyz, gd, ungroup=ungroup) ==
            combine(d -> (xyz=sum(d.b) + sum(d.c),), gd, ungroup=ungroup)
        if eltype(cols) === Bool
            cols2 = [[false, true, false], [false, false, true]]
            @test_throws MethodError combine((xyz = cols[1] => sum, xzz = cols2[2] => sum),
                                             gd, ungroup=ungroup)
            @test_throws MethodError combine((xyz = cols[1] => sum, xzz = cols2[1] => sum),
                                             gd, ungroup=ungroup)
            @test_throws MethodError combine((xyz = cols[1] => sum, xzz = cols2[2] => x -> first(x)),
                                             gd, ungroup=ungroup)
        else
            cols2 = cols
            @test combine(gd, cols2[1] => sum => :xyz, cols2[2] => sum => :xzz, ungroup=ungroup) ==
                combine(d -> (xyz=sum(d.b), xzz=sum(d.c)), gd, ungroup=ungroup)
            @test combine(gd, cols2[1] => sum => :xyz, cols2[1] => sum => :xzz, ungroup=ungroup) ==
                combine(d -> (xyz=sum(d.b), xzz=sum(d.b)), gd, ungroup=ungroup)
            @test combine(gd, cols2[1] => sum => :xyz,
                    cols2[2] => (x -> first(x)) => :xzz, ungroup=ungroup) ==
                combine(d -> (xyz=sum(d.b), xzz=first(d.c)), gd, ungroup=ungroup)
            @test combine(gd, cols2[1] => vexp => :xyz,
                    cols2[2] => sum => :xzz, ungroup=ungroup) ==
                combine(d -> (xyz=vexp(d.b), xzz=fill(sum(d.c), length(vexp(d.b)))),
                        gd, ungroup=ungroup)
        end

        @test_throws ArgumentError combine(cols => (b,c) -> (y=exp.(b), z=sum(c)),
                                           gd, ungroup=ungroup)
        @test_throws ArgumentError combine(cols2 => ((b,c) -> DataFrame(y=exp.(b),
                                           z=sum(c))) => :xyz, gd, ungroup=ungroup)
        @test_throws ArgumentError combine(cols2 => ((b,c) -> [exp.(b) c]) => :xyz,
                                           gd, ungroup=ungroup)
    end
end

struct TestType end
Base.isless(::TestType, ::Int) = true
Base.isless(::Int, ::TestType) = false
Base.isless(::TestType, ::TestType) = false

@testset "combine with aggregation functions (skipmissing=$skip, sort=$sort, indices=$indices)" for
    skip in (false, true), sort in (false, true), indices in (false, true)
    Random.seed!(1)
    df = DataFrame(a = rand([1:5;missing], 20), x1 = rand(1:100, 20),
                   x2 = rand(1:100, 20) +im*rand(1:100, 20))

    for f in (sum, prod, maximum, minimum, mean, var, std, first, last, length)
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices

        res = combine(gd, :x1 => f => :y)
        expected = combine(gd, :x1 => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        for T in (Union{Missing, Int}, Union{Int, Int8},
                  Union{Missing, Int, Int8})
            df.x3 = Vector{T}(df.x1)
            gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
            indices && @test gd.idx !== nothing # Trigger computation of indices
            res = combine(gd, :x3 => f => :y)
            expected = combine(gd, :x3 => (x -> f(x)) => :y)
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
        end

        f === length && continue

        df.x3 = allowmissing(df.x1)
        df.x3[1] = missing
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        res = combine(gd, :x3 => f => :y)
        expected = combine(gd, :x3 => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
        res = combine(gd, :x3 => f∘skipmissing => :y)
        expected = combine(gd, :x3 => (x -> f(collect(skipmissing(x)))) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        # Test reduction over group with only missing values
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        gd[1][:, :x3] .= missing
        if f in (maximum, minimum, first, last)
            @test_throws ArgumentError combine(gd, :x3 => f∘skipmissing => :y)
        else
            res = combine(gd, :x3 => f∘skipmissing => :y)
            expected = combine(gd, :x3 => (x -> f(collect(skipmissing(x)))) => :y)
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
        end
    end
    # Test complex numbers
    for f in (sum, prod, mean, var, std, first, last, length)
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices

        res = combine(gd, :x2 => f => :y)
        expected = combine(gd, :x2 => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
    end
    # Test CategoricalArray
    for f in (maximum, minimum, first, last, length),
        (T, m) in ((Int, false),
                   (Union{Missing, Int}, false), (Union{Missing, Int}, true))
        df.x3 = CategoricalVector{T}(df.x1)
        m && (df.x3[1] = missing)
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        res = combine(gd, :x3 => f => :y)
        expected = combine(gd, :x3 => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        f === length && continue

        res = combine(gd, :x3 => f∘skipmissing => :y)
        expected = combine(gd, :x3 => (x -> f(collect(skipmissing(x)))) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
        if m
            gd[1][:, :x3] .= missing
            @test_throws ArgumentError combine(gd, :x3 => f∘skipmissing => :y)
        end
    end
    @test combine(gd, :x1 => maximum => :y, :x2 => sum => :z) ≅
        combine(gd, :x1 => (x -> maximum(x)) => :y, :x2 => (x -> sum(x)) => :z)

    # Test floating point corner cases
    df = DataFrame(a = [1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6],
                   x1 = [0.0, 1.0, 2.0, NaN, NaN, NaN, Inf, Inf, Inf, 1.0, NaN, 0.0, -0.0])

    for f in (sum, prod, maximum, minimum, mean, var, std, first, last, length)
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices

        res = combine(gd, :x1 => f => :y)
        expected = combine(gd, :x1 => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        f === length && continue

        df.x3 = allowmissing(df.x1)
        df.x3[1] = missing
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        res = combine(gd, :x3 => f => :y)
        expected = combine(gd, :x3 => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
        res = combine(gd, :x3 => f∘skipmissing => :y)
        expected = combine(gd, :x3 => (x -> f(collect(skipmissing(x)))) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
    end

    df = DataFrame(x = [1, 1, 2, 2], y = Any[1, 2.0, 3.0, 4.0])
    res = combine(groupby_checked(df, :x), :y => maximum => :z)
    @test res.z isa Vector{Float64}
    @test res.z == combine(groupby_checked(df, :x), :y => (x -> maximum(x)) => :z).z

    # Test maximum when no promotion rule exists
    df = DataFrame(x = [1, 1, 2, 2], y = [1, TestType(), TestType(), TestType()])
    gd = groupby_checked(df, :x, skipmissing=skip, sort=sort)
    indices && @test gd.idx !== nothing # Trigger computation of indices
    for f in (maximum, minimum)
        res = combine(gd, :y => maximum => :z)
        @test res.z isa Vector{Any}
        @test res.z == combine(gd, :y => (x -> maximum(x)) => :z).z
    end
end

@testset "combine with columns named like grouping keys" begin
    df = DataFrame(x=["a", "a", "b", missing], y=1:4)
    gd = groupby_checked(df, :x)
    @test combine(identity, gd) ≅ df
    @test combine(d -> d[:, [2, 1]], gd) ≅ df
    @test_throws ArgumentError combine(f -> DataFrame(x=["a", "b"], z=[1, 1]), gd)
    @test validate_gdf(combine(identity, gd, ungroup=false)) ≅ gd
    @test combine(d -> d[:, [2, 1]], gd, ungroup=false) ≅ gd
    @test_throws ArgumentError combine(f -> DataFrame(x=["a", "b"], z=[1, 1]), gd,
                                       ungroup=false)

    gd = groupby_checked(df, :x, skipmissing=true)
    @test isequal_typed(combine(identity, gd), df[1:3, :])
    @test isequal_typed(combine(d -> d[:, [2, 1]], gd), df[1:3, :])
    @test_throws ArgumentError combine(f -> DataFrame(x=["a", "b"], z=[1, 1]), gd)
    @test validate_gdf(combine(identity, gd, ungroup=false)) == gd
    @test validate_gdf(combine(d -> d[:, [2, 1]], gd, ungroup=false)) == gd
    @test_throws ArgumentError combine(f -> DataFrame(x=["a", "b"], z=[1, 1]), gd,
                                       ungroup=false)
end

@testset "iteration protocol" begin
    gd = groupby_checked(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
    count = 0
    for v in gd
        count += 1
        @test v ≅ gd[count]
    end
    @test count == length(gd)
end

@testset "type stability of index fields" begin
    gd = groupby_checked(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
    idx(gd::GroupedDataFrame) = gd.idx
    starts(gd::GroupedDataFrame) = gd.starts
    ends(gd::GroupedDataFrame) = gd.ends
    @inferred idx(gd) == getfield(gd, :idx)
    @inferred starts(gd) == getfield(gd, :starts)
    @inferred ends(gd) == getfield(gd, :ends)
end

@testset "Array-like getindex" begin
    df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                   b = 1:8)
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
    @test last(gd) == gd[4]
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
    idx4 = [2,1]
    gd4 = gd[idx4]
    @test gd4 isa GroupedDataFrame
    @test length(gd4) == 2
    for (i, j) in enumerate(idx4)
        @test gd4[i] == gd[j]
    end
    @test gd4.groups == [2, 1, 0, 0, 2, 1, 0, 0]
    @test gd4.starts == [3,1]
    @test gd4.ends == [4,2]
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
    summary_str = summary(gd)
    @test summary_str == "$GroupedDataFrame with 4 groups based on key: A"
    @test str == """
    $summary_str
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
    $summary_str
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
        "<p><b>$GroupedDataFrame with 4 groups based on key: A</b></p>" *
        "<p><i>First Group (1 row): A = 1</i></p><table class=\"data-frame\">" *
        "<thead><tr><th></th><th>A</th><th>B</th><th>C</th></tr><tr><th></th>" *
        "<th>Int64</th><th>String</th><th>Float32</th></tr></thead>" *
        "<tbody><tr><th>1</th><td>1</td><td>x\"</td><td>1.0</td></tr></tbody>" *
        "</table><p>&vellip;</p><p><i>Last Group (1 row): A = 4</i></p>" *
        "<table class=\"data-frame\"><thead><tr><th></th><th>A</th><th>B</th><th>C</th></tr>" *
        "<tr><th></th><th>Int64</th><th>String</th><th>Float32</th></tr></thead>" *
        "<tbody><tr><th>1</th><td>4</td><td>A\\nC</td><td>4.0</td></tr></tbody></table>"

    @test sprint(show, "text/latex", gd) == """
        $GroupedDataFrame with 4 groups based on key: A

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

    gd = groupby_checked(DataFrame(a=[Symbol("&")], b=["&"]), [1,2])
    summary_str = summary(gd)
    @test summary_str == "$GroupedDataFrame with 1 group based on keys: a, b"
    @test sprint(show, gd) === """
        $summary_str
        Group 1 (1 row): a = :&, b = "&"
        │ Row │ a      │ b      │
        │     │ Symbol │ String │
        ├─────┼────────┼────────┤
        │ 1   │ &      │ &      │"""

    @test sprint(show, "text/html", gd) ==
        "<p><b>$summary_str</b></p><p><i>" *
        "First Group (1 row): a = :&amp;, b = \"&amp;\"</i></p>" *
        "<table class=\"data-frame\"><thead><tr><th></th><th>a</th><th>b</th></tr>" *
        "<tr><th></th><th>Symbol</th><th>String</th></tr></thead><tbody><tr><th>1</th>" *
        "<td>&amp;</td><td>&amp;</td></tr></tbody></table>"

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

        gd = groupby_checked(DataFrame(a = [1,2], b = [1.0, 2.0]), :a)
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
        @test eltype.(eachcol(DataFrame(gd))) == [Union{Missing, Symbol}, Int]

        gd2 = gd[[3,2]]
        @test isequal_typed(DataFrame(gd2), df[[3,5,2,4], :])

        gd = groupby_checked(df, :A, skipmissing=true)
        @test sort(DataFrame(gd), :B) ==
              sort(dropmissing(df, disallowmissing=false), :B)
        @test eltype.(eachcol(DataFrame(gd))) == [Union{Missing, Symbol}, Int]

        gd2 = gd[[2,1]]
        @test isequal_typed(DataFrame(gd2), df[[3,5,2,4], :])

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
    df = DataFrame(A = [missing, :A, :B, :A, :B, missing], B = 1:6)
    gd = groupby_checked(df, :A)
    @inferred groupindices(gd)
    @test groupindices(gd) == [1, 2, 3, 2, 3, 1]
    @test groupcols(gd) == [:A]
    @test valuecols(gd) == [:B]
    gd2 = gd[[3,2]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 2, 1, 2, 1, missing]
    @test groupcols(gd2) == [:A]
    @test valuecols(gd2) == [:B]

    gd = groupby_checked(df, :A, skipmissing=true)
    @inferred groupindices(gd)
    @test groupindices(gd) ≅ [missing, 1, 2, 1, 2, missing]
    @test groupcols(gd) == [:A]
    @test valuecols(gd) == [:B]
    gd2 = gd[[2,1]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 2, 1, 2, 1, missing]
    @test groupcols(gd2) == [:A]
    @test valuecols(gd2) == [:B]

    df2 = DataFrame(A = vcat(df.A, df.A), B = repeat([:X, :Y], inner=6), C = 1:12)

    gd = groupby_checked(df2, [:A, :B])
    @inferred groupindices(gd)
    @test groupindices(gd) == [1, 2, 3, 2, 3, 1, 4, 5, 6, 5, 6, 4]
    @test groupcols(gd) == [:A, :B]
    @test valuecols(gd) == [:C]
    gd2 = gd[[3,2,5]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 2, 1, 2, 1, missing, missing, 3, missing, 3, missing, missing]
    @test groupcols(gd2) == [:A, :B]
    @test valuecols(gd) == [:C]

    gd = groupby_checked(df2, [:A, :B], skipmissing=true)
    @inferred groupindices(gd)
    @test groupindices(gd) ≅ [missing, 1, 2, 1, 2, missing, missing, 3, 4, 3, 4, missing]
    @test groupcols(gd) == [:A, :B]
    @test valuecols(gd) == [:C]
    gd2 = gd[[4,2,1]]
    @inferred groupindices(gd2)
    @test groupindices(gd2) ≅ [missing, 3, 2, 3, 2, missing, missing, missing, 1, missing, 1, missing]
    @test groupcols(gd2) == [:A, :B]
    @test valuecols(gd) == [:C]
end

@testset "non standard cols arguments" begin
    df = DataFrame(x1=Int64[1,2,2], x2=Int64[1,1,2], y=Int64[1,2,3])
    gdf = groupby_checked(df, r"x")
    @test groupcols(gdf) == [:x1, :x2]
    @test valuecols(gdf) == [:y]
    @test groupindices(gdf) == [1,2,3]

    gdf = groupby_checked(df, Not(r"x"))
    @test groupcols(gdf) == [:y]
    @test valuecols(gdf) == [:x1, :x2]
    @test groupindices(gdf) == [1,2,3]

    gdf = groupby_checked(df, [])
    @test groupcols(gdf) == []
    @test valuecols(gdf) == [:x1, :x2, :y]
    @test groupindices(gdf) == [1,1,1]

    gdf = groupby_checked(df, r"z")
    @test groupcols(gdf) == []
    @test valuecols(gdf) == [:x1, :x2, :y]
    @test groupindices(gdf) == [1,1,1]

    @test combine(groupby_checked(df, []),
                  :x1 => sum => :a, :x2=>length => :b) == DataFrame(a=5, b=3)

    gdf = groupby_checked(df, [])
    @test isequal_typed(gdf[1], df)
    @test_throws BoundsError gdf[2]
    @test gdf[:] == gdf
    @test gdf[1:1] == gdf

    @test validate_gdf(combine(nrow => :x1, gdf, ungroup=false)) ==
          groupby_checked(DataFrame(x1=3), [])
    @test validate_gdf(combine(:x2 => identity => :x2_identity, gdf, ungroup=false)) ==
          groupby_checked(DataFrame(x2_identity=[1,1,2]), [])
    @test isequal_typed(DataFrame(gdf), df)

    @test sprint(show, groupby_checked(df, [])) == "GroupedDataFrame with 1 group based on key: \n" *
        "Group 1 (3 rows): \n│ Row │ x1    │ x2    │ y     │\n│     │ Int64 │ Int64 │ Int64 │\n" *
        "├─────┼───────┼───────┼───────┤\n│ 1   │ 1     │ 1     │ 1     │\n" *
        "│ 2   │ 2     │ 1     │ 2     │\n│ 3   │ 2     │ 2     │ 3     │"

    df = DataFrame(a=[1, 1, 2, 2, 2], b=1:5)
    gd = groupby_checked(df, :a)
    @test size(combine(gd)) == (0, 1)
    @test names(combine(gd)) == ["a"]
end

@testset "GroupedDataFrame dictionary interface" begin
    df = DataFrame(a = repeat([:A, :B, missing], outer=4), b = repeat(1:2, inner=6), c = 1:12)
    gd = groupby_checked(df, [:a, :b])

    @test gd[1] == DataFrame(a=[:A, :A], b=[1, 1], c=[1, 4])

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
    end

    # Equivalent value of different type
    @test gd[(a=:A, b=1.0)] ≅ gd[1]

    @test get(gd, (a=:A, b=1), nothing) ≅ gd[1]
    @test get(gd, (a=:A, b=3), nothing) == nothing

    # Wrong values
    @test_throws KeyError gd[(a=:A, b=3)]
    @test_throws KeyError gd[(:A, 3)]
    @test_throws KeyError gd[(a=:A, b="1")]
    # Wrong length
    @test_throws KeyError gd[(a=:A,)]
    @test_throws KeyError gd[(:A,)]
    @test_throws KeyError gd[(a=:A, b=1, c=1)]
    @test_throws KeyError gd[(:A, 1, 1)]
    # Out of order
    @test_throws KeyError gd[(b=1, a=:A)]
    @test_throws KeyError gd[(1, :A)]
    # Empty
    @test_throws KeyError gd[()]
    @test_throws KeyError gd[NamedTuple()]
end

@testset "GroupKey and GroupKeys" begin
    df = DataFrame(a = repeat([:A, :B, missing], outer=4), b = repeat([:X, :Y], inner=6), c = 1:12)
    cols = [:a, :b]
    gd = groupby_checked(df, cols)
    gdkeys = keys(gd)

    expected =
        [(a=:A, b=:X), (a=:B, b=:X), (a=missing, b=:X), (a=:A, b=:Y), (a=:B, b=:Y), (a=missing, b=:Y)]

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
        @test key == gdkeys[i]

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
        @test convert(Tuple, key) ≅ values(nt)
        @test NamedTuple(key) ≅ nt
        @test convert(NamedTuple, key) ≅ nt
        @test copy(key) ≅ nt

        # other conversions
        @test Vector(key) ≅ collect(nt)
        @test eltype(Vector(key)) === eltype([v for v in key])
        @test convert(Vector, key) ≅ collect(nt)
        @test Array(key) ≅ collect(nt)
        @test eltype(Array(key)) === eltype([v for v in key])
        @test convert(Array, key) ≅ collect(nt)
        @test Vector{Any}(key) ≅ collect(nt)
        @test eltype(Vector{Any}(key)) === Any
        @test convert(Vector{Any}, key) ≅ collect(nt)
        @test Array{Any}(key) ≅ collect(nt)
        @test eltype(Array{Any}(key)) === Any
        @test convert(Array{Any}, key) ≅ collect(nt)

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
    @test collect(keys(gd)) == gdkeys  # These are new instances
    @test all(Ref(gdkeys[1]) .!= gdkeys[2:end])  # Keys should not be equal to each other
    @test !any(collect(keys(gd2)) .== keys(gd3))  # Same values but different (but equal) parent

    # Printing of GroupKey
    df = DataFrame(a = repeat([:foo, :bar, :baz], outer=[4]),
                   b = repeat(1:2, outer=[6]),
                   c = 1:12)

    gd = groupby_checked(df, [:a, :b])

    @test map(repr, keys(gd)) == [
        "GroupKey: (a = :foo, b = 1)",
        "GroupKey: (a = :bar, b = 2)",
        "GroupKey: (a = :baz, b = 1)",
        "GroupKey: (a = :foo, b = 2)",
        "GroupKey: (a = :bar, b = 1)",
        "GroupKey: (a = :baz, b = 2)",
    ]
end

@testset "GroupedDataFrame indexing with array of keys" begin
    df_ref = DataFrame(a = repeat([:A, :B, missing], outer=4),
                       b = repeat(1:2, inner=6), c = 1:12)
    Random.seed!(1234)
    for df in [df_ref, df_ref[randperm(nrow(df_ref)), :]], grpcols = [[:a, :b], :a, :b],
        dosort in [true, false], doskipmissing in [true, false]

        gd = groupby_checked(df, grpcols, sort=dosort, skipmissing=doskipmissing)

        ints = unique(min.(length(gd), [4, 6, 2, 1]))
        gd2 = gd[ints]
        gkeys = keys(gd)[ints]

        # Test with GroupKeys, Tuples, and NamedTuples
        for converter in [identity, Tuple, NamedTuple]
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
    df = DataFrame(a = repeat([:A, :B, missing], outer=4),
                   b = repeat(1:2, inner=6), c = 1:12)
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
    df = DataFrame(a = repeat([:A, :B, missing], outer=4),
                   b = repeat(1:2, inner=6), c = 1:12)
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
    df = DataFrame(a = repeat([:A, :B, missing], outer=4), b = repeat([:X, :Y], inner=6), c = 1:12)
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
    @test_throws MethodError haskey(gdf, true)

    @test haskey(gdf, k)
    @test_throws ArgumentError haskey(gdf, keys(groupby_checked(DataFrame(a=1,b=2,c=3), [:a, :b]))[1])
    @test_throws BoundsError haskey(gdf, DataFrames.GroupKey(gdf, 0))
    @test_throws BoundsError haskey(gdf, DataFrames.GroupKey(gdf, 2))
    @test haskey(gdf, (1,2))
    @test !haskey(gdf, (1,3))
    @test_throws ArgumentError haskey(gdf, (1,2,3))
    @test haskey(gdf, (a=1,b=2))
    @test !haskey(gdf, (a=1,b=3))
    @test_throws ArgumentError haskey(gdf, (a=1,c=3))
    @test_throws ArgumentError haskey(gdf, (a=1,c=2))
    @test_throws ArgumentError haskey(gdf, (a=1,b=2,c=3))
end

@testset "Check aggregation of DataFrameRow" begin
    df = DataFrame(a=1)
    dfr = DataFrame(x=1, y="1")[1, 2:2]
    gdf = groupby_checked(df, :a)
    @test combine(sdf -> dfr, gdf) == DataFrame(a=1, y="1")

    df = DataFrame(a=[1,1,2,2,3,3], b='a':'f', c=string.(1:6))
    gdf = groupby_checked(df, :a)
    @test isequal_typed(combine(sdf -> sdf[1, [3,2,1]], gdf), df[1:2:5, [1,3,2]])
end

@testset "Allow returning DataFrame() or NamedTuple() to drop group" begin
    N = 4
    for (i, x1) in enumerate(collect.(Iterators.product(repeat([[true, false]], N)...))),
        er in (DataFrame(), view(DataFrame(ones(2,2)), 2:1, 2:1),
               view(DataFrame(ones(2,2)), 1:2, 2:1),
               NamedTuple(), rand(0,0), rand(5,0),
               DataFrame(x1=Int[]), DataFrame(x1=Any[]),
               (x1=Int[],), (x1=Any[],), rand(0,1)),
        fr in (DataFrame(x1=[true]), (x1=[true],))

        df = DataFrame(a = 1:N, x1 = x1)
        gdf = groupby_checked(df, :a)
        res = combine(sdf -> sdf.x1[1] ? fr : er, gdf)
        @test res == DataFrame(validate_gdf(combine(sdf -> sdf.x1[1] ? fr : er,
                                                    groupby_checked(df, :a), ungroup=false)))
        if fr isa AbstractVector && df.x1[1]
            @test res == combine(:x1 => (x1 -> x1[1] ? fr : er) => :x1, gdf)
        else
            @test res == combine(:x1 => x1 -> x1[1] ? fr : er, gdf)
        end
        if nrow(res) == 0 && length(propertynames(er)) == 0 && er != rand(0, 1)
            @test res == DataFrame(a=[])
            @test typeof(res.a) == Vector{Int}
        else
            @test res == df[df.x1, :]
        end
        if 1 < i < 2^N
            @test_throws ArgumentError combine(sdf -> sdf.x1[1] ? (x1=true,) : er, gdf)
            if df.x1[1] || !(fr isa AbstractVector)
                @test_throws ArgumentError combine(sdf -> sdf.x1[1] ? fr : (x2=[true],), gdf)
            else
                res = combine(sdf -> sdf.x1[1] ? fr : (x2=[true],), gdf)
                @test names(res) == ["a", "x2"]
            end
            @test_throws ArgumentError combine(sdf -> sdf.x1[1] ? true : er, gdf)
        end
    end
end

@testset "auto-splatting, ByRow, and column renaming" begin
    df = DataFrame(g=[1,1,1,2,2,2], x1=1:6, x2=1:6)
    gdf = groupby_checked(df, :g)
    @test combine(gdf, r"x" => cor) == DataFrame(g=[1,2], x1_x2_cor = [1.0, 1.0])
    @test combine(gdf, Not(:g) => ByRow(/)) == DataFrame(:g => [1,1,1,2,2,2], Symbol("x1_x2_/") => 1.0)
    @test combine(gdf, Between(:x2, :x1) => () -> 1) == DataFrame(:g => 1:2, Symbol("function") => 1)
    @test combine(gdf, :x1 => :z) == combine(gdf, [:x1 => :z]) == combine(:x1 => :z, gdf) ==
          DataFrame(g=[1,1,1,2,2,2], z=1:6)
    @test validate_gdf(combine(:x1 => :z, groupby_checked(df, :g), ungroup=false)) ==
          groupby_checked(DataFrame(g=[1,1,1,2,2,2], z=1:6), :g)
end

@testset "hard tabular return value cases" begin
    Random.seed!(1)
    df = DataFrame(b = repeat([2, 1], outer=[4]), x = randn(8))
    gdf = groupby_checked(df, :b)
    res = combine(sdf -> sdf.x[1:2], gdf)
    @test names(res) == ["b", "x1"]
    res2 = combine(:x => x -> x[1:2], gdf)
    @test names(res2) == ["b", "x_function"]
    @test Matrix(res) == Matrix(res2)
    res2 = combine(:x => (x -> x[1:2]) => :z, gdf)
    @test names(res2) == ["b", "z"]
    @test Matrix(res) == Matrix(res2)

    @test_throws ArgumentError combine(gdf) do sdf
        if sdf.b[1] == 2
            return (c=sdf.x[1:2],)
        else
            return sdf.x[1:2]
        end
    end
    @test_throws ArgumentError combine(gdf) do sdf
        if sdf.b[1] == 1
            return (c=sdf.x[1:2],)
        else
            return sdf.x[1:2]
        end
    end
    @test_throws ArgumentError combine(gdf) do sdf
        if sdf.b[1] == 2
            return (c=sdf.x[1],)
        else
            return sdf.x[1]
        end
    end
    @test_throws ArgumentError combine(gdf) do sdf
        if sdf.b[1] == 1
            return (c=sdf.x[1],)
        else
            return sdf.x[1]
        end
    end

    for i in 1:2, v1 in [1, 1:2], v2 in [1, 1:2]
        @test_throws ArgumentError combine([:b, :x] => ((b,x) -> b[1] == i ? x[v1] : (c=x[v2],)) => :v, gdf)
        @test_throws ArgumentError combine([:b, :x] => ((b,x) -> b[1] == i ? x[v1] : (v=x[v2],)) => :v, gdf)
    end
end

@testset "last Pair interface with multiple return values" begin
    df = DataFrame(g=[1,1,1,2,2,2], x1=1:6)
    gdf = groupby_checked(df, :g)
    @test_throws ArgumentError combine(gdf, :x1 => x -> DataFrame())
    @test_throws ArgumentError combine(gdf, :x1 => x -> (x=1, y=2))
    @test_throws ArgumentError combine(gdf, :x1 => x -> (x=[1], y=[2]))
    @test_throws ArgumentError combine(gdf, :x1 => x -> (x=[1],y=2))
    @test_throws ArgumentError combine(:x1 => x -> (x=[1], y=2), gdf)
    @test_throws ArgumentError combine(gdf, :x1 => x -> ones(2, 2))
    @test_throws ArgumentError combine(gdf, :x1 => x -> df[1, Not(:g)])
end

@testset "keepkeys" begin
    df = DataFrame(g=[1,1,1,2,2,2], x1=1:6)
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x1 => identity => :g, keepkeys=false) == DataFrame(g=1:6)
    @test combine(x -> (z=x.x1,), gdf, keepkeys=false) == DataFrame(z=1:6)
end

@testset "additional do_call tests" begin
    Random.seed!(1234)
    df = DataFrame(g = rand(1:10, 100), x1 = rand(1:1000, 100))
    gdf = groupby_checked(df, :g)

    @test combine(gdf, [] => () -> 1, :x1 => length) == combine(gdf) do sdf
        (;[:function => 1, :x1_length => nrow(sdf)]...)
    end
    @test combine(gdf, [] => () -> 1) == combine(gdf) do sdf
        (;:function => 1)
    end
    for i in 1:5
        @test combine(gdf, fill(:x1, i) => ((x...) -> sum(+(x...))) => :res, :x1 => length) ==
              combine(gdf) do sdf
                  (;[:res => i*sum(sdf.x1), :x1_length => nrow(sdf)]...)
              end
        @test combine(gdf, fill(:x1, i) => ((x...) -> sum(+(x...))) => :res) ==
              combine(gdf) do sdf
                  (;:res => i*sum(sdf.x1))
              end
    end
end

@testset "mixing of different return lengths and pseudo-broadcasting" begin
    df = DataFrame(g=[1,1,1,2,2])
    gdf = groupby_checked(df, :g)

    f1(i) = i[1] == 1 ? ["a", "b"] : ["c"]
    f2(i) = i[1] == 1 ? ["d"] : ["e", "f"]
    @test_throws ArgumentError combine(gdf, :g => f1, :g => f2)

    f1(i) = i[1] == 1 ? ["a"] : ["c"]
    f2(i) = i[1] == 1 ? "d" : "e"
    @test combine(gdf, :g => f1, :g => f2) ==
          DataFrame(g=[1,2], g_f1=["a", "c"], g_f2 = ["d", "e"])

    f1(i) = i[1] == 1 ? ["a","c"] : []
    f2(i) = i[1] == 1 ? "d" : "e"
    @test combine(gdf, :g => f1, :g => f2) ==
          DataFrame(g = [1,1], g_f1 = ["a", "c"], g_f2 = ["d", "d"])

    @test combine(gdf, :g => Ref) == DataFrame(g=[1,2], g_Ref=[[1,1,1], [2,2]])
    @test combine(gdf, :g => x -> view([x],1)) == DataFrame(g=[1,2], g_function=[[1,1,1], [2,2]])

    Random.seed!(1234)
    df = DataFrame(g=1:100)
    gdf = groupby_checked(df, :g)
    for i in 1:10
        @test combine(gdf, :g => x -> rand([x[1], Ref(x[1]), view(x, 1)])) ==
              DataFrame(g=1:100, g_function=1:100)
    end

    df_ref = DataFrame(rand(10, 4))
    df_ref.g = shuffle!([1,2,2,3,3,3,4,4,4,4])

    for i in 0:nrow(df_ref), dosort in [true, false], dokeepkeys in [true, false]
        df = df_ref[1:i, :]
        gdf = groupby_checked(df, :g, sort=dosort)
        @test combine(gdf, :x1 => sum => :x1, :x2 => identity => :x2,
                      :x3 => (x -> Ref(sum(x))) => :x3, nrow, :x4 => ByRow(sin) => :x4,
                      keepkeys=dokeepkeys) ==
              combine(gdf, keepkeys=dokeepkeys) do sdf
                      DataFrame(x1 = sum(sdf.x1), x2 = sdf.x2, x3 = sum(sdf.x3),
                                nrow = nrow(sdf), x4 = sin.(sdf.x4))
              end
    end
end

@testset "passing columns" begin
    df = DataFrame(rand(10, 4))
    df.g = shuffle!([1,2,2,3,3,3,4,4,4,4])
    gdf = groupby_checked(df, :g)

    for selector in [All(), :, r"x", Between(:x1, :x4), Not(:g), [:x1, :x2, :x3, :x4],
                     [1, 2, 3, 4], [true, true, true, true, false]]
        @test combine(gdf, selector, :x1 => ByRow(sin) => :x1, :x2 => ByRow(sin) => :x3) ==
              combine(gdf) do sdf
                  DataFrame(x1 = sin.(sdf.x1), x2 = sdf.x2, x3 = sin.(sdf.x2), x4 = sdf.x4)
              end
    end

    for selector in [All(), :, r"x", Between(:x1, :x4), Not(:g), [:x1, :x2, :x3, :x4],
                     [1, 2, 3, 4], [true, true, true, true, false]]
        @test combine(gdf, :x1 => ByRow(sin) => :x1, :x2 => ByRow(sin) => :x3, selector) ==
              combine(gdf) do sdf
                  DataFrame(x1 = sin.(sdf.x1), x3 = sin.(sdf.x2), x2 = sdf.x2, x4 = sdf.x4)
              end
    end

    for selector in [Between(:x1, :x3), Not(:x4), [:x1, :x2, :x3], [1, 2, 3],
                     [true, true, true, false, false]]
        @test combine(gdf, :x2 => ByRow(sin) => :x3, selector, :x1 => ByRow(sin) => :x1) ==
              combine(gdf) do sdf
                  DataFrame(x3 = sin.(sdf.x2), x1 = sin.(sdf.x1), x2 = sdf.x2)
              end
    end

    @test combine(gdf, 4, :x1 => ByRow(sin) => :x1, :x2 => ByRow(sin) => :x3, :x2) ==
          combine(gdf) do sdf
              DataFrame(x4 = sdf.x4, x1 = sin.(sdf.x1), x3 = sin.(sdf.x2), x2 = sdf.x2)
          end

    @test combine(gdf, 4 => :h, :x1 => ByRow(sin) => :z, :x2 => ByRow(sin) => :x3, :x2) ==
          combine(gdf) do sdf
              DataFrame(h = sdf.x4, z = sin.(sdf.x1), x3 = sin.(sdf.x2), x2 = sdf.x2)
          end

    @test_throws ArgumentError combine(gdf, 4 => :h, :x1 => ByRow(sin) => :h)
    @test_throws ArgumentError combine(gdf, :x1 => :x1_sin, :x1 => ByRow(sin))
    @test_throws ArgumentError combine(gdf, 1, :x1 => ByRow(sin) => :x1)
end

@testset "correct dropping of groups" begin
    df = DataFrame(g = 10:-1:1)
    gdf = groupby_checked(df, :g)
    sgdf = groupby_checked(df, :g, sort=true)
    for keep in [[3,2,1], [5,3,1], [9], Int[]]
        @test combine(gdf, :g => first => :keep, :g => x -> x[1] in keep ? x : Int[]) ==
              DataFrame(g=keep, keep=keep, g_function=keep)
        @test combine(sgdf, :g => first => :keep, :g => x -> x[1] in keep ? x : Int[]) ==
              sort(DataFrame(g=keep, keep=keep, g_function=keep))
    end
end

@testset "AsTable tests" begin
    df = DataFrame(g=[1,1,1,2,2], x=1:5, y=6:10)
    gdf = groupby_checked(df, :g)

    # whole column 4 options of single pair passed
    @test combine(gdf , AsTable([:x, :y]) => Ref) ==
          combine(AsTable([:x, :y]) => Ref, gdf) ==
          DataFrame(g=1:2, x_y_Ref=[(x=[1,2,3], y=[6,7,8]), (x=[4,5], y=[9,10])])
    @test validate_gdf(combine(AsTable([:x, :y]) => Ref, gdf, ungroup=false)) ==
          groupby_checked(combine(gdf, AsTable([:x, :y]) => Ref), :g)

    @test combine(gdf, AsTable(1) => Ref) ==
          DataFrame(g=1:2, g_Ref=[(g=[1,1,1],),(g=[2,2],)])


    # ByRow 4 options of single pair passed
    @test combine(gdf, AsTable([:x, :y]) => ByRow(x -> [x])) ==
          combine(AsTable([:x, :y]) => ByRow(x -> [x]), gdf) ==
          DataFrame(g=[1,1,1,2,2],
                    x_y_function=[[(x=1,y=6)], [(x=2,y=7)], [(x=3,y=8)], [(x=4,y=9)], [(x=5,y=10)]])
    @test validate_gdf(combine(AsTable([:x, :y]) => ByRow(x -> [x]), gdf, ungroup=false)) ==
          groupby_checked(combine(gdf, AsTable([:x, :y]) => ByRow(x -> [x])), :g)

    # whole column and ByRow test for multiple pairs passed
    @test combine(gdf, [:x, :y], [AsTable(v) => (x -> -x[1]) for v in [:x, :y]]) ==
          [df DataFrame(x_function=-df.x, y_function=-df.y)]
    @test combine(gdf, [:x, :y], [AsTable(v) => ByRow(x -> (-x[1],)) for v in [:x, :y]]) ==
          [df DataFrame(x_function=[(-1,), (-2,) ,(-3,) ,(-4,) ,(-5,)],
                        y_function=[(-6,), (-7,) ,(-8,) ,(-9,) ,(-10,)])]

    @test_throws ArgumentError combine(gdf, AsTable([:x, :y]) => ByRow(identity))
    @test_throws ArgumentError combine(gdf, AsTable([:x, :y]) => ByRow(x -> df[1, :]))
end

@testset "test correctness of ungrouping" begin
    df = DataFrame(g=[2,2,1,3,1,2,1,2,3])
    gdf = groupby_checked(df, :g)
    gdf2 = validate_gdf(combine(identity, gdf, ungroup=false))
    @test combine(gdf, :g => sum) == combine(gdf2, :g => sum)

    df.id = 1:9
    @test select(gdf, :g => sum) ==
          sort!(combine(gdf, :g => sum, :id), :id)[:, Not(end)]
    @test select(gdf2, :g => sum) == combine(gdf2, :g => sum, :g)
end

@testset "combine GroupedDataFrame" begin
    for df in (DataFrame(g=[3,1,1,missing],x=1:4, y=5:8),
               DataFrame(g=categorical([3,1,1,missing]),x=1:4, y=5:8))
        if !(df.g isa CategoricalVector)
            gdf = groupby_checked(df, :g, sort=false, skipmissing=false)

            @test combine(gdf, :x => sum, keepkeys=false, ungroup=true) ==
                  DataFrame(x_sum = [1, 5, 4])
            @test_throws ArgumentError combine(gdf, :x => sum, keepkeys=false, ungroup=false)
            @test combine(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
                  DataFrame(g = [3, 1, missing], x_sum = [1, 5, 4])
            gdf2 = validate_gdf(combine(gdf, :x => sum, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test gdf2.groups == 1:3
            @test DataFrame(gdf2) ≅ DataFrame(g = [3, 1, missing], x_sum = [1, 5, 4])
            @test DataFrame(gdf2, keepkeys=false) == DataFrame(x_sum = [1, 5, 4])

            @test combine(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
                  DataFrame(x_sum = [1, 5, 5, 4], g = [3, 1, 1, missing])
            @test combine(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
                  DataFrame(g = [3, 1, 1, missing], x_sum = [1, 5, 5, 4])
            gdf2 = validate_gdf(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test gdf2.groups == [1, 2, 2, 3]
            @test DataFrame(gdf2) ≅ DataFrame(g = [3, 1, 1, missing], x_sum = [1, 5, 5, 4])
            @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [1, 5, 5, 4])

            @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=false, ungroup=true) ==
                  DataFrame(x_sum = [1, 5, 4])
            @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true) ≅
                  DataFrame(g = [3, 1, missing], x_sum = [1, 5, 4])
            gdf2 = validate_gdf(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test gdf2.groups == 1:3
            @test DataFrame(gdf2) ≅ DataFrame(g = [3, 1, missing], x_sum = [1, 5, 4])
            @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [1, 5, 4])

            gdf = groupby_checked(df, :g, sort=false, skipmissing=true)

            @test combine(gdf, :x => sum, keepkeys=false, ungroup=true) ==
                  DataFrame(x_sum = [1, 5])
            @test_throws ArgumentError combine(gdf, :x => sum, keepkeys=false, ungroup=false)
            @test combine(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
                  DataFrame(g = [3, 1], x_sum = [1, 5])
            gdf2 = validate_gdf(combine(gdf, :x => sum, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test gdf2.groups == 1:2
            @test DataFrame(gdf2) ≅ DataFrame(g = [3, 1], x_sum = [1, 5])
            @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [1, 5])

            @test combine(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
                  DataFrame(x_sum = [1, 5, 5], g = [3, 1, 1])
            @test combine(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
                  DataFrame(g = [3, 1, 1], x_sum = [1, 5, 5])
            gdf2 = validate_gdf(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test gdf2.groups == [1, 2, 2]
            @test DataFrame(gdf2) ≅ DataFrame(g = [3, 1, 1], x_sum = [1, 5, 5])
            @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [1, 5, 5])

            @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=false, ungroup=true) ==
                  DataFrame(x_sum = [1, 5])
            @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true) ≅
                  DataFrame(g = [3, 1], x_sum = [1, 5])
            gdf2 = validate_gdf(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test gdf2.groups == 1:2
            @test DataFrame(gdf2) ≅ DataFrame(g = [3, 1], x_sum = [1, 5])
            @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [1, 5])
        end

        gdf = groupby_checked(df, :g, sort=true, skipmissing=false)

        @test combine(gdf, :x => sum, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum = [5, 1, 4])
        @test_throws ArgumentError validate_gdf(combine(gdf, :x => sum, keepkeys=false, ungroup=false))
        @test combine(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
              DataFrame(g = [1, 3, missing], x_sum = [5, 1, 4])
        gdf2 = validate_gdf(combine(gdf, :x => sum, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == 1:3
        @test DataFrame(gdf2) ≅ DataFrame(g = [1, 3, missing], x_sum = [5, 1, 4])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [5, 1, 4])

        @test combine(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
              DataFrame(x_sum = [5, 5, 1, 4], g = [1, 1, 3, missing])
        @test combine(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
              DataFrame(g = [1, 1, 3, missing], x_sum = [5, 5, 1, 4])
        gdf2 = validate_gdf(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == [1, 1, 2, 3]
        @test DataFrame(gdf2) ≅ DataFrame(g = [1, 1, 3, missing], x_sum = [5, 5, 1, 4])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [5, 5, 1, 4])

        @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum = [5, 1, 4])
        @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true) ≅
              DataFrame(g = [1, 3, missing], x_sum = [5, 1, 4])
        gdf2 = validate_gdf(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == 1:3
        @test DataFrame(gdf2) ≅ DataFrame(g = [1, 3, missing], x_sum = [5, 1, 4])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [5, 1, 4])

        gdf = groupby_checked(df, :g, sort=true, skipmissing=true)

        @test combine(gdf, :x => sum, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum = [5, 1])
        @test_throws ArgumentError combine(gdf, :x => sum, keepkeys=false, ungroup=false)
        @test combine(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
              DataFrame(g = [1, 3], x_sum = [5, 1])
        gdf2 = validate_gdf(combine(gdf, :x => sum, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == 1:2
        @test DataFrame(gdf2) ≅ DataFrame(g = [1, 3], x_sum = [5, 1])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [5, 1])

        @test combine(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
              DataFrame(x_sum = [5, 5, 1], g = [1, 1, 3])
        @test combine(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
              DataFrame(g = [1, 1, 3], x_sum = [5, 5, 1])
        gdf2 = validate_gdf(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == [1, 1, 2]
        @test DataFrame(gdf2) ≅ DataFrame(g = [1, 1, 3], x_sum = [5, 5, 1])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [5, 5, 1])

        @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum = [5, 1])
        @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true) ≅
              DataFrame(g = [1, 3], x_sum = [5, 1])
        gdf2 = validate_gdf(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == 1:2
        @test DataFrame(gdf2) ≅ DataFrame(g = [1, 3], x_sum = [5, 1])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum = [5, 1])
    end
end

@testset "select and transform GroupedDataFrame" begin
    for df in (DataFrame(g=[3,1,1,missing],x=1:4, y=5:8),
               DataFrame(g=categorical([3,1,1,missing]),x=1:4, y=5:8)),
        dosort in (true, false)

        gdf = groupby_checked(df, :g, sort=dosort, skipmissing=false)

        @test select(gdf, :x => sum, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum = [1, 5, 5, 4])
        @test_throws ArgumentError select(gdf, :x => sum, keepkeys=false, ungroup=false)
        @test select(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
              DataFrame(g = df.g, x_sum = [1, 5, 5, 4])
        gdf2 = validate_gdf(select(gdf, :x => sum, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == gdf.groups
        @test parent(gdf2).g ≅ df.g
        @test parent(gdf2).g !== df.g

        @test select(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
              DataFrame(x_sum = [1, 5, 5, 4], g = df.g)
        @test select(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
              DataFrame(g = df.g, x_sum = [1, 5, 5, 4])
        gdf2 = validate_gdf(select(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == gdf.groups
        @test parent(gdf2).g ≅ df.g
        @test parent(gdf2).g !== df.g

        @test transform(gdf, :x => sum, keepkeys=false, ungroup=true) ≅
              [df DataFrame(x_sum = [1, 5, 5, 4])]
        @test_throws ArgumentError transform(gdf, :x => sum, keepkeys=false, ungroup=false)
        @test transform(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
              DataFrame(g = df.g, x = df.x, y = df.y, x_sum = [1, 5, 5, 4])
        gdf2 = validate_gdf(transform(gdf, :x => sum, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == gdf.groups
        @test parent(gdf2).g ≅ df.g
        @test parent(gdf2).x ≅ df.x
        @test parent(gdf2).y ≅ df.y
        @test parent(gdf2).g !== df.g

        @test transform(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
              [df DataFrame(x_sum = [1, 5, 5, 4])]
        @test transform(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
              [df DataFrame(x_sum = [1, 5, 5, 4])]
        gdf2 = validate_gdf(transform(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == gdf.groups
        @test parent(gdf2).g ≅ df.g
        @test parent(gdf2).x ≅ df.x
        @test parent(gdf2).y ≅ df.y
        @test parent(gdf2).g !== df.g

        df2 = transform(gdf, :x => sum, :g, keepkeys=false, ungroup=true, copycols=false)
        @test df2 ≅ [df DataFrame(x_sum = [1, 5, 5, 4])]
        @test df2.g === df.g
        @test df2.x === df.x
        @test df2.y === df.y
        df2 = transform(gdf, :x => sum, :g, keepkeys=true, ungroup=true, copycols=false)
        @test df2 ≅ [df DataFrame(x_sum = [1, 5, 5, 4])]
        @test df2.g === df.g
        @test df2.x === df.x
        @test df2.y === df.y
        gdf2 = validate_gdf(transform(gdf, :x => sum, :g, keepkeys=true, ungroup=false, copycols=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == gdf.groups
        @test parent(gdf2).g ≅ df.g
        @test parent(gdf2).x ≅ df.x
        @test parent(gdf2).y ≅ df.y
        @test parent(gdf2).g === df.g

        gdf = groupby_checked(df, :g, sort=dosort, skipmissing=true)
        @test_throws ArgumentError select(gdf, :x => sum)
        @test_throws ArgumentError select(gdf, :x => sum, ungroup=false)
        @test_throws ArgumentError transform(gdf, :x => sum)
        @test_throws ArgumentError transform(gdf, :x => sum, ungroup=false)
    end

    # show the difference between the ordering of rows in select and combine
    Random.seed!(1)
    for df in (DataFrame(g=rand(1:20, 1000), x=rand(1000), id=1:1000),
               DataFrame(g=categorical(rand(1:20, 1000)), x=rand(1000), id=1:1000)),
        dosort in (true, false)

        gdf = groupby_checked(df, :g, sort=dosort)

        res1 = select(gdf, :x => mean, :x => x -> x .- mean(x), :id)
        @test res1.g == df.g
        @test res1.id == df.id
        @test res1.x_mean + res1.x_function ≈ df.x

        res2 = combine(gdf, :x => mean, :x => x -> x .- mean(x), :id)
        @test unique(res2.g) ==
              (dosort || df.g isa CategoricalVector ? sort! : identity)(unique(df.g))
        for i in unique(res2.g)
            @test issorted(filter(:g => x -> x == i, res2).id)
        end
    end
end

@testset "select! and transform! GroupedDataFrame" begin
    for df in (DataFrame(g=[3,1,1,missing],x=1:4, y=5:8),
               DataFrame(g=categorical([3,1,1,missing]),x=1:4, y=5:8)),
        dosort in (true, false)

        @test_throws MethodError select!(groupby_checked(view(df, :, :), :g), :x)
        @test_throws MethodError transform!(groupby_checked(view(df, :, :), :g), :x)

        dfc = copy(df)
        g = dfc.g
        gdf = groupby_checked(dfc, :g, sort=dosort, skipmissing=false)
        @test select!(gdf, :x => sum) === dfc
        @test dfc.g === g
        @test dfc.x_sum == [1, 5, 5, 4]
        @test propertynames(dfc) == [:g, :x_sum]

        dfc = copy(df)
        g = dfc.g
        x = dfc.x
        y = dfc.y
        gdf = groupby_checked(dfc, :g, sort=dosort, skipmissing=false)
        @test transform!(gdf, :g => first => :g, :x => first) === dfc
        @test dfc.g === g
        @test dfc.x === x
        @test dfc.y === y
        @test dfc.x_first == [1, 2, 2, 4]
        @test propertynames(dfc) == [:g, :x, :y, :x_first]

        dfc = copy(df)
        g = dfc.g
        gdf = groupby_checked(dfc, :g, sort=dosort, skipmissing=false)
        @test validate_gdf(select!(gdf, :x => sum, ungroup=false)) === gdf
        @test dfc.g === g
        @test dfc.x_sum == [1, 5, 5, 4]
        @test propertynames(dfc) == [:g, :x_sum]

        dfc = copy(df)
        g = dfc.g
        x = dfc.x
        y = dfc.y
        gdf = groupby_checked(dfc, :g, sort=dosort, skipmissing=false)
        @test validate_gdf(transform!(gdf, :g => first => :g, :x => first, ungroup=false)) === gdf
        @test dfc.g === g
        @test dfc.x === x
        @test dfc.y === y
        @test dfc.x_first == [1, 2, 2, 4]
        @test propertynames(dfc) == [:g, :x, :y, :x_first]

        dfc = copy(df)
        gdf = groupby_checked(dfc, :g, sort=dosort, skipmissing=true)
        @test_throws ArgumentError select!(gdf, :x => sum)
        @test_throws ArgumentError select!(gdf, :x => sum, ungroup=false)
        @test_throws ArgumentError transform!(gdf, :x => sum)
        @test_throws ArgumentError transform!(gdf, :x => sum, ungroup=false)
        @test dfc ≅ df
    end
end

@testset "group ordering after select/transform" begin
    df = DataFrame(g=[3, 1, 1, 2, 3], x=1:5)
    gdf1 = groupby_checked(df, :g)
    gdf2 = gdf1[[2, 3, 1]]
    @test select(gdf1, :x) == select(gdf2, :x) == df
    @test select(gdf1, :x, ungroup=false) == gdf1
    @test select(gdf2, :x, ungroup=false) == gdf2
    @test select(gdf1, ungroup=false) ==
          groupby_checked(DataFrame(g=[3, 1, 1, 2, 3]), :g)
    @test select(gdf2, ungroup=false) ==
          groupby_checked(DataFrame(g=[3, 1, 1, 2, 3]), :g)[[2, 3, 1]]

    gdf1′ = deepcopy(gdf1)
    df1′ = parent(gdf1′)
    gdf2′ = deepcopy(gdf2)
    df2′ = parent(gdf2′)
    @test select!(gdf1, :x, ungroup=false) == gdf1′
    @test select!(gdf2, :x, ungroup=false) == gdf2′
    @test select!(gdf1′, ungroup=false) ==
          groupby_checked(DataFrame(g=[3, 1, 1, 2, 3]), :g)
    @test select!(gdf2′, ungroup=false) ==
          groupby_checked(DataFrame(g=[3, 1, 1, 2, 3]), :g)[[2, 3, 1]]
    @test df1′ == DataFrame(g=[3, 1, 1, 2, 3])
    @test df2′ == DataFrame(g=[3, 1, 1, 2, 3])
end

@testset "handling empty data frame / selectors / groupcols" begin
    df = DataFrame(x=[], g=[])
    gdf = groupby_checked(df, :g)

    @test size(combine(gdf)) == (0, 1)
    @test names(combine(gdf)) == ["g"]
    @test combine(gdf, keepkeys=false) == DataFrame()
    @test combine(gdf, ungroup=false) == groupby(DataFrame(g=[]), :g)
    @test size(select(gdf)) == (0, 1)
    @test names(select(gdf)) == ["g"]
    @test groupcols(validate_gdf(select(gdf, ungroup=false))) == [:g]
    @test size(parent(select(gdf, ungroup=false))) == (0, 1)
    @test names(parent(select(gdf, ungroup=false))) == ["g"]
    @test parent(select(gdf, ungroup=false)).g !== df.g
    @test parent(select(gdf, ungroup=false, copycols=false)).g === df.g
    @test select(gdf, keepkeys=false) == DataFrame()
    @test size(transform(gdf)) == (0, 2)
    @test names(transform(gdf)) == ["x", "g"]
    @test isequal_typed(transform(gdf, keepkeys=false), df)
    @test groupcols(validate_gdf(transform(gdf, ungroup=false))) == [:g]
    @test size(parent(transform(gdf, ungroup=false))) == (0, 2)
    @test names(parent(transform(gdf, ungroup=false))) == ["x", "g"]
    @test parent(transform(gdf, ungroup=false)).g !== df.g
    @test parent(transform(gdf, ungroup=false, copycols=false)).g === df.g

    @test size(combine(x -> DataFrame(col=1), gdf)) == (0, 2)
    @test names(combine(x -> DataFrame(col=1), gdf)) == ["g", "col"]
    @test combine(x -> DataFrame(col=1), gdf, ungroup=false) == groupby(DataFrame(g=[], col=[]), :g)
    @test combine(x -> DataFrame(col=1), gdf, keepkeys=false) == DataFrame(col=[])
    @test size(select(gdf, :x => :y)) == (0, 2)
    @test names(select(gdf, :x => :y)) == ["g", "y"]
    @test groupcols(validate_gdf(select(gdf, :x => :y, ungroup=false))) == [:g]
    @test size(parent(select(gdf, :x => :y, ungroup=false))) == (0, 2)
    @test names(parent(select(gdf, :x => :y, ungroup=false))) == ["g", "y"]
    @test parent(select(gdf, :x => :y, ungroup=false)).g !== df.g
    @test parent(select(gdf, :x => :y, ungroup=false, copycols=false)).g === df.g
    @test select(gdf, :x => :y, keepkeys=false) == DataFrame(y=[])
    @test size(transform(gdf, :x => :y)) == (0, 3)
    @test names(transform(gdf, :x => :y)) == ["x", "g", "y"]
    @test transform(gdf, :x => :y, keepkeys=false) == DataFrame(x=[], g=[], y=[])
    @test groupcols(validate_gdf(transform(gdf, :x => :y, ungroup=false))) == [:g]
    @test size(parent(transform(gdf, :x => :y, ungroup=false))) == (0, 3)
    @test names(parent(transform(gdf, :x => :y, ungroup=false))) == ["x", "g", "y"]
    @test parent(transform(gdf, :x => :y, ungroup=false)).g !== df.g
    @test parent(transform(gdf, :x => :y, ungroup=false, copycols=false)).g === df.g

    df = DataFrame(x=[1], g=[1])
    gdf = groupby_checked(df, :g)

    @test size(combine(gdf)) == (0, 1)
    @test names(combine(gdf)) == ["g"]
    @test combine(gdf, ungroup=false) isa GroupedDataFrame
    @test length(combine(gdf, ungroup=false)) == 0
    @test parent(combine(gdf, ungroup=false)) == DataFrame(g=[])
    @test combine(gdf, keepkeys=false) == DataFrame()
    @test size(select(gdf)) == (1, 1)
    @test names(select(gdf)) == ["g"]
    @test groupcols(validate_gdf(select(gdf, ungroup=false))) == [:g]
    @test size(parent(select(gdf, ungroup=false))) == (1, 1)
    @test names(parent(select(gdf, ungroup=false))) == ["g"]
    @test parent(select(gdf, ungroup=false)).g !== df.g
    @test parent(select(gdf, ungroup=false, copycols=false)).g === df.g
    @test select(gdf, keepkeys=false) == DataFrame()
    @test size(transform(gdf)) == (1, 2)
    @test names(transform(gdf)) == ["x", "g"]
    @test isequal_typed(transform(gdf, keepkeys=false), df)
    @test groupcols(validate_gdf(transform(gdf, ungroup=false))) == [:g]
    @test size(parent(transform(gdf, ungroup=false))) == (1, 2)
    @test names(parent(transform(gdf, ungroup=false))) == ["x", "g"]
    @test parent(transform(gdf, ungroup=false)).g !== df.g
    @test parent(transform(gdf, ungroup=false, copycols=false)).g === df.g
    @test parent(transform(gdf, ungroup=false)).x !== df.x
    @test parent(transform(gdf, ungroup=false, copycols=false)).x === df.x

    @test size(combine(gdf, r"z")) == (0, 1)
    @test names(combine(gdf, r"z")) == ["g"]
    @test size(select(gdf, r"z")) == (1, 1)
    @test names(select(gdf, r"z")) == ["g"]
    @test select(gdf, r"z", keepkeys=false) == DataFrame()
    @test names(select(gdf, r"z")) == ["g"]
    @test select(gdf, :x => (x -> 10x) => :g, keepkeys=false) == DataFrame(g=10)

    gdf = gdf[1:0]
    @test size(combine(gdf)) == (0, 1)
    @test names(combine(gdf)) == ["g"]
    @test size(combine(x -> DataFrame(z=1), gdf)) == (0, 2)
    @test names(combine(x -> DataFrame(z=1), gdf)) == ["g", "z"]
    @test combine(x -> DataFrame(z=1), gdf, keepkeys=false) == DataFrame(z=[])
    @test combine(x -> DataFrame(z=1), gdf, ungroup=false) isa GroupedDataFrame
    @test isempty(combine(x -> DataFrame(z=1), gdf, ungroup=false))
    @test parent(combine(x -> DataFrame(z=1), gdf, ungroup=false)) == DataFrame(g=[], z=[])
    @test_throws ArgumentError select(gdf)
    @test_throws ArgumentError transform(gdf)

    @test select(groupby_checked(df, []), r"zzz") == DataFrame()
    @test select(groupby_checked(df, [])) == DataFrame()
    @test isequal_typed(transform(groupby_checked(df, [])), df)
    @test select(groupby_checked(df, []), r"zzz", keepkeys=false) == DataFrame()
    @test select(groupby_checked(df, []), keepkeys=false) == DataFrame()
    @test isequal_typed(transform(groupby_checked(df, []), keepkeys=false), df)

    gdf_tmp = validate_gdf(select(groupby_checked(df, []), ungroup=false))
    @test length(gdf_tmp) == 0
    @test isempty(gdf_tmp.cols)
end

@testset "groupcols order after select/transform" begin
    df = DataFrame(x=1:2, g=3:4)
    gdf = groupby_checked(df, :g)
    gdf2 = validate_gdf(transform(gdf, ungroup=false))
    @test groupcols(gdf2) == [:g]
    @test parent(gdf2) == select(df, :x, :g)

    df = DataFrame(g2=1:2, x=3:4, g1=5:6)
    gdf = groupby_checked(df, [:g1, :g2])
    gdf2 = validate_gdf(transform(gdf, ungroup=false))
    @test groupcols(gdf2) == [:g1, :g2]
    @test parent(gdf2) == select(df, :g2, :x, :g1)
end

@testset "corner cases of group_reduce" begin
    df = DataFrame(g=[1,1,1,2,2,2], x=Any[1,1,1,1.5,1.5,1.5])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum) == DataFrame(g=1:2, x_sum=[3.0, 4.5])

    @test combine(gdf, :x => sum∘skipmissing) == DataFrame(g=1:2, x_sum_skipmissing=[3.0, 4.5])
    @test combine(gdf, :x => mean∘skipmissing) == DataFrame(g=1:2, x_mean_skipmissing=[1.0, 1.5])
    @test combine(gdf, :x => var∘skipmissing) == DataFrame(g=1:2, x_var_skipmissing=[0.0, 0.0])
    @test combine(gdf, :x => mean) == DataFrame(g=1:2, x_mean=[1.0, 1.5])
    @test combine(gdf, :x => var) == DataFrame(g=1:2, x_var=[0.0, 0.0])

    df = DataFrame(g=[1,1,1,2,2,2], x=Any[1,1,1,1,1,missing])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum∘skipmissing) == DataFrame(g=1:2, x_sum_skipmissing=[3, 2])
    @test combine(gdf, :x => mean∘skipmissing) == DataFrame(g=1:2, x_mean_skipmissing=[1.0, 1.0])
    @test combine(gdf, :x => var∘skipmissing) == DataFrame(g=1:2, x_var_skipmissing=[0.0, 0.0])
    @test combine(gdf, :x => sum) ≅ DataFrame(g=1:2, x_sum=[3, missing])
    @test combine(gdf, :x => mean) ≅ DataFrame(g=1:2, x_mean=[1.0, missing])
    @test combine(gdf, :x => var) ≅ DataFrame(g=1:2, x_var=[0.0, missing])

    df = DataFrame(g=[1,1,1,2,2,2], x=Union{Real, Missing}[1,1,1,1,1,missing])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum∘skipmissing) == DataFrame(g=1:2, x_sum_skipmissing=[3, 2])
    @test combine(gdf, :x => mean∘skipmissing) == DataFrame(g=1:2, x_mean_skipmissing=[1.0, 1.0])
    @test combine(gdf, :x => var∘skipmissing) == DataFrame(g=1:2, x_var_skipmissing=[0.0, 0.0])
    @test combine(gdf, :x => sum) ≅ DataFrame(g=1:2, x_sum=[3, missing])
    @test combine(gdf, :x => mean) ≅ DataFrame(g=1:2, x_mean=[1.0, missing])
    @test combine(gdf, :x => var) ≅ DataFrame(g=1:2, x_var=[0.0, missing])

    Random.seed!(1)
    df = DataFrame(g = rand(1:2, 1000), x1 = rand(Int, 1000))
    df.x2 = big.(df.x1)
    gdf = groupby_checked(df, :g)

    res = combine(gdf, :x1 => sum, :x2 => sum, :x1 => x -> sum(x), :x2 => x -> sum(x))
    @test res.x1_sum == res.x1_function
    @test res.x2_sum == res.x2_function
    @test res.x1_sum != res.x2_sum # we are large enough to be sure we differ

    res = combine(gdf, :x1 => mean, :x2 => mean, :x1 => x -> mean(x), :x2 => x -> mean(x))
    if VERSION >= v"1.5"
        @test res.x1_mean ≈ res.x1_function
    else
        @test !(res.x1_mean ≈ res.x1_function) # we are large enough to be sure we differ
    end
    @test res.x2_mean ≈ res.x2_function
    @test res.x1_mean ≈ res.x2_mean

    # make sure we do correct promotions in corner case similar to Base
    df = DataFrame(g=[1,1,1,1,1,1], x=Real[1,1,big(typemax(Int)),1,1,1])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] == sum(df.x)
    @test eltype(combine(gdf, :x => sum)[!, 2]) === BigInt
    df = DataFrame(g=[1,1,1,1,1,1], x=Real[1,1,typemax(Int),1,1,1])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] == sum(df.x)
    @test eltype(combine(gdf, :x => sum)[!, 2]) === Int
    df = DataFrame(g=[1,1,1,1,1,1], x=fill(missing, 6))
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] isa Missing
    @test eltype(combine(gdf, :x => sum)[!, 2]) === Missing
    @test_throws MethodError combine(gdf, :x => sum∘skipmissing)
    df = DataFrame(g=[1,1,1,1,1,1], x=convert(Vector{Union{Real, Missing}}, fill(missing, 6)))
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] isa Missing
    @test eltype(combine(gdf, :x => sum)[!, 2]) === Missing
    @test combine(gdf, :x => sum∘skipmissing) == DataFrame(g=1, x_sum_skipmissing=0)
    @test eltype(combine(gdf, :x => sum∘skipmissing)[!, 2]) === Int
    df = DataFrame(g=[1,1,1,1,1,1], x=convert(Vector{Union{Int, Missing}}, fill(missing, 6)))
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] isa Missing
    @test eltype(combine(gdf, :x => sum)[!, 2]) === Missing
    @test combine(gdf, :x => sum∘skipmissing)[1, 2] == 0
    @test eltype(combine(gdf, :x => sum∘skipmissing)[!, 2]) === Int
    df = DataFrame(g=[1,1,1,1,1,1], x=convert(Vector{Any}, fill(missing, 6)))
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] isa Missing
    @test eltype(combine(gdf, :x => sum)[!, 2]) === Missing
    @test_throws MethodError combine(gdf, :x => sum∘skipmissing)

    # these questions can go to a final exam in "mastering combine" class
    df = DataFrame(g=[1, 2, 3], x=["a", "b", "c"])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum => :a, :x => prod => :b) ==
          combine(gdf, :x => (x -> sum(x)) => :a, :x => (x -> prod(x)) => :b)
    df = DataFrame(g=[1, 2, 3], x=Any["a", "b", "c"])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum => :a, :x => prod => :b) ==
          combine(gdf, :x => (x -> sum(x)) => :a, :x => (x -> prod(x)) => :b)
    df = DataFrame(g=[1, 1], x=[missing, "a"])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum∘skipmissing => :a, :x => prod∘skipmissing => :b) ==
          combine(gdf, :x => (x -> sum(skipmissing(x))) => :a, :x => (x -> prod(skipmissing(x))) => :b)
    df = DataFrame(g=[1, 1], x=Any[missing, "a"])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum∘skipmissing => :a, :x => prod∘skipmissing => :b) ==
          combine(gdf, :x => (x -> sum(skipmissing(x))) => :a, :x => (x -> prod(skipmissing(x))) => :b)

    df = DataFrame(g=[1, 2], x=Any[nothing, "a"])
    gdf = groupby_checked(df, :g)
    df2 = combine(gdf, :x => sum => :a, :x => prod => :b)
    @test df2 == DataFrame(g=[1, 2], a=[nothing, "a"], b=[nothing, "a"])
    @test eltype(df2.a) === eltype(df2.b) === Union{Nothing, String}
    df = DataFrame(g=[1, 2], x=Any[1, 1.0])
    gdf = groupby_checked(df, :g)
    df2 = combine(gdf, :x => sum => :a, :x => prod => :b)
    @test df2 == DataFrame(g=[1, 2], a=ones(2), b=ones(2))
    @test eltype(df2.a) === eltype(df2.b) === Float64
    df = DataFrame(g=[1, 2], x=[1, "1"])
    gdf = groupby_checked(df, :g)
    df2 = combine(gdf, :x => sum => :a, :x => prod => :b)
    @test df2 == DataFrame(g=[1, 2], a=[1, "1"], b=[1, "1"])
    @test eltype(df2.a) === eltype(df2.b) === Any
    df = DataFrame(g=[1, 1, 2], x=[UInt8(1), UInt8(1), missing])
    gdf = groupby_checked(df, :g)
    df2 = combine(gdf, :x => sum => :a, :x => prod => :b)
    @test df2 ≅ DataFrame(g=[1, 2], a=[2, missing], b=[1, missing])
    @test eltype(df2.a) === eltype(df2.b) === Union{UInt, Missing}
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
            gdf2 = filter(predicate, gdf1)
            if cutoff == 1
                @test getindex.(keys(gdf2), 1) == 1:2
            elseif cutoff == 0
                @test gdf1 == gdf2
            elseif cutoff == 10
                @test isempty(gdf2)
            end
        end

        @test_throws TypeError filter(x -> 1, groupby_checked(df, :g1))
        @test_throws TypeError filter(r"x" => (x...) -> 1, groupby_checked(df, :g1))
        @test_throws TypeError filter(AsTable(r"x") => (x...) -> 1, groupby_checked(df, :g1))

        @test_throws ArgumentError filter(r"y" => (x...) -> true, groupby_checked(df, :g1))
        @test_throws ArgumentError filter([] => (x...) -> true, groupby_checked(df, :g1))
        @test_throws ArgumentError filter(AsTable(r"y") => (x...) -> true, groupby_checked(df, :g1))
        @test_throws ArgumentError filter(AsTable([]) => (x...) -> true, groupby_checked(df, :g1))
    end
end

@testset "select/transform column order" begin
    df = DataFrame(c=1, b=2, d=3, a=4)
    gdf = groupby_checked(df, [:a, :b])
    dc_gdf = deepcopy(gdf)
    @test select(gdf) == DataFrame(a=4, b=2)
    @test gdf == dc_gdf
    @test select(gdf, ungroup=false) == groupby_checked(DataFrame(a=4, b=2), [:a, :b])
    @test select(gdf, keepkeys=false) == DataFrame()
    @test gdf == dc_gdf

    df = DataFrame(c=1, b=2, d=3, a=4)
    gdf = groupby_checked(df, [:a, :b])
    @test select!(gdf) === df
    @test df == DataFrame(a=4, b=2)
    @test gdf == groupby_checked(df, [:a, :b])

    df = DataFrame(c=1, b=2, d=3, a=4)
    gdf = groupby_checked(df, [:a, :b])
    @test select!(gdf, ungroup=false) === gdf
    @test df == DataFrame(a=4, b=2)
    @test gdf == groupby_checked(df, [:a, :b])

    df = DataFrame(c=1, b=2, d=3, a=4)
    gdf = groupby_checked(df, [:a, :b])
    dc_gdf = deepcopy(gdf)
    @test transform(gdf, :c => :e) == DataFrame(c=1, b=2, d=3, a=4, e=1)
    @test gdf == dc_gdf
    @test transform(gdf, :c => :e, ungroup=false) ==
          groupby_checked(DataFrame(c=1, b=2, d=3, a=4, e=1), [:a, :b])
    @test transform(gdf, :c => :e, keepkeys=false) == DataFrame(c=1, b=2, d=3, a=4, e=1)
    @test gdf == dc_gdf

    df = DataFrame(c=1, b=2, d=3, a=4)
    gdf = groupby_checked(df, [:a, :b])
    @test transform!(gdf, :c => :e) === df
    @test df == DataFrame(c=1, b=2, d=3, a=4, e=1)
    @test gdf == groupby_checked(df, [:a, :b])

    df = DataFrame(c=1, b=2, d=3, a=4)
    gdf = groupby_checked(df, [:a, :b])
    @test transform!(gdf, :c => :e, ungroup=false) === gdf
    @test df == DataFrame(c=1, b=2, d=3, a=4, e=1)
    @test gdf == groupby_checked(df, [:a, :b])

    df = DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[])
    gdf = groupby_checked(df, [:a, :b])
    dc_gdf = deepcopy(gdf)
    df_res = select(gdf, :c => :e)
    @test isequal_typed(df_res, DataFrame(a=Char[], b=Float64[], e=String[]))
    @test gdf == dc_gdf
    @test select(gdf, :c => :e, ungroup=false) ==
          groupby_checked(DataFrame(a=[], b=[], e=[]), [:a, :b])
    @test select(gdf, keepkeys=false) == DataFrame()
    df_res = select(gdf, :c => :e, keepkeys=false)
    @test isequal_typed(df_res, DataFrame(e=String[]))
    @test eltype(df_res.e) == String
    @test gdf == dc_gdf

    df = DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[])
    gdf = groupby_checked(df, [:a, :b])
    @test select!(gdf, :c => :e) === df
    @test isequal_typed(df, DataFrame(a=Char[], b=Float64[], e=String[]))
    @test gdf == groupby_checked(df, [:a, :b])

    df = DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[])
    gdf = groupby_checked(df, [:a, :b])
    @test select!(gdf, :c => :e, ungroup=false) === gdf
    @test isequal_typed(df, DataFrame(a=Char[], b=Float64[], e=String[]))
    @test gdf == groupby_checked(df, [:a, :b])

    df = DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[])
    gdf = groupby_checked(df, [:a, :b])
    dc_gdf = deepcopy(gdf)
    @test isequal_typed(transform(gdf, :c => :e),
                        DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[], e=String[]))
    @test gdf == dc_gdf
    @test transform(gdf, :c => :e, ungroup=false) ==
          groupby_checked(DataFrame(c=[], b=[], d=[], a=[], e=[]), [:a, :b])
    @test isequal_typed(transform(gdf, :c => :e, keepkeys=false),
                        DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[], e=String[]))
    @test gdf == dc_gdf

    df = DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[])
    gdf = groupby_checked(df, [:a, :b])
    @test transform!(gdf, :c => :e) === df
    @test isequal_typed(df, DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[], e=String[]))
    @test gdf == groupby_checked(df, [:a, :b])

    df = DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[])
    gdf = groupby_checked(df, [:a, :b])
    @test transform!(gdf, :c => :e, ungroup=false) === gdf
    @test isequal_typed(df, DataFrame(c=String[], b=Float64[], d=Bool[], a=Char[], e=String[]))
    @test gdf == groupby_checked(df, [:a, :b])
end

@testset "combine on empty data frame" begin
    df = DataFrame(x=Int[])
    @test isequal_typed(combine(df, nrow), DataFrame(nrow=0))
    @test isequal_typed(combine(df, nrow => :z), DataFrame(z=0))
    @test isequal_typed(combine(df, [nrow => :z]), DataFrame(z=0))
    @test isequal_typed(combine(df, :x => (x -> 1:2) => :y), DataFrame(y=1:2))
    @test isequal_typed(combine(df, :x => (x -> x isa Vector{Int} ? "a" : 'a') => :y),
                        DataFrame(y="a"))

    # in the future this should be DataFrame(nrow=0)
    @test_throws ArgumentError combine(nrow, df)

    # in the future this should be DataFrame(a=1,b=2)
    @test_throws ArgumentError combine(sdf -> DataFrame(a=1,b=2), df)
end

@testset "disallowed tuple column selector" begin
    df = DataFrame(g=1:3)
    gdf = groupby(df, :g)
    @test_throws ArgumentError combine((:g, :g) => identity, gdf)
    @test_throws ArgumentError combine(gdf, (:g, :g) => identity)
end

@testset "new map behavior" begin
    df = DataFrame(g=[1,2,3])
    gdf = groupby(df, :g)
    @test map(nrow, gdf) == [1, 1, 1]
end

@testset "check isagg correctly uses fast path only when it should" begin
    for fun in (sum, prod, mean, var, std, sum∘skipmissing, prod∘skipmissing,
                mean∘skipmissing, var∘skipmissing, std∘skipmissing),
        col in ([1, 2, 3], [big(1.5), big(2.5), big(3.5)], [1 + 0.5im, 2 + 0.5im, 3 + 0.5im],
                [true, false, true], [pi, pi, pi], [1//2, 1//3, 1//4],
                Real[1, 1.5, 1//2], Number[1, 1.5, 1//2], Any[1, 1.5, 1//2],
                [1, 2, missing], [big(1.5), big(2.5), missing], [1 + 0.5im, 2 + 0.5im, missing],
                [true, false, missing], [pi, pi, missing], [1//2, 1//3, missing],
                Union{Missing,Real}[1, 1.5, missing],
                Union{Missing,Number}[1, 1.5, missing], Any[1, 1.5, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        @test isequal_coltyped(combine(gdf, :x => fun => :y), combine(gdf, :x => (x -> fun(x)) => :y))
    end

    for fun in (maximum, minimum, maximum∘skipmissing, minimum∘skipmissing),
        col in ([1, 2, 3], [big(1.5), big(2.5), big(3.5)],
                [true, false, true], [pi, pi, pi], [1//2, 1//3, 1//4],
                Real[1, 1.5, 1//2], Number[1, 1.5, 1//2], Any[1, 1.5, 1//2],
                [1, 2, missing], [big(1.5), big(2.5), missing],
                [true, false, missing], [pi, pi, missing], [1//2, 1//3, missing],
                Union{Missing,Real}[1, 1.5, missing],
                Union{Missing,Number}[1, 1.5, missing], Any[1, 1.5, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        @test isequal_coltyped(combine(gdf, :x => fun => :y), combine(gdf, :x => (x -> fun(x)) => :y))
    end

    for fun in (first, last, length, first∘skipmissing, last∘skipmissing),
        col in ([1, 2, 3], [big(1.5), big(2.5), big(3.5)], [1 + 0.5im, 2 + 0.5im, 3 + 0.5im],
                [true, false, true], [pi, pi, pi], [1//2, 1//3, 1//4],
                Real[1, 1.5, 1//2], Number[1, 1.5, 1//2], Any[1, 1.5, 1//2],
                [1, 2, missing], [big(1.5), big(2.5), missing], [1 + 0.5im, 2 + 0.5im, missing],
                [true, false, missing], [pi, pi, missing], [1//2, 1//3, missing],
                Union{Missing,Real}[1, 1.5, missing],
                Union{Missing,Number}[1, 1.5, missing], Any[1, 1.5, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        if fun === last∘skipmissing
            # corner case - it fails in slow path, but works in fast path
            if eltype(col) === Any
                @test_throws MethodError combine(gdf, :x => fun => :y)
            else
                @test isequal_coltyped(combine(gdf, :x => fun => :y),
                                       combine(groupby_checked(dropmissing(parent(gdf)), :g), :x => fun => :y))
            end
            @test_throws MethodError combine(gdf, :x => (x -> fun(x)) => :y)
        else
            @test isequal_coltyped(combine(gdf, :x => fun => :y), combine(gdf, :x => (x -> fun(x)) => :y))
        end
    end

    for fun in (sum, mean, var, std),
        col in ([1:3, 4:6, 7:9], [1:3, 4:6, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        if eltype(col) >: Missing
            @test_throws MethodError combine(gdf, :x => fun => :y)
            @test_throws MethodError combine(gdf, :x => (x -> fun(x)) => :y)
        else
            @test isequal_coltyped(combine(gdf, :x => fun => :y), combine(gdf, :x => (x -> fun(x)) => :y))
        end
    end

    for fun in (sum∘skipmissing, mean∘skipmissing),
        col in ([1:3, 4:6, 7:9], [1:3, 4:6, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        @test isequal_coltyped(combine(gdf, :x => fun => :y), combine(gdf, :x => (x -> fun(x)) => :y))
    end

    # see https://github.com/JuliaLang/julia/issues/36979
    for fun in (var∘skipmissing, std∘skipmissing),
        col in ([1:3, 4:6, 7:9], [1:3, 4:6, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        @test_throws MethodError combine(gdf, :x => fun => :y)
        @test_throws MethodError combine(gdf, :x => (x -> fun(x)) => :y)
    end

    for fun in (maximum, minimum, maximum∘skipmissing, minimum∘skipmissing,
                first, last, length, first∘skipmissing, last∘skipmissing),
        col in ([1:3, 4:6, 7:9], [1:3, 4:6, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        if fun isa typeof(last∘skipmissing)
            @test_throws MethodError combine(gdf, :x => fun => :y)
            @test_throws MethodError combine(gdf, :x => (x -> fun(x)) => :y)
        else
            @test isequal_coltyped(combine(gdf, :x => fun => :y), combine(gdf, :x => (x -> fun(x)) => :y))
        end
    end

    for fun in (prod, prod∘skipmissing),
        col in ([1:3, 4:6, 7:9], [1:3, 4:6, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        @test_throws MethodError combine(gdf, :x => fun => :y)
        @test_throws MethodError combine(gdf, :x => (x -> fun(x)) => :y)
    end

    for fun in (sum, prod, mean, var, std, sum∘skipmissing, prod∘skipmissing,
                mean∘skipmissing, var∘skipmissing, std∘skipmissing,
                maximum, minimum, maximum∘skipmissing, minimum∘skipmissing,
                first, last, length, first∘skipmissing, last∘skipmissing),
        col in ([ones(2,2), zeros(2,2), ones(2,2)], [ones(2,2), zeros(2,2), missing],
                [DataFrame(ones(2,2)), DataFrame(zeros(2,2)), DataFrame(ones(2,2))],
                [DataFrame(ones(2,2)), DataFrame(zeros(2,2)), ones(2,2)],
                [DataFrame(ones(2,2)), DataFrame(zeros(2,2)), missing],
                [(a=1, b=2), (a=3, b=4), (a=5, b=6)], [(a=1, b=2), (a=3, b=4), missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        if fun === length
            @test isequal_coltyped(combine(gdf, :x => fun => :y), DataFrame(g=1, y=3))
            @test isequal_coltyped(combine(gdf, :x => (x -> fun(x)) => :y), DataFrame(g=1, y=3))
        elseif (fun === last && ismissing(last(col))) ||
               (fun in (maximum, minimum) && col ≅ [(a=1, b=2), (a=3, b=4), missing])
            # this case is a situation when the vector type would not be accepted in
            # general as it contains entries that we do not allow but accidentally
            # its last element is accepted because it is missing
            @test isequal_coltyped(combine(gdf, :x => fun => :y), DataFrame(g=1, y=missing))
            @test isequal_coltyped(combine(gdf, :x => (x -> fun(x)) => :y), DataFrame(g=1, y=missing))
        else
            @test_throws Union{ArgumentError, MethodError} combine(gdf, :x => fun => :y)
            @test_throws Union{ArgumentError, MethodError} combine(gdf, :x => (x -> fun(x)) => :y)
        end
    end
end

end # module
