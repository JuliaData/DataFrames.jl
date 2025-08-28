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

@testset "groupby and combine(::Function, ::GroupedDataFrame)" begin
    Random.seed!(1)
    df = DataFrame(a=repeat(Union{Int, Missing}[1, 3, 2, 4], outer=[2]),
                   b=repeat(Union{Int, Missing}[2, 1], outer=[4]),
                   c=repeat([0, 1], outer=[4]),
                   x=Vector{Union{Float64, Missing}}(randn(8)))

    f1(df) = DataFrame(xmax=maximum(df.x))
    f2(df) = (xmax = maximum(df.x),)
    f3(df) = maximum(df.x)
    f4(df) = [maximum(df.x), minimum(df.x)]
    f5(df) = reshape([maximum(df.x), minimum(df.x)], 2, 1)
    f6(df) = [maximum(df.x) minimum(df.x)]
    f7(df) = (x2 = df.x.^2,)
    f8(df) = DataFrame(x2=df.x.^2)

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

        # groupby_checked() without groups sorting
        sres = sort(res)
        sres2 = sort(res2)
        sres3 = sort(res3)
        sres4 = sort(res4)
        gd = groupby_checked(df, cols)
        @test names(parent(gd), gd.cols) == string.(colssym)
        df_comb = combine(identity, gd)
        @test sort(df_comb, colssym) == shcatdf
        @test sort(combine(df -> df[1, :], gd), colssym) ==
              shcatdf[.!nonunique(shcatdf, colssym), :]
        @test sort(combine(df -> Tables.Row(df[1, :]), gd), colssym) ==
              shcatdf[.!nonunique(shcatdf, colssym), :]
        df_ref = DataFrame(gd)
        @test sort(hcat(df_ref[!, cols], df_ref[!, Not(cols)]), colssym) == shcatdf
        @test df_ref.x == df_comb.x
        @test sort(combine(f1, gd)) == sres
        @test sort(combine(f2, gd)) == sres
        @test sort(rename(combine(f3, gd), :x1 => :xmax)) == sres
        @test sort(combine(f4, gd)) == sres2
        @test sort(combine(f5, gd)) == sres2
        @test sort(combine(f6, gd)) == sres3
        @test sort(combine(f7, gd)) == sres4
        @test sort(combine(f8, gd)) == sres4

        # groupby_checked() with groups sorting
        sres = sort(res, colssym)
        sres2 = sort(res2, colssym)
        sres3 = sort(res3, colssym)
        sres4 = sort(res4, colssym)
        gd = groupby_checked(df, cols, sort=true)
        @test names(parent(gd), gd.cols) == string.(colssym)
        for i in 1:length(gd)
            @test all(gd[i][!, colssym[1]] .== sres[i, colssym[1]])
            @test all(gd[i][!, colssym[2]] .== sres[i, colssym[2]])
        end
        @test combine(identity, gd) == shcatdf
        @test combine(df -> df[1, :], gd) ==
              shcatdf[.!nonunique(shcatdf, colssym), :]
        @test combine(df -> Tables.Row(df[1, :]), gd) ==
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
        for dosort in (false, true, nothing)
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
    df2 = DataFrame(v1=levels!(categorical(rand(1:N, 100)), collect(1:N)),
                    v2=levels!(categorical(rand(1:N, 100)), collect(1:N)),
                    v3=levels!(categorical(rand(1:N, 100)), collect(1:N)))
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
    @test df2 == DataFrame(x=[1, 2], x1=["a", "a"])
    @test combine(e -> "a", groupby_checked(DataFrame(x=[1, 2]), :x), ungroup=false) ==
          groupby_checked(df2, :x)

    df2 = combine(groupby_checked(DataFrame(x=[1, 2]), :x), :x => (e -> "a") => :x1)
    @test df2 == DataFrame(x=[1, 2], x1=["a", "a"])
    @test combine(groupby_checked(DataFrame(x=[1, 2]), :x), ungroup=false, :x => (e -> "a") => :x1) ==
          groupby_checked(df2, :x)

    # Check that reordering levels does not confuse groupby
    for df in (DataFrame(Key1=CategoricalArray(["A", "A", "B", "B", "B", "A"]),
                         Key2=CategoricalArray(["A", "B", "A", "B", "B", "A"]),
                         Value=1:6),
                DataFrame(Key1=PooledArray(["A", "A", "B", "B", "B", "A"]),
                          Key2=PooledArray(["A", "B", "A", "B", "B", "A"]),
                          Value=1:6))
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

    df = DataFrame(Key1=CategoricalArray(["A", "A", "B", "B", "B", "A"]),
                    Key2=CategoricalArray(["A", "B", "A", "B", "B", "A"]),
                    Value=1:6)
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

    df = DataFrame(x=[1, 2, 3], y=[2, 3, 1])
    gdf = groupby_checked(df, :x)

    # Test function returning DataFrameRow
    res = combine(d -> DataFrameRow(d, 1, :), gdf)
    @test res == DataFrame(x=df.x, y=df.y)

    # Test function returning Tables.AbstractRow
    res = combine(d -> Tables.Row(DataFrameRow(d, 1, :)), gdf)
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
    @test res.b isa Vector{Union{String, Missing}}
    @test res.b ≅ ["a", "a", missing]
    # Corner case: two columns need to be widened at the same time
    res = combine(d -> (a=d.x == [1] ? 1 : 2.0, b=d.x == [1] ? missing : "a"), gdf)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String, Missing}}
    @test res.b ≅ [missing, "a", "a"]

    # Test that returning values of different types works with DataFrame
    res = combine(d -> DataFrame(x1=d.x == [1] ? 1 : 2.0), gdf)
    @test res.x1 isa Vector{Float64}
    @test res.x1 == [1, 2, 2]
    # Two columns need to be widened at different times
    res = combine(d -> DataFrame(a=d.x == [1] ? 1 : 2.0, b=d.x == [3] ? missing : "a"), gdf)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String, Missing}}
    @test res.b ≅ ["a", "a", missing]
    # Corner case: two columns need to be widened at the same time
    res = combine(d -> DataFrame(a=d.x == [1] ? 1 : 2.0, b=d.x == [1] ? missing : "a"), gdf)
    @test res.a isa Vector{Float64}
    @test res.a == [1, 2, 2]
    @test res.b isa Vector{Union{String, Missing}}
    @test res.b ≅ [missing, "a", "a"]

    # Test return values with columns in different orders
    @test_throws ArgumentError combine(d -> d.x == [1] ? (x1=1, x2=3) : (x2=2, x1=4), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? d[1, [1, 2]] : d[1, [2, 1]], gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? (x1=[1], x2=[3]) : (x2=[2], x1=[4]), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? [1 3] : (x2=[2], x1=[4]), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? d[1:1, [1, 2]] : d[1:1, [2, 1]], gdf)
    # but this should work
    @test combine(d -> d.x == [1] ? [1 3] : (x1=[2], x2=[4]), gdf) == DataFrame(x=1:3, x1=[1, 2, 2], x2=[3, 4, 4])

    # wrong mixing tests
    @test_throws ArgumentError combine(d -> d.x == [1] ? d[1:1, :] : d[1, :], gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? d[1, :] : d[1:1, :], gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? [1 2] : d[1, :], gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? d[1, :] : [1 2], gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? d[1:1, :] : NamedTuple(d[1, :]), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? NamedTuple(d[1, :]) : d[1:1, :], gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? Tables.columntable(d[1:1, :]) : NamedTuple(d[1, :]), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? NamedTuple(d[1, :]) : Tables.columntable(d[1:1, :]), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? d[1:1, :] : Tables.Row(d[1, :]), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? Tables.Row(d[1, :]) : d[1:1, :], gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? [1 2] : Tables.Row(d[1, :]), gdf)
    @test_throws ArgumentError combine(d -> d.x == [1] ? Tables.Row(d[1, :]) : [1 2], gdf)

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
    df = DataFrame(A=[1, 2])
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

    @test_throws ArgumentError combine(:x => identity, groupby_checked(DataFrame(x=[1, 2, 3]), :x))
    @test_throws ArgumentError select(groupby_checked(DataFrame(x=[1, 2, 3], y=1), :x), [] => identity)
    @test_throws ArgumentError select(groupby_checked(DataFrame(x=[1, 2, 3], y=1), :x), [:x, :y] => identity)
    @test_throws ArgumentError select(groupby_checked(DataFrame(x=[1, 2, 3], y=1), :x), [] => identity => :z)
    @test_throws ArgumentError select(groupby_checked(DataFrame(x=[1, 2, 3], y=1), :x), [:x, :y] => identity => :z)
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

@testset "combine with pair interface" begin
    vexp = x -> exp.(x)
    Random.seed!(1)
    df = DataFrame(a=repeat([1, 3, 2, 4], outer=[2]),
                   b=repeat([2, 1], outer=[4]),
                   c=rand(Int, 8))

    gd = groupby_checked(df, :a)

    # Only test that different combine syntaxes work,
    # and rely on tests below for deeper checks
    @test combine(gd, :c => sum) ==
        combine(gd, :c => sum => :c_sum) ==
        combine(gd, [:c => sum]) ==
        combine(gd, [:c => sum => :c_sum]) ==
        combine(d -> (c_sum=sum(d.c),), gd) ==
        combine(gd, d -> (c_sum=sum(d.c),)) ==
        combine(gd, d -> (c_sum=[sum(d.c)],)) ==
        combine(gd, d -> DataFrame(c_sum=sum(d.c))) ==
        combine(gd, :c => (x -> [sum(x)]) => [:c_sum]) ==
        combine(gd, :c => (x -> [(c_sum=sum(x),)]) => AsTable) ==
        combine(gd, :c => (x -> [Tables.Row((c_sum=sum(x),))]) => AsTable) ==
        combine(gd, :c => (x -> fill(sum(x), 1, 1)) => [:c_sum]) ==
        combine(gd, :c => (x -> [Dict(:c_sum => sum(x))]) => AsTable)
    @test_throws ArgumentError combine(:c => sum, gd)
    @test_throws ArgumentError combine(:, gd)

    @test combine(gd, :c => vexp) ==
        combine(gd, :c => vexp => :c_function) ==
        combine(gd, [:c => vexp]) ==
        combine(gd, [:c => vexp => :c_function]) ==
        combine(d -> (c_function=exp.(d.c),), gd) ==
        combine(gd, d -> (c_function=exp.(d.c),)) ==
        combine(gd, :c => (x -> (c_function=exp.(x),)) => AsTable) ==
        combine(gd, :c => ByRow(exp) => :c_function) ==
        combine(gd, :c => ByRow(x -> [exp(x)]) => [:c_function])
    @test_throws ArgumentError combine(gd, :c => c -> (c_function = vexp(c),))

    @test combine(gd, :b => sum, :c => sum) ==
        combine(gd, :b => sum => :b_sum, :c => sum => :c_sum) ==
        combine(gd, [:b => sum, :c => sum]) ==
        combine(gd, [:b => sum => :b_sum, :c => sum => :c_sum]) ==
        combine(d -> (b_sum=sum(d.b), c_sum=sum(d.c)), gd) ==
        combine(gd, d -> (b_sum=sum(d.b), c_sum=sum(d.c))) ==
        combine(gd, d -> (b_sum=sum(d.b),), d -> (c_sum=sum(d.c),))

    @test combine(gd, :b => vexp, :c => identity) ==
        combine(gd, :b => vexp => :b_function, :c => identity => :c_identity) ==
        combine(gd, [:b => vexp, :c => identity]) ==
        combine(gd, [:b => vexp => :b_function, :c => identity => :c_identity]) ==
        combine(d -> (b_function=vexp(d.b), c_identity=d.c), gd) ==
        combine(gd, [:b, :c] => ((b, c) -> (b_function=vexp(b), c_identity=c)) => AsTable) ==
        combine(gd, d -> (b_function=vexp(d.b), c_identity=d.c))
    @test_throws ArgumentError combine(gd, [:b, :c] => (b, c) -> (b_function=vexp(b), c_identity=c))

    @test combine(x -> extrema(x.c), gd) == combine(gd, :c => (x -> extrema(x)) => :x1)
    @test combine(x -> hcat(extrema(x.c)...), gd) == combine(gd, :c => (x -> [extrema(x)]) => AsTable)
    @test combine(x -> x.b+x.c, gd) == combine(gd, [:b, :c] => (+) => :x1)
    @test combine(x -> (p=x.b, q=x.c), gd) == combine(gd, [:b, :c] => ((b, c) -> (p=b, q=c)) => AsTable)
    @test_throws ArgumentError combine(gd, [:b, :c] => (b, c) -> (p=b, q=c))

    @test combine(x -> DataFrame(p=x.b, q=x.c), gd) ==
          combine(gd, [:b, :c] => ((b, c) -> DataFrame(p=b, q=c)) => AsTable) ==
          combine(gd, x -> DataFrame(p=x.b, q=x.c))
    @test_throws ArgumentError combine(gd, [:b, :c] => (b, c) -> DataFrame(p=b, q=c))

    @test combine(x -> [1 2; 3 4], gd) ==
          combine(gd, [:b, :c] => ((b, c) -> [1 2; 3 4]) => AsTable)
    @test_throws ArgumentError combine(gd, [:b, :c] => (b, c) -> [1 2; 3 4])

    @test combine(nrow, gd) == combine(gd, nrow) == combine(gd, [nrow => :nrow]) ==
          combine(gd, 1 => length => :nrow)
    @test combine(gd, nrow => :res) ==
          combine(gd, [nrow => :res]) == combine(gd, 1 => length => :res)
    @test combine(gd, nrow => :res, nrow, [nrow => :res2]) ==
          combine(gd, 1 => length => :res, 1 => length => :nrow, 1 => length => :res2)
    @test_throws ArgumentError combine([:b, :c] => ((b, c) -> [1 2; 3 4]) => :xxx, gd)
    @test_throws ArgumentError combine(gd, [:b, :c] => ((b, c) -> [1 2; 3 4]) => :xxx)
    @test_throws ArgumentError combine(gd, nrow, nrow)
    @test_throws ArgumentError combine(gd, [nrow])

    for col in (:c, 3)
        @test combine(gd, col => sum) == combine(d -> (c_sum=sum(d.c),), gd)
        @test combine(gd, col => x -> sum(x)) == combine(d -> (c_function=sum(d.c),), gd)
        @test combine(gd, col => (x -> (z=sum(x),)) => AsTable) == combine(d -> (z=sum(d.c),), gd)
        @test combine(gd, col => (x -> Tables.Row((z=sum(x),))) => AsTable) == combine(d -> (z=sum(d.c),), gd)
        @test combine(gd, col => (x -> DataFrame(z=sum(x),)) => AsTable) == combine(d -> (z=sum(d.c),), gd)
        @test combine(gd, col => identity) == combine(d -> (c_identity=d.c,), gd)
        @test combine(gd, col => (x -> (z=x,)) => AsTable) == combine(d -> (z=d.c,), gd)

        @test combine(gd, col => sum => :xyz) == combine(d -> (xyz=sum(d.c),), gd)
        @test combine(gd, col => (x -> sum(x)) => :xyz) == combine(d -> (xyz=sum(d.c),), gd)
        @test combine(gd, col => (x -> (sum(x),)) => :xyz) == combine(d -> (xyz=(sum(d.c),),), gd)
        @test combine(nrow, gd) == combine(d -> (nrow=length(d.c),), gd)
        @test combine(gd, nrow => :res) == combine(d -> (res=length(d.c),), gd)
        @test combine(gd, col => sum => :res) == combine(d -> (res=sum(d.c),), gd)
        @test combine(gd, col => (x -> sum(x)) => :res) == combine(d -> (res=sum(d.c),), gd)

        @test_throws ArgumentError combine(gd, col => (x -> (z=sum(x),)) => :xyz)
        @test_throws ArgumentError combine(gd, col => (x -> DataFrame(z=sum(x),)) => :xyz)
        @test_throws ArgumentError combine(gd, col => (x -> (z=x,)) => :xyz)
        @test_throws ArgumentError combine(gd, col => x -> (z=1, xzz=[1]))
    end

    for cols in ([:b, :c], 2:3, [2, 3], [false, true, true]), ungroup in (true, false)
        @test combine(gd, cols => ((b, c) -> (y=exp.(b), z=c)) => AsTable, ungroup=ungroup) ==
            combine(gd, d -> (y=exp.(d.b), z=d.c), ungroup=ungroup)
        @test combine(gd, cols => ((b, c) -> [exp.(b) c]) => AsTable, ungroup=ungroup) ==
            combine(d -> [exp.(d.b) d.c], gd, ungroup=ungroup)
        @test combine(gd, cols => ((b, c) -> sum(b) + sum(c)) => :xyz, ungroup=ungroup) ==
            combine(d -> (xyz=sum(d.b) + sum(d.c),), gd, ungroup=ungroup)
        if eltype(cols) !== Bool
            @test combine(gd, cols[1] => sum => :xyz, cols[2] => sum => :xzz, ungroup=ungroup) ==
                combine(d -> (xyz=sum(d.b), xzz=sum(d.c)), gd, ungroup=ungroup)
            @test combine(gd, cols[1] => sum => :xyz, cols[1] => sum => :xzz, ungroup=ungroup) ==
                combine(d -> (xyz=sum(d.b), xzz=sum(d.b)), gd, ungroup=ungroup)
            @test combine(gd, cols[1] => sum => :xyz,
                    cols[2] => (x -> first(x)) => :xzz, ungroup=ungroup) ==
                combine(d -> (xyz=sum(d.b), xzz=first(d.c)), gd, ungroup=ungroup)
            @test combine(gd, cols[1] => vexp => :xyz,
                    cols[2] => sum => :xzz, ungroup=ungroup) ==
                combine(d -> (xyz=vexp(d.b), xzz=fill(sum(d.c), length(vexp(d.b)))),
                        gd, ungroup=ungroup)
        end

        @test_throws ArgumentError combine(gd, cols => (b, c) -> (y=exp.(b), z=sum(c)),
                                           ungroup=ungroup)
        @test_throws ArgumentError combine(gd, cols => ((b, c) -> DataFrame(y=exp.(b),
                                           z=sum(c))) => :xyz, ungroup=ungroup)
        @test_throws ArgumentError combine(gd, cols => ((b, c) -> [exp.(b) c]) => :xyz,
                                           ungroup=ungroup)
    end
end

struct TestType end
Base.isless(::TestType, ::Int) = true
Base.isless(::Int, ::TestType) = false
Base.isless(::TestType, ::TestType) = false

@testset "combine with aggregation functions (skipmissing=$skip, sort=$sort, indices=$indices)" for
    skip in (false, true), sort in (false, true), indices in (false, true)
    Random.seed!(1)
    # 5 is there to ensure we test a single-row group
    df = DataFrame(a=[rand([1:4;missing], 19); 5],
                   x1=rand(1:100, 20),
                   x2=rand(1:100, 20) + im*rand(1:100, 20),
                   x3=rand(1:100, 20) .* u"m")

    for f in (sum, prod, maximum, minimum, mean, var, std, first, last, length)
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices

        res = combine(gd, :x1 => f => :y)
        expected = combine(gd, :x1 => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        for T in (Union{Missing, Int}, Union{Int, Int8},
                  Union{Missing, Int, Int8})
            df.x1u = Vector{T}(df.x1)
            gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
            indices && @test gd.idx !== nothing # Trigger computation of indices
            res = combine(gd, :x1u => f => :y)
            expected = combine(gd, :x1u => (x -> f(x)) => :y)
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
        end

        f === length && continue

        df.x1m = allowmissing(df.x1)
        df.x1m[1] = missing
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        res = combine(gd, :x1m => f => :y)
        expected = combine(gd, :x1m => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
        res = combine(gd, :x1m => f∘skipmissing => :y)
        expected = combine(gd, :x1m => (x -> f(collect(skipmissing(x)))) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        # Test reduction over group with only missing values
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        gd[1][:, :x1m] .= missing
        if f in (maximum, minimum, first, last)
            @test_throws ArgumentError combine(gd, :x1m => f∘skipmissing => :y)
        else
            res = combine(gd, :x1m => f∘skipmissing => :y)
            expected = combine(gd, :x1m => (x -> f(collect(skipmissing(x)))) => :y)
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

        f === length && continue

        df.x2m = allowmissing(df.x1)
        df.x2m[1] = missing
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        res = combine(gd, :x2m => f => :y)
        expected = combine(gd, :x2m => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
        res = combine(gd, :x2m => f∘skipmissing => :y)
        expected = combine(gd, :x2m => (x -> f(collect(skipmissing(x)))) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        # Test reduction over group with only missing values
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        gd[1][:, :x2m] .= missing
        if f in (maximum, minimum, first, last)
            @test_throws ArgumentError combine(gd, :x2m => f∘skipmissing => :y)
        else
            res = combine(gd, :x2m => f∘skipmissing => :y)
            expected = combine(gd, :x2m => (x -> f(collect(skipmissing(x)))) => :y)
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
        end
    end
    # Test Unitful numbers
    for f in (sum, mean, minimum, maximum, var, std, first, last, length)
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices

        res = combine(gd, :x3 => f => :y)
        expected = combine(gd, :x3 => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        f === length && continue

        df.x3m = allowmissing(df.x1)
        df.x3m[1] = missing
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        res = combine(gd, :x3m => f => :y)
        expected = combine(gd, :x3m => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
        res = combine(gd, :x3m => f∘skipmissing => :y)
        expected = combine(gd, :x3m => (x -> f(collect(skipmissing(x)))) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        # Test reduction over group with only missing values
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        gd[1][:, :x3m] .= missing
        if f in (maximum, minimum, first, last)
            @test_throws ArgumentError combine(gd, :x3m => f∘skipmissing => :y)
        else
            res = combine(gd, :x3m => f∘skipmissing => :y)
            expected = combine(gd, :x3m => (x -> f(collect(skipmissing(x)))) => :y)
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
        end
    end
    # Test CategoricalArray
    for f in (maximum, minimum, first, last, length),
        (T, m) in ((Int, false),
                   (Union{Missing, Int}, false), (Union{Missing, Int}, true))
        df.x1c = CategoricalVector{T}(df.x1)
        m && (df.x1c[1] = missing)
        gd = groupby_checked(df, :a, skipmissing=skip, sort=sort)
        indices && @test gd.idx !== nothing # Trigger computation of indices
        res = combine(gd, :x1c => f => :y)
        expected = combine(gd, :x1c => (x -> f(x)) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)

        f === length && continue

        res = combine(gd, :x1c => f∘skipmissing => :y)
        expected = combine(gd, :x1c => (x -> f(collect(skipmissing(x)))) => :y)
        @test res ≅ expected
        @test typeof(res.y) == typeof(expected.y)
        if m
            gd[1][:, :x1c] .= missing
            @test_throws Union{MethodError, ArgumentError} combine(gd, :x1c => f∘skipmissing => :y)
        end
    end
    @test combine(gd, :x1 => maximum => :y, :x2 => sum => :z) ≅
        combine(gd, :x1 => (x -> maximum(x)) => :y, :x2 => (x -> sum(x)) => :z)

    # Test floating point corner cases
    df = DataFrame(a=[1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6],
                   x1=[0.0, 1.0, 2.0, NaN, NaN, NaN, Inf, Inf, Inf, 1.0, NaN, 0.0, -0.0])

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

    df = DataFrame(x=[1, 1, 2, 2], y=Any[1, 2.0, 3.0, 4.0])
    res = combine(groupby_checked(df, :x), :y => maximum => :z)
    @test res.z isa Vector{Float64}
    @test res.z == combine(groupby_checked(df, :x), :y => (x -> maximum(x)) => :z).z

    # Test maximum when no promotion rule exists
    df = DataFrame(x=[1, 1, 2, 2], y=[1, TestType(), TestType(), TestType()])
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

@testset "Check aggregation of DataFrameRow and Tables.AbstractRow" begin
    df = DataFrame(a=1)
    dfr = DataFrame(x=1, y="1")[1, 2:2]
    gdf = groupby_checked(df, :a)
    @test combine(sdf -> dfr, gdf) == DataFrame(a=1, y="1")
    @test combine(sdf -> Tables.Row(dfr), gdf) == DataFrame(a=1, y="1")

    df = DataFrame(a=[1, 1, 2, 2, 3, 3], b='a':'f', c=string.(1:6))
    gdf = groupby_checked(df, :a)
    @test isequal_typed(combine(sdf -> sdf[1, [3, 2, 1]], gdf), df[1:2:5, [1, 3, 2]])
    @test isequal_typed(combine(sdf -> Tables.Row(sdf[1, [3, 2, 1]]), gdf), df[1:2:5, [1, 3, 2]])
end

@testset "Allow returning DataFrame() or NamedTuple() to drop group" begin
    N = 4
    for (i, x1) in enumerate(collect.(Iterators.product(repeat([[true, false]], N)...))),
        er in (DataFrame(), view(DataFrame(ones(2, 2), :auto), 2:1, 2:1),
               view(DataFrame(ones(2, 2), :auto), 1:2, 2:1),
               NamedTuple(), rand(0, 0), rand(5, 0),
               DataFrame(x1=Int[]), DataFrame(x1=Any[]),
               (x1=Int[],), (x1=Any[],), rand(0, 1)),
        fr in (DataFrame(x1=[true]), (x1=[true],))

        df = DataFrame(a=1:N, x1=x1)
        gdf = groupby_checked(df, :a)
        res = combine(sdf -> sdf.x1[1] ? fr : er, gdf)
        @test res == DataFrame(validate_gdf(combine(sdf -> sdf.x1[1] ? fr : er,
                                                    groupby_checked(df, :a), ungroup=false)))
        if fr isa AbstractVector && df.x1[1]
            @test res == combine(gdf, :x1 => (x1 -> x1[1] ? fr : er) => :x1)
        else
            @test res == combine(gdf, :x1 => (x1 -> x1[1] ? fr : er) => AsTable)
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
    df = DataFrame(g=[1, 1, 1, 2, 2, 2], x1=1:6, x2=1:6)
    gdf = groupby_checked(df, :g)
    @test combine(gdf, r"x" => cor) == DataFrame(g=[1, 2], x1_x2_cor=[1.0, 1.0])
    @test combine(gdf, Not(:g) => ByRow(/)) == DataFrame(:g => [1, 1, 1, 2, 2, 2], Symbol("x1_x2_/") => 1.0)
    @test combine(gdf, Between(:x2, :x1) => () -> 1) == DataFrame(:g => 1:2, Symbol("function") => 1)
    @test combine(gdf, :x1 => :z) == combine(gdf, [:x1 => :z]) == DataFrame(g=[1, 1, 1, 2, 2, 2], z=1:6)
    @test validate_gdf(combine(groupby_checked(df, :g), :x1 => :z, ungroup=false)) ==
          groupby_checked(DataFrame(g=[1, 1, 1, 2, 2, 2], z=1:6), :g)
end

@testset "hard tabular return value cases" begin
    Random.seed!(1)
    df = DataFrame(b=repeat([2, 1], outer=[4]), x=randn(8))
    gdf = groupby_checked(df, :b)
    res = combine(sdf -> sdf.x[1:2], gdf)
    @test names(res) == ["b", "x1"]
    res2 = combine(gdf, :x => x -> x[1:2])
    @test names(res2) == ["b", "x_function"]
    @test Matrix(res) == Matrix(res2)
    res2 = combine(gdf, :x => (x -> x[1:2]) => :z)
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
        @test_throws ArgumentError combine(gdf, [:b, :x] => ((b, x) -> b[1] == i ? x[v1] : (c=x[v2],)) => :v)
        @test_throws ArgumentError combine(gdf, [:b, :x] => ((b, x) -> b[1] == i ? x[v1] : (v=x[v2],)) => :v)
    end
end

@testset "last Pair interface with multiple return values" begin
    df = DataFrame(g=[1, 1, 1, 2, 2, 2], x1=1:6)
    gdf = groupby_checked(df, :g)
    @test_throws ArgumentError combine(gdf, :x1 => x -> DataFrame())
    @test_throws ArgumentError combine(gdf, :x1 => x -> (x=1, y=2))
    @test_throws ArgumentError combine(gdf, :x1 => x -> (x=[1], y=[2]))
    @test_throws ArgumentError combine(gdf, :x1 => (x -> (x=[1], y=2)) => AsTable)
    @test_throws ArgumentError combine(gdf, :x1 => x -> (x=[1], y=2))
    @test_throws ArgumentError combine(gdf, :x1 => x -> ones(2, 2))
    @test_throws ArgumentError combine(gdf, :x1 => x -> df[1, Not(:g)])
    @test_throws ArgumentError combine(gdf, :x1 => x -> Tables.Row(df[1, Not(:g)]))
end

@testset "keepkeys" begin
    df = DataFrame(g=[1, 1, 1, 2, 2, 2], x1=1:6)
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x1 => identity => :g, keepkeys=false) == DataFrame(g=1:6)
    @test combine(x -> (z=x.x1,), gdf, keepkeys=false) == DataFrame(z=1:6)
end

@testset "additional do_call tests" begin
    Random.seed!(1234)
    df = DataFrame(g=rand(1:10, 100), x1=rand(1:1000, 100))
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
    df = DataFrame(g=[1, 1, 1, 2, 2])
    gdf = groupby_checked(df, :g)

    f1a(i) = i[1] == 1 ? ["a", "b"] : ["c"]
    f2a(i) = i[1] == 1 ? ["d"] : ["e", "f"]
    @test_throws ArgumentError combine(gdf, :g => f1a, :g => f2a)

    f1b(i) = i[1] == 1 ? ["a"] : ["c"]
    f2b(i) = i[1] == 1 ? "d" : "e"
    @test combine(gdf, :g => f1b, :g => f2b) ==
          DataFrame(g=[1, 2], g_f1b=["a", "c"], g_f2b=["d", "e"])

    f1c(i) = i[1] == 1 ? ["a", "c"] : []
    f2c(i) = i[1] == 1 ? "d" : "e"
    @test combine(gdf, :g => f1c, :g => f2c) ==
          DataFrame(g=[1, 1], g_f1c=["a", "c"], g_f2c=["d", "d"])

    @test combine(gdf, :g => Ref) == DataFrame(g=[1, 2], g_Ref=[[1, 1, 1], [2, 2]])
    @test combine(gdf, :g => x -> view([x], 1)) == DataFrame(g=[1, 2], g_function=[[1, 1, 1], [2, 2]])

    Random.seed!(1234)
    df = DataFrame(g=1:100)
    gdf = groupby_checked(df, :g)
    for i in 1:10
        @test combine(gdf, :g => x -> rand([x[1], Ref(x[1]), view(x, 1)])) ==
              DataFrame(g=1:100, g_function=1:100)
    end

    df_ref = DataFrame(rand(10, 4), :auto)
    df_ref.g = shuffle!([1, 2, 2, 3, 3, 3, 4, 4, 4, 4])

    for i in 0:nrow(df_ref), dosort in (true, false, nothing), dokeepkeys in (true, false)
        df = df_ref[1:i, :]
        gdf = groupby_checked(df, :g, sort=dosort)
        @test combine(gdf, :x1 => sum => :x1, :x2 => identity => :x2,
                      :x3 => (x -> Ref(sum(x))) => :x3, nrow, :x4 => ByRow(sin) => :x4,
                      keepkeys=dokeepkeys) ==
              combine(gdf, keepkeys=dokeepkeys) do sdf
                      DataFrame(x1=sum(sdf.x1), x2=sdf.x2, x3=sum(sdf.x3),
                                nrow=nrow(sdf), x4=sin.(sdf.x4))
              end
    end
end

@testset "passing columns" begin
    df = DataFrame(rand(10, 4), :auto)
    df.g = shuffle!([1, 2, 2, 3, 3, 3, 4, 4, 4, 4])
    gdf = groupby_checked(df, :g)

    for selector in [Cols(:), All(), :, r"x", Between(:x1, :x4), Not(:g), [:x1, :x2, :x3, :x4],
                     [1, 2, 3, 4], [true, true, true, true, false]]
        @test combine(gdf, selector, :x1 => ByRow(sin) => :x1, :x2 => ByRow(sin) => :x3) ==
              combine(gdf) do sdf
                  DataFrame(x1=sin.(sdf.x1), x2=sdf.x2, x3=sin.(sdf.x2), x4=sdf.x4)
              end
    end

    for selector in [Cols(:), All(), :, r"x", Between(:x1, :x4), Not(:g), [:x1, :x2, :x3, :x4],
                     [1, 2, 3, 4], [true, true, true, true, false]]
        @test combine(gdf, :x1 => ByRow(sin) => :x1, :x2 => ByRow(sin) => :x3, selector) ==
              combine(gdf) do sdf
                  DataFrame(x1=sin.(sdf.x1), x3=sin.(sdf.x2), x2=sdf.x2, x4=sdf.x4)
              end
    end

    for selector in [Between(:x1, :x3), Not(:x4), [:x1, :x2, :x3], [1, 2, 3],
                     [true, true, true, false, false]]
        @test combine(gdf, :x2 => ByRow(sin) => :x3, selector, :x1 => ByRow(sin) => :x1) ==
              combine(gdf) do sdf
                  DataFrame(x3=sin.(sdf.x2), x1=sin.(sdf.x1), x2=sdf.x2)
              end
    end

    @test combine(gdf, 4, :x1 => ByRow(sin) => :x1, :x2 => ByRow(sin) => :x3, :x2) ==
          combine(gdf) do sdf
              DataFrame(x4=sdf.x4, x1=sin.(sdf.x1), x3=sin.(sdf.x2), x2=sdf.x2)
          end

    @test combine(gdf, 4 => :h, :x1 => ByRow(sin) => :z, :x2 => ByRow(sin) => :x3, :x2) ==
          combine(gdf) do sdf
              DataFrame(h=sdf.x4, z=sin.(sdf.x1), x3=sin.(sdf.x2), x2=sdf.x2)
          end

    @test_throws ArgumentError combine(gdf, 4 => :h, :x1 => ByRow(sin) => :h)
    @test_throws ArgumentError combine(gdf, :x1 => :x1_sin, :x1 => ByRow(sin))
    @test_throws ArgumentError combine(gdf, 1, :x1 => ByRow(sin) => :x1)
end

@testset "correct dropping of groups" begin
    df = DataFrame(g=1:10)
    gdf = groupby_checked(df, :g)
    sgdf = groupby_checked(df, :g, sort=true)
    for keep in [[3, 2, 1], [5, 3, 1], [9], Int[]]
        @test sort(combine(gdf, :g => first => :keep, :g => x -> x[1] in keep ? x : Int[])) ==
              combine(sgdf, :g => first => :keep, :g => x -> x[1] in keep ? x : Int[]) ==
              sort(DataFrame(g=keep, keep=keep, g_function=keep))
    end
end

@testset "AsTable tests" begin
    df = DataFrame(g=[1, 1, 1, 2, 2], x=1:5, y=6:10)
    gdf = groupby_checked(df, :g)

    # whole column 4 options of single pair passed
    @test combine(gdf , AsTable([:x, :y]) => Ref) ==
          combine(gdf, AsTable([:x, :y]) => Ref) ==
          DataFrame(g=1:2, x_y_Ref=[(x=[1, 2, 3], y=[6, 7, 8]), (x=[4, 5], y=[9, 10])])
    @test validate_gdf(combine(gdf, AsTable([:x, :y]) => Ref, ungroup=false)) ==
          groupby_checked(combine(gdf, AsTable([:x, :y]) => Ref), :g)

    @test combine(gdf, AsTable(1) => Ref) ==
          DataFrame(g=1:2, g_Ref=[(g=[1, 1, 1],), (g=[2, 2],)])


    # ByRow 4 options of single pair passed
    @test combine(gdf, AsTable([:x, :y]) => ByRow(x -> [x])) ==
          combine(gdf, AsTable([:x, :y]) => ByRow(x -> [x])) ==
          DataFrame(g=[1, 1, 1, 2, 2],
                    x_y_function=[[(x=1, y=6)], [(x=2, y=7)], [(x=3, y=8)], [(x=4, y=9)], [(x=5, y=10)]])
    @test validate_gdf(combine(gdf, AsTable([:x, :y]) => ByRow(x -> [x]), ungroup=false)) ==
          groupby_checked(combine(gdf, AsTable([:x, :y]) => ByRow(x -> [x])), :g)

    # whole column and ByRow test for multiple pairs passed
    @test combine(gdf, [:x, :y], [AsTable(v) => (x -> -x[1]) for v in [:x, :y]]) ==
          [df DataFrame(x_function=-df.x, y_function=-df.y)]
    @test combine(gdf, [:x, :y], [AsTable(v) => ByRow(x -> (-x[1],)) for v in [:x, :y]]) ==
          [df DataFrame(x_function=[(-1,), (-2,) , (-3,) , (-4,) , (-5,)],
                        y_function=[(-6,), (-7,) , (-8,) , (-9,) , (-10,)])]

    @test combine(gdf, AsTable([:x, :y]) => ByRow(identity)) ==
          DataFrame(g=[1, 1, 1, 2, 2], x_y_identity=ByRow(identity)((x=1:5, y=6:10)))
    @test combine(gdf, AsTable([:x, :y]) => ByRow(x -> df[1, :])) ==
          DataFrame(g=[1, 1, 1, 2, 2], x_y_function=fill(df[1, :], 5))
    @test combine(gdf, AsTable([:x, :y]) => ByRow(x -> Tables.Row(df[1, :]))) ==
          DataFrame(g=[1, 1, 1, 2, 2], x_y_function=fill(Tables.Row(df[1, :]), 5))
end

@testset "test correctness of ungrouping" begin
    df = DataFrame(g=[2, 2, 1, 3, 1, 2, 1, 2, 3])
    gdf = groupby_checked(df, :g)
    gdf2 = validate_gdf(combine(identity, gdf, ungroup=false))
    @test combine(gdf, :g => sum) == combine(gdf2, :g => sum)

    df.id = 1:9
    @test select(gdf, :g => sum) ==
          sort!(combine(gdf, :g => sum, :id), :id)[:, Not(end)]
    @test select(gdf2, :g => sum) == combine(gdf2, :g => sum, :g)
end

@testset "combine GroupedDataFrame" begin
    for df in (DataFrame(g=[3, 1, 1, missing], x=1:4, y=5:8),
               DataFrame(g=categorical([3, 1, 1, missing]), x=1:4, y=5:8))
        if !(df.g isa CategoricalVector)
            gdf = groupby_checked(df, :g, sort=false, skipmissing=false)

            @test sort(combine(gdf, :x => sum, keepkeys=true, ungroup=true)) ≅
                  DataFrame(g=[1, 3, missing], x_sum=[5, 1, 4])
            @test combine(gdf, :x => sum, keepkeys=false, ungroup=true) ==
                select(combine(gdf, :x => sum, keepkeys=true, ungroup=true), :x_sum)
            @test_throws ArgumentError combine(gdf, :x => sum, keepkeys=false, ungroup=false)
            gdf2 = validate_gdf(combine(gdf, :x => sum, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test sort(DataFrame(gdf2)) ≅ DataFrame(g=[1, 3, missing], x_sum=[5, 1, 4])
            @test DataFrame(gdf2, keepkeys=false) == select(DataFrame(gdf2), :x_sum)

            @test sort(combine(gdf, :x => sum, :g, keepkeys=false, ungroup=true)) ≅
                  DataFrame(x_sum=[1, 4, 5, 5], g=[3, missing, 1, 1])
            @test sort(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=true)) ≅
                  DataFrame(g=[1, 1, 3, missing], x_sum=[5, 5, 1, 4])
            gdf2 = validate_gdf(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test sort(DataFrame(gdf2)) ≅ DataFrame(g=[1, 1, 3, missing], x_sum=[5, 5, 1, 4])
            @test DataFrame(gdf2, keepkeys=false) ≅ select(DataFrame(gdf2), :x_sum)

            @test sort(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true)) ≅
                  DataFrame(g=[1, 3, missing], x_sum=[5, 1, 4])
            @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=false, ungroup=true) ==
                select(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true), :x_sum)
            gdf2 = validate_gdf(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test sort(DataFrame(gdf2)) ≅ DataFrame(g=[1, 3, missing], x_sum=[5, 1, 4])
            @test DataFrame(gdf2, keepkeys=false) ≅ select(DataFrame(gdf2), :x_sum)

            gdf = groupby_checked(df, :g, sort=false, skipmissing=true)

            @test sort(combine(gdf, :x => sum, keepkeys=false, ungroup=true)) ≅
                  DataFrame(x_sum=[1, 5])
            @test_throws ArgumentError combine(gdf, :x => sum, keepkeys=false, ungroup=false)
            @test sort(combine(gdf, :x => sum, keepkeys=true, ungroup=true)) ≅
                  DataFrame(g=[1, 3], x_sum=[5, 1])
            gdf2 = validate_gdf(combine(gdf, :x => sum, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test sort(DataFrame(gdf2)) ≅ DataFrame(g=[1, 3], x_sum=[5, 1])
            @test DataFrame(gdf2, keepkeys=false) ≅ select(DataFrame(gdf2), :x_sum)

            @test sort(combine(gdf, :x => sum, :g, keepkeys=false, ungroup=true)) ≅
                  DataFrame(x_sum=[1, 5, 5], g=[3, 1, 1])
            @test sort(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=true)) ≅
                  DataFrame(g=[1, 1, 3], x_sum=[5, 5, 1])
            gdf2 = validate_gdf(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test sort(DataFrame(gdf2)) ≅ DataFrame(g=[1, 1, 3], x_sum=[5, 5, 1])
            @test DataFrame(gdf2, keepkeys=false) ≅ select(DataFrame(gdf2), :x_sum)

            @test sort(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true)) ≅
                  DataFrame(g=[1, 3], x_sum=[5, 1])
            @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=false, ungroup=true) ≅
                select(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true), :x_sum)
            gdf2 = validate_gdf(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=false))
            @test gdf2 isa GroupedDataFrame{DataFrame}
            @test sort(DataFrame(gdf2)) ≅ DataFrame(g=[1, 3], x_sum=[5, 1])
            @test DataFrame(gdf2, keepkeys=false) ≅ select(DataFrame(gdf2), :x_sum)
        end

        gdf = groupby_checked(df, :g, sort=true, skipmissing=false)

        @test combine(gdf, :x => sum, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum=[5, 1, 4])
        @test_throws ArgumentError validate_gdf(combine(gdf, :x => sum, keepkeys=false, ungroup=false))
        @test combine(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
              DataFrame(g=[1, 3, missing], x_sum=[5, 1, 4])
        gdf2 = validate_gdf(combine(gdf, :x => sum, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test DataFrame(gdf2) ≅ DataFrame(g=[1, 3, missing], x_sum=[5, 1, 4])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum=[5, 1, 4])

        @test combine(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
              DataFrame(x_sum=[5, 5, 1, 4], g=[1, 1, 3, missing])
        @test combine(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
              DataFrame(g=[1, 1, 3, missing], x_sum=[5, 5, 1, 4])
        gdf2 = validate_gdf(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test DataFrame(gdf2) ≅ DataFrame(g=[1, 1, 3, missing], x_sum=[5, 5, 1, 4])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum=[5, 5, 1, 4])

        @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum=[5, 1, 4])
        @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true) ≅
              DataFrame(g=[1, 3, missing], x_sum=[5, 1, 4])
        gdf2 = validate_gdf(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test DataFrame(gdf2) ≅ DataFrame(g=[1, 3, missing], x_sum=[5, 1, 4])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum=[5, 1, 4])

        gdf = groupby_checked(df, :g, sort=true, skipmissing=true)

        @test combine(gdf, :x => sum, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum=[5, 1])
        @test_throws ArgumentError combine(gdf, :x => sum, keepkeys=false, ungroup=false)
        @test combine(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
              DataFrame(g=[1, 3], x_sum=[5, 1])
        gdf2 = validate_gdf(combine(gdf, :x => sum, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test DataFrame(gdf2) ≅ DataFrame(g=[1, 3], x_sum=[5, 1])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum=[5, 1])

        @test combine(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
              DataFrame(x_sum=[5, 5, 1], g=[1, 1, 3])
        @test combine(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
              DataFrame(g=[1, 1, 3], x_sum=[5, 5, 1])
        gdf2 = validate_gdf(combine(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test DataFrame(gdf2) ≅ DataFrame(g=[1, 1, 3], x_sum=[5, 5, 1])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum=[5, 5, 1])

        @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum=[5, 1])
        @test combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=true) ≅
              DataFrame(g=[1, 3], x_sum=[5, 1])
        gdf2 = validate_gdf(combine(x -> (x_sum = sum(x.x),), gdf, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test DataFrame(gdf2) ≅ DataFrame(g=[1, 3], x_sum=[5, 1])
        @test DataFrame(gdf2, keepkeys=false) ≅ DataFrame(x_sum=[5, 1])
    end
end

@testset "select and transform GroupedDataFrame" begin
    for df in (DataFrame(g=[3, 1, 1, missing], x=1:4, y=5:8),
               DataFrame(g=categorical([3, 1, 1, missing]), x=1:4, y=5:8)),
        dosort in (true, false, nothing)

        gdf = groupby_checked(df, :g, sort=dosort, skipmissing=false)

        @test select(gdf, :x => sum, keepkeys=false, ungroup=true) ==
              DataFrame(x_sum=[1, 5, 5, 4])
        @test_throws ArgumentError select(gdf, :x => sum, keepkeys=false, ungroup=false)
        @test select(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
              DataFrame(g=df.g, x_sum=[1, 5, 5, 4])
        gdf2 = validate_gdf(select(gdf, :x => sum, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == gdf.groups
        @test parent(gdf2).g ≅ df.g
        @test parent(gdf2).g !== df.g

        @test select(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
              DataFrame(x_sum=[1, 5, 5, 4], g=df.g)
        @test select(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
              DataFrame(g=df.g, x_sum=[1, 5, 5, 4])
        gdf2 = validate_gdf(select(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == gdf.groups
        @test parent(gdf2).g ≅ df.g
        @test parent(gdf2).g !== df.g

        @test transform(gdf, :x => sum, keepkeys=false, ungroup=true) ≅
              [df DataFrame(x_sum=[1, 5, 5, 4])]
        @test_throws ArgumentError transform(gdf, :x => sum, keepkeys=false, ungroup=false)
        @test transform(gdf, :x => sum, keepkeys=true, ungroup=true) ≅
              DataFrame(g=df.g, x=df.x, y=df.y, x_sum=[1, 5, 5, 4])
        gdf2 = validate_gdf(transform(gdf, :x => sum, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == gdf.groups
        @test parent(gdf2).g ≅ df.g
        @test parent(gdf2).x ≅ df.x
        @test parent(gdf2).y ≅ df.y
        @test parent(gdf2).g !== df.g

        @test transform(gdf, :x => sum, :g, keepkeys=false, ungroup=true) ≅
              [df DataFrame(x_sum=[1, 5, 5, 4])]
        @test transform(gdf, :x => sum, :g, keepkeys=true, ungroup=true) ≅
              [df DataFrame(x_sum=[1, 5, 5, 4])]
        gdf2 = validate_gdf(transform(gdf, :x => sum, :g, keepkeys=true, ungroup=false))
        @test gdf2 isa GroupedDataFrame{DataFrame}
        @test gdf2.groups == gdf.groups
        @test parent(gdf2).g ≅ df.g
        @test parent(gdf2).x ≅ df.x
        @test parent(gdf2).y ≅ df.y
        @test parent(gdf2).g !== df.g

        df2 = transform(gdf, :x => sum, :g, keepkeys=false, ungroup=true, copycols=false)
        @test df2 ≅ [df DataFrame(x_sum=[1, 5, 5, 4])]
        @test df2.g === df.g
        @test df2.x === df.x
        @test df2.y === df.y
        df2 = transform(gdf, :x => sum, :g, keepkeys=true, ungroup=true, copycols=false)
        @test df2 ≅ [df DataFrame(x_sum=[1, 5, 5, 4])]
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
        dosort in (true, false, nothing)

        gdf = groupby_checked(df, :g, sort=dosort)

        res1 = select(gdf, :x => mean, :x => x -> x .- mean(x), :id)
        @test res1.g == df.g
        @test res1.id == df.id
        @test res1.x_mean + res1.x_function ≈ df.x

        res2 = combine(gdf, :x => mean, :x => x -> x .- mean(x), :id)
        @test sort(unique(res2.g)) == sort(unique(df.g))
        for i in unique(res2.g)
            @test issorted(filter(:g => x -> x == i, res2).id)
        end
    end
end

@testset "select! and transform! GroupedDataFrame" begin
    for df in (DataFrame(g=[3, 1, 1, missing], x=1:4, y=5:8),
               DataFrame(g=categorical([3, 1, 1, missing]), x=1:4, y=5:8)),
        dosort in (true, false, nothing)

        dfc = copy(df)
        select!(groupby_checked(view(dfc, :, :), :g), :x)
        @test dfc ≅ df[!, [:g, :x]]
        dfc = copy(df)
        transform!(groupby_checked(view(dfc, :, :), :g), :x)
        @test dfc ≅ df

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
    df = DataFrame(g=[1, 1, 1, 2, 2, 2], x=Any[1, 1, 1, 1.5, 1.5, 1.5])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum) == DataFrame(g=1:2, x_sum=[3.0, 4.5])

    @test combine(gdf, :x => sum∘skipmissing) == DataFrame(g=1:2, x_sum_skipmissing=[3.0, 4.5])
    @test combine(gdf, :x => mean∘skipmissing) == DataFrame(g=1:2, x_mean_skipmissing=[1.0, 1.5])
    @test combine(gdf, :x => var∘skipmissing) == DataFrame(g=1:2, x_var_skipmissing=[0.0, 0.0])
    @test combine(gdf, :x => mean) == DataFrame(g=1:2, x_mean=[1.0, 1.5])
    @test combine(gdf, :x => var) == DataFrame(g=1:2, x_var=[0.0, 0.0])

    df = DataFrame(g=[1, 1, 1, 2, 2, 2], x=Any[1, 1, 1, 1, 1, missing])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum∘skipmissing) == DataFrame(g=1:2, x_sum_skipmissing=[3, 2])
    @test combine(gdf, :x => mean∘skipmissing) == DataFrame(g=1:2, x_mean_skipmissing=[1.0, 1.0])
    @test combine(gdf, :x => var∘skipmissing) == DataFrame(g=1:2, x_var_skipmissing=[0.0, 0.0])
    @test combine(gdf, :x => sum) ≅ DataFrame(g=1:2, x_sum=[3, missing])
    @test combine(gdf, :x => mean) ≅ DataFrame(g=1:2, x_mean=[1.0, missing])
    @test combine(gdf, :x => var) ≅ DataFrame(g=1:2, x_var=[0.0, missing])

    df = DataFrame(g=[1, 1, 1, 2, 2, 2], x=Union{Real, Missing}[1, 1, 1, 1, 1, missing])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum∘skipmissing) == DataFrame(g=1:2, x_sum_skipmissing=[3, 2])
    @test combine(gdf, :x => mean∘skipmissing) == DataFrame(g=1:2, x_mean_skipmissing=[1.0, 1.0])
    @test combine(gdf, :x => var∘skipmissing) == DataFrame(g=1:2, x_var_skipmissing=[0.0, 0.0])
    @test combine(gdf, :x => sum) ≅ DataFrame(g=1:2, x_sum=[3, missing])
    @test combine(gdf, :x => mean) ≅ DataFrame(g=1:2, x_mean=[1.0, missing])
    @test combine(gdf, :x => var) ≅ DataFrame(g=1:2, x_var=[0.0, missing])

    Random.seed!(1)
    df = DataFrame(g=rand(1:2, 1000), x1=rand(Int, 1000))
    df.x2 = big.(df.x1)
    gdf = groupby_checked(df, :g)

    res = combine(gdf, :x1 => sum, :x2 => sum, :x1 => x -> sum(x), :x2 => x -> sum(x))
    @test res.x1_sum == res.x1_function
    @test res.x2_sum == res.x2_function
    @test res.x1_sum != res.x2_sum # we are large enough to be sure we differ

    res = combine(gdf, :x1 => mean, :x2 => mean, :x1 => x -> mean(x), :x2 => x -> mean(x))
    @test res.x1_mean ≈ res.x1_function
    @test res.x2_mean ≈ res.x2_function
    @test res.x1_mean ≈ res.x2_mean

    # make sure we do correct promotions in corner case similar to Base
    df = DataFrame(g=[1, 1, 1, 1, 1, 1], x=Real[1, 1, big(typemax(Int)), 1, 1, 1])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] == sum(df.x)
    @test eltype(combine(gdf, :x => sum)[!, 2]) === BigInt
    df = DataFrame(g=[1, 1, 1, 1, 1, 1], x=Real[1, 1, typemax(Int), 1, 1, 1])
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] == sum(df.x)
    @test eltype(combine(gdf, :x => sum)[!, 2]) === Int
    df = DataFrame(g=[1, 1, 1, 1, 1, 1], x=fill(missing, 6))
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] isa Missing
    @test eltype(combine(gdf, :x => sum)[!, 2]) === Missing
    @test_throws Exception combine(gdf, :x => sum∘skipmissing)
    df = DataFrame(g=[1, 1, 1, 1, 1, 1], x=convert(Vector{Union{Real, Missing}}, fill(missing, 6)))
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] isa Missing
    @test eltype(combine(gdf, :x => sum)[!, 2]) === Missing
    @test combine(gdf, :x => sum∘skipmissing) == DataFrame(g=1, x_sum_skipmissing=0)
    @test eltype(combine(gdf, :x => sum∘skipmissing)[!, 2]) === Int
    df = DataFrame(g=[1, 1, 1, 1, 1, 1], x=convert(Vector{Union{Int, Missing}}, fill(missing, 6)))
    gdf = groupby_checked(df, :g)
    @test combine(gdf, :x => sum)[1, 2] isa Missing
    @test eltype(combine(gdf, :x => sum)[!, 2]) === Missing
    @test combine(gdf, :x => sum∘skipmissing)[1, 2] == 0
    @test eltype(combine(gdf, :x => sum∘skipmissing)[!, 2]) === Int
    df = DataFrame(g=[1, 1, 1, 1, 1, 1], x=convert(Vector{Any}, fill(missing, 6)))
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
    @test combine(nrow, df) == DataFrame(nrow=0)
    @test combine(sdf -> DataFrame(a=1, b=2), df) == DataFrame(a=1, b=2)
end

@testset "disallowed tuple column selector" begin
    df = DataFrame(g=1:3)
    gdf = groupby(df, :g)
    @test_throws ArgumentError combine((:g, :g) => identity, gdf)
    @test_throws ArgumentError combine(gdf, (:g, :g) => identity)
end

@testset "check isagg correctly uses fast path only when it should" begin
    for fun in (sum, prod, mean, var, std, sum∘skipmissing, prod∘skipmissing,
                mean∘skipmissing, var∘skipmissing, std∘skipmissing),
        col in ([1, 2, 3], [big(1.5), big(2.5), big(3.5)], [1 + 0.5im, 2 + 0.5im, 3 + 0.5im],
                [true, false, true], [pi, pi, pi], [1//2, 1//3, 1//4],
                Real[1, 1.5, 1//2], Number[1, 1.5, 1//2], Any[1, 1.5, 1//2],
                [1, 2, missing], [big(1.5), big(2.5), missing], [1 + 0.5im, 2 + 0.5im, missing],
                [true, false, missing], [pi, pi, missing], [1//2, 1//3, missing],
                Union{Missing, Real}[1, 1.5, missing],
                Union{Missing, Number}[1, 1.5, missing], Any[1, 1.5, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        @test isequal_coltyped(combine(gdf, :x => fun => :y), combine(gdf, :x => (x -> fun(x)) => :y))
    end

    for fun in (maximum, minimum, maximum∘skipmissing, minimum∘skipmissing),
        col in ([1, 2, 3], [big(1.5), big(2.5), big(3.5)],
                [true, false, true], [pi, pi, pi], [1//2, 1//3, 1//4],
                Real[1, 1.5, 1//2], Number[1, 1.5, 1//2], Any[1, 1.5, 1//2],
                [1, 2, missing], [big(1.5), big(2.5), missing],
                [true, false, missing], [pi, pi, missing], [1//2, 1//3, missing],
                Union{Missing, Real}[1, 1.5, missing],
                Union{Missing, Number}[1, 1.5, missing], Any[1, 1.5, missing])
        gdf = groupby_checked(DataFrame(g=[1, 1, 1], x=col), :g)
        @test isequal_coltyped(combine(gdf, :x => fun => :y), combine(gdf, :x => (x -> fun(x)) => :y))
    end

    for fun in (first, last, length, first∘skipmissing, last∘skipmissing),
        col in ([1, 2, 3], [big(1.5), big(2.5), big(3.5)], [1 + 0.5im, 2 + 0.5im, 3 + 0.5im],
                [true, false, true], [pi, pi, pi], [1//2, 1//3, 1//4],
                Real[1, 1.5, 1//2], Number[1, 1.5, 1//2], Any[1, 1.5, 1//2],
                [1, 2, missing], [big(1.5), big(2.5), missing], [1 + 0.5im, 2 + 0.5im, missing],
                [true, false, missing], [pi, pi, missing], [1//2, 1//3, missing],
                Union{Missing, Real}[1, 1.5, missing],
                Union{Missing, Number}[1, 1.5, missing], Any[1, 1.5, missing])
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
        col in ([ones(2, 2), zeros(2, 2), ones(2, 2)], [ones(2, 2), zeros(2, 2), missing],
                [DataFrame(ones(2, 2), :auto), DataFrame(zeros(2, 2), :auto),
                DataFrame(ones(2, 2), :auto)], [DataFrame(ones(2, 2), :auto),
                DataFrame(zeros(2, 2), :auto), ones(2, 2)],
                [DataFrame(ones(2, 2), :auto), DataFrame(zeros(2, 2), :auto), missing],
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

@testset "renamecols=false tests" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9, d=10:12)
    gdf = groupby_checked(df, :a)

    @test select(gdf, :a => +, [:a, :b] => +, All() => +, renamecols=false) ==
          DataFrame(a=1:3, a_b=5:2:9, a_b_etc=22:4:30)
    @test select(gdf, :a => +, [:a, :b] => +, Cols(:) => +, renamecols=false) ==
          DataFrame(a=1:3, a_b=5:2:9, a_b_etc=22:4:30)
    @test_throws ArgumentError select(gdf, [] => () -> 10, renamecols=false)
    @test transform(gdf, :a => +, [:a, :b] => +, All() => +, renamecols=false) ==
          DataFrame(a=1:3, b=4:6, c=7:9, d=10:12, a_b=5:2:9, a_b_etc=22:4:30)
    @test combine(gdf, :a => +, [:a, :b] => +, All() => +, renamecols=false) ==
          DataFrame(a=1:3, a_b=5:2:9, a_b_etc=22:4:30)
    @test combine(gdf, [:a, :b] => +, renamecols=false) == DataFrame(a=1:3, a_b=5:2:9)
    @test combine(identity, gdf, renamecols=false) == df

    df = DataFrame(a=1:3, b=4:6, c=7:9, d=10:12)
    gdf = groupby_checked(df, :a)
    @test select!(gdf, :a => +, [:a, :b] => +, All() => +, renamecols=false) == df
    @test df == DataFrame(a=1:3, a_b=5:2:9, a_b_etc=22:4:30)

    df = DataFrame(a=1:3, b=4:6, c=7:9, d=10:12)
    gdf = groupby_checked(df, :a)
    @test transform!(gdf, :a => +, [:a, :b] => +, All() => +, renamecols=false) == df
    @test df == DataFrame(a=1:3, b=4:6, c=7:9, d=10:12, a_b=5:2:9, a_b_etc=22:4:30)
end

@testset "empty ByRow" begin
    inc0 = let
        state = 0
        () -> (state += 1)
    end

    inc1 = let
        state = 0
        x -> (state += 1)
    end

    df = DataFrame(a=[1, 1, 1, 2, 2, 3, 4, 4, 5, 5, 5, 5], b=1:12)
    gdf = groupby_checked(df, :a)

    @test select(gdf, [] => ByRow(inc0) => :bin) ==
          DataFrame(a=df.a, bin=1:12)
    @test combine(gdf, [] => ByRow(inc0) => :bin) ==
          DataFrame(a=df.a, bin=13:24)
    @test select(gdf, AsTable([]) => ByRow(inc1) => :bin) ==
          DataFrame(a=df.a, bin=1:12)
    @test combine(gdf, AsTable([]) => ByRow(inc1) => :bin) ==
          DataFrame(a=df.a, bin=13:24)
    @test combine(gdf[Not(2)], [] => ByRow(inc0) => :bin) ==
          DataFrame(a=df.a[Not(4:5)], bin=25:34)
    @test combine(gdf[Not(2)], AsTable([]) => ByRow(inc1) => :bin) ==
          DataFrame(a=df.a[Not(4:5)], bin=25:34)

    # note that type inference in a comprehension does not always work
    @test isequal_coltyped(combine(gdf[[]], [] => ByRow(inc0) => :bin),
                           DataFrame(a=Int[], bin=Any[]))
    @test isequal_coltyped(combine(gdf[[]], [] => ByRow(rand) => :bin),
                           DataFrame(a=Int[], bin=Float64[]))
    @test isequal_coltyped(combine(gdf[[]], AsTable([]) => ByRow(inc1) => :bin),
                           DataFrame(a=Int[], bin=Any[]))
    @test isequal_coltyped(combine(gdf[[]], AsTable([]) => ByRow(x -> rand()) => :bin),
                           DataFrame(a=Int[], bin=Float64[]))

    @test_throws MethodError select(gdf, [] => ByRow(inc1) => :bin)
    @test_throws MethodError select(gdf, AsTable([]) => ByRow(inc0) => :bin)
end

@testset "aggregation of reordered groups" begin
    df = DataFrame(id=[1, 2, 3, 1, 3, 2], x=1:6)
    gdf = groupby(df, :id)
    @test select(df, :id, :x => x -> 2x) == select(gdf, :x => x -> 2x)
    @test select(df, identity) == select(gdf, identity)
    @test select(df, :id, x -> (a=x.x, b=x.x)) == select(gdf, x -> (a=x.x, b=x.x))
    @test transform(df, :x => x -> 2x) == transform(gdf, :x => x -> 2x)
    @test transform(df, identity) == transform(gdf, identity)
    @test transform(df, x -> (a=x.x, b=x.x)) == transform(gdf, x -> (a=x.x, b=x.x))
    @test combine(gdf, :x => x -> 2x) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], x_function=[2, 8, 4, 12, 6, 10])
    @test combine(gdf, identity) == DataFrame(gdf)
    @test combine(gdf, x -> (a=x.x, b=x.x)) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], a=[1, 4, 2, 6, 3, 5], b=[1, 4, 2, 6, 3, 5])
    gdf = groupby(df, :id)[[3, 1, 2]]
    @test select(df, :id, :x => x -> 2x) == select(gdf, :x => x -> 2x)
    @test select(df, identity) == select(gdf, identity)
    @test select(df, :id, x -> (a=x.x, b=x.x)) == select(gdf, x -> (a=x.x, b=x.x))
    @test transform(df, :x => x -> 2x) == transform(gdf, :x => x -> 2x)
    @test transform(df, identity) == transform(gdf, identity)
    @test transform(df, x -> (a=x.x, b=x.x)) == transform(gdf, x -> (a=x.x, b=x.x))
    @test combine(gdf, :x => x -> 2x) ==
          DataFrame(id=[3, 3, 1, 1, 2, 2], x_function=[6, 10, 2, 8, 4, 12])
    @test combine(gdf, identity) == df[[3, 5, 1, 4, 2, 6], :]
    @test combine(gdf, x -> (a=x.x, b=x.x)) ==
          DataFrame(id=[3, 3, 1, 1, 2, 2], a=[3, 5, 1, 4, 2, 6], b=[3, 5, 1, 4, 2, 6])

    df = DataFrame(id=[3, 2, 1, 3, 1, 2], x=1:6)
    gdf = groupby(df, :id, sort=true)
    @test select(df, :id, :x => x -> 2x) == select(gdf, :x => x -> 2x)
    @test select(df, identity) == select(gdf, identity)
    @test select(df, :id, x -> (a=x.x, b=x.x)) == select(gdf, x -> (a=x.x, b=x.x))
    @test transform(df, :x => x -> 2x) == transform(gdf, :x => x -> 2x)
    @test transform(df, identity) == transform(gdf, identity)
    @test transform(df, x -> (a=x.x, b=x.x)) == transform(gdf, x -> (a=x.x, b=x.x))
    @test combine(gdf, :x => x -> 2x) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], x_function=[6, 10, 4, 12, 2, 8])
    @test combine(gdf, identity) == DataFrame(id=[1, 1, 2, 2, 3, 3], x=[3, 5, 2, 6, 1, 4])
    @test combine(gdf, x -> (a=x.x, b=x.x)) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], a=[3, 5, 2, 6, 1, 4], b=[3, 5, 2, 6, 1, 4])

    gdf = groupby(df, :id)[[3, 1, 2]]
    @test select(df, :id, :x => x -> 2x) == select(gdf, :x => x -> 2x)
    @test select(df, identity) == select(gdf, identity)
    @test select(df, :id, x -> (a=x.x, b=x.x)) == select(gdf, x -> (a=x.x, b=x.x))
    @test transform(df, :x => x -> 2x) == transform(gdf, :x => x -> 2x)
    @test transform(df, identity) == transform(gdf, identity)
    @test transform(df, x -> (a=x.x, b=x.x)) == transform(gdf, x -> (a=x.x, b=x.x))
    @test sort(combine(gdf, :x => x -> 2x)) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], x_function=[6, 10, 4, 12, 2, 8])
    @test sort(combine(gdf, identity)) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], x=[3, 5, 2, 6, 1, 4])
    @test sort(combine(gdf, x -> (a=x.x, b=x.x))) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], a=[3, 5, 2, 6, 1, 4], b=[3, 5, 2, 6, 1, 4])
end

@testset "basic tests of advanced rules with multicolumn output" begin
    df = DataFrame(id=[1, 2, 3, 1, 3, 2], x=1:6)
    gdf = groupby(df, :id)

    @test combine(gdf, x -> reshape(1:4, 2, 2)) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], x1=[1, 2, 1, 2, 1, 2], x2=[3, 4, 3, 4, 3, 4])
    @test combine(gdf, x -> DataFrame(a=1:2, b=3:4)) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], a=[1, 2, 1, 2, 1, 2], b=[3, 4, 3, 4, 3, 4])
    @test combine(gdf, x -> DataFrame(a=1:2, b=3:4)[1, :]) ==
          DataFrame(id=[1, 2, 3], a=[1, 1, 1], b=[3, 3, 3])
    @test combine(gdf, x -> Tables.Row(DataFrame(a=1:2, b=3:4)[1, :])) ==
          DataFrame(id=[1, 2, 3], a=[1, 1, 1], b=[3, 3, 3])
    @test combine(gdf, x -> (a=1, b=3)) ==
          DataFrame(id=[1, 2, 3], a=[1, 1, 1], b=[3, 3, 3])
    @test combine(gdf, x -> (a=1:2, b=3:4)) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], a=[1, 2, 1, 2, 1, 2], b=[3, 4, 3, 4, 3, 4])
    @test combine(gdf, :x => (x -> Dict(:a => 1:2, :b => 3:4)) => AsTable) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], a=[1, 2, 1, 2, 1, 2], b=[3, 4, 3, 4, 3, 4])
    @test combine(gdf, :x => ByRow(x -> [x, x+1, x+2]) => AsTable) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], x1=[1, 4, 2, 6, 3, 5], x2=[2, 5, 3, 7, 4, 6], x3=[3, 6, 4, 8, 5, 7])
    @test combine(gdf, :x => ByRow(x -> (x, x+1, x+2)) => AsTable) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], x1=[1, 4, 2, 6, 3, 5], x2=[2, 5, 3, 7, 4, 6], x3=[3, 6, 4, 8, 5, 7])
    @test combine(gdf, :x => ByRow(x -> (a=x, b=x+1, c=x+2)) => AsTable) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], a=[1, 4, 2, 6, 3, 5], b=[2, 5, 3, 7, 4, 6], c=[3, 6, 4, 8, 5, 7])
    @test combine(gdf, :x => ByRow(x -> [x, x+1, x+2]) => [:p, :q, :r]) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], p=[1, 4, 2, 6, 3, 5], q=[2, 5, 3, 7, 4, 6], r=[3, 6, 4, 8, 5, 7])
    @test combine(gdf, :x => ByRow(x -> (x, x+1, x+2)) => [:p, :q, :r]) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], p=[1, 4, 2, 6, 3, 5], q=[2, 5, 3, 7, 4, 6], r=[3, 6, 4, 8, 5, 7])
    @test combine(gdf, :x => ByRow(x -> (a=x, b=x+1, c=x+2)) => [:p, :q, :r]) ==
          DataFrame(id=[1, 1, 2, 2, 3, 3], p=[1, 4, 2, 6, 3, 5], q=[2, 5, 3, 7, 4, 6], r=[3, 6, 4, 8, 5, 7])
    @test combine(gdf, :x => ByRow(x -> 1) => [:p]) == DataFrame(id=[1, 1, 2, 2, 3, 3], p=1)
    @test_throws ArgumentError combine(gdf, :x => (x -> 1) => [:p])

    @test select(gdf, x -> reshape(1:4, 2, 2)) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], x1=[1, 1, 1, 2, 2, 2], x2=[3, 3, 3, 4, 4, 4])
    @test select(gdf, x -> DataFrame(a=1:2, b=3:4)) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], a=[1, 1, 1, 2, 2, 2], b=[3, 3, 3, 4, 4, 4])
    @test select(gdf, x -> DataFrame(a=1:2, b=3:4)[1, :]) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], a=[1, 1, 1, 1, 1, 1], b=[3, 3, 3, 3, 3, 3])
    @test select(gdf, x -> Tables.Row(DataFrame(a=1:2, b=3:4)[1, :])) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], a=[1, 1, 1, 1, 1, 1], b=[3, 3, 3, 3, 3, 3])
    @test select(gdf, x -> (a=1, b=3)) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], a=[1, 1, 1, 1, 1, 1], b=[3, 3, 3, 3, 3, 3])
    @test select(gdf, x -> (a=1:2, b=3:4)) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], a=[1, 1, 1, 2, 2, 2], b=[3, 3, 3, 4, 4, 4])
    @test select(gdf, :x => (x -> Dict(:a => 1:2, :b => 3:4)) => AsTable) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], a=[1, 1, 1, 2, 2, 2], b=[3, 3, 3, 4, 4, 4])
    @test select(gdf, :x => ByRow(x -> [x, x+1, x+2]) => AsTable) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], x1=[1, 2, 3, 4, 5, 6], x2=[2, 3, 4, 5, 6, 7], x3=[3, 4, 5, 6, 7, 8])
    @test select(gdf, :x => ByRow(x -> (x, x+1, x+2)) => AsTable) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], x1=[1, 2, 3, 4, 5, 6], x2=[2, 3, 4, 5, 6, 7], x3=[3, 4, 5, 6, 7, 8])
    @test select(gdf, :x => ByRow(x -> (a=x, b=x+1, c=x+2)) => AsTable) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], a=[1, 2, 3, 4, 5, 6], b=[2, 3, 4, 5, 6, 7], c=[3, 4, 5, 6, 7, 8])
    @test select(gdf, :x => ByRow(x -> [x, x+1, x+2]) => [:p, :q, :r]) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], p=[1, 2, 3, 4, 5, 6], q=[2, 3, 4, 5, 6, 7], r=[3, 4, 5, 6, 7, 8])
    @test select(gdf, :x => ByRow(x -> (x, x+1, x+2)) => [:p, :q, :r]) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], p=[1, 2, 3, 4, 5, 6], q=[2, 3, 4, 5, 6, 7], r=[3, 4, 5, 6, 7, 8])
    @test select(gdf, :x => ByRow(x -> (a=x, b=x+1, c=x+2)) => [:p, :q, :r]) ==
          DataFrame(id=[1, 2, 3, 1, 3, 2], p=[1, 2, 3, 4, 5, 6], q=[2, 3, 4, 5, 6, 7], r=[3, 4, 5, 6, 7, 8])
    @test select(gdf, :x => ByRow(x -> 1) => [:p]) == DataFrame(id=[1, 2, 3, 1, 3, 2], p=1)
    @test_throws ArgumentError select(gdf, :x => (x -> 1) => [:p])
end

@testset "tests of invariants of transformation functions" begin
    Random.seed!(1234)
    df = DataFrame(x=rand(1000), id=rand(1:20, 1000), y=rand(1000), z=rand(1000))
    gdf = groupby_checked(df, :id)
    gdf2 = gdf[20:-1:1]
    @test transform(df, x -> sum(df.x), x -> (p=2x.x, q=2x.y), :id => :id2, :z => :x,
                    [:x, :y, :z] => +, [:y, :z] => ByRow(minmax) => [:min, :max], :y) ==
          transform(gdf, x -> sum(parent(x).x), x -> (p=2x.x, q=2x.y), :id => :id2, :z => :x,
                    [:x, :y, :z] => +, [:y, :z] => ByRow(minmax) => [:min, :max], :y) ==
          transform(gdf2, x -> sum(parent(x).x), x -> (p=2x.x, q=2x.y), :id => :id2, :z => :x,
                    [:x, :y, :z] => +, [:y, :z] => ByRow(minmax) => [:min, :max], :y) ==
          DataFrame(:x => df.z, :id => df.id, :y => df.y, :z => df.z, :x1 => sum(df.x),
                    :p => 2df.x, :q => 2df.y, :id2 => df.id, Symbol("x_y_z_+") => df.x+df.y+df.z,
                    :min => min.(df.y, df.z), :max => max.(df.y, df.z))

    @test select(df, x -> sum(df.x), x -> (p=2x.x, q=2x.y), :id => :id2, :z => :x,
                [:x, :y, :z] => +, [:y, :z] => ByRow(minmax) => [:min, :max], :y) ==
          select(gdf, x -> sum(parent(x).x), x -> (p=2x.x, q=2x.y), :id => :id2, :z => :x,
                [:x, :y, :z] => +, [:y, :z] => ByRow(minmax) => [:min, :max], :y, keepkeys=false) ==
          select(gdf2, x -> sum(parent(x).x), x -> (p=2x.x, q=2x.y), :id => :id2, :z => :x,
                [:x, :y, :z] => +, [:y, :z] => ByRow(minmax) => [:min, :max], :y, keepkeys=false) ==
          DataFrame(:x1 => sum(df.x), :p => 2df.x, :q => 2df.y, :id2 => df.id,
                    :x => df.z, Symbol("x_y_z_+") => df.x+df.y+df.z,
                    :min => min.(df.y, df.z), :max => max.(df.y, df.z), :y => df.y)

    @test combine(df, x -> sum(df.x), x -> (p=2x.x, q=2x.y), :id => :id2, :z => :x,
                  [:x, :y, :z] => +, [:y, :z] => ByRow(minmax) => [:min, :max], :y) |> sort ==
          combine(gdf, x -> sum(parent(x).x), x -> (p=2x.x, q=2x.y), :id => :id2, :z => :x,
                  [:x, :y, :z] => +, [:y, :z] => ByRow(minmax) => [:min, :max], :y, keepkeys=false) |> sort ==
          combine(gdf2, x -> sum(parent(x).x), x -> (p=2x.x, q=2x.y), :id => :id2, :z => :x,
                  [:x, :y, :z] => +, [:y, :z] => ByRow(minmax) => [:min, :max], :y, keepkeys=false) |> sort ==
          DataFrame(:x1 => sum(df.x), :p => 2df.x, :q => 2df.y, :id2 => df.id,
                    :x => df.z, Symbol("x_y_z_+") => df.x+df.y+df.z,
                    :min => min.(df.y, df.z), :max => max.(df.y, df.z), :y => df.y) |> sort
end

@testset "extra CategoricalArray aggregation tests" begin
    for ord in (true, false)
        df = DataFrame(id=[1, 1, 1, 2, 2, 2], x=categorical(1:6, ordered=ord))
        gdf = groupby_checked(df, :id)
        res = combine(gdf, :x .=> [minimum, maximum, first, last, length])
        @test res == DataFrame(id=[1, 2], x_minimum=[1, 4], x_maximum=[3, 6],
                               x_first=[1, 4], x_last=[3, 6], x_length=[3, 3])
        @test res.x_minimum isa CategoricalVector
        @test res.x_maximum isa CategoricalVector
        @test res.x_first isa CategoricalVector
        @test res.x_last isa CategoricalVector
        @test isordered(res.x_minimum) == ord
        @test isordered(res.x_maximum) == ord
        @test isordered(res.x_first) == ord
        @test isordered(res.x_last) == ord
        @test DataAPI.refpool(res.x_minimum) == DataAPI.refpool(df.x)
        @test DataAPI.refpool(res.x_maximum) == DataAPI.refpool(df.x)
        @test DataAPI.refpool(res.x_first) == DataAPI.refpool(df.x)
        @test DataAPI.refpool(res.x_last) == DataAPI.refpool(df.x)
        @test DataAPI.refpool(res.x_minimum) !== DataAPI.refpool(df.x)
        @test DataAPI.refpool(res.x_maximum) !== DataAPI.refpool(df.x)
        @test DataAPI.refpool(res.x_first) !== DataAPI.refpool(df.x)
        @test DataAPI.refpool(res.x_last) !== DataAPI.refpool(df.x)
        @test res.x_minimum.pool !== df.x.pool
        @test res.x_maximum.pool !== df.x.pool
        @test res.x_first.pool !== df.x.pool
        @test res.x_last.pool !== df.x.pool
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

@testset "column selection and renaming" begin
    df = DataFrame(id=[1, 1, 2, 3, 3, 1], x=1:6, y=11:16, z=21:26)
    gdf = groupby_checked(df, :id)

    @test combine(gdf, :x) == DataFrame(id=[1, 1, 1, 2, 3, 3], x=[1, 2, 6, 3, 4, 5])
    @test combine(gdf, :x => :y ) == DataFrame(id=[1, 1, 1, 2, 3, 3], y=[1, 2, 6, 3, 4, 5])
    @test combine(gdf, [:x, :y]) ==
          DataFrame(id=[1, 1, 1, 2, 3, 3], x=[1, 2, 6, 3, 4, 5], y=[11, 12, 16, 13, 14, 15])
    @test combine(gdf, [:x, :y], :z) ==
          DataFrame(id=[1, 1, 1, 2, 3, 3], x=[1, 2, 6, 3, 4, 5],
                    y=[11, 12, 16, 13, 14, 15], z=[21, 22, 26, 23, 24, 25])
    @test_throws ArgumentError combine(gdf, :x, :x)
    @test_throws ArgumentError combine(gdf, :x => :y, :y)

    @test select(gdf, :x) == select(df, :id, :x)
    @test select(gdf, :x => :y) == select(df, :id, :x => :y)
    @test select(gdf, [:x, :y]) == select(df, :id, [:x, :y])
    @test select(gdf, [:x, :y], :z) == select(df, :id, [:x, :y], :z)
    @test_throws ArgumentError select(gdf, :x, :x)
    @test_throws ArgumentError select(gdf, :x => :y, :y)
end

@testset "corner cases of wrong transformation" begin
    df = DataFrame(id=[1, 1, 2, 3, 3, 1], x=1:6)
    gdf = groupby_checked(df, :id)
    @test_throws ArgumentError combine(gdf, :x, :x)
    @test_throws ErrorException combine(gdf, :x => (x -> Dict("a" => 1)) => AsTable)
    # changed in Tables.jl 1.8
    @test combine(gdf, :x => (x -> Dict("a" => [1])) => AsTable) == DataFrame(id=1:3, a=1)
    @test_throws ErrorException combine(gdf, :x => (x -> Dict(:a => 1)) => AsTable)
    @test_throws ArgumentError combine(gdf, sdf -> sdf.id[1] == 1 ? Ref(1) : [1])
    @test_throws ArgumentError combine(gdf, sdf -> sdf.id[1] == 2 ? Ref(1) : [1])
    @test_throws ArgumentError combine(gdf, sdf -> sdf.id[1] == 1 ? (a=1, b=2) : (a=1,))
    @test_throws ArgumentError combine(gdf, sdf -> sdf.id[1] == 1 ? (a=1, b=2) : (a=1, c=2))
    @test_throws ArgumentError combine(gdf, sdf -> sdf.id[1] == 1 ? (a=[1], b=[2]) : (a=[1],))
    @test_throws ArgumentError combine(gdf, sdf -> sdf.id[1] == 1 ? (a=[1], b=[2]) : (a=[1], c=[2]))
    @test_throws ArgumentError combine(gdf, sdf -> sdf.id[1] == 1 ? (a=[1], b=[2]) : (a=1,))
    @test_throws ArgumentError combine(gdf, sdf -> sdf.id[1] == 2 ? (a=[1], b=[2]) : (a=1,))
    @test_throws ArgumentError combine(gdf, :id => (x -> x[1] == 1 ? [[1, 2]] : [[1]]) => AsTable)
    @test_throws ArgumentError combine(gdf, :id => (x -> x[1] == 1 ? [[1]] : [[1]]) => [:a, :b])
    @test_throws ArgumentError combine(gdf, :x, :id => (x -> fill([1], length(x))) => [:x])
    @test select(gdf, [:x], :id => (x -> fill([1], length(x))) => [:x]) ==
          DataFrame(id=df.id, x=1)
    @test_throws ArgumentError select(gdf, x -> [1, 2])

    df = DataFrame(id=[1, 1, 2, 3, 3, 1], x=categorical(1:6, ordered=true))
    gdf = groupby_checked(df, :id)
    @test combine(gdf, :x => minimum => :x) == df[[1, 3, 4], :]
end

@testset "select and transform! tests with function as first argument" begin
    df = DataFrame(id=[1, 1, 2, 3, 3, 1], x=1:6)
    gdf = groupby_checked(df, :id)
    df2 = select(sdf -> sdf.id .* sdf.x, gdf)
    @test df2 == DataFrame(id=df.id, x1=df.id .* df.x)
    select!(sdf -> sdf.id .* sdf.x, gdf)
    @test df == df2

    df = DataFrame(id=[1, 1, 2, 3, 3, 1], x=1:6)
    gdf = groupby_checked(df, :id)
    df2 = transform(sdf -> sdf.id .* sdf.x, gdf)
    @test df2 == DataFrame(id=df.id, x=df.x, x1=df.id .* df.x)
    transform!(sdf -> sdf.id .* sdf.x, gdf)
    @test df == df2
end

@testset "make sure we handle idx correctly when groups are reordered" begin
    df = DataFrame(g=[2, 2, 1, 1, 1], id=1:5)
    @test select(df, :g, :id, :id => ByRow(identity) => :id2) ==
          select(groupby_checked(df, :g), :id, :id => ByRow(identity) => :id2) ==
          select(groupby_checked(df, :g, sort=true), :id, :id => ByRow(identity) => :id2) ==
          select(groupby_checked(df, :g)[[2,1]], :id, :id => ByRow(identity) => :id2) ==
          [df DataFrame(id2=df.id)]
end

@testset "permutations of operations with combine" begin
    Random.seed!(1)
    df = DataFrame(id=rand(1:10, 20))
    gd = groupby_checked(df, :id)

    trans = [:id => (y -> sum(y)) => :v1,
             :id => (y -> 10maximum(y)) => :v2,
             :id => sum => :v3,
             y -> (v4=100y.id[1],),
             y -> (v5=fill(1000y.id[1],y.id[1]+1),)]

    for p in permutations(1:length(trans)), i in 1:length(trans)
        res = combine(gd, trans[p[1:i]]...)
        for j in 1:i
            expected = nrow(res) <= 10 ? combine(gd, trans[p[j]]) :
                # Second operation is there only to generate as many rows as in res
                combine(gd, trans[p[j]], y -> (xxx=fill(1000y.id[1],y.id[1]+1),))
            nms = intersect(names(expected), names(res))
            @test res[!, nms] == expected[!, nms]
        end
    end

    trans = [:id => (y -> sum(y)) => :v1,
             :id => (y -> 10maximum(y)) => :v2,
             :id => sum => :v3,
             y -> (v4=100y.id[1],),
             y -> (v5=1000 .* y.id[1],),
             :id => :v6]

    for p in permutations(1:length(trans)), i in 1:length(trans)
        res = combine(gd, trans[p[1:i]]...)
        for j in 1:i
            expected = nrow(res) <= 10 ? combine(gd, trans[p[j]]) :
                # Second operation is there only to generate as many rows as in res
                combine(gd, trans[p[j]], y -> (xxx=1000 .* y.id,))
            nms = intersect(names(expected), names(res))
            @test res[!, nms] == expected[!, nms]
        end
    end
end

@testset "result eltype widening from different tasks" begin
    Random.seed!(1)
    for y in (Any[1, missing, missing, 2, 4],
              Any[1, missing, nothing, 2.1, 'a'],
              Any[1, 1, missing, 1, nothing, 1, 2.1, 1, 'a'],
              Any[1, 2, 3, 4, 5, 6, 2.1, missing, 'a'],
              Any[1, 2, 3.1, 4, 5, 6, 2.1, missing, 'a']),
        x in (1:length(y), rand(1:2, length(y)), rand(1:3, length(y)))
        df = DataFrame(x=x, y1=y, y2=reverse(y))
        gd = groupby(df, :x)
        res = combine(gd, :y1 => (y -> y[1]) => :y1, :y2 => (y -> y[end]) => :y2)
        # sleep ensures one task will widen the result after the other is done,
        # so that data has to be copied at the end
        @test res ≅
              combine(gd, [:x, :y1] => ((x, y) -> (sleep((x == [5])/10); y[1])) => :y1,
                          [:x, :y2] => ((x, y) -> (sleep((x == [5])/10); y[end])) => :y2) ≅
              combine(gd, [:x, :y1] => ((x, y) -> (sleep(x[1]/100); y[1])) => :y1,
                          [:x, :y2] => ((x, y) -> (sleep(x[1]/100); y[end])) => :y2) ≅
              combine(gd, [:x, :y1] => ((x, y) -> (sleep(rand()/10); y[1])) => :y1,
                          [:x, :y2] => ((x, y) -> (sleep(rand()/10); y[end])) => :y2)

        if df.x == 1:nrow(df)
            @test res ≅ df
        end

        res = combine(gd, :y1 => (y -> (y1=y[1], y2=y[end])) => AsTable,
                          :y2 => (y -> (y3=y[1], y4=y[end])) => AsTable)
        # sleep ensures one task will widen the result after the other is done,
        # so that data has to be copied at the end
        @test res ≅
              combine(gd, [:x, :y1] => ((x, y) -> (sleep((x == [5])/10); (y1=y[1], y2=y[end]))) => AsTable,
                          [:x, :y2] => ((x, y) -> (sleep((x == [5])/10); (y3=y[1], y4=y[end]))) => AsTable) ≅
              combine(gd, [:x, :y1] => ((x, y) -> (sleep(x[1]/100); (y1=y[1], y2=y[end]))) => AsTable,
                          [:x, :y2] => ((x, y) -> (sleep(x[1]/100); (y3=y[1], y4=y[end]))) => AsTable) ≅
              combine(gd, [:x, :y1] => ((x, y) -> (sleep(rand()/10); (y1=y[1], y2=y[end]))) => AsTable,
                          [:x, :y2] => ((x, y) -> (sleep(rand()/10); (y3=y[1], y4=y[end]))) => AsTable)
    end
end

@testset "CategoricalArray thread safety" begin
    # These tests do not actually trigger multithreading bugs,
    # but at least they check that the code that disables multithreading
    # with CategoricalArray when levels are different works
    Random.seed!(35)
    df = DataFrame(x=rand(1:10, 100),
                   y=categorical(rand(10:15, 100)),
                   z=categorical(rand(0:20, 100)))
    df.y2 = reverse(df.y) # Same levels
    gd = groupby(df, :x)

    @test combine(gd, :y => (y -> y[1]) => :res) ==
        combine(gd, [:y, :y2] => ((y, x) -> y[1]) => :res) ==
        combine(gd, [:y, :x] => ((y, x) -> y[1]) => :res) ==
        combine(gd, [:y, :z] => ((y, z) -> y[1]) => :res) ==
        combine(gd, :y => (y -> unwrap(y[1])) => :res)

    @test combine(gd, [:x, :y, :y2] =>
                          ((x, y, y2) -> x[1] <= 5 ? y[1] : y2[1]) => :res) ==
        combine(gd, [:x, :y, :y2] =>
                        ((x, y, y2) -> x[1] <= 5 ? unwrap(y[1]) : unwrap(y2[1])) => :res)

    @test combine(gd, [:x, :y, :z] =>
                          ((x, y, z) -> x[1] <= 5 ? y[1] : z[1]) => :res) ==
        combine(gd, [:x, :y, :z] =>
                        ((x, y, z) -> x[1] <= 5 ? unwrap(y[1]) : unwrap(z[1])) => :res)
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
    for x in (PooledArray(rand(1:10, 210_000)),
              PooledArray(rand([1:9; missing], 210_000))),
        y in (PooledArray(rand(["a", "b", "c", "d"], 210_000)),
              PooledArray(rand(["a"; "b"; "c"; missing], 210_000)))
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

@testset "aggregation of PooledArray" begin
    df = DataFrame(x=PooledArray(Int32(1):Int32(3)))
    gdf = groupby_checked(df, :x)
    df2 = combine(gdf, :x => sum, :x => prod)
    @test df2 == DataFrame(x=1:3, x_sum=1:3, x_prod=1:3)
    @test df2.x isa PooledVector{Int32}
    @test df2.x_sum isa Vector{Int}
    @test df2.x_prod isa Vector{Int}
end

@testset "extra tests of wrapper corner cases" begin
    df = DataFrame(a=1:2)
    gdf = groupby_checked(df, :a)
    @test_throws ArgumentError combine(gdf, x -> x.a[1] == 1 ? 1 : x[1, :])
    @test_throws ArgumentError combine(gdf, x -> x.a[1] == 1 ? 1 : Tables.Row(x[1, :]))
    @test_throws ArgumentError combine(gdf, x -> x.a[1] == 1 ? (a=1, b=2) : Ref(1))
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
    for levs in (100, 89_000), sz in (90_000, 210_000)
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

@testset "aggregation with matrix of Pair" begin
    df = DataFrame(a=["a", "b","a", "b"], x=1:4, y=11:14)
    gdf = groupby_checked(df, :a)

    @test combine(df, [:x, :y] .=> [minimum maximum]) ==
          DataFrame(x_minimum=1, y_minimum=11, x_maximum=4, y_maximum=14)
    @test combine(gdf, [:x, :y] .=> [minimum maximum]) ==
          DataFrame(a=["a", "b"], x_minimum=[1, 2], y_minimum=[11, 12],
                    x_maximum=[3, 4], y_maximum=[13, 14])
    @test select(df, [:x, :y] .=> [minimum maximum]) ==
          DataFrame(x_minimum=[1, 1, 1, 1],
                    y_minimum=[11, 11, 11, 11],
                    x_maximum=[4, 4, 4, 4],
                    y_maximum=[14, 14, 14, 14])
    @test select(gdf, [:x, :y] .=> [minimum maximum]) ==
          DataFrame(a=["a", "b", "a", "b"],
                    x_minimum=[1, 2, 1, 2],
                    y_minimum=[11, 12, 11, 12],
                    x_maximum=[3, 4, 3, 4],
                    y_maximum=[13, 14, 13, 14])
    @test transform(df, [:x, :y] .=> [minimum maximum]) ==
          DataFrame(a=["a", "b","a", "b"], x=1:4, y=11:14,
                    x_minimum=[1, 1, 1, 1],
                    y_minimum=[11, 11, 11, 11],
                    x_maximum=[4, 4, 4, 4],
                    y_maximum=[14, 14, 14, 14])
    @test transform(gdf, [:x, :y] .=> [minimum maximum]) ==
          DataFrame(a=["a", "b","a", "b"], x=1:4, y=11:14,
                    x_minimum=[1, 2, 1, 2],
                    y_minimum=[11, 12, 11, 12],
                    x_maximum=[3, 4, 3, 4],
                    y_maximum=[13, 14, 13, 14])
    @test select!(copy(df), [:x, :y] .=> [minimum maximum]) ==
        DataFrame(x_minimum=[1, 1, 1, 1],
                    y_minimum=[11, 11, 11, 11],
                    x_maximum=[4, 4, 4, 4],
                    y_maximum=[14, 14, 14, 14])
    @test select!(groupby_checked(copy(df), :a), [:x, :y] .=> [minimum maximum]) ==
          DataFrame(a=["a", "b", "a", "b"],
                    x_minimum=[1, 2, 1, 2],
                    y_minimum=[11, 12, 11, 12],
                    x_maximum=[3, 4, 3, 4],
                    y_maximum=[13, 14, 13, 14])
    @test transform!(copy(df), [:x, :y] .=> [minimum maximum]) ==
          DataFrame(a=["a", "b","a", "b"], x=1:4, y=11:14,
                    x_minimum=[1, 1, 1, 1],
                    y_minimum=[11, 11, 11, 11],
                    x_maximum=[4, 4, 4, 4],
                    y_maximum=[14, 14, 14, 14])
    @test transform!(groupby_checked(copy(df), :a), [:x, :y] .=> [minimum maximum]) ==
          DataFrame(a=["a", "b","a", "b"], x=1:4, y=11:14,
                    x_minimum=[1, 2, 1, 2],
                    y_minimum=[11, 12, 11, 12],
                    x_maximum=[3, 4, 3, 4],
                    y_maximum=[13, 14, 13, 14])
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

@testset "eachindex, groupindices, and proprow tests" begin
    # Basic tests
    df = DataFrame(id=["a", "c", "b", "b", "a", "a"])
    gdf = groupby(df, :id);
    @test combine(gdf, groupindices, groupindices => :gidx,
                  proprow, proprow => :freq) ==
          DataFrame(id=["a", "c", "b"],
                    groupindices=[1, 2, 3],
                    gidx=[1, 2, 3],
                    proprow=[1/2, 1/6, 1/3],
                    freq=[1/2, 1/6, 1/3])
    @test getfield(gdf, :idx) === nothing
    gdf = groupby(df, :id)
    @test combine(gdf, eachindex, eachindex => :idx,
                  groupindices, groupindices => :gidx,
                  proprow, proprow => :freq) ==
          combine(gdf, eachindex, eachindex => "idx",
                  groupindices, groupindices => "gidx",
                  proprow, proprow => "freq") ==
          # note that this style is supported but we do not cover it in the docs
          # the point is that groupindices and proprow do not require source column
          # and work on whole grouped data frame
          combine(gdf, 1 => eachindex => :eachindex, 1 => eachindex => "idx",
                  [] => groupindices, [] => groupindices => "gidx",
                  [] => proprow, [] => proprow => "freq") ==
          DataFrame(id=["a", "a", "a", "c", "b", "b"],
                    eachindex=[1, 2, 3, 1, 1, 2],
                    idx=[1, 2, 3, 1, 1, 2],
                    groupindices=[1, 1, 1, 2, 3, 3],
                    gidx=[1, 1, 1, 2, 3, 3],
                    proprow=[1/2, 1/2, 1/2, 1/6, 1/3, 1/3],
                    freq=[1/2, 1/2, 1/2, 1/6, 1/3, 1/3])
    @test combine(gdf, groupindices, groupindices => :gidx,
                  proprow, proprow => :freq) ==
          DataFrame(id=["a", "c", "b"],
                    groupindices=[1, 2, 3],
                    gidx=[1, 2, 3],
                    proprow=[1/2, 1/6, 1/3],
                    freq=[1/2, 1/6, 1/3])
    @test select(gdf, eachindex, eachindex => :idx,
                  groupindices, groupindices => :gidx,
                  proprow, proprow => :freq) ==
          DataFrame(id=df.id,
                    eachindex=[1, 1, 1, 2, 2, 3],
                    idx=[1, 1, 1, 2, 2, 3],
                    groupindices=[1, 2, 3, 3, 1, 1],
                    gidx=[1, 2, 3, 3, 1, 1],
                    proprow=[1/2, 1/6, 1/3, 1/3, 1/2, 1/2],
                    freq=[1/2, 1/6, 1/3, 1/3, 1/2, 1/2])
    @test select(gdf, groupindices, groupindices => :gidx,
                  proprow, proprow => :freq) ==
          DataFrame(id=df.id,
                    groupindices=[1, 2, 3, 3, 1, 1],
                    gidx=[1, 2, 3, 3, 1, 1],
                    proprow=[1/2, 1/6, 1/3, 1/3, 1/2, 1/2],
                    freq=[1/2, 1/6, 1/3, 1/3, 1/2, 1/2])
    gdf2 = gdf[[3, 1]]
    @test combine(gdf2, eachindex, eachindex => :idx,
                  groupindices, groupindices => :gidx,
                  proprow, proprow => :freq) ==
          DataFrame(id=["b", "b", "a", "a", "a"],
                    eachindex=[1, 2, 1, 2, 3],
                    idx=[1, 2, 1, 2, 3],
                    groupindices=[1, 1, 2, 2, 2],
                    gidx=[1, 1, 2, 2, 2],
                    proprow=[2/5, 2/5, 3/5, 3/5, 3/5],
                    freq=[2/5, 2/5, 3/5, 3/5, 3/5])
    @test combine(gdf2, groupindices, groupindices => :gidx,
                  proprow, proprow => :freq) ==
          DataFrame(id=["b", "a"],
                    groupindices=[1, 2],
                    gidx=[1, 2],
                    proprow=[2/5, 3/5],
                    freq=[2/5, 3/5])
    gdf3 = gdf2[[1]]
    @test combine(gdf3, eachindex, eachindex => :idx,
                  groupindices, groupindices => :gidx,
                  proprow, proprow => :freq) ==
          DataFrame(id=["b", "b"],
                    eachindex=[1, 2],
                    idx=[1, 2],
                    groupindices=[1, 1],
                    gidx=[1, 1],
                    proprow=[1.0, 1.0],
                    freq=[1.0, 1.0])
    @test combine(gdf3, groupindices, groupindices => :gidx,
                  proprow, proprow => :freq) ==
          DataFrame(id=["b"],
                    groupindices=[1],
                    gidx=[1],
                    proprow=[1.0],
                    freq=[1.0])
    gdf4 = gdf3[[false]]
    @test isequal_coltyped(combine(gdf4, eachindex, eachindex => :idx,
                                   groupindices, groupindices => :gidx,
                                   proprow, proprow => :freq),
                           DataFrame(id=String[], eachindex=Int[], idx=Int[],
                                     groupindices=Int[], gidx=Int[],
                                     proprow=Float64[], freq=Float64[]))
    @test isequal_coltyped(combine(gdf4, groupindices, groupindices => :gidx,
                                   proprow, proprow => :freq),
                           DataFrame(id=String[], groupindices=Int[], gidx=Int[],
                                     proprow=Float64[], freq=Float64[]))
    gdf5 = groupby(DataFrame(), [])
    @test isequal_coltyped(combine(gdf5, eachindex, eachindex => :idx,
                                   groupindices, groupindices => :gidx,
                                   proprow, proprow => :freq),
                           DataFrame(eachindex=Int[], idx=Int[],
                                     groupindices=Int[], gidx=Int[],
                                     proprow=Float64[], freq=Float64[]))
    @test isequal_coltyped(combine(gdf5, groupindices, groupindices => :gidx,
                                   proprow, proprow => :freq),
                           DataFrame(groupindices=Int[], gidx=Int[],
                                     proprow=Float64[], freq=Float64[]))

    # eachindex on DataFrame
    @test combine(df, eachindex) == DataFrame(eachindex=1:6)
    @test isequal_coltyped(combine(DataFrame(), eachindex),
                           DataFrame(eachindex=Int[]))

    # Disallowed operations
    @test_throws ArgumentError groupindices(df)
    @test_throws ArgumentError proprow(df)
    @test_throws ArgumentError combine(df, groupindices)
    @test_throws ArgumentError combine(df, proprow)

    # test column replacement
    df = DataFrame(id=["a", "c", "b", "b", "a", "a"], x=-1, y=-2)
    gdf = groupby(df, :id);
    @test transform(gdf, groupindices => :x, proprow => :y) ==
          DataFrame(id=df.id,
                    x=[1, 2, 3, 3, 1, 1],
                    y=[1/2, 1/6, 1/3, 1/3, 1/2, 1/2])
    @test_throws ArgumentError combine(gdf, :x, groupindices => :x)
    @test_throws ArgumentError combine(gdf, :x, proprow => :x)

    df = DataFrame(x = [1, 1, 1, 2, 3, 3], id = 1:6)
    gdf = groupby(df, :x)
    @test combine(gdf, proprow) == combine(proprow, gdf) ==
          rename(combine(gdf, proprow => :a), :a => :proprow) ==
          DataFrame(x = [1, 2, 3], proprow = [1/2, 1/6, 1/3])
    @test transform(gdf, proprow) == transform(proprow, gdf) ==
          rename(transform(gdf, proprow => :a), :a => :proprow) ==
          DataFrame(x = df.x, id = df.id, proprow = [1/2, 1/2, 1/2, 1/6, 1/3, 1/3])
    gdf = gdf[[2, 1, 3]]
    @test combine(gdf, proprow) == combine(proprow, gdf) ==
          rename(combine(gdf, proprow => :a), :a => :proprow) ==
          DataFrame(x = [2, 1, 3], proprow = [1/6, 1/2, 1/3])
    # note that transform retains the original row order
    @test transform(gdf, proprow) == transform(proprow, gdf) ==
          rename(transform(gdf, proprow => :a), :a => :proprow) ==
          DataFrame(x = df.x, id = df.id, proprow = [1/2, 1/2, 1/2, 1/6, 1/3, 1/3])
    gdf = gdf[[3, 1]]
    @test combine(gdf, proprow) == combine(proprow, gdf) ==
          rename(combine(gdf, proprow => :a), :a => :proprow) ==
          DataFrame(x = [3, 2], proprow = [2/3, 1/3])
    @test combine(gdf, :id, proprow) ==
          rename(combine(gdf, :id, proprow => :a), :a => :proprow) ==
          DataFrame(x = [3, 3, 2], id = [5, 6, 4], proprow = [2/3, 2/3, 1/3])
    @test_throws ArgumentError transform(gdf, proprow)
    gdf = gdf[[]]
    @test isequal_coltyped(combine(gdf, proprow), DataFrame(x=Int[], proprow=Float64[]))
    @test_throws ArgumentError transform(gdf, proprow)

    gdf = groupby(df, :x)
    @test combine(gdf, eachindex) == combine(eachindex, gdf) ==
          rename(combine(gdf, eachindex => :a), :a => :eachindex) ==
          DataFrame(x = [1, 1, 1, 2, 3, 3], eachindex = [1, 2, 3, 1, 1, 2])
    @test transform(gdf, eachindex) == transform(eachindex, gdf) ==
          rename(transform(gdf, eachindex => :a), :a => :eachindex) ==
          DataFrame(x = df.x, id = df.id, eachindex = [1, 2, 3, 1, 1, 2])
    gdf = gdf[[2, 1, 3]]
    @test combine(gdf, eachindex) == combine(eachindex, gdf) ==
          rename(combine(gdf, eachindex => :a), :a => :eachindex) ==
          DataFrame(x = [2, 1, 1, 1, 3, 3], eachindex = [1, 1, 2, 3, 1, 2])
    # note that transform retains the original row order
    @test transform(gdf, eachindex) == transform(eachindex, gdf) ==
          rename(transform(gdf, eachindex => :a), :a => :eachindex) ==
          DataFrame(x = df.x, id = df.id, eachindex = [1, 2, 3, 1, 1, 2])
    gdf = gdf[[3, 1]]
    @test combine(gdf, eachindex) == combine(eachindex, gdf) ==
          rename(combine(gdf, eachindex => :a), :a => :eachindex) ==
          DataFrame(x = [3, 3, 2], eachindex = [1, 2, 1])
    @test_throws ArgumentError transform(gdf, eachindex)
    gdf = gdf[[]]
    @test isequal_coltyped(combine(gdf, eachindex), DataFrame(x=Int[], eachindex=Int[]))
    @test_throws ArgumentError transform(gdf, eachindex)
    @test combine(df, eachindex) == combine(eachindex, df) ==
          rename(combine(df, eachindex => :a), :a => :eachindex) ==
          DataFrame(eachindex = 1:6)
    @test transform(df, eachindex) == transform(eachindex, df) ==
          rename(transform(df, eachindex => :a), :a => :eachindex) ==
          DataFrame(x = df.x, id = df.id, eachindex = 1:6)
    df = view(df, [], :)
    df2 = combine(df, eachindex)
    @test isequal_coltyped(df2, DataFrame(eachindex = Int[]))
    @test isequal_coltyped(df2, combine(eachindex, df))
    @test isequal_coltyped(df2, rename(combine(df, eachindex => :a), :a => :eachindex))

    df2 = transform(df, eachindex)
    @test isequal_coltyped(df2, DataFrame(x = Int[], id = Int[], eachindex = Int[]))
    @test isequal_coltyped(df2, transform(eachindex, df))
    @test isequal_coltyped(df2, rename(transform(df, eachindex => :a), :a => :eachindex))
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

@testset "maximum and minimum on missing" begin
    df = DataFrame(id=[1,1,2,2], x=fill(missing, 4))
    gdf = groupby_checked(df, :id)
    @test combine(gdf, :x => maximum => :x) ≅ DataFrame(id=1:2, x=fill(missing, 2))
    @test combine(gdf, :x => minimum => :x) ≅ DataFrame(id=1:2, x=fill(missing, 2))
    @test_throws ArgumentError combine(gdf, :x => maximum∘skipmissing)
    @test_throws ArgumentError combine(gdf, :x => minimum∘skipmissing)
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

@testset "aggregation of empty GroupedDataFrame with table output" begin
    df = DataFrame(:a => Int[])
    gdf = groupby(df, :a)
    @test isequal_typed(combine(gdf, :a => (x -> [(x=1, y="a")]) => AsTable, :a => :b),
                        DataFrame(a=Int[], x=Int[], y=String[], b=Int[]))
    @test isequal_typed(combine(gdf, :a => (x -> [(1, "a")]) => AsTable, :a => :b),
                        DataFrame(a=Int[], x1=Int[], x2=String[], b=Int[]))
    @test isequal_typed(combine(gdf, :a => (x -> ["ab"]) => AsTable, :a => :b),
                        DataFrame(a=Int[], x1=Char[], x2=Char[], b=Int[]))
    # test below errors because keys for strings do not support == comparison
    @test_throws ArgumentError combine(gdf, :a => (x -> ["ab", "cd"]) => AsTable, :a => :b)
    @test isequal_typed(combine(gdf, :a => (x -> []) => AsTable, :a => :b),
                        DataFrame(a=Int[], b=Int[]))
    @test_throws ArgumentError combine(gdf, :a => (x -> [(a=x, b=x), (a=x, c=x)]) => AsTable)
    @test isequal_typed(combine(gdf, :a => (x -> [(x=1, y=2), (x=3, y="a")]) => AsTable),
                        DataFrame(a=Int[], x=Int[], y=Any[]))
    @test isequal_typed(combine(gdf, :a => (x -> [(x=[1], y=2), (x=[3], y="a")]) => AsTable),
                        DataFrame(a=Int[], x=Vector{Int}[], y=Any[]))
    @test isequal_typed(combine(gdf, :a => (x -> [(x=[1], y=2), (x=[3], y="a")]) => [:z1, :z2]),
                        DataFrame(a=Int[], z1=Vector{Int}[], z2=Any[]))
    @test_throws ArgumentError combine(gdf, :a => (x -> [(x=[1], y=2), (x=[3], y="a")]) => [:z1, :z2, :z3])

    df = DataFrame(:a => [1, 2])
    gdf = groupby(df, :a)[2:1]
    @test isequal_typed(combine(gdf, :a => (x -> [(x=1, y="a")]) => AsTable, :a => :b),
                        DataFrame(a=Int[], x=Int[], y=String[], b=Int[]))
    @test isequal_typed(combine(gdf, :a => (x -> [(1, "a")]) => AsTable, :a => :b),
                        DataFrame(a=Int[], x1=Int[], x2=String[], b=Int[]))
    @test isequal_typed(combine(gdf, :a => (x -> ["ab"]) => AsTable, :a => :b),
                        DataFrame(a=Int[], x1=Char[], x2=Char[], b=Int[]))
    # test below errors because keys for strings do not support == comparison
    @test_throws ArgumentError combine(gdf, :a => (x -> ["ab", "cd"]) => AsTable, :a => :b)
    @test isequal_typed(combine(gdf, :a => (x -> []) => AsTable, :a => :b),
                        DataFrame(a=Int[], b=Int[]))
    @test_throws ArgumentError combine(gdf, :a => (x -> [(a=x, b=x), (a=x, c=x)]) => AsTable)
    @test isequal_typed(combine(gdf, :a => (x -> [(x=1, y=2), (x=3, y="a")]) => AsTable),
                        DataFrame(a=Int[], x=Int[], y=Any[]))
    @test isequal_typed(combine(gdf, :a => (x -> [(x=[1], y=2), (x=[3], y="a")]) => AsTable),
                        DataFrame(a=Int[], x=Vector{Int}[], y=Any[]))
    @test isequal_typed(combine(gdf, :a => (x -> [(x=[1], y=2), (x=[3], y="a")]) => [:z1, :z2]),
                        DataFrame(a=Int[], z1=Vector{Int}[], z2=Any[]))
    @test_throws ArgumentError combine(gdf, :a => (x -> [(x=[1], y=2), (x=[3], y="a")]) => [:z1, :z2, :z3])

    df = DataFrame(:a => [1, 2])
    gdf = groupby(df, :a)
    @test isequal_typed(combine(gdf, :a => (x -> [(x=1, y="a")]) => AsTable, :a => :b),
                        DataFrame(a=1:2, x=[1, 1], y=["a", "a"], b=1:2))
    @test isequal_typed(combine(gdf, :a => (x -> [(1, "a")]) => AsTable, :a => :b),
                        DataFrame(a=1:2, x1=[1, 1], x2=["a", "a"], b=1:2))
    @test isequal_typed(combine(gdf, :a => (x -> ["ab"]) => AsTable, :a => :b),
                        DataFrame(a=1:2, x1=['a', 'a'], x2=['b', 'b'], b=1:2))
    # test below errors because keys for strings do not support == comparison
    @test_throws ArgumentError combine(gdf, :a => (x -> ["ab", "cd"]) => AsTable, :a => :b)
    @test isequal_typed(combine(gdf, :a => (x -> []) => AsTable, :a => :b),
                        DataFrame(a=1:2, b=1:2))
    @test_throws ArgumentError combine(gdf, :a => (x -> [(a=x, b=x), (a=x, c=x)]) => AsTable)
    @test isequal_typed(combine(gdf, :a => (x -> [(x=1, y=2), (x=3, y="a")]) => AsTable),
                        DataFrame(a=[1, 1, 2, 2], x=[1, 3, 1, 3], y=Any[2, "a", 2, "a"]))
    @test isequal_typed(combine(gdf, :a => (x -> [(x=[1], y=2), (x=[3], y="a")]) => AsTable),
                        DataFrame(a=[1, 1, 2, 2], x=[[1], [3], [1], [3]], y=Any[2, "a", 2, "a"]))
    @test isequal_typed(combine(gdf, :a => (x -> [(x=[1], y=2), (x=[3], y="a")]) => [:z1, :z2]),
                        DataFrame(a=[1, 1, 2, 2], z1=[[1], [3], [1], [3]], z2=Any[2, "a", 2, "a"]))
    @test_throws ArgumentError combine(gdf, :a => (x -> [(x=[1], y=2), (x=[3], y="a")]) => [:z1, :z2, :z3])
    @test_throws ArgumentError combine(gdf, :a => (x -> [Dict('x' => 1)]) => AsTable)
end

@testset "sorting API" begin
    # simple tests
    df = DataFrame(x=["b", "c", "b", "a", "c"])
    @test getindex.(keys(groupby(df, :x)), 1) == ["b", "c", "a"]
    @test getindex.(keys(groupby(df, :x, sort=true)), 1) == ["a", "b", "c"]
    @test getindex.(keys(groupby(df, :x, sort=NamedTuple())), 1) == ["a", "b", "c"]
    @test getindex.(keys(groupby(df, :x, sort=false)), 1) == ["b", "c", "a"]
    @test getindex.(keys(groupby(df, order(:x))), 1) == ["a", "b", "c"]
    @test getindex.(keys(groupby(df, order(:x), sort=true)), 1) == ["a", "b", "c"]
    @test_throws ArgumentError groupby(df, order(:x), sort=false)
    @test getindex.(keys(groupby(df, order(:x), sort=NamedTuple())), 1) == ["a", "b", "c"]
    @test getindex.(keys(groupby(df, [order(:x)])), 1) == ["a", "b", "c"]
    @test getindex.(keys(groupby(df, [order(:x)], sort=true)), 1) == ["a", "b", "c"]
    @test_throws ArgumentError groupby(df, [order(:x)], sort=false)
    @test getindex.(keys(groupby(df, [order(:x)], sort=NamedTuple())), 1) == ["a", "b", "c"]
    @test getindex.(keys(groupby(df, order(:x, rev=true))), 1) == ["c", "b", "a"]
    @test getindex.(keys(groupby(df, order(:x, rev=true), sort=true)), 1) == ["c", "b", "a"]
    @test getindex.(keys(groupby(df, order(:x, rev=true), sort=NamedTuple())), 1) == ["c", "b", "a"]
    @test getindex.(keys(groupby(df, [order(:x, rev=true)])), 1) == ["c", "b", "a"]
    @test getindex.(keys(groupby(df, [order(:x, rev=true)], sort=true)), 1) == ["c", "b", "a"]
    @test getindex.(keys(groupby(df, [order(:x, rev=true)], sort=NamedTuple())), 1) == ["c", "b", "a"]
    @test getindex.(keys(groupby(df, :x, sort=(;rev=true))), 1) == ["c", "b", "a"]
    @test getindex.(keys(groupby(df, [:x], sort=(;rev=true))), 1) == ["c", "b", "a"]

    # by default sorting is not applied as range of values is wide
    df = DataFrame(x=[2, 100, 2, 1, 100])
    @test getindex.(keys(groupby(df, :x)), 1) == [2, 100, 1]
    @test getindex.(keys(groupby(df, :x, sort=true)), 1) == [1, 2, 100]
    @test getindex.(keys(groupby(df, :x, sort=NamedTuple())), 1) == [1, 2, 100]
    @test getindex.(keys(groupby(df, :x, sort=false)), 1) == [2, 100, 1]
    @test getindex.(keys(groupby(df, order(:x))), 1) == [1, 2, 100]
    @test getindex.(keys(groupby(df, [order(:x)])), 1) == [1, 2, 100]
    @test getindex.(keys(groupby(df, order(:x, rev=true))), 1) == [100, 2, 1]
    @test getindex.(keys(groupby(df, [order(:x, rev=true)])), 1) == [100, 2, 1]
    @test getindex.(keys(groupby(df, :x, sort=(;rev=true))), 1) == [100, 2, 1]
    @test getindex.(keys(groupby(df, [:x], sort=(;rev=true))), 1) == [100, 2, 1]

    # by default sorting is applied as range of values is narrow
    df = DataFrame(x=[2, 3, 2, 1, 3])
    @test getindex.(keys(groupby(df, :x)), 1) == [1, 2, 3]
    @test getindex.(keys(groupby(df, :x, sort=true)), 1) == [1, 2, 3]
    @test getindex.(keys(groupby(df, :x, sort=NamedTuple())), 1) == [1, 2, 3]
    @test getindex.(keys(groupby(df, :x, sort=false)), 1) == [2, 3, 1]
    @test getindex.(keys(groupby(df, order(:x))), 1) == [1, 2, 3]
    @test getindex.(keys(groupby(df, [order(:x)])), 1) == [1, 2, 3]
    @test getindex.(keys(groupby(df, order(:x, rev=true))), 1) == [3, 2, 1]
    @test getindex.(keys(groupby(df, [order(:x, rev=true)])), 1) == [3, 2, 1]
    @test getindex.(keys(groupby(df, :x, sort=(;rev=true))), 1) == [3, 2, 1]
    @test getindex.(keys(groupby(df, [:x], sort=(;rev=true))), 1) == [3, 2, 1]

    # randomized tests
    Random.seed!(1234)
    df1 = DataFrame(a=rand(-10:10, 100), b=rand(-10:10, 100), c=1:100)
    df2 = string.(df1, pad=3)

    for df in (df1, df2)
        for col in (:a, "a", 1, :b, "b", 2, :c, "c", 3)
            gdf = groupby(df, order(col))
            @test issorted(DataFrame(gdf)[:, col])
            @test all(x -> issorted(x.c), gdf)
            gdf = groupby(df, col, sort=true)
            @test issorted(DataFrame(gdf)[:, col])
            @test all(x -> issorted(x.c), gdf)
            gdf = groupby(df, order(col), sort=true)
            @test issorted(DataFrame(gdf)[:, col])
            @test all(x -> issorted(x.c), gdf)
            gdf = groupby(df, col, sort=NamedTuple())
            @test issorted(DataFrame(gdf)[:, col])
            @test all(x -> issorted(x.c), gdf)
            gdf = groupby(df, order(col), sort=NamedTuple())
            @test issorted(DataFrame(gdf)[:, col])
            @test all(x -> issorted(x.c), gdf)
            gdf = groupby(df, col, sort=(rev=true,))
            @test issorted(DataFrame(gdf)[:, col], rev=true)
            @test all(x -> issorted(x.c), gdf)
            if eltype(df[!, col]) === Int
                gdf = groupby(df, order(col, by=abs), sort=(rev=true,))
                @test issorted(DataFrame(gdf)[:, col], rev=true, by=abs)
            else
                gdf = groupby(df, order(col, by=abs∘(x -> parse(Int, x))), sort=(rev=true,))
                @test issorted(DataFrame(gdf)[:, col], rev=true, by=abs∘(x -> parse(Int, x)))
            end
            @test all(x -> issorted(x.c), gdf)
            gdf = groupby(df, col, sort=false)
            @test getindex.(keys(gdf), 1) == unique(df[!, col])
            @test all(x -> issorted(x.c), gdf)
        end

        gdf = groupby(df, [:a, :b], sort=true)
        @test issorted(DataFrame(gdf), [:a, :b])
        @test all(x -> issorted(x.c), gdf)
        gdf = groupby(df, [:a, order(:b)])
        @test issorted(DataFrame(gdf), [:a, :b])
        @test all(x -> issorted(x.c), gdf)
        gdf = groupby(df, [:a, order(:b)], sort=true)
        @test issorted(DataFrame(gdf), [:a, :b])
        @test all(x -> issorted(x.c), gdf)
        gdf = groupby(df, [:a, :b], sort=NamedTuple())
        @test issorted(DataFrame(gdf), [:a, :b])
        @test all(x -> issorted(x.c), gdf)
        gdf = groupby(df, [:a, order(:b)], sort=NamedTuple())
        @test issorted(DataFrame(gdf), [:a, :b])
        @test all(x -> issorted(x.c), gdf)
        gdf = groupby(df, [:a, :b], sort=(rev=true,))
        @test issorted(DataFrame(gdf), [:a, :b], rev=true)
        @test all(x -> issorted(x.c), gdf)
        if eltype(df[!, :a]) === Int
            gdf = groupby(df, [order(:a, by=abs), :b], sort=(rev=true,))
            @test issorted(DataFrame(gdf), [order(:a, by=abs), :b], rev=true)
            @test all(x -> issorted(x.c), gdf)
        else
            gdf = groupby(df, [order(:a, by=abs∘(x -> parse(Int, x))), :b], sort=(rev=true,))
            @test issorted(DataFrame(gdf), [order(:a, by=abs∘(x -> parse(Int, x))), :b], rev=true)
            @test all(x -> issorted(x.c), gdf)
        end
        gdf = groupby(df, [:a, order(:b, rev=false)], sort=(rev=true,))
        @test issorted(DataFrame(gdf), [:a, order(:b, rev=false)], rev=true)
        @test all(x -> issorted(x.c), gdf)
        gdf = groupby(df, [:a, :b], sort=false)
        @test Tuple.(keys(gdf)) == unique(Tuple.(eachrow(df[!, [:a, :b]])))
        @test all(x -> issorted(x.c), gdf)

        @test_throws ArgumentError groupby(df, order(:a), sort=false)
        @test_throws ArgumentError groupby(df, [:b, order(:a)], sort=false)
        @test_throws MethodError groupby(df, :a, sort=(x=1,))
    end
end

@testset "no levels in pooled grouping bug #3393" begin
    @test isempty(groupby_checked(DataFrame(x=PooledArray([missing])), :x, skipmissing=true))
    @test isempty(groupby_checked(DataFrame(x=categorical([missing])), :x, skipmissing=true))
end

end # module
