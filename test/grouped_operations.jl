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

@testset "Check aggregation of DataFrameRow" begin
    df = DataFrame(a=1)
    dfr = DataFrame(x=1, y="1")[1, 2:2]
    gdf = groupby_checked(df, :a)
    @test combine(sdf -> dfr, gdf) == DataFrame(a=1, y="1")

    df = DataFrame(a=[1, 1, 2, 2, 3, 3], b='a':'f', c=string.(1:6))
    gdf = groupby_checked(df, :a)
    @test isequal_typed(combine(sdf -> sdf[1, [3, 2, 1]], gdf), df[1:2:5, [1, 3, 2]])
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
    @test_throws MethodError combine(gdf, :x => sum∘skipmissing)
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
    @test_throws ArgumentError combine(gdf, x -> x.a[1] == 1 ? (a=1, b=2) : Ref(1))
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

@testset "maximum and minimum on missing" begin
    df = DataFrame(id=[1,1,2,2], x=fill(missing, 4))
    gdf = groupby_checked(df, :id)
    @test combine(gdf, :x => maximum => :x) ≅ DataFrame(id=1:2, x=fill(missing, 2))
    @test combine(gdf, :x => minimum => :x) ≅ DataFrame(id=1:2, x=fill(missing, 2))
    @test_throws ArgumentError combine(gdf, :x => maximum∘skipmissing)
    @test_throws ArgumentError combine(gdf, :x => minimum∘skipmissing)
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

