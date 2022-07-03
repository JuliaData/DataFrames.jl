module TestMetadata

using Test, DataFrames, Random

@testset "hasmetadata & metadata" begin
    for x in (DataFrame(), DataFrame(a=1))
        @test !hasmetadata(x)
        m = metadata(x)
        @test !hasmetadata(x)
        @test isempty(m)
        m["name"] = "empty"
        @test hasmetadata(x)
        @test metadata(x) === m
        empty!(m)
        @test !hasmetadata(x)
        @test metadata(x) === m
    end

    for fun in (eachcol, eachrow, x -> groupby(x, :a),
                x -> x[1, :], x -> @view x[:, :])
        x = fun(DataFrame(a=1))
        @test !hasmetadata(x)
        m = metadata(x)
        @test !hasmetadata(x)
        @test isempty(m)
        m["name"] = "empty"
        @test hasmetadata(x)
        @test metadata(x) === m
        empty!(m)
        @test !hasmetadata(x)
        @test metadata(x) === m
    end
end

@testset "hascolmetadata & colmetadata" begin
    for fun in (identity, eachcol, eachrow, x -> groupby(x, :a),
                x -> x[1, :], x -> @view x[:, :]),
        a in (:a, "a", 2), b in (:b, "b", 1), x in (:x, "x")
        y = fun(DataFrame(b=2, a=1))
        @test !hascolmetadata(y)
        @test_throws ArgumentError hascolmetadata(y, x)
        @test !hascolmetadata(y, a)
        @test !hascolmetadata(y, b)
        @test_throws ArgumentError colmetadata(y, x)
        m = colmetadata(y, a)
        @test !hascolmetadata(y)
        @test_throws ArgumentError hascolmetadata(y, x)
        @test !hascolmetadata(y, a)
        @test !hascolmetadata(y, b)
        @test isempty(m)
        m["name"] = "empty"
        @test hascolmetadata(y)
        @test_throws ArgumentError hascolmetadata(y, x)
        @test hascolmetadata(y, a)
        @test !hascolmetadata(y, b)
        @test colmetadata(y, a) === m
        empty!(m)
        @test !hascolmetadata(y)
        @test_throws ArgumentError hascolmetadata(y, x)
        @test !hascolmetadata(y, a)
        @test !hascolmetadata(y, b)
        @test colmetadata(y, a) === m
    end

    df = DataFrame(a=1, b=2, c=3)
    colmetadata(df, :a)["name"] = "empty"
    for x in (df[1, [3, 1]], @view df[1:1, [3, 1]])
        @test !hascolmetadata(x, :c)
        @test !hascolmetadata(x, 1)
        @test hascolmetadata(x, :a)
        @test hascolmetadata(x, 2)
        @test colmetadata(x, :a) === colmetadata(x, 2) === colmetadata(df, :a)
        @test_throws ArgumentError hascolmetadata(x, :x)
        @test_throws BoundsError hascolmetadata(x, :b)
    end
end

@testset "dropallmetadata!" begin
    for fun in (identity, eachcol, eachrow, x -> groupby(x, :a),
                x -> x[1, :], x -> @view x[:, :])
        df = DataFrame(b=2, a=1)
        metadata(df)["name"] = "empty"
        colmetadata(df, :a)["name"] = "a"
        x = fun(df)
        @test hasmetadata(x)
        @test hascolmetadata(x)
        dropallmetadata!(x)
        @test !hasmetadata(x)
        @test !hascolmetadata(x)
        @test getfield(df, :metadata) === nothing
        @test getfield(df, :colmetadata) === nothing
    end
end

@testset "rename & rename!" begin
    df = DataFrame()
    df2 = rename(df)
    @test !hasmetadata(df2)
    @test !hascolmetadata(df2)
    df2 = rename!(df)
    @test !hasmetadata(df2)
    @test !hascolmetadata(df2)

    df = DataFrame()
    metadata(df)["name"] = "empty"
    df2 = rename(df)
    @test hasmetadata(df2)
    @test metadata(df2)["name"] == "empty"
    @test !hascolmetadata(df2)
    df2 = rename!(df)
    @test hasmetadata(df2)
    @test metadata(df2)["name"] == "empty"
    @test !hascolmetadata(df2)

    df = DataFrame(a=1, b=2, c=3, d=4, e=5, f=6, g=7, h=8)
    metadata(df)["name"] = "empty"
    colmetadata(df, :a)["name"] = "a"
    colmetadata(df, :b)["name"] = "b"
    colmetadata(df, :c)["name"] = "c"
    colmetadata(df, :e)["name"] = "e"
    colmetadata(df, :g)["name"] = "g"
    # other renaming methods rely on the same mechanism as this one
    # so it is enough to run these tests
    df2 = rename(df, :a => :c, :b => :d, :c => :a, :d => :b, :e => :e1, :h => :h1)
    @test hasmetadata(df2)
    @test metadata(df2)["name"] == "empty"
    @test hascolmetadata(df2)
    @test colmetadata(df2, "c") == Dict("name" => "a") == colmetadata(df, "a")
    @test colmetadata(df2, "c") !== colmetadata(df, "a")
    @test colmetadata(df2, "d") == Dict("name" => "b") == colmetadata(df, "b")
    @test colmetadata(df2, "d") !== colmetadata(df, "b")
    @test colmetadata(df2, "a") == Dict("name" => "c") == colmetadata(df, "c")
    @test colmetadata(df2, "a") !== colmetadata(df, "c")
    @test !hascolmetadata(df2, "b")
    @test colmetadata(df2, "e1") == Dict("name" => "e") == colmetadata(df, "e")
    @test colmetadata(df2, "e1") !== colmetadata(df, "e")
    @test !hascolmetadata(df2, "f")
    @test colmetadata(df2, "g") == Dict("name" => "g") == colmetadata(df, "g")
    @test colmetadata(df2, "g") !== colmetadata(df, "g")
    @test !hascolmetadata(df2, :h1)
    @test getfield(df, :metadata) == getfield(df2, :metadata)
    @test getfield(df, :colmetadata) == getfield(df2, :colmetadata)
    @test getfield(df, :metadata) !== getfield(df2, :metadata)
    @test getfield(df, :colmetadata) !== getfield(df2, :colmetadata)

    m1 = copy(metadata(df))
    mc1 = copy(getfield(df, :colmetadata))
    m2 = metadata(df)
    mc2 = getfield(df, :colmetadata)
    df2 = rename!(df)
    @test metadata(df2) == m1
    @test getfield(df2, :colmetadata) == mc1
    @test metadata(df2) === m2
    @test getfield(df2, :colmetadata) === mc2
end

@testset "similar, empty, empty!" begin

    for fun in (x -> similar(x, 2), empty, empty!)
        df = DataFrame()
        df2 = fun(df)
        @test getfield(df2, :metadata) === nothing
        @test getfield(df2, :colmetadata) === nothing

        df = DataFrame(a=1, b=2)
        df2 = fun(df)
        @test getfield(df2, :metadata) === nothing
        @test getfield(df2, :colmetadata) === nothing
    end

    df = DataFrame(a=1, b=2)
    metadata(df)["name"] = "empty"
    colmetadata(df, :b)["name"] = "some"

    for fun in (x -> similar(x, 2), empty)
        df2 = fun(df)
        @test getfield(df2, :metadata) == getfield(df, :metadata)
        @test getfield(df2, :metadata) !== getfield(df, :metadata)
        @test getfield(df2, :colmetadata) == getfield(df, :colmetadata)
        @test getfield(df2, :colmetadata) !== getfield(df, :colmetadata)
    end

    m1 = copy(metadata(df))
    mc1 = copy(getfield(df, :colmetadata))
    m2 = metadata(df)
    mc2 = getfield(df, :colmetadata)
    df2 = empty!(df)
    @test getfield(df2, :metadata) == m1
    @test getfield(df2, :metadata) === m2
    @test getfield(df2, :colmetadata) == mc1
    @test getfield(df2, :colmetadata) === mc2
end

@testset "only, first, last" begin
    for fun in (only, first, last,
                x -> first(x, 1, view=true),
                x -> last(x, 1, view=true))
        df = DataFrame(a=1, b=2)
        x = fun(df)
        @test getfield(parent(x), :metadata) === nothing
        @test getfield(parent(x), :colmetadata) === nothing

        df = DataFrame(a=1, b=2)
        metadata(df)["name"] = "empty"
        colmetadata(df, :b)["name"] = "some"
        x = fun(df)
        @test getfield(parent(x), :metadata) === getfield(df, :metadata)
        @test getfield(parent(x), :colmetadata) === getfield(df, :colmetadata)
    end

    for fun in (x -> first(x, 1),
                x -> last(x, 1))
        df = DataFrame(a=1, b=2)
        x = fun(df)
        @test getfield(x, :metadata) === nothing
        @test getfield(x, :colmetadata) === nothing

        df = DataFrame(a=1, b=2)
        metadata(df)["name"] = "empty"
        colmetadata(df, :b)["name"] = "some"
        x = fun(df)
        @test getfield(x, :metadata) == getfield(df, :metadata)
        @test getfield(x, :metadata) !== getfield(df, :metadata)
        @test getfield(x, :colmetadata) == getfield(df, :colmetadata)
        @test getfield(x, :colmetadata) !== getfield(df, :colmetadata)
    end
end

@testset "describe" begin
    df = DataFrame()
    x = describe(df)
    @test getfield(x, :metadata) === nothing
    @test getfield(x, :colmetadata) === nothing

    metadata(df)["name"] = "empty"
    x = describe(df)
    @test metadata(x) == Dict("name" => "empty")
    @test metadata(x) !== metadata(df)
    @test getfield(x, :colmetadata) === nothing

    df = DataFrame(a=1, b="x")
    metadata(df)["name"] = "empty"
    colmetadata(df, :a)["name"] = "a"
    x = describe(df)
    @test metadata(x) == Dict("name" => "empty")
    @test metadata(x) !== metadata(df)
    @test getfield(x, :colmetadata) === nothing
end

@testset "functions that keep all metadata" begin
    # Tested functions:
    #   dropmissing, dropmissing!, filter, filter!, unique, unique!, repeat, repeat!,
    #   disallowmissing, allowmissing, disallowmissing!, allowmissing!, flatten,
    #   reverse, reverse!, permute!, invpermute!, shuffle, shuffle!,
    #   insertcols, insertcols!, mapcols, mapcols!

    for fun in (dropmissing,
                x -> dropmissing(x, disallowmissing=false),
                x -> filter(v -> true, x),
                x -> filter(v -> false, x),
                unique,
                x -> repeat(x, 3),
                x -> repeat(x, inner=2, outer=2),
                x -> disallowmissing(x, error=false),
                allowmissing,
                x -> flatten(x, []),
                reverse,
                shuffle,
                x -> insertcols(x, :newcol => 1),
                x -> mapcols(v -> copy(v), x))
        df = DataFrame()
        x = fun(df)
        @test getfield(x, :metadata) === nothing
        @test getfield(x, :colmetadata) === nothing

        metadata(df)["name"] = "empty"
        x = fun(df)
        @test metadata(x) == Dict("name" => "empty")
        @test metadata(x) !== metadata(df)
        @test getfield(x, :colmetadata) === nothing

        df = DataFrame(a=1, b="x")
        metadata(df)["name"] = "empty"
        colmetadata(df, :a)["name"] = "a"
        x = fun(df)
        @test metadata(x) == Dict("name" => "empty")
        @test metadata(x) !== metadata(df)
        @test getfield(x, :colmetadata) == getfield(df, :colmetadata)
        @test getfield(x, :colmetadata) !== getfield(df, :colmetadata)
        @test colmetadata(x, :a) == colmetadata(df, :a) == Dict("name" => "a")
        @test colmetadata(x, :a) !== colmetadata(df, :a)

        df = DataFrame(a=[1, missing], b=["x", "y"])
        metadata(df)["name"] = "empty"
        colmetadata(df, :a)["name"] = "a"
        x = fun(df)
        @test metadata(x) == Dict("name" => "empty")
        @test metadata(x) !== metadata(df)
        @test getfield(x, :colmetadata) == getfield(df, :colmetadata)
        @test getfield(x, :colmetadata) !== getfield(df, :colmetadata)
        @test colmetadata(x, :a) == colmetadata(df, :a) == Dict("name" => "a")
        @test colmetadata(x, :a) !== colmetadata(df, :a)
        x = fun(view(df, :, 1:1))
        @test metadata(x) == Dict("name" => "empty")
        @test metadata(x) !== metadata(df)
        @test getfield(x, :colmetadata) == getfield(df, :colmetadata)
        @test getfield(x, :colmetadata) !== getfield(df, :colmetadata)
        @test colmetadata(x, :a) == colmetadata(df, :a) == Dict("name" => "a")
        @test colmetadata(x, :a) !== colmetadata(df, :a)
        x = fun(view(df, :, 2:2))
        @test metadata(x) == Dict("name" => "empty")
        @test metadata(x) !== metadata(df)
        @test getfield(x, :colmetadata) === nothing
    end

    df = DataFrame(a=[1, 2], b=["x", "y"])
    metadata(df)["name"] = "empty"
    colmetadata(df, :a)["name"] = "a"
    x = flatten(df, 1)
    @test metadata(x) == Dict("name" => "empty")
    @test metadata(x) !== metadata(df)
    @test getfield(x, :colmetadata) == getfield(df, :colmetadata)
    @test getfield(x, :colmetadata) !== getfield(df, :colmetadata)
    @test colmetadata(x, :a) == colmetadata(df, :a) == Dict("name" => "a")
    @test colmetadata(x, :a) !== colmetadata(df, :a)
    x = flatten(view(df, :, 1:1), 1)
    @test metadata(x) == Dict("name" => "empty")
    @test metadata(x) !== metadata(df)
    @test getfield(x, :colmetadata) == getfield(df, :colmetadata)
    @test getfield(x, :colmetadata) !== getfield(df, :colmetadata)
    @test colmetadata(x, :a) == colmetadata(df, :a) == Dict("name" => "a")
    @test colmetadata(x, :a) !== colmetadata(df, :a)
    x = flatten(view(df, :, 2:2), 1)
    @test metadata(x) == Dict("name" => "empty")
    @test metadata(x) !== metadata(df)
    @test getfield(x, :colmetadata) === nothing

    for fun in (dropmissing!,
                x -> dropmissing!(x, disallowmissing=false),
                x -> dropmissing(x, view=true),
                x -> filter!(v -> true, x),
                x -> filter!(v -> false, x),
                x -> filter(v -> true, x, view=true),
                x -> filter(v -> false, x, view=true),
                x -> filter(v -> true, groupby(x, ncol(x) == 0 ? [] : 1), ungroup=true),
                x -> filter(v -> true, groupby(x, ncol(x) == 0 ? [] : 1), ungroup=false),
                x -> filter(v -> false, groupby(x, ncol(x) == 0 ? [] : 1), ungroup=true),
                x -> filter(v -> false, groupby(x, ncol(x) == 0 ? [] : 1), ungroup=false),
                unique!,
                x -> unique(x, view=true),
                x -> repeat!(x, 3),
                x -> repeat!(x, inner=2, outer=2),
                x -> disallowmissing!(x, error=false),
                allowmissing!,
                reverse!,
                x -> permute!(x, 1:nrow(x),),
                x -> invpermute!(x, 1:nrow(x),),
                shuffle!,
                x -> insertcols!(x, :newcol => 1, makeunique=true),
                x -> mapcols!(v -> copy(v), x))
        df = DataFrame()
        x = fun(df)
        @test getfield(parent(x), :metadata) === nothing
        @test getfield(parent(x), :colmetadata) === nothing

        metadata(df)["name"] = "empty"
        x = fun(df)
        @test metadata(x) == Dict("name" => "empty")
        @test getfield(parent(x), :colmetadata) === nothing

        df = DataFrame(a=1, b="x")
        metadata(df)["name"] = "empty"
        colmetadata(df, :a)["name"] = "a"
        x = fun(df)
        @test metadata(x) == Dict("name" => "empty")
        @test colmetadata(x, :a) == Dict("name" => "a")

        df = DataFrame(a=[1, missing], b=["x", "y"])
        metadata(df)["name"] = "empty"
        colmetadata(df, :a)["name"] = "a"
        x = fun(df)
        @test metadata(x) == Dict("name" => "empty")
        @test colmetadata(x, :a) == Dict("name" => "a")
    end
end

@testset "fillcombinations" begin
    for df in (DataFrame(x=1:2, y='a':'b', z=["x", "y"]), DataFrame(x=[], y=[], z=[]))
        df2 = fillcombinations(df, [:x, :y])
        @test getfield(df2, :metadata) === nothing
        @test getfield(df2, :colmetadata) === nothing
        metadata(df)["name"] = "something"
        colmetadata(df, "z")["name"] = "z"
        df2 = fillcombinations(df, [:x, :y])
        @test metadata(df2) == Dict("name" => "something")
        @test metadata(df2) !== metadata(df)
        @test getfield(df2, :colmetadata) == getfield(df, :colmetadata)
        @test getfield(df2, :colmetadata) !== getfield(df, :colmetadata)
    end
end

@testset "hcat" begin
    df1 = DataFrame(a=1:3, b=11:13)
    df2 = DataFrame(c=111:113, d=1111:1113)

    res = hcat(df1, df2)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    res = hcat(df1)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata(df2)["name"] = "some"
    res = hcat(df1, df2)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata(df1)["type"] = "other"
    res = hcat(df1, df2)
    @test getfield(res, :metadata) == Dict()
    @test getfield(res, :colmetadata) === nothing

    res = hcat(df1)
    @test getfield(res, :metadata) == Dict("type" => "other")
    @test getfield(res, :colmetadata) === nothing

    metadata(df1)["name"] = "some2"
    res = hcat(df1, df2)
    @test getfield(res, :metadata) == Dict()
    @test getfield(res, :colmetadata) === nothing

    metadata(df1)["name"] = "some"
    res = hcat(df1, df2)
    @test getfield(res, :metadata) == Dict("name" => "some")
    @test getfield(res, :colmetadata) === nothing

    colmetadata(df1, :b)["m1"] = "val1"
    colmetadata(df2, :d)["m2"] = "val2"
    res = hcat(df1, df2)
    @test getfield(res, :metadata) == Dict("name" => "some")
    @test getfield(res, :colmetadata) == Dict(2 => Dict("m1" => "val1"),
                                              4 => Dict("m2" => "val2"))
    res = hcat(df1)
    @test getfield(res, :metadata) == Dict("type" => "other", "name" => "some")
    @test getfield(res, :colmetadata) == Dict(2 => Dict("m1" => "val1"))

    res = hcat(df1, df1, df1, makeunique=true)
    @test getfield(res, :metadata) == Dict("type" => "other", "name" => "some")
    @test getfield(res, :colmetadata) == Dict(2 => Dict("m1" => "val1"),
                                              4 => Dict("m1" => "val1"),
                                              6 => Dict("m1" => "val1"))
    @test colmetadata(res, :b) !== colmetadata(res, :b_1)
    @test colmetadata(res, :b) !== colmetadata(res, :b_2)
    @test colmetadata(res, :b_1) !== colmetadata(res, :b_2)
end

@testset "vcat" begin
    df1 = DataFrame(a=1)
    df2 = DataFrame(b=2)
    df3 = DataFrame(a=11, c=3)
    df4 = DataFrame(a=111, c=33, d=4)

    res = vcat(df1, df2, df3, df4, cols=Symbol[])
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata(df1)["a"] = 1
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = vcat(DataFrame(), df1, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata(df2)["a"] = 1
    metadata(df3)["a"] = 1
    metadata(df4)["a"] = 2
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata(df4)["a"] = 1
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1, df2, df3, df4, cols=Symbol[])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1)
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) === nothing

    colmetadata(df1, :a)["x"] = "y"
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1, df2, df3, df4, cols=Symbol[])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) === nothing
    res = vcat(df1)
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) == Dict(1 => Dict("x" => "y"))
    res = vcat(df1, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) == Dict(1 => Dict("x" => "y"))
    res = vcat(df1, cols=[:c, :b, :a, :d, :e])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) == Dict(3 => Dict("x" => "y"))

    colmetadata(df3, :a)["x"] = "y"
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) === nothing

    colmetadata(df4, :a)["x"] = "z"
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) === nothing

    colmetadata(df4, :a)["x"] = "y"
    colmetadata(df4, :c)["a"] = "b"
    colmetadata(df4, :d)["p"] = "q"
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) == Dict(1 => Dict("x" => "y"),
                                              4 => Dict("p" => "q"))

    colmetadata(df3, :c)["a"] = "b"
    res = vcat(df1, df2, df3, df4, cols=[:a, :b, :c, :d, :e])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) == Dict(1 => Dict("x" => "y"),
                                              3 => Dict("a" => "b"),
                                              4 => Dict("p" => "q"))
    res = vcat(df1, df2, df3, df4, cols=[:b, :a, :e, :c, :d])
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) == Dict(2 => Dict("x" => "y"),
                                              4 => Dict("a" => "b"),
                                              5 => Dict("p" => "q"))
    res = vcat(df1, df2, df3, df4, cols=:union)
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) == Dict(1 => Dict("x" => "y"),
                                              3 => Dict("a" => "b"),
                                              4 => Dict("p" => "q"))
    res = vcat(df1, df2, df3, df4, cols=:intersect)
    @test getfield(res, :metadata) == Dict("a" => 1)
    @test getfield(res, :colmetadata) === nothing
end

@testset "stack" begin
    df = DataFrame(a=repeat(1:3, inner=2),
                   b=repeat(1:2, inner=3),
                   c=repeat(1:1, inner=6),
                   d=repeat(1:6, inner=1),
                   e=string.('a':'f'))
    res = stack(df, [:c, :d])
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = stack(df, [:c, :d], view=true)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata(df)["name"] = "empty"
    res = stack(df, [:c, :d])
    @test metadata(res) == Dict("name" => "empty")
    @test metadata(res) !== metadata(df)
    @test getfield(res, :colmetadata) === nothing
    res = stack(df, [:c, :d], view=true)
    @test metadata(res) == Dict("name" => "empty")
    @test metadata(res) !== metadata(df)
    @test getfield(res, :colmetadata) === nothing

    colmetadata(df, :e)["name"] = "e"
    colmetadata(df, :d)["name"] = "d"
    res = stack(df, [:c, :d])
    @test metadata(res) == Dict("name" => "empty")
    @test metadata(res) !== metadata(df)
    @test getfield(res, :colmetadata) == Dict(3 => Dict("name" => "e"))
    @test colmetadata(res, :e) == colmetadata(df, :e)
    @test colmetadata(res, :e) !== colmetadata(df, :e)
    res = stack(df, [:c, :d], view=true)
    @test metadata(res) == Dict("name" => "empty")
    @test metadata(res) !== metadata(df)
    @test getfield(res, :colmetadata) == Dict(3 => Dict("name" => "e"))
    @test colmetadata(res, :e) == colmetadata(df, :e)
    @test colmetadata(res, :e) !== colmetadata(df, :e)
end

@testset "unstack" begin
    wide = DataFrame(id=1:6,
                     a=repeat(1:3, inner=2),
                     b=repeat(1.0:2.0, inner=3),
                     c=repeat(1.0:1.0, inner=6),
                     d=repeat(1.0:3.0, inner=2))
    long = stack(wide)

    res = unstack(long)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = unstack(long, :id, :variable, :value)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing
    res = unstack(long, :a, :variable, :value, valuestransform=copy)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata(long)["name"] = "some"
    colmetadata(long, :variable)["name"] = "var"
    colmetadata(long, :value)["name"] = "val"
    res = unstack(long)
    @test metadata(res) == metadata(long)
    @test metadata(res) !== metadata(long)
    @test getfield(res, :colmetadata) === nothing
    res = unstack(long, :id, :variable, :value)
    @test metadata(res) == metadata(long)
    @test metadata(res) !== metadata(long)
    @test getfield(res, :colmetadata) === nothing
    res = unstack(long, :a, :variable, :value, valuestransform=copy)
    @test metadata(res) == metadata(long)
    @test metadata(res) !== metadata(long)
    @test getfield(res, :colmetadata) === nothing

    colmetadata(long, :a)["name"] = "a"
    res = unstack(long)
    @test metadata(res) == metadata(long)
    @test metadata(res) !== metadata(long)
    @test getfield(res, :colmetadata) == Dict(2 => Dict("name" => "a"))
    @test colmetadata(res, :a) == colmetadata(long, :a)
    @test colmetadata(res, :a) !== colmetadata(long, :a)
    res = unstack(long, :id, :variable, :value)
    @test metadata(res) == metadata(long)
    @test metadata(res) !== metadata(long)
    @test getfield(res, :colmetadata) === nothing
    res = unstack(long, :a, :variable, :value, valuestransform=copy)
    @test metadata(res) == metadata(long)
    @test metadata(res) !== metadata(long)
    @test getfield(res, :colmetadata) == Dict(1 => Dict("name" => "a"))
    @test colmetadata(res, :a) == colmetadata(long, :a)
    @test colmetadata(res, :a) !== colmetadata(long, :a)
end

@testset "permutedims" begin
    df = DataFrame(a=["x", "y"], b=[1.0, 2.0], c=[3, 4], d=[true, false])
    res = permutedims(df, 1)
    @test getfield(res, :metadata) === nothing
    @test getfield(res, :colmetadata) === nothing

    metadata(df)["name"] = "df"
    colmetadata(df, :a)["name"] = "a"
    colmetadata(df, :b)["name"] = "b"
    colmetadata(df, :c)["name"] = "c"
    colmetadata(df, :d)["name"] = "d"
    res = permutedims(df, 1)
    @test metadata(res) == metadata(df)
    @test metadata(res) !== metadata(df)
    @test getfield(res, :colmetadata) === nothing
end

end # module