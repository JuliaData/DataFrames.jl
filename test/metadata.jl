module TestMetadata

using Test, DataFrames

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

@testset "dropmissing & dropmissing!" begin
    for fun in (dropmissing,
                x -> dropmissing(x, disallowmissing=false))
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
    end

    for fun in (dropmissing!,
                x -> dropmissing!(x, disallowmissing=false),
                x -> dropmissing(x, view=true))
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

end # module