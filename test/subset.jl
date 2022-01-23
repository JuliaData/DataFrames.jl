module TestSubset

using Test, DataFrames, Statistics, Random

const ≅ = isequal

@testset "subset and subset!" begin
    refdf = DataFrame(x=repeat(Any[true, false], 4),
                      y=repeat([true, false, missing, missing], 2),
                      z=repeat([1, 2, 3, 3], 2),
                      id=1:8)

    for df in (copy(refdf), @view copy(refdf)[1:end-1, :])
        df2 = copy(df)
        @test subset(df, :x) ≅ filter(:x => identity, df)
        @test df ≅ df2
        @test subset(df, :x) isa DataFrame
        @test subset(df, :x, view=true) ≅ filter(:x => identity, df)
        @test subset(df, :x, view=true) isa SubDataFrame
        @test_throws ArgumentError subset(df, :y)
        @test_throws ArgumentError subset(df, :y, :x)
        @test subset(df, :y, skipmissing=true) ≅ filter(:y => x -> x === true, df)
        @test subset(df, :y, skipmissing=true, view=true) ≅ filter(:y => x -> x === true, df)
        @test subset(df, :y, :y, skipmissing=true) ≅ filter(:y => x -> x === true, df)
        @test subset(df, :y, :y, skipmissing=true, view=true) ≅ filter(:y => x -> x === true, df)
        @test subset(df, :x, :y, skipmissing=true) ≅
              filter([:x, :y] => (x, y) -> x && y === true, df)
        @test subset(df, :y, :x, skipmissing=true) ≅
              filter([:x, :y] => (x, y) -> x && y === true, df)
        @test subset(df, :x, :y, skipmissing=true, view=true) ≅
              filter([:x, :y] => (x, y) -> x && y === true, df)
        @test subset(df, :x, :y, :id => ByRow(<(4)), skipmissing=true) ≅
              filter([:x, :y, :id] => (x, y, id) -> x && y === true && id < 4, df)
        @test subset(df, :x, :y, :id => ByRow(<(4)), skipmissing=true, view=true) ≅
              filter([:x, :y, :id] => (x, y, id) -> x && y === true && id < 4, df)
        @test subset(df, :x, :id => ByRow(<(4))) ≅
              filter([:x, :id] => (x, id) -> x && id < 4, df)
        @test subset(df, :x, :id => ByRow(<(4)), view=true) ≅
              filter([:x, :id] => (x, id) -> x && id < 4, df)
        @test_throws ArgumentError subset(df)
        @test isempty(subset(df, :x, :x => ByRow(!)))
        @test_throws ArgumentError subset(df, :x => x -> false, :x => x -> missing)
        @test_throws ArgumentError subset(df, :x => x -> true, :x => x -> missing)
        @test_throws ArgumentError subset(df, :x => x -> true, :x => x -> 2)
    end

    for df in (copy(refdf), @view copy(refdf)[1:end-1, :]),
        gdf in (groupby(df, :z), groupby(df, :z)[[3, 2, 1]])
        df2 = copy(df)
        @test subset(gdf, :x) ≅ filter(:x => identity, df)
        @test df ≅ df2
        @test subset(gdf, :x) isa DataFrame
        @test subset(gdf, :x, ungroup=false) ≅
              groupby(filter(:x => identity, df), :z)
        @test subset(gdf, :x, ungroup=false) isa GroupedDataFrame{DataFrame}
        @test subset(gdf, :x, view=true) ≅ filter(:x => identity, df)
        @test subset(gdf, :x, view=true) isa SubDataFrame
        @test subset(gdf, :x, view=true, ungroup=false) ≅
              groupby(filter(:x => identity, df), :z)
        @test subset(gdf, :x, view=true, ungroup=false) isa GroupedDataFrame{<:SubDataFrame}
        @test_throws ArgumentError subset(gdf, :y)
        @test_throws ArgumentError subset(gdf, :y, :x)
        @test subset(gdf, :y, skipmissing=true) ≅ filter(:y => x -> x === true, df)
        @test subset(gdf, :y, skipmissing=true, view=true) ≅ filter(:y => x -> x === true, df)
        @test subset(gdf, :y, :y, skipmissing=true) ≅ filter(:y => x -> x === true, df)
        @test subset(gdf, :y, :y, skipmissing=true, view=true) ≅ filter(:y => x -> x === true, df)
        @test subset(gdf, :x, :y, skipmissing=true) ≅
              filter([:x, :y] => (x, y) -> x && y === true, df)
        @test subset(gdf, :y, :x, skipmissing=true) ≅
              filter([:x, :y] => (x, y) -> x && y === true, df)
        @test subset(gdf, :x, :y, skipmissing=true, view=true) ≅
              filter([:x, :y] => (x, y) -> x && y === true, df)
        @test subset(gdf, :x, :y, :id => ByRow(<(4)), skipmissing=true) ≅
              filter([:x, :y, :id] => (x, y, id) -> x && y === true && id < 4, df)
        @test subset(gdf, :x, :y, :id => ByRow(<(4)), skipmissing=true, view=true) ≅
              filter([:x, :y, :id] => (x, y, id) -> x && y === true && id < 4, df)
        @test subset(gdf, :x, :id => ByRow(<(4))) ≅
              filter([:x, :id] => (x, id) -> x && id < 4, df)
        @test subset(gdf, :x, :id => ByRow(<(4)), view=true) ≅
              filter([:x, :id] => (x, id) -> x && id < 4, df)
        @test_throws ArgumentError subset(gdf)
        @test isempty(subset(gdf, :x, :x => ByRow(!)))
        @test_throws ArgumentError subset(gdf, :x => x -> false, :x => x -> missing)
        @test_throws ArgumentError subset(gdf, :x => x -> true, :x => x -> missing)
        @test_throws ArgumentError subset(gdf, :x => x -> true, :x => x -> 2)
    end

    df = copy(refdf)
    @test subset!(df, :x) === df
    @test subset!(df, :x) ≅ df ≅ filter(:x => identity, refdf)
    df = copy(refdf)
    @test_throws ArgumentError subset!(df, :y)
    @test df ≅ refdf
    df = copy(refdf)
    @test subset!(df, :y, skipmissing=true) === df
    @test subset!(df, :y, skipmissing=true) ≅ df ≅ filter(:y => x -> x === true, refdf)
    df = copy(refdf)
    @test subset!(df, :x, :y, skipmissing=true) === df
    @test subset!(df, :x, :y, skipmissing=true) ≅ df ≅
          filter([:x, :y] => (x, y) -> x && y === true, refdf)
    df = copy(refdf)
    @test subset!(df, :x, :y, :id => ByRow(<(4)), skipmissing=true) ≅ df ≅
          filter([:x, :y, :id] => (x, y, id) -> x && y === true && id < 4, refdf)
    df = copy(refdf)
    @test subset!(df, :x, :id => ByRow(<(4))) ≅ df ≅
          filter([:x, :id] => (x, id) -> x && id < 4, refdf)
    df = copy(refdf)
    @test_throws ArgumentError subset!(df)
    df = copy(refdf)
    @test isempty(subset!(df, :x, :x => ByRow(!)))
    @test isempty(df)

    df = copy(refdf)
    @test_throws ArgumentError subset!(df, :x => x -> false, :x => x -> missing)
    @test_throws ArgumentError subset!(df, :x => x -> true, :x => x -> missing)
    @test_throws ArgumentError subset!(df, :x => x -> true, :x => x -> 2)

    df = copy(refdf)
    gdf = groupby(df, :z)
    @test subset!(gdf, :x) === df

    df = copy(refdf)
    gdf = groupby(df, :z)
    gdf2 = subset!(gdf, :x, ungroup=false)
    @test gdf2 isa GroupedDataFrame{DataFrame}
    @test parent(gdf2) === df
    @test gdf2 ≅ groupby(df, :z) ≅ groupby(filter(:x => identity, refdf), :z)

    df = copy(refdf)
    gdf = groupby(df, :z)
    @test subset!(gdf, :x) ≅ df ≅ filter(:x => identity, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test_throws ArgumentError subset!(gdf, :y)
    @test df ≅ refdf
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test subset!(gdf, :y, skipmissing=true) === df
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test subset!(gdf, :y, skipmissing=true) ≅ df ≅ filter(:y => x -> x === true, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test subset!(gdf, :x, :y, skipmissing=true) === df
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test subset!(gdf, :x, :y, skipmissing=true) ≅ df ≅
          filter([:x, :y] => (x, y) -> x && y === true, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test subset!(gdf, :x, :y, :id => ByRow(<(4)), skipmissing=true) ≅ df ≅
          filter([:x, :y, :id] => (x, y, id) -> x && y === true && id < 4, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test subset!(gdf, :x, :id => ByRow(<(4))) ≅ df ≅
          filter([:x, :id] => (x, id) -> x && id < 4, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test_throws ArgumentError subset!(gdf)
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test isempty(subset!(gdf, :x, :x => ByRow(!)))
    @test isempty(df)
    df = copy(refdf)
    gdf = groupby(df, :z)
    @test_throws ArgumentError subset!(gdf, :x => x -> false, :x => x -> missing)
    @test_throws ArgumentError subset!(gdf, :x => x -> true, :x => x -> missing)
    @test_throws ArgumentError subset!(gdf, :x => x -> true, :x => x -> 2)

    df = copy(refdf)
    gdf = groupby(df, :z)[[3, 2, 1]]
    @test subset!(gdf, :x) ≅ df ≅ filter(:x => identity, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)[[3, 2, 1]]
    @test_throws ArgumentError subset!(gdf, :y)
    @test df ≅ refdf
    df = copy(refdf)
    gdf = groupby(df, :z)[[3, 2, 1]]
    @test subset!(gdf, :y, skipmissing=true) ≅ df ≅ filter(:y => x -> x === true, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)[[3, 2, 1]]
    @test subset!(gdf, :x, :y, skipmissing=true) ≅ df ≅
          filter([:x, :y] => (x, y) -> x && y === true, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)[[3, 2, 1]]
    @test subset!(gdf, :x, :y, :id => ByRow(<(4)), skipmissing=true) ≅ df ≅
          filter([:x, :y, :id] => (x, y, id) -> x && y === true && id < 4, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)[[3, 2, 1]]
    @test subset!(gdf, :x, :id => ByRow(<(4))) ≅ df ≅
          filter([:x, :id] => (x, id) -> x && id < 4, refdf)
    df = copy(refdf)
    gdf = groupby(df, :z)[[3, 2, 1]]
    @test_throws ArgumentError subset!(gdf)
    df = copy(refdf)
    gdf = groupby(df, :z)[[3, 2, 1]]
    @test isempty(subset!(gdf, :x, :x => ByRow(!)))
    @test isempty(df)

    df = copy(refdf)
    gdf = groupby(df, :z)[[3, 2, 1]]
    @test_throws ArgumentError subset!(gdf, :x => x -> false, :x => x -> missing)
    @test_throws ArgumentError subset!(gdf, :x => x -> true, :x => x -> missing)
    @test_throws ArgumentError subset!(gdf, :x => x -> true, :x => x -> 2)

    @test_throws ArgumentError subset!(view(refdf, :, :), :x)
    @test_throws ArgumentError subset!(groupby(view(refdf, :, :), :z), :x)

    df = DataFrame(g=[2, 2, 1, 1, 1, 1, 3, 3, 3], x=1:9)
    @test subset(df, :x => x -> x .< mean(x)) == DataFrame(g=[2, 2, 1, 1], x=1:4)
    @test subset(groupby(df, :g), :x => x -> x .< mean(x)) ==
          DataFrame(g=[2, 1, 1, 3], x=[1, 3, 4, 7])

    @test_throws ArgumentError subset(df, :x => x -> missing)
    @test isempty(subset(df, :x => ByRow(x -> missing), skipmissing=true))
    @test_throws ArgumentError isempty(subset(df, :x => x -> missing, skipmissing=true))
    @test isempty(subset(df, :x => ByRow(x -> false)))
    @test_throws ArgumentError isempty(subset(df, :x => x -> false))
    @test subset(df, :x => ByRow(x -> true)) ≅ df
    @test_throws ArgumentError subset(df, :x => x -> true) ≅ df
    @test_throws ArgumentError subset(df, :x => x -> (a=x,))
    @test_throws ArgumentError subset(df, :x => (x -> (a=x,)) => AsTable)

    @test_throws ArgumentError subset(DataFrame(x=false, y=missing), :x, :y)
    @test_throws ArgumentError subset(DataFrame(x=missing, y=false), :x, :y)
    @test_throws ArgumentError subset(DataFrame(x=missing, y=false), :x)
    @test_throws ArgumentError subset(DataFrame(x=false, y=missing), :y)
    @test_throws ArgumentError subset(DataFrame(x=false, y=1), :x, :y)
    @test_throws ArgumentError subset(DataFrame(x=1, y=false), :x, :y)
    @test_throws ArgumentError subset(DataFrame(x=1, y=false), :y, :x)
    @test_throws ArgumentError subset(DataFrame(x=false, y=1), :y)

    @test_throws ArgumentError subset(DataFrame(x=false, y=1), :x, :y, skipmissing=true)
    @test_throws ArgumentError subset(DataFrame(x=1, y=false), :x, :y, skipmissing=true)
    @test_throws ArgumentError subset(DataFrame(x=1, y=false), :y, :x, skipmissing=true)
    @test_throws ArgumentError subset(DataFrame(x=false, y=1), :y, skipmissing=true)

    @test_throws ArgumentError DataFrames._and()
    @test_throws ArgumentError DataFrames._and_missing()
end

@testset "subsetting requires passing vector" begin
    @test_throws ArgumentError subset(DataFrame(x=[]), :x => x -> 1)
    @test_throws AssertionError subset(DataFrame(x=[]), :x => ByRow(x -> 1))
    @test_throws ArgumentError subset(DataFrame(x=1:3), [] => () -> true)
    @test subset(DataFrame(x=1:3), [] => ByRow(() -> true)) == DataFrame(x=1:3)
    @test_throws ArgumentError subset(DataFrame(x=[0, 1]), :x => ==(0))
    @test_throws ArgumentError subset(DataFrame(x=[0, missing]), :x => ismissing)
    @test_throws ArgumentError subset(groupby(DataFrame(id=[0, 0, 1, 1],
                                                        x=[-1, 1, 3, 4]), :id),
                                      :x => (x -> sum(x) > 0))
    @test_throws ArgumentError subset(DataFrame(x=1:3), :x => x -> fill(missing, length(x)))
    @test isempty(subset(DataFrame(x=1:3), :x => x -> fill(missing, length(x)),
                         skipmissing=true))
    @test subset(DataFrame(x=1:3), :x => x -> fill(true, length(x))) == DataFrame(x=1:3)
    @test subset(DataFrame(x=1:3), :x => x -> trues(length(x))) == DataFrame(x=1:3)
    @test isempty(subset(DataFrame(x=1:3), :x => x -> fill(false, length(x))))
    @test isempty(subset(DataFrame(x=1:3), :x => x -> falses(length(x))))
    @test_throws AssertionError subset(DataFrame(), [] => () -> Union{}[])
    @test_throws AssertionError subset(DataFrame(), [] => () -> Union{}[], skipmissing=true)
    @test subset(DataFrame(x=1:3:15, y=1:5), [:x, :y] => (x, y) -> iseven.(x) .& iseven.(y)) ==
          DataFrame(x=[4, 10], y=[2, 4])
    @test subset(DataFrame(x=1:3:15, y=1:5),
                 AsTable([:x, :y]) => v -> iseven.(v.x) .& iseven.(v.y)) ==
          DataFrame(x=[4, 10], y=[2, 4])

    @test subset(DataFrame(x=1:3), :x => x -> Any[true, false, true]) ==
          DataFrame(x=[1, 3])
    @test subset(DataFrame(x=1:3), :x => x -> view(Any[true, false, true], :)) ==
          DataFrame(x=[1, 3])
    @test_throws ArgumentError subset(DataFrame(x=1:3), :x => x -> Any[true, false, missing])
    @test subset(DataFrame(x=1:3), :x => x -> Any[true, false, missing], skipmissing=true) ==
          DataFrame(x=[1])
    @test_throws ArgumentError subset(DataFrame(x=1:3), :x => x -> Any[true, false, 1])
    @test_throws ArgumentError subset(DataFrame(x=1:3), :x => x -> Any[true, false, 1],
                                      skipmissing=true)
    @test_throws ArgumentError subset(DataFrame(x=1:3), :x => x -> (true for i in 1:3))
end

@testset "multicolumn selectors" begin
    dfref = DataFrame(1:10 .!= permutedims(1:11), :auto)

    for df in (dfref, groupby(dfref, :x2), groupby(dfref, :x11))
        @test subset(df, Not(:x1) .=> identity) ==
            subset(df, Not(:x1) .=> [identity identity]) ==
            subset(df, (names(df, Not(:x1)) .=> identity)...)
        @test subset(df, Not(:x1)) == subset(df, names(df, Not(:x1))...)
        @test subset(df, :) == subset(df, names(df)) == subset(df, names(df)...)
        @test subset(df, Cols(:) .=> ByRow(x -> true)) == subset(df, (names(df, Cols(:)) .=> ByRow(x -> true))...)
        @test subset(df, [], [:x1]) == subset(df, :x1)
        @test subset(df, Matrix{Any}(undef, 0, 0), [:x1]) == subset(df, :x1)
        @test subset(df, [:x1, :x3, :x5]) == subset(df, [1, 3, 5]) == subset(df, :x1, :x3, :x5)
        @test subset(df, [false; fill(true, 10)]) == subset(df, (2:11)...)
        @test_throws ArgumentError subset(df, [:x1 => identity => :x1])
        @test_throws ArgumentError subset(df, Not(:x1) .=> identity .=> Not(:x1))
        @test_throws ArgumentError subset(df, Not(:x1) .=> [identity identity] .=> Not(:x1))
        @test_throws ArgumentError subset(df, [:y])
        @test_throws BoundsError subset(df, [-1])
        @test_throws BoundsError subset(df, [true])

        Random.seed!(12345)
        df = DataFrame(rand(100, 5), :auto)
        columns = names(df, Not(Between(:x3, ncol(df))))
        @test subset(df, vcat.(columns, "x5") .=> ByRow(<)) ==
              subset(df, [:x1, :x5] => ByRow(<), [:x2, :x5] => ByRow(<)) ==
              df[(df.x1 .< df.x5) .& (df.x2 .< df.x5), :]
    end
end

@testset "wide subsetting" begin
    # current threshold is at 16
    for i in 1:20
        df = DataFrame(a=1:10)
        @test subset(df, [:a => ByRow(x -> true) for _ in 1:i]) == df
        @test subset(df, [:a => ByRow(x -> true) for _ in 1:i], :a => ByRow(x -> false)) ==
              DataFrame(a=Int[])
        @test_throws ArgumentError subset(df, [:a => ByRow(x -> true) for _ in 1:i],
                                          :a => ByRow(x -> "a"))
        @test subset(df, [:a => ByRow(x -> true) for _ in 1:i], skipmissing=true) == df
        @test subset(df, [:a => ByRow(x -> true) for _ in 1:i],
                     :a => ByRow(x -> false), skipmissing=true) ==
              DataFrame(a=Int[])
        @test subset(df, [:a => ByRow(x -> true) for _ in 1:i],
                     :a => ByRow(x -> missing), skipmissing=true) ==
              DataFrame(a=Int[])
        @test subset(df, :a => ByRow(x -> missing), [:a => ByRow(x -> true) for _ in 1:i],
                     skipmissing=true) ==
              DataFrame(a=Int[])
        @test_throws ArgumentError subset(df, :a => ByRow(x -> missing),
                                          [:a => ByRow(x -> true) for _ in 1:i],
                                          :a => ByRow(x -> "a"), skipmissing=true)
        @test_throws ArgumentError subset(df, [:a => ByRow(x -> true) for _ in 1:i],
                                          :a => ByRow(x -> missing), :a => ByRow(x -> "a"),
                                          skipmissing=true)

        # randomized correctness tests across two selection options
        Random.seed!(1234)
        mat1 = rand(Bool, 10_000, 5)
        df = DataFrame(mat1, string.("y", 1:5))
        df.id = 1:nrow(df)
        df2 = [DataFrame(trues(10_000, i), :auto) df]
        @test subset(df, Not(:id)).id ==
              subset(df2, Not(:id)).id ==
              df.id[all.(eachrow(mat1))]

        mat2 = rand([true, false, missing], 10_000, 5)
        df = DataFrame(mat2, string.("y", 1:5))
        df.id = 1:nrow(df)
        df2 = [DataFrame(trues(10_000, i), :auto) df]
        @test subset(df, Not(:id), skipmissing=true).id ==
              subset(df2, Not(:id), skipmissing=true).id ==
              df.id[all.(isequal(true), eachrow(mat2))]
        # with so many rows we must see missing somewhere
        # so this condition is true in practice
        if any(ismissing, mat2)
            @test_throws ArgumentError subset(df, Not(:id))
            @test_throws ArgumentError subset(df2, Not(:id))
        end
    end
end

end
