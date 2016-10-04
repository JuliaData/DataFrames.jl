module TestGrouping
    using Base.Test
    using DataFrames

    df = DataFrame(a = repeat([4, 3, 2, 1], outer=[2]),
                   b = repeat([2, 1], outer=[4]),
                   c = randn(8))
    #df[6, :a] = Nullable()
    #df[7, :b] = Nullable()

    cols = [:a, :b]

    f(df) = DataFrame(cmax = maximum(df[:c]))

    sdf = unique(df[cols])

    # by() without groups sorting
    bdf = by(df, cols, f)
    @test bdf[cols] == sdf
    @test isequal(bdf[cols], unique(sdf[cols]))

    # by() with groups sorting
    sbdf = by(df, cols, f, sort=true)
    @test sbdf[cols] == sort(sdf)
    @test isequal(sbdf[cols], sort(sdf))

    byf = by(df, :a, df -> DataFrame(bsum = sum(df[:b])))

    @test all(T -> T <: AbstractVector, map(typeof, colwise([sum], df)))
    @test all(T -> T <: AbstractVector, map(typeof, colwise(sum, df)))

    # groupby() without groups sorting
    gd = groupby(df, cols)
    ga = map(f, gd)
    @test bdf == combine(ga)
    @test isequal(bdf, combine(ga))

    # groupby() with groups sorting
    gd = groupby(df, cols, sort=true)
    ga = map(f, gd)
    @test sbdf == combine(ga)
    @test isequal(sbdf, combine(ga))

    g(df) = DataFrame(cmax1 = Vector(df[:cmax]) + 1)
    h(df) = g(f(df))

    @test isequal(combine(map(h, gd)), combine(map(g, ga)))

    # testing categorical overflow
    df2 = DataFrame(v1 = categorical(collect(1:1000)), v2 = categorical(fill(1, 1000)))
    @test groupby(df2, [:v1, :v2]).starts == collect(1:1000)
    @test groupby(df2, [:v2, :v1]).starts == collect(1:1000)

    # grouping empty frame
    @test groupby(DataFrame(A=Int[]), :A).starts == Int[]
    # grouping single row
    @test groupby(DataFrame(A=Int[1]), :A).starts == Int[1]

    # issue #960
    x = CategoricalArray(collect(1:20))
    df = DataFrame(v1=x, v2=x)
    @test isa(groupby(df, [:v1, :v2]), GroupedDataFrame)

    df2 = by(e->1, DataFrame(x=Int64[]), :x)
    @test size(df2) == (0,1)
    @test isequal(sum(df2[:x]), Nullable(0))

    # Check that reordering levels does not confuse groupby()
    df = DataFrame(Key1 = CategoricalArray(["A", "A", "B", "B"], ordered=true),
                   Key2 = CategoricalArray(["A", "B", "A", "B"], ordered=true),
                   Value = 1:4)
    gd = groupby(df, :Key1, sort=true)
    @test isequal(gd[1], DataFrame(Key1=["A", "A"], Key2=["A", "B"], Value=1:2))
    @test isequal(gd[2], DataFrame(Key1=["B", "B"], Key2=["A", "B"], Value=3:4))
    gd = groupby(df, [:Key1, :Key2], sort=true)
    @test isequal(gd[1], DataFrame(Key1="A", Key2="A", Value=1))
    @test isequal(gd[2], DataFrame(Key1="A", Key2="B", Value=2))
    @test isequal(gd[3], DataFrame(Key1="B", Key2="A", Value=3))
    @test isequal(gd[4], DataFrame(Key1="B", Key2="B", Value=4))

    # Reorder levels, add unused level
    levels!(df[:Key1], ["Z", "B", "A"])
    levels!(df[:Key2], ["Z", "B", "A"])
    gd = groupby(df, :Key1, sort=true)
    @test isequal(gd[1], DataFrame(Key1=["B", "B"], Key2=["A", "B"], Value=3:4))
    @test isequal(gd[2], DataFrame(Key1=["A", "A"], Key2=["A", "B"], Value=1:2))
    gd = groupby(df, [:Key1, :Key2], sort=true)
    @test isequal(gd[1], DataFrame(Key1="B", Key2="B", Value=4))
    @test isequal(gd[2], DataFrame(Key1="B", Key2="A", Value=3))
    @test isequal(gd[3], DataFrame(Key1="A", Key2="B", Value=2))
    @test isequal(gd[4], DataFrame(Key1="A", Key2="A", Value=1))
end
