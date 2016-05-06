module TestGrouping
    using Base.Test
    using DataFrames

    df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                   b = repeat([2, 1], outer=[4]),
                   c = randn(8))
    #df[6, :a] = Nullable()
    #df[7, :b] = Nullable()

    cols = [:a, :b]

    f(df) = DataFrame(cmax = maximum(df[:c]))

    sdf = sort(df, cols=cols)
    bdf = by(df, cols, f)

    @test get(bdf[cols] == unique(sdf[cols]))

    byf = by(df, :a, df -> DataFrame(bsum = sum(df[:b])))

    @test all(T -> T <: AbstractVector, map(typeof, colwise([sum], df)))
    @test all(T -> T <: AbstractVector, map(typeof, colwise(sum, df)))

    gd = groupby(df, cols)
    ga = map(f, gd)

    @test get(bdf == combine(ga))

    # FIXME: shouldn't need Vector here
    g(df) = DataFrame(cmax1 = Vector(df[:cmax]) + 1)
    h(df) = g(f(df))

    @test get(combine(map(h, gd)) == combine(map(g, ga)))

    # issue #960
    x = NominalArray(collect(1:20))
    df = DataFrame(v1=x, v2=x)
    groupby(df, [:v1, :v2])

    df2 = by(e->1, DataFrame(x=Int64[]), :x)
    @test size(df2) == (0,1)
    @test isequal(sum(df2[:x]), 0)
end
