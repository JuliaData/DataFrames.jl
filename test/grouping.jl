module TestGrouping
    using Base.Test
    using DataFrames

    df = DataFrame(a=rep(1:4, 2), b=rep(2:-1:1, 4), c=randn(8))
    #df[6, :a] = NA
    #df[7, :b] = NA

    cols = [:a, :b]

    f(df) = DataFrame(cmax = maximum(df[:c]))

    sdf = sort(df, cols=cols)
    bdf = by(df, cols, f)

    @test bdf[cols] == unique(sdf[cols])

    byf = by(df, :a, df -> DataFrame(bsum = sum(df[:b])))

    @test all(T -> T <: AbstractVector, map(typeof, colwise([sum], df)))
    @test all(T -> T <: AbstractVector, map(typeof, colwise(sum, df)))

    gd = groupby(df, cols)
    ga = map(f, gd)

    @test bdf == combine(ga)

    g(df) = DataFrame(cmax1 = df[:cmax] + 1)
    h(df) = g(f(df))

    @test combine(map(h, gd)) == combine(map(g, ga))
end
