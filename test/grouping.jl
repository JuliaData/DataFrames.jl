module TestGrouping
    using Base.Test
    using DataFrames

    df = DataFrame(a=rep(1:4, 2), b=rep(2:-1:1, 4), c=randn(8))
    #df[6, :a] = NA
    #df[7, :b] = NA

    cols = [:a, :b]

    sdf = sort(df, cols=cols)
    bdf = by(df, cols, df -> DataFrame(cmax = maximum(df[:c])))

    @test isequal(bdf[cols], unique(sdf[cols]))

    byf = by(df, :a, df -> DataFrame(bsum = sum(df[:b])))
end
