module TestGrouping
    using Base.Test
    using DataArrays
    using DataFrames

    df = DataFrame(a=rep(1:4, 2), b=rep(2:-1:1, 4), c=randn(8))
    #df[6, "a"] = NA
    #df[7, "b"] = NA

    cols = [:a, :b]

    sdf = sort(df, cols=cols)
    bdf = by(df, cols, :(cmax = maximum(c)))

    @test isequal(bdf[cols], unique(sdf[cols]))
end