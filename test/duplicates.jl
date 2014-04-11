module TestDuplicates
    using Base.Test
    using DataFrames

    df = DataFrame(a = [1, 2, 3, 3, 4])
    udf = DataFrame(a = [1, 2, 3, 4])
    @test isequal(nonunique(df), [false, false, false, true, false])
    @test isequal(udf, unique(df))
    unique!(df)
    @test isequal(df, udf)
end
