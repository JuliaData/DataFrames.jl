module TestDuplicates
    using Base.Test
    using DataFrames

    df = DataFrame(a = [1, 2, 3, 3, 4])
    udf = DataFrame(a = [1, 2, 3, 4])
    @test isequal(nonunique(df), [false, false, false, true, false])
    @test isequal(udf, unique(df))
    unique!(df)
    @test isequal(df, udf)

    pdf = DataFrame( a = PooledDataArray( @data ["a", "a", NA, NA, "b", NA, "a", NA] ),
                     b = PooledDataArray( @data ["a", "b", NA, NA, "b", "a", "a", "a"] ) )
    updf = DataFrame( a = PooledDataArray( @data ["a", "a", NA, "b", NA] ),
                      b = PooledDataArray( @data ["a", "b", NA, "b", "a"] ) )
    @test isequal(nonunique(pdf), [false, false, false, true, false, false, true, true])
    @test isequal(nonunique(updf), falses(5) )
    @test isequal(updf, unique(pdf))
    unique!(pdf)
    @test isequal(pdf, updf)
end
