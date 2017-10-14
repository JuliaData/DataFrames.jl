module TestDuplicates
    using Base.Test
    using DataFrames
    using DataArrays

    df = DataFrame(a = [1, 2, 3, 3, 4])
    udf = DataFrame(a = [1, 2, 3, 4])
    @test isequal(nonunique(df), [false, false, false, true, false])
    @test isequal(udf, unique(df))
    unique!(df)
    @test isequal(df, udf)

    pdf = DataFrame( a = PooledDataArray( @data ["a", "a", null, null, "b", null, "a", null] ),
                     b = PooledDataArray( @data ["a", "b", null, null, "b", "a", "a", "a"] ) )
    updf = DataFrame( a = PooledDataArray( @data ["a", "a", null, "b", null] ),
                      b = PooledDataArray( @data ["a", "b", null, "b", "a"] ) )
    @test isequal(nonunique(pdf), [false, false, false, true, false, false, true, true])
    @test isequal(nonunique(updf), falses(5) )
    @test isequal(updf, unique(pdf))
    unique!(pdf)
    @test isequal(pdf, updf)
end
