module TestDataFrameRow
    using Base.Test
    using DataFrames, Compat

    df = DataFrame(a=@data([1,   2,   3,   1,   2,   2 ]),
                   b=@data([2.0, NA,  1.2, 2.0, NA,  NA]),
                   c=@data(["A", "B", "C", "A", "B", NA]),
                   d=PooledDataArray(
                     @data([:A,  NA,  :C,  :A,  NA,  :C])))
    df2 = DataFrame(a = @data([1, 2, 3]))

    #
    # Equality
    #
    @test_throws ArgumentError isequal(DataFrameRow(df, 1), DataFrameRow(df2, 1))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df, 2))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df, 3))
    @test isequal(DataFrameRow(df, 1), DataFrameRow(df, 4))
    @test isequal(DataFrameRow(df, 2), DataFrameRow(df, 5))
    @test !isequal(DataFrameRow(df, 2), DataFrameRow(df, 6))

    # hashing
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 2)))
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 3)))
    @test isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 4)))
    @test isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df, 5)))
    @test !isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df, 6)))
end
