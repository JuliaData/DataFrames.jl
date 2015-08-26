module TestDataFrameRow
    using Base.Test
    using DataFrames, Compat

    df = DataFrame(a=NullableArray([1,   2,   3,   1,   2,   2 ]),
                   b=NullableArray(Nullable{Float64}[2.0, Nullable(),
                                                     1.2, 2.0,
                                                     Nullable(), Nullable()]),
                   c=NullableArray(Nullable{String}["A", "B", "C", "A", "B", Nullable()]),
                   d=NullableCategoricalArray(Nullable{Symbol}[:A,  Nullable(),  :C,  :A,
                                                           Nullable(),  :C]))
    df2 = DataFrame(a = NullableArray([1, 2, 3]))

    # test the same frame
    #
    # Equality
    #
    @test_throws ArgumentError isequal(DataFrameRow(df, 1), DataFrameRow(df2, 1))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df, 2))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df, 3))
    @test isequal(DataFrameRow(df, 1), DataFrameRow(df, 4))
    @test isequal(DataFrameRow(df, 2), DataFrameRow(df, 5))
    @test !isequal(DataFrameRow(df, 2), DataFrameRow(df, 6))

    # isless()
    df4 = DataFrame(a=NullableArray(Nullable{Int}[1, 1, 2, 2, 2, 2, Nullable(), Nullable()]),
                    b=NullableArray([2.0, 3.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0]),
                    c=NullableArray(Nullable{Symbol}[:B, Nullable(), :A, :C, :D, :D, :A, :A]))
    @test isless(DataFrameRow(df4, 1), DataFrameRow(df4, 2))
    @test !isless(DataFrameRow(df4, 2), DataFrameRow(df4, 1))
    @test !isless(DataFrameRow(df4, 1), DataFrameRow(df4, 1))
    @test isless(DataFrameRow(df4, 1), DataFrameRow(df4, 3))
    @test !isless(DataFrameRow(df4, 3), DataFrameRow(df4, 1))
    @test isless(DataFrameRow(df4, 3), DataFrameRow(df4, 4))
    @test !isless(DataFrameRow(df4, 4), DataFrameRow(df4, 3))
    @test isless(DataFrameRow(df4, 4), DataFrameRow(df4, 5))
    @test !isless(DataFrameRow(df4, 5), DataFrameRow(df4, 4))
    @test !isless(DataFrameRow(df4, 6), DataFrameRow(df4, 5))
    @test !isless(DataFrameRow(df4, 5), DataFrameRow(df4, 6))
    @test isless(DataFrameRow(df4, 7), DataFrameRow(df4, 8))
    @test !isless(DataFrameRow(df4, 8), DataFrameRow(df4, 7))
    @test isless(DataFrameRow(df4, 1), DataFrameRow(df4, 8))
    @test !isless(DataFrameRow(df4, 8), DataFrameRow(df4, 1))

    # hashing
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 2)))
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 3)))
    @test isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 4)))
    @test isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df, 5)))
    @test !isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df, 6)))

    # test compatible frames
    #
    # Equality
    #
    @test isequal(DataFrameRow(df, 1), DataFrameRow(df2, 6))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df2, 5))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df2, 4))
    @test isequal(DataFrameRow(df, 1), DataFrameRow(df2, 3))
    @test isequal(DataFrameRow(df, 2), DataFrameRow(df2, 2))
    @test !isequal(DataFrameRow(df, 2), DataFrameRow(df2, 1))
    @test isequal(DataFrameRow(df, 2), DataFrameRow(df2, 5))

    # hashing
    @test isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df2, 6)))
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df2, 5)))
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df2, 4)))
    @test isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df2, 3)))
    @test isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df2, 2)))
    @test !isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df2, 1)))
    @test isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df2, 5)))

    # test incompatible frames
    @test_throws ArgumentError isequal(DataFrameRow(df, 1), DataFrameRow(df3, 1))
end
