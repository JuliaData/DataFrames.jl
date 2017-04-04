module TestConstructors
    using Base.Test
    using DataFrames, DataFrames.Index

    #
    # DataFrame
    #

    df = DataFrame()
    @test isequal(df.columns, Any[])
    @test isequal(df.colindex, Index())

    df = DataFrame(Any[NullableCategoricalVector(zeros(3)),
                       NullableCategoricalVector(ones(3))],
                   Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    @test isequal(df, DataFrame(Any[NullableCategoricalVector(zeros(3)),
                                    NullableCategoricalVector(ones(3))]))
    @test isequal(df, DataFrame(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0]))

    df2 = convert(DataFrame, [0.0 1.0;
                              0.0 1.0;
                              0.0 1.0])
    names!(df2, [:x1, :x2])
    @test isequal(df[:x1], NullableArray(df2[:x1]))
    @test isequal(df[:x2], NullableArray(df2[:x2]))

    @test isequal(df, DataFrame(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0]))
    @test isequal(df, DataFrame(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0],
                                x3 = [2.0, 2.0, 2.0])[[:x1, :x2]])

    df = DataFrame(Int, 2, 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Nullable{Int}, Nullable{Int}]

    df = DataFrame([Int, Float64], [:x1, :x2], 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Nullable{Int}, Nullable{Float64}]

    @test isequal(df, DataFrame([Int, Float64], 2))

    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0)
    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0)
    @test isequal(SubDataFrame(DataFrame(A=1), 1), DataFrame(A=1))
    @test isequal(SubDataFrame(DataFrame(A=1:10), 1:4), DataFrame(A=1:4))
    @test isequal(view(SubDataFrame(DataFrame(A=1:10), 1:4), 2), DataFrame(A=2))
    @test isequal(view(SubDataFrame(DataFrame(A=1:10), 1:4), [true, true, false, false]), DataFrame(A=1:2))
end
