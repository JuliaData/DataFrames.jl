module TestConstructors
    using Base.Test
    using DataFrames, DataFrames.Index

    #
    # DataFrame
    #

    df = DataFrame()
    @test isequal(df.columns, Any[])
    @test isequal(df.colindex, Index())

    df = DataFrame(Any[NullableNominalVector(zeros(3)),
                       NullableNominalVector(ones(3))],
                   Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    @test isequal(df, DataFrame(Any[NullableNominalVector(zeros(3)),
                                    NullableNominalVector(ones(3))]))
    @test isequal(df, DataFrame(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0]))

    df2 = convert(DataFrame, [0.0 1.0;
                              0.0 1.0;
                              0.0 1.0])
    names!(df2, [:x1, :x2])
    @test isequal(df, df2)

    @test isequal(df, convert(DataFrame, [0.0 1.0;
                                          0.0 1.0;
                                          0.0 1.0]))

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

end
