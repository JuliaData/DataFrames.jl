module TestConstructors
    using Base.Test
    using DataFrames, DataFrames.Index
    using DataArrays

    #
    # DataFrame
    #

    df = DataFrame()
    @test isequal(df.columns, Any[])
    @test isequal(df.colindex, Index())

    df = DataFrame(Any[data(zeros(3)), data(ones(3))],
                   Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    @test isequal(df,
                  DataFrame(Any[data(zeros(3)), data(ones(3))]))
    @test isequal(df,
                  DataFrame(x1 = [0.0, 0.0, 0.0],
                            x2 = [1.0, 1.0, 1.0]))

    df2 = convert(DataFrame, [0.0 1.0;
                              0.0 1.0;
                              0.0 1.0])
    names!(df2, [:x1, :x2])
    @test isequal(df, df2)

    @test isequal(df,
                  convert(DataFrame, [0.0 1.0;
                                      0.0 1.0;
                                      0.0 1.0]))

    @test isequal(df, DataFrame(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0]))
    @test isequal(df, DataFrame(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0],
                                x3 = [2.0, 2.0, 2.0])[[:x1, :x2]])

    df = DataFrame(Int, 2, 2)
    @test size(df) == (2, 2)
    @test all(eltypes(df) .== [Union{Int, Null}, Union{Int, Null}])

    df = DataFrame([Union{Int, Null}, Union{Float64, Null}], [:x1, :x2], 2)
    @test size(df) == (2, 2)
    @test all(eltypes(df) .== Any[Union{Int, Null}, Union{Float64, Null}])

    @test isequal(df, DataFrame([Union{Int, Null}, Union{Float64, Null}], 2))




end
