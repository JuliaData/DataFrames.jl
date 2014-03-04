module TestConstructors
    using Base.Test
    using DataArrays
    using DataFrames, DataFrames.Index

    #
    # DataFrame
    #

    df = DataFrame()
    @test isequal(df.columns, {})
    @test isequal(df.colindex, Index())

    df = DataFrame({data(zeros(3)), data(ones(3))},
                    Index([:x, :x_1]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    @test isequal(df,
                  DataFrame({data(zeros(3)), data(ones(3))}))
    @test isequal(df,
                  DataFrame(x = [0.0, 0.0, 0.0],
                            x_1 = [1.0, 1.0, 1.0]))

    df2 = convert(DataFrame, [0.0 1.0;
                              0.0 1.0;
                              0.0 1.0])
    names!(df2, [:x, :x_1])
    @test isequal(df, df2)
                  
    @test isequal(df,
                  convert(DataFrame, [0.0 1.0;
                                      0.0 1.0;
                                      0.0 1.0]))
    @test isequal(df,
                  DataFrame(data(zeros(3)), data(ones(3))))

    @test isequal(df, DataFrame(x = [0.0, 0.0, 0.0],
                                x_1 = [1.0, 1.0, 1.0]))
    @test isequal(df, DataFrame(x = [0.0, 0.0, 0.0],
                                x_1 = [1.0, 1.0, 1.0],
                                x_2 = [2.0, 2.0, 2.0])[[:x, :x_1]])

    df = DataFrame(Int, 2, 2)
    @test size(df) == (2, 2)
    @test all(eltypes(df) .== [Int, Int])

    df = DataFrame([Int, Float64], [:x, :x_1], 2)
    @test size(df) == (2, 2)
    @test all(eltypes(df) .== {Int, Float64})

    @test isequal(df, DataFrame([Int, Float64], 2))
end
