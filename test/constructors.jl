module TestConstructors
    using Base.Test
    using DataFrames, DataFrames.Index

    #
    # DataFrame
    #

    df = DataFrame()
    @test isequal(df.columns, {})
    @test isequal(df.colindex, Index())

    df = DataFrame({data(zeros(3)), data(ones(3))},
                    Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    @test isequal(df,
                  DataFrame({data(zeros(3)), data(ones(3))}))
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
    @test all(eltypes(df) .== [Int, Int])

    df = DataFrame([Int, Float64], [:x1, :x2], 2)
    @test size(df) == (2, 2)
    @test all(eltypes(df) .== {Int, Float64})

    @test isequal(df, DataFrame([Int, Float64], 2))

    a = [1.0,2.0]
    b = [-0.1,3]
    c = [-3.1,7]
    di = ["a" => a, "b" => b, "c" => c ]
    df = DataFrame(di)

    @test names(df) == Symbol[x for x in sort(collect(keys(di)))]
    @test df[:a] == a
    @test df[:b] == b
    @test df[:c] == c

    di = ["c" => c, "b" => b, "a" => a ]
    df = DataFrame(di)
    @test names(df) == Symbol[x for x in sort(collect(keys(di)))]

    a = [1.0]
    di = ["a" => a, "b" => b, "c" => c ]
    @test_throws ArgumentError DataFrame(di)
end
