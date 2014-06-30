module TestConversions
    using Base.Test
    using DataFrames

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = [:A, :B, :C, :D, :E]
    @test isa(array(df), Matrix{Any})
    # @test isa(array(df, Any), Matrix{Any})

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = 1.0:5.0
    @test isa(array(df), Matrix{Real})
    # @test isa(array(df, Any), Matrix{Any})
    # @test isa(array(df, Float64), Matrix{Float64})

    df = DataFrame()
    df[:A] = 1.0:5.0
    df[:B] = 1.0:5.0
    @test isa(array(df), Matrix{Float64})
    # @test isa(matrix(df, Any), Matrix{Any})
    # @test isa(matrix(df, Int), Matrix{Int})

    df1 = convert(DataFrame, {"a" => [1 2], "b" => [3 4]})
    df2 = convert(DataFrame, {:a => [1 2], :b => [3 4]})
    df3 = convert(DataFrame, {"a" => [1 2], "b" => [3 4]}, ["a", "b"])
    df4 = convert(DataFrame, {:a => [1 2], :b => [3 4]}, ["a", "b"])
    @test isequal(df1, df2)
    @test isequal(df1, df3)
    @test isequal(df1, df4)
end
