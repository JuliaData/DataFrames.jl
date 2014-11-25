module TestConversions
    using Base.Test
    using DataFrames

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = [:A, :B, :C, :D, :E]
    @test isa(array(df), Matrix{Any})
    @test array(df) == array(DataArray(df))
    # @test isa(array(df, Any), Matrix{Any})

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = 1.0:5.0
    @test isa(array(df), Matrix{Real})
    @test array(df) == array(DataArray(df))
    # @test isa(array(df, Any), Matrix{Any})
    # @test isa(array(df, Float64), Matrix{Float64})

    df = DataFrame()
    df[:A] = 1.0:5.0
    df[:B] = 1.0:5.0
    @test isa(array(df), Matrix{Float64})
    @test array(df) == array(DataArray(df))
    # @test isa(matrix(df, Any), Matrix{Any})
    # @test isa(matrix(df, Int), Matrix{Int})

    df[1,1] = NA
    @test_throws ErrorException array(df)
    @test isa(DataArray(df), DataMatrix{Float64})

    a = [1.0,2.0]
    b = [-0.1,3]
    c = [-3.1,7]
    di = Dict([("a", a), ("b", b), ("c", c)])

    df = convert(DataFrame,di)
    @test isa(df,DataFrame)
    @test names(df) == Symbol[x for x in sort(collect(keys(di)))]
    @test df[:a] == a
    @test df[:b] == b
    @test df[:c] == c

    a = [1.0]
    di = Dict([("a", a), ("b", b), ("c", c)])
    @test_throws ArgumentError convert(DataFrame,di)

end
