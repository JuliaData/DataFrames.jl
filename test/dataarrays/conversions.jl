module TestConversions
    using Base.Test
    using DataFrames
    using DataArrays
    using DataStructures: OrderedDict, SortedDict

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = [:A, :B, :C, :D, :E]
    @test isa(convert(Array, df), Matrix{Any})
    @test isa(convert(Array{Any}, df), Matrix{Any})

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = 1.0:5.0
    @test isa(convert(Array, df), Matrix{Union{Float64, Null}})
    @test isa(convert(Array{Any}, df), Matrix{Any})
    @test isa(convert(Array{Float64}, df), Matrix{Float64})

    df = DataFrame()
    df[:A] = 1.0:5.0
    df[:B] = 1.0:5.0
    a = convert(Array, df)
    aa = convert(Array{Any}, df)
    ai = convert(Array{Int}, df)
    @test isa(a, Matrix{Union{Float64, Null}})
    @test a == convert(Matrix, df)
    @test isa(aa, Matrix{Any})
    @test aa == convert(Matrix{Any}, df)
    @test isa(ai, Matrix{Int})
    @test ai == convert(Matrix{Int}, df)

    df[1,1] = NA
    da = convert(Array, df)
    daa = convert(Array{Any}, df)
    dai = convert(Array{Union{Int, Null}}, df)
    @test isa(da, Matrix{Union{Float64, Null}})
    @test isequal(da, convert(Matrix, df))
    @test isa(daa, Matrix{Any})
    @test isequal(daa, convert(Matrix{Any}, df))
    @test isa(dai, Matrix{Union{Int, Null}})
    @test isequal(dai, convert(Matrix{Union{Int, Null}}, df))

    a = [1.0,2.0]
    b = [-0.1,3]
    c = [-3.1,7]
    di = Dict("a"=>a, "b"=>b, "c"=>c)

    df = convert(DataFrame,di)
    @test isa(df,DataFrame)
    @test names(df) == Symbol[x for x in sort(collect(keys(di)))]
    @test df[:a] == a
    @test df[:b] == b
    @test df[:c] == c

    od = OrderedDict("c"=>c, "a"=>a, "b"=>b)
    df = convert(DataFrame,od)
    @test isa(df, DataFrame)
    @test names(df) == Symbol[x for x in keys(od)]
    @test df[:a] == a
    @test df[:b] == b
    @test df[:c] == c

    sd = SortedDict("c"=>c, "a"=>a, "b"=>b)
    df = convert(DataFrame,sd)
    @test isa(df, DataFrame)
    @test names(df) == Symbol[x for x in keys(sd)]
    @test df[:a] == a
    @test df[:b] == b
    @test df[:c] == c

    a = [1.0]
    di = Dict("a"=>a, "b"=>b, "c"=>c)
    @test_throws DimensionMismatch convert(DataFrame,di)

end
