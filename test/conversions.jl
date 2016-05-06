module TestConversions
    using Base.Test
    using DataFrames
    using DataStructures: OrderedDict, SortedDict

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = [:A, :B, :C, :D, :E]
    @test isa(convert(Array, df), Matrix{Any})
    @test convert(Array, df) == convert(Array, convert(NullableArray, df))
    @test isa(convert(Array{Any}, df), Matrix{Any})

    df = DataFrame()
    df[:A] = 1:5
    df[:B] = 1.0:5.0
    @test isa(convert(Array, df), Matrix{Float64})
    @test convert(Array, df) == convert(Array, convert(NullableArray, df))
    @test isa(convert(Array{Any}, df), Matrix{Any})
    @test isa(convert(Array{Float64}, df), Matrix{Float64})

    df = DataFrame()
    df[:A] = 1.0:5.0
    df[:B] = 1.0:5.0
    a = convert(Array, df)
    aa = convert(Array{Any}, df)
    ai = convert(Array{Int}, df)
    @test isa(a, Matrix{Float64})
    @test a == convert(Array, convert(NullableArray, df))
    @test a == convert(Matrix, df)
    @test isa(aa, Matrix{Any})
    @test aa == convert(Matrix{Any}, df)
    @test isa(ai, Matrix{Int})
    @test ai == convert(Matrix{Int}, df)

    df[1,1] = Nullable()
    @test_throws ErrorException convert(Array, df)
    na = convert(NullableArray, df)
    naa = convert(NullableArray{Any}, df)
    nai = convert(NullableArray{Int}, df)
    @test isa(na, NullableMatrix{Float64})
    @test isequal(na, convert(NullableMatrix, df))
    @test isa(naa, NullableMatrix{Any})
    @test isequal(naa, convert(NullableMatrix{Any}, df))
    @test isa(nai, NullableMatrix{Int})
    @test isequal(nai, convert(NullableMatrix{Int}, df))

    a = [1.0,2.0]
    b = [-0.1,3]
    c = [-3.1,7]
    di = Dict("a"=>a, "b"=>b, "c"=>c)

    df = convert(DataFrame,di)
    @test isa(df,DataFrame)
    @test names(df) == Symbol[x for x in sort(collect(keys(di)))]
    @test get(df[:a] == NullableArray(a))
    @test get(df[:b] == NullableArray(b))
    @test get(df[:c] == NullableArray(c))

    od = OrderedDict("c"=>c, "a"=>a, "b"=>b)
    df = convert(DataFrame,od)
    @test isa(df, DataFrame)
    @test names(df) == Symbol[x for x in keys(od)]
    @test get(df[:a] == NullableArray(a))
    @test get(df[:b] == NullableArray(b))
    @test get(df[:c] == NullableArray(c))

    sd = SortedDict("c"=>c, "a"=>a, "b"=>b)
    df = convert(DataFrame,sd)
    @test isa(df, DataFrame)
    @test names(df) == Symbol[x for x in keys(sd)]
    @test get(df[:a] == NullableArray(a))
    @test get(df[:b] == NullableArray(b))
    @test get(df[:c] == NullableArray(c))

    a = [1.0]
    di = Dict("a"=>a, "b"=>b, "c"=>c)
    @test_throws ArgumentError convert(DataFrame,di)

end
