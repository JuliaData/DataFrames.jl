module TestConversions
    using Base.Test
    using DataTables
    using DataStructures: OrderedDict, SortedDict

    dt = DataTable()
    dt[:A] = 1:5
    dt[:B] = [:A, :B, :C, :D, :E]
    @test isa(convert(Array, dt), Matrix{Any})
    @test convert(Array, dt) == convert(Array, convert(NullableArray, dt))
    @test isa(convert(Array{Any}, dt), Matrix{Any})

    dt = DataTable()
    dt[:A] = 1:5
    dt[:B] = 1.0:5.0
    # Fails on Julia 0.4 since promote_type(Nullable{Int}, Nullable{Float64}) gives Nullable{T}
    if VERSION >= v"0.5.0-dev"
        @test isa(convert(Array, dt), Matrix{Float64})
    end
    @test convert(Array, dt) == convert(Array, convert(NullableArray, dt))
    @test isa(convert(Array{Any}, dt), Matrix{Any})
    @test isa(convert(Array{Float64}, dt), Matrix{Float64})

    dt = DataTable()
    dt[:A] = NullableArray(1.0:5.0)
    dt[:B] = NullableArray(1.0:5.0)
    a = convert(Array, dt)
    aa = convert(Array{Any}, dt)
    ai = convert(Array{Int}, dt)
    @test isa(a, Matrix{Float64})
    @test a == convert(Array, convert(NullableArray, dt))
    @test a == convert(Matrix, dt)
    @test isa(aa, Matrix{Any})
    @test aa == convert(Matrix{Any}, dt)
    @test isa(ai, Matrix{Int})
    @test ai == convert(Matrix{Int}, dt)

    dt[1,1] = Nullable()
    @test_throws ErrorException convert(Array, dt)
    na = convert(NullableArray, dt)
    naa = convert(NullableArray{Any}, dt)
    nai = convert(NullableArray{Int}, dt)
    @test isa(na, NullableMatrix{Float64})
    @test isequal(na, convert(NullableMatrix, dt))
    @test isa(naa, NullableMatrix{Any})
    @test isequal(naa, convert(NullableMatrix{Any}, dt))
    @test isa(nai, NullableMatrix{Int})
    @test isequal(nai, convert(NullableMatrix{Int}, dt))

    a = NullableArray([1.0,2.0])
    b = NullableArray([-0.1,3])
    c = NullableArray([-3.1,7])
    di = Dict("a"=>a, "b"=>b, "c"=>c)

    dt = convert(DataTable,di)
    @test isa(dt,DataTable)
    @test names(dt) == Symbol[x for x in sort(collect(keys(di)))]
    @test isequal(dt[:a], NullableArray(a))
    @test isequal(dt[:b], NullableArray(b))
    @test isequal(dt[:c], NullableArray(c))

    od = OrderedDict("c"=>c, "a"=>a, "b"=>b)
    dt = convert(DataTable,od)
    @test isa(dt, DataTable)
    @test names(dt) == Symbol[x for x in keys(od)]
    @test isequal(dt[:a], NullableArray(a))
    @test isequal(dt[:b], NullableArray(b))
    @test isequal(dt[:c], NullableArray(c))

    sd = SortedDict("c"=>c, "a"=>a, "b"=>b)
    dt = convert(DataTable,sd)
    @test isa(dt, DataTable)
    @test names(dt) == Symbol[x for x in keys(sd)]
    @test isequal(dt[:a], NullableArray(a))
    @test isequal(dt[:b], NullableArray(b))
    @test isequal(dt[:c], NullableArray(c))

    a = [1.0]
    di = Dict("a"=>a, "b"=>b, "c"=>c)
    @test_throws DimensionMismatch convert(DataTable,di)

end
