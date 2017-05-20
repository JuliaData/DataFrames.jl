module TestConversions
    using Base.Test, DataTables
    using DataStructures: OrderedDict, SortedDict

    dt = DataTable()
    dt[:A] = 1:5
    dt[:B] = [:A, :B, :C, :D, :E]
    @test isa(convert(Array, dt), Matrix{Any})
    @test isa(convert(Array{Any}, dt), Matrix{Any})

    dt = DataTable()
    dt[:A] = 1:5
    dt[:B] = 1.0:5.0
    @test isa(convert(Array, dt), Matrix{Float64})
    @test isa(convert(Array{Any}, dt), Matrix{Any})
    @test isa(convert(Array{Float64}, dt), Matrix{Float64})

    dt = DataTable()
    dt[:A] = Vector{Union{Float64, Null}}(1.0:5.0)
    dt[:B] = Vector{Union{Float64, Null}}(1.0:5.0)
    a = convert(Array, dt)
    aa = convert(Array{Any}, dt)
    ai = convert(Array{Int}, dt)
    @test isa(a, Matrix{Union{Float64, Null}})
    @test a == convert(Array, convert(Array{Union{Float64, Null}}, dt))
    @test a == convert(Matrix, dt)
    @test isa(aa, Matrix{Any})
    @test aa == convert(Matrix{Any}, dt)
    @test isa(ai, Matrix{Int})
    @test ai == convert(Matrix{Int}, dt)

    dt[1,1] = null
    @test_throws ErrorException convert(Array{Float64}, dt)
    na = convert(Array{Union{Float64, Null}}, dt)
    naa = convert(Array{Union{Any, Null}}, dt)
    nai = convert(Array{Union{Int, Null}}, dt)
    @test isa(na, Matrix{Union{Float64, Null}})
    @test na == convert(Matrix, dt)
    @test isa(naa, Matrix{Union{Any, Null}})
    @test naa == convert(Matrix{Union{Any, Null}}, dt)
    @test isa(nai, Matrix{Union{Int, Null}})
    @test nai == convert(Matrix{Union{Int, Null}}, dt)

    a = Union{Float64, Null}[1.0,2.0]
    b = Union{Float64, Null}[-0.1,3]
    c = Union{Float64, Null}[-3.1,7]
    di = Dict("a"=>a, "b"=>b, "c"=>c)

    dt = convert(DataTable, di)
    @test isa(dt, DataTable)
    @test names(dt) == Symbol[x for x in sort(collect(keys(di)))]
    @test dt[:a] == a
    @test dt[:b] == b
    @test dt[:c] == c

    od = OrderedDict("c"=>c, "a"=>a, "b"=>b)
    dt = convert(DataTable,od)
    @test isa(dt, DataTable)
    @test names(dt) == Symbol[x for x in keys(od)]
    @test dt[:a] == a
    @test dt[:b] == b
    @test dt[:c] == c

    sd = SortedDict("c"=>c, "a"=>a, "b"=>b)
    dt = convert(DataTable,sd)
    @test isa(dt, DataTable)
    @test names(dt) == Symbol[x for x in keys(sd)]
    @test dt[:a] == a
    @test dt[:b] == b
    @test dt[:c] == c

    a = [1.0]
    di = Dict("a"=>a, "b"=>b, "c"=>c)
    @test_throws DimensionMismatch convert(DataTable,di)

end
