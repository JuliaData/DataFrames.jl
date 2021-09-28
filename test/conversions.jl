module TestConversions

using Test, DataFrames
using DataStructures: OrderedDict, SortedDict
const ≅ = isequal

@testset "Constructors to Base types" begin
    df = DataFrame()
    @test Matrix(df) isa Matrix{Union{}}
    @test size(Matrix(df)) == (0, 0)
    df[!, :A] = 1:5
    df[!, :B] = 1.0:5.0
    @test isa(Matrix(df), Matrix{Float64})
    @test isa(Matrix{Any}(df), Matrix{Any})
    @test isa(Matrix{Float64}(df), Matrix{Float64})
    @test isa(Array(df), Matrix{Float64})
    @test isa(Array{Any}(df), Matrix{Any})
    @test isa(Array{Float64}(df), Matrix{Float64})

    df = DataFrame()
    df.A = Vector{Union{Float64, Missing}}(1.0:5.0)
    df.B = Vector{Union{Float64, Missing}}(1.0:5.0)
    @test [df.A df.B] == Matrix(df)
    @test Matrix(df) isa Matrix{Union{Float64, Missing}}
    @test [df.A df.B] == Matrix{Any}(df)
    @test Matrix{Any}(df) isa Matrix{Any}
    @test [df.A df.B] == Matrix(df)
    @test Matrix{Int}(df) isa Matrix{Int}

    df[1, 1] = missing
    @test_throws ArgumentError Matrix{Float64}(df)
    na = Matrix{Union{Float64, Missing}}(df)
    naa = Matrix{Any}(df)
    nai = Matrix{Union{Int, Missing}}(df)
    @test isa(na, Matrix{Union{Float64, Missing}})
    @test isa(naa, Matrix{Union{Any, Missing}})
    @test isa(nai, Matrix{Union{Int, Missing}})
    @test na ≅ [df.A df.B]
    @test naa ≅ [df.A df.B]
    @test nai ≅ [df.A df.B]

    df = DataFrame()
    df.A = Vector{Union{Float64, Missing}}(1.0:5.0)
    df.B = Vector{Union{Float64, Missing}}(1.0:5.0)
    @test [df.A df.B] == Array(df)
    @test Array(df) isa Matrix{Union{Float64, Missing}}
    @test [df.A df.B] == Array{Any}(df)
    @test Array{Any}(df) isa Matrix{Any}
    @test [df.A df.B] == Array(df)
    @test Array{Int}(df) isa Matrix{Int}

    df[1, 1] = missing
    @test_throws ArgumentError Array{Float64}(df)
    na = Array{Union{Float64, Missing}}(df)
    naa = Array{Any}(df)
    nai = Array{Union{Int, Missing}}(df)
    @test isa(na, Matrix{Union{Float64, Missing}})
    @test isa(naa, Matrix{Union{Any, Missing}})
    @test isa(nai, Matrix{Union{Int, Missing}})
    @test na ≅ [df.A df.B]
    @test naa ≅ [df.A df.B]
    @test nai ≅ [df.A df.B]


    a = Union{Float64, Missing}[1.0, 2.0]
    b = Union{Float64, Missing}[-0.1, 3]
    c = Union{Float64, Missing}[-3.1, 7]
    di = Dict("a"=>a, "b"=>b, "c"=>c)

    df = DataFrame(di)
    @test isa(df, DataFrame)
    @test names(df) == [x for x in sort(collect(keys(di)))]
    @test df[!, :a] == a
    @test df[!, :b] == b
    @test df[!, :c] == c

    od = OrderedDict("c"=>c, "a"=>a, "b"=>b)
    df = DataFrame(od)
    @test isa(df, DataFrame)
    @test names(df) == [x for x in keys(od)]
    @test df[!, :a] == a
    @test df[!, :b] == b
    @test df[!, :c] == c

    sd = SortedDict("c"=>c, "a"=>a, "b"=>b)
    df = DataFrame(sd)
    @test isa(df, DataFrame)
    @test names(df) == [x for x in keys(sd)]
    @test df[!, :a] == a
    @test df[!, :b] == b
    @test df[!, :c] == c

    a = [1.0]
    di = Dict("a"=>a, "b"=>b, "c"=>c)
    @test_throws DimensionMismatch DataFrame(di)
end

@testset "conversion tests" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)
    dfv = @view df[2:3, 2:3]
    @test convert(DataFrame, dfv) == df[2:3, 2:3]
    @test convert(DataFrame, dfv) isa DataFrame
    @test DataFrame(dfv) == df[2:3, 2:3]
    @test DataFrame(dfv) isa DataFrame

    dfr = df[2, 2:3]
    @test convert(NamedTuple, dfr) == (b=5, c=8)
    @test convert(NamedTuple, dfr) isa NamedTuple
    @test NamedTuple(dfr) == (b=5, c=8)
    @test NamedTuple(dfr) isa NamedTuple

    gdf = groupby(df, [:b, :c])
    gk = keys(gdf)[2]
    @test convert(NamedTuple, gk) == (b=5, c=8)
    @test convert(NamedTuple, gk) isa NamedTuple
    @test NamedTuple(gk) == (b=5, c=8)
    @test NamedTuple(gk) isa NamedTuple
end

end # module
