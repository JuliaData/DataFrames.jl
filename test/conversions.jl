module TestConversions

using Test, DataFrames, InteractiveUtils
using DataStructures: OrderedDict, SortedDict
const ≅ = isequal

df = DataFrame()
df[:A] = 1:5
df[:B] = [:A, :B, :C, :D, :E]
@test isa(convert(Array, df), Matrix{Any})
@test isa(convert(Array{Any}, df), Matrix{Any})

df = DataFrame()
df[:A] = 1:5
df[:B] = 1.0:5.0
@test isa(convert(Array, df), Matrix{Float64})
@test isa(convert(Array{Any}, df), Matrix{Any})
@test isa(convert(Array{Float64}, df), Matrix{Float64})
@test isa(Matrix(df), Matrix{Float64})
@test isa(Matrix{Any}(df), Matrix{Any})
@test isa(Matrix{Float64}(df), Matrix{Float64})

df = DataFrame()
df[:A] = Vector{Union{Float64, Missing}}(1.0:5.0)
df[:B] = Vector{Union{Float64, Missing}}(1.0:5.0)
a = convert(Array, df)
aa = convert(Array{Any}, df)
ai = convert(Array{Int}, df)
@test isa(a, Matrix{Union{Float64, Missing}})
@test a == convert(Array, convert(Array{Union{Float64, Missing}}, df))
@test a == convert(Matrix, df)
@test a == Matrix(df)
@test isa(aa, Matrix{Any})
@test aa == convert(Matrix{Any}, df)
@test aa == Matrix{Any}(df)
@test isa(ai, Matrix{Int})
@test ai == convert(Matrix{Int}, df)
@test ai == Matrix{Int}(df)

df[1,1] = missing
@test_throws ErrorException convert(Array{Float64}, df)
na = convert(Array{Union{Float64, Missing}}, df)
naa = convert(Array{Any}, df)
nai = convert(Array{Union{Int, Missing}}, df)
@test isa(na, Matrix{Union{Float64, Missing}})
@test na ≅ convert(Matrix, df)
@test na ≅ Matrix(df)
@test isa(naa, Matrix{Union{Any, Missing}})
@test naa ≅ convert(Matrix{Any}, df)
@test naa ≅ Matrix{Any}(df)
@test isa(nai, Matrix{Union{Int, Missing}})
@test nai ≅ convert(Matrix{Union{Int, Missing}}, df)
@test nai ≅ Matrix{Union{Int, Missing}}(df)

a = Union{Float64, Missing}[1.0,2.0]
b = Union{Float64, Missing}[-0.1,3]
c = Union{Float64, Missing}[-3.1,7]
di = Dict("a"=>a, "b"=>b, "c"=>c)

df = convert(DataFrame, di)
@test isa(df, DataFrame)
@test names(df) == [Symbol(x) for x in sort(collect(keys(di)))]
@test df[:a] == a
@test df[:b] == b
@test df[:c] == c

od = OrderedDict("c"=>c, "a"=>a, "b"=>b)
df = convert(DataFrame,od)
@test isa(df, DataFrame)
@test names(df) == [Symbol(x) for x in keys(od)]
@test df[:a] == a
@test df[:b] == b
@test df[:c] == c

sd = SortedDict("c"=>c, "a"=>a, "b"=>b)
df = convert(DataFrame,sd)
@test isa(df, DataFrame)
@test names(df) == [Symbol(x) for x in keys(sd)]
@test df[:a] == a
@test df[:b] == b
@test df[:c] == c

a = [1.0]
di = Dict("a"=>a, "b"=>b, "c"=>c)
@test_throws DimensionMismatch convert(DataFrame,di)

end # module
