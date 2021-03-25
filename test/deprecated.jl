module TestDeprecated

using Test, DataFrames
using DataStructures: OrderedDict, SortedDict

const ≅ = isequal

@testset "by and aggregate" begin
    @test_throws ArgumentError by()
    @test_throws ArgumentError aggregate()
end

@testset "deprecated broadcasting assignment" begin
    df = DataFrame(a=1:4, b=1, c=2)
    df.a .= 'a':'d'
    @test df == DataFrame(a=97:100, b=1, c=2)
    dfv = view(df, 2:3, 2:3)
    dfv.b .= 0
    @test df.b == [1, 0, 0, 1]
end

@testset "All indexing" begin
    df = DataFrame(a=1, b=2, c=3)

    @test select(df, All(1, 2)) == df[:, 1:2]
    @test select(df, All(1, :b)) == df[:, 1:2]
    @test select(df, All(:a, 2)) == df[:, 1:2]
    @test select(df, All(:a, :b)) == df[:, 1:2]
    @test select(df, All(2, 1)) == df[:, [2, 1]]
    @test select(df, All(:b, 1)) == df[:, [2, 1]]
    @test select(df, All(2, :a)) == df[:, [2, 1]]
    @test select(df, All(:b, :a)) == df[:, [2, 1]]

    @test df[:, All(1, 2)] == df[:, 1:2]
    @test df[:, All(1, :b)] == df[:, 1:2]
    @test df[:, All(:a, 2)] == df[:, 1:2]
    @test df[:, All(:a, :b)] == df[:, 1:2]
    @test df[:, All(2, 1)] == df[:, [2, 1]]
    @test df[:, All(:b, 1)] == df[:, [2, 1]]
    @test df[:, All(2, :a)] == df[:, [2, 1]]
    @test df[:, All(:b, :a)] == df[:, [2, 1]]

    @test df[:, All(1, 1, 2)] == df[:, 1:2]
    @test df[:, All(:a, 1, :b)] == df[:, 1:2]
    @test df[:, All(:a, 2, :b)] == df[:, 1:2]
    @test df[:, All(:a, :b, 2)] == df[:, 1:2]
    @test df[:, All(2, 1, :a)] == df[:, [2, 1]]

    @test select(df, All(1, "b")) == df[:, 1:2]
    @test select(df, All("a", 2)) == df[:, 1:2]
    @test select(df, All("a", "b")) == df[:, 1:2]
    @test select(df, All("b", 1)) == df[:, [2, 1]]
    @test select(df, All(2, "a")) == df[:, [2, 1]]
    @test select(df, All("b", "a")) == df[:, [2, 1]]

    @test df[:, All(1, "b")] == df[:, 1:2]
    @test df[:, All("a", 2)] == df[:, 1:2]
    @test df[:, All("a", "b")] == df[:, 1:2]
    @test df[:, All("b", 1)] == df[:, [2, 1]]
    @test df[:, All(2, "a")] == df[:, [2, 1]]
    @test df[:, All("b", "a")] == df[:, [2, 1]]

    @test df[:, All("a", 1, "b")] == df[:, 1:2]
    @test df[:, All("a", 2, "b")] == df[:, 1:2]
    @test df[:, All("a", "b", 2)] == df[:, 1:2]
    @test df[:, All(2, 1, "a")] == df[:, [2, 1]]

    df = DataFrame(a1=1, a2=2, b1=3, b2=4)
    @test df[:, All(r"a", Not(r"1"))] == df[:, [1, 2, 4]]
    @test df[:, All(Not(r"1"), r"a")] == df[:, [2, 4, 1]]
end

@testset "indicator in joins" begin
    name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
    job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])

    @test outerjoin(name, job, on = :ID, indicator=:source) ≅
          outerjoin(name, job, on = :ID, source=:source)
    @test leftjoin(name, job, on = :ID, indicator=:source) ≅
          leftjoin(name, job, on = :ID, source=:source)
    @test rightjoin(name, job, on = :ID, indicator=:source) ≅
          rightjoin(name, job, on = :ID, source=:source)

    @test_throws ArgumentError outerjoin(name, job, on = :ID,
                                         indicator=:source, source=:source)
    @test_throws ArgumentError leftjoin(name, job, on = :ID,
                                       indicator=:source, source=:source)
    @test_throws ArgumentError rightjoin(name, job, on = :ID,
                                         indicator=:source, source=:source)
end

@testset "map on GroupedDataFrame" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)
    dfv = @view df[1:3, 1:3]
    gdf = groupby(df, :a)
    gdfv = groupby(dfv, :a)

    for x in (gdf, gdfv)
        @test collect(x) == map(identity, x)
    end
end

@testset "new map behavior" begin
    df = DataFrame(g=[1, 2, 3])
    gdf = groupby(df, :g)
    @test map(nrow, gdf) == [1, 1, 1]
end

@testset "Conversion tests" begin
    df = DataFrame()
    df[!, :A] = 1:5
    df[!, :B] = [:A, :B, :C, :D, :E]
    @test isa(convert(Matrix, df), Matrix{Any})
    @test isa(convert(Matrix{Any}, df), Matrix{Any})
    @test isa(convert(Array, df), Matrix{Any})
    @test isa(convert(Array{Any}, df), Matrix{Any})

    df = DataFrame()
    df[!, :A] = 1:5
    df[!, :B] = 1.0:5.0
    @test isa(convert(Matrix, df), Matrix{Float64})
    @test isa(convert(Matrix{Any}, df), Matrix{Any})
    @test isa(convert(Matrix{Float64}, df), Matrix{Float64})
    @test isa(convert(Array, df), Matrix{Float64})
    @test isa(convert(Array{Any}, df), Matrix{Any})
    @test isa(convert(Array{Float64}, df), Matrix{Float64})

    df = DataFrame()
    df[!, :A] = Vector{Union{Float64, Missing}}(1.0:5.0)
    df[!, :B] = Vector{Union{Float64, Missing}}(1.0:5.0)
    a = convert(Matrix, df)
    aa = convert(Matrix{Any}, df)
    ai = convert(Matrix{Int}, df)
    @test isa(a, Matrix{Union{Float64, Missing}})
    @test a == convert(Array, convert(Matrix{Union{Float64, Missing}}, df))
    @test a == convert(Matrix, df)
    @test a == Matrix(df)
    @test isa(aa, Matrix{Any})
    @test aa == convert(Matrix{Any}, df)
    @test aa == Matrix{Any}(df)
    @test isa(ai, Matrix{Int})
    @test ai == convert(Matrix{Int}, df)
    @test ai == Matrix{Int}(df)

    df[1, 1] = missing
    @test_throws ArgumentError convert(Matrix{Float64}, df)
    na = convert(Matrix{Union{Float64, Missing}}, df)
    naa = convert(Matrix{Any}, df)
    nai = convert(Matrix{Union{Int, Missing}}, df)
    @test isa(na, Matrix{Union{Float64, Missing}})
    @test na ≅ convert(Matrix, df)
    @test na ≅ Matrix(df)
    @test isa(naa, Matrix{Union{Any, Missing}})
    @test naa ≅ convert(Matrix{Any}, df)
    @test naa ≅ Matrix{Any}(df)
    @test isa(nai, Matrix{Union{Int, Missing}})
    @test nai ≅ convert(Matrix{Union{Int, Missing}}, df)
    @test nai ≅ Matrix{Union{Int, Missing}}(df)

    df = DataFrame()
    df[!, :A] = Vector{Union{Float64, Missing}}(1.0:5.0)
    df[!, :B] = Vector{Union{Float64, Missing}}(1.0:5.0)
    a = convert(Array, df)
    aa = convert(Array{Any}, df)
    ai = convert(Array{Int}, df)
    @test isa(a, Matrix{Union{Float64, Missing}})
    @test a == convert(Array, convert(Array{Union{Float64, Missing}}, df))
    @test a == convert(Array, df)
    @test a == Array(df)
    @test isa(aa, Matrix{Any})
    @test aa == convert(Array{Any}, df)
    @test aa == Array{Any}(df)
    @test isa(ai, Matrix{Int})
    @test ai == convert(Array{Int}, df)
    @test ai == Array{Int}(df)

    df[1, 1] = missing
    @test_throws ArgumentError convert(Array{Float64}, df)
    na = convert(Array{Union{Float64, Missing}}, df)
    naa = convert(Array{Any}, df)
    nai = convert(Array{Union{Int, Missing}}, df)
    @test isa(na, Matrix{Union{Float64, Missing}})
    @test na ≅ convert(Array, df)
    @test na ≅ Array(df)
    @test isa(naa, Matrix{Union{Any, Missing}})
    @test naa ≅ convert(Array{Any}, df)
    @test naa ≅ Array{Any}(df)
    @test isa(nai, Matrix{Union{Int, Missing}})
    @test nai ≅ convert(Array{Union{Int, Missing}}, df)
    @test nai ≅ Array{Union{Int, Missing}}(df)

    a = Union{Float64, Missing}[1.0, 2.0]
    b = Union{Float64, Missing}[-0.1, 3]
    c = Union{Float64, Missing}[-3.1, 7]
    di = Dict("a"=>a, "b"=>b, "c"=>c)

    df = convert(DataFrame, di)
    @test isa(df, DataFrame)
    @test names(df) == [x for x in sort(collect(keys(di)))]
    @test df[!, :a] == a
    @test df[!, :b] == b
    @test df[!, :c] == c

    od = OrderedDict("c"=>c, "a"=>a, "b"=>b)
    df = convert(DataFrame, od)
    @test isa(df, DataFrame)
    @test names(df) == [x for x in keys(od)]
    @test df[!, :a] == a
    @test df[!, :b] == b
    @test df[!, :c] == c

    sd = SortedDict("c"=>c, "a"=>a, "b"=>b)
    df = convert(DataFrame, sd)
    @test isa(df, DataFrame)
    @test names(df) == [x for x in keys(sd)]
    @test df[!, :a] == a
    @test df[!, :b] == b
    @test df[!, :c] == c

    a = [1.0]
    di = Dict("a"=>a, "b"=>b, "c"=>c)
    @test_throws DimensionMismatch convert(DataFrame, di)
end

@testset "deprecated conversion for DataFrameRow and GroupKey" begin
    df = DataFrame(a=1)
    dfr = df[1, :]
    key = keys(groupby(df, :a))[1]

    @test convert(Vector, dfr) == Vector(dfr) == [1]
    @test convert(Vector{Any}, dfr) == Vector{Any}(dfr) == Any[1]
    @test convert(Array, dfr) == Vector(dfr) == [1]
    @test convert(Array{Any}, dfr) == Vector{Any}(dfr) == Any[1]
    @test convert(Tuple, dfr) == Tuple(dfr) == (1,)
    @test convert(Vector, key) == Vector(key) == [1]
    @test convert(Vector{Any}, key) == Vector{Any}(key) == Any[1]
    @test convert(Array, key) == Vector(key) == [1]
    @test convert(Array{Any}, key) == Vector{Any}(key) == Any[1]
    @test convert(Tuple, key) == Tuple(key) == (1,)

    @test convert(Vector, dfr) isa Vector{Int}
    @test convert(Vector{Any}, dfr) isa Vector{Any}
    @test convert(Array, dfr) isa Vector{Int}
    @test convert(Array{Any}, dfr) isa Vector{Any}
    @test convert(Tuple, dfr) isa Tuple{Int}
    @test convert(Vector, key) isa Vector{Int}
    @test convert(Vector{Any}, key) isa Vector{Any}
    @test convert(Array, key) isa Vector{Int}
    @test convert(Array{Any}, key) isa Vector{Any}
    @test convert(Tuple, key) isa Tuple{Int}
end

@testset "DataFrameRow convert" begin
    df = DataFrame(a=[1, missing, missing], b=[2.0, 3.0, 0.0])
    dfr = DataFrameRow(df, 1, :)
    @test convert(Vector, dfr)::Vector{Union{Float64, Missing}} == [1.0, 2.0]
    @test convert(Vector{Int}, dfr)::Vector{Int} == [1, 2]
    @test convert(Array, dfr)::Vector{Union{Float64, Missing}} == [1.0, 2.0]
    @test convert(Array{Int}, dfr)::Vector{Int} == [1, 2]

    dfr = DataFrame(a=1, b=2)[1, :]
    dfr2 = DataFrame(c=3, d=4)[1, :]
    @test convert(Tuple, dfr) == (1, 2)
end

end # module
