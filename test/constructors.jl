module TestConstructors

using Test, DataFrames
using DataFrames: Index, _columns, index
const ≅ = isequal

#
# DataFrame
#
@testset "constructors" begin
    df = DataFrame()
    @inferred DataFrame()

    @test isempty(_columns(df))
    @test _columns(df) isa Vector{AbstractVector}
    @test index(df) == Index()

    df = DataFrame(Any[CategoricalVector{Union{Float64, Missing}}(zeros(3)),
                   CategoricalVector{Union{Float64, Missing}}(ones(3))],
                   Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    @test df == DataFrame([CategoricalVector{Union{Float64, Missing}}(zeros(3)),
                           CategoricalVector{Union{Float64, Missing}}(ones(3))])
    @test df == DataFrame([CategoricalVector{Union{Float64, Missing}}(zeros(3)),
                           CategoricalVector{Union{Float64, Missing}}(ones(3))], [:x1, :x2])
    @test df == DataFrame(Any[CategoricalVector{Union{Float64, Missing}}(zeros(3)),
                              CategoricalVector{Union{Float64, Missing}}(ones(3))])
    @test df == DataFrame(Any[CategoricalVector{Union{Float64, Missing}}(zeros(3)),
                              CategoricalVector{Union{Float64, Missing}}(ones(3))], [:x1, :x2])
    @test df == DataFrame(AbstractVector[CategoricalVector{Union{Float64, Missing}}(zeros(3)),
                                         CategoricalVector{Union{Float64, Missing}}(ones(3))], [:x1, :x2])
    @test df == DataFrame((CategoricalVector{Union{Float64, Missing}}(zeros(3)),
                           CategoricalVector{Union{Float64, Missing}}(ones(3))))
    @test df == DataFrame((CategoricalVector{Union{Float64, Missing}}(zeros(3)),
                           CategoricalVector{Union{Float64, Missing}}(ones(3))), (:x1, :x2))
    @test df == DataFrame(x1 = Union{Int, Missing}[0.0, 0.0, 0.0],
                          x2 = Union{Int, Missing}[1.0, 1.0, 1.0])
    @test df == DataFrame([:x1=>Union{Int, Missing}[0.0, 0.0, 0.0],
                           :x2=>Union{Int, Missing}[1.0, 1.0, 1.0]])
    @test df == DataFrame((:x1=>Union{Int, Missing}[0.0, 0.0, 0.0],
                           :x2=>Union{Int, Missing}[1.0, 1.0, 1.0]))

    @test DataFrame([1:3, 1:3]) == DataFrame(Any[1:3, 1:3]) ==
          DataFrame(UnitRange[1:3, 1:3]) == DataFrame(AbstractVector[1:3, 1:3]) ==
          DataFrame([[1,2,3], [1,2,3]]) == DataFrame(Any[[1,2,3], [1,2,3]]) ==
          DataFrame(([1,2,3], [1,2,3])) == DataFrame((1:3, 1:3)) ==
          DataFrame((1:3, [1,2,3])) == DataFrame([1:3, [1,2,3]])
          DataFrame((:x1=>1:3, :x2=>[1,2,3])) == DataFrame([:x1=>1:3, :x2=>[1,2,3]])

    @inferred DataFrame([1:3, 1:3])
    @inferred DataFrame((1:3, 1:3))
    @inferred DataFrame([1:3, 1:3], [:a, :b])
    @inferred DataFrame((1:3, 1:3), (:a, :b))

    @inferred DataFrame((:x1=>1:3, :x2=>[1,2,3]))
    @inferred DataFrame([:x1=>1:3, :x2=>[1,2,3]])

    @test df !== DataFrame(df)
    @test df == DataFrame(df)

    df2 = convert(DataFrame, Union{Float64, Missing}[0.0 1.0;
                                                     0.0 1.0;
                                                     0.0 1.0])
    names!(df2, [:x1, :x2])
    @test df[:x1] == df2[:x1]
    @test df[:x2] == df2[:x2]

    df2 = DataFrame([0.0 1.0;
                     0.0 1.0;
                     0.0 1.0])
    names!(df2, [:x1, :x2])
    @test df[:x1] == df2[:x1]
    @test df[:x2] == df2[:x2]

    df2 = DataFrame([0.0 1.0;
                     0.0 1.0;
                     0.0 1.0], [:a, :b])
    names!(df2, [:a, :b])
    @test df[:x1] == df2[:a]
    @test df[:x2] == df2[:b]

    @test df == DataFrame(x1 = Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Missing}[1.0, 1.0, 1.0])
    @test df == DataFrame(x1 = Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Missing}[1.0, 1.0, 1.0],
                          x3 = Union{Float64, Missing}[2.0, 2.0, 2.0])[[:x1, :x2]]

    df = DataFrame(Union{Int, Missing}, 2, 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Union{Int, Missing}, Union{Int, Missing}]

    df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}], [:x1, :x2], 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Union{Int, Missing}, Union{Float64, Missing}]

    @test df ≅ DataFrame([Union{Int, Missing}, Union{Float64, Missing}], 2)

    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0:0, :)
    @test_throws ArgumentError SubDataFrame(DataFrame(A=1), 0, :)
    @test_throws BoundsError DataFrame(A=1)[0, :]
    @test_throws BoundsError DataFrame(A=1)[0, 1:1]
    @test SubDataFrame(DataFrame(A=1), 1:1, :) == DataFrame(A=1)
    @test SubDataFrame(DataFrame(A=1:10), 1:4, :) == DataFrame(A=1:4)
    @test view(SubDataFrame(DataFrame(A=1:10), 1:4, :), 2:2, :) == DataFrame(A=2)
    @test view(SubDataFrame(DataFrame(A=1:10), 1:4, :), [true, true, false, false], :) == DataFrame(A=1:2)

    @test DataFrame(a=1, b=1:2) == DataFrame(a=[1,1], b=[1,2])
end

@testset "DataFrame keyword argument constructor" begin
    x = [1,2,3]
    y = [4,5,6]
    df = DataFrame(x=x, y=y)
    @test size(df) == (3, 2)
    @test names(df) == [:x, :y]
    @test df.x == x
    @test df.y == y
    @test df.x !== x
    @test df.y !== y
    df = DataFrame(x=x, y=y, copycols=true)
    @test size(df) == (3, 2)
    @test names(df) == [:x, :y]
    @test df.x == x
    @test df.y == y
    @test df.x !== x
    @test df.y !== y
    df = DataFrame(x=x, y=y, copycols=false)
    @test size(df) == (3, 2)
    @test names(df) == [:x, :y]
    @test df.x === x
    @test df.y === y
    @test_throws ArgumentError DataFrame(x=x, y=y, copycols=1)
end

@testset "DataFrame constructor" begin
    df1 = DataFrame(x=1:3, y=1:3)

    df2 = DataFrame(df1)
    df3 = copy(df1)
    @test df1 == df2 == df3
    @test df1.x !== df2.x
    @test df1.x !== df3.x
    @test df1.y !== df2.y
    @test df1.y !== df3.y

    df2 = DataFrame(df1, copycols=false)
    df3 = copy(df1, copycols=false)
    @test df1 == df2 == df3
    @test df1.x === df2.x
    @test df1.x === df3.x
    @test df1.y === df2.y
    @test df1.y === df3.y

    df1 = view(df1, :, :)
    df2 = DataFrame(df1)
    df3 = copy(df1)
    @test df1 == df2 == df3
    @test df1.x !== df2.x
    @test df1.x !== df3.x
    @test df1.y !== df2.y
    @test df1.y !== df3.y

    df2 = DataFrame(df1, copycols=false)
    @test df1 == df2
    @test df1.x === df2.x
    @test df1.y === df2.y
end

@testset "pair constructor" begin
    df = DataFrame(:x1 => zeros(3), :x2 => ones(3))
    @inferred DataFrame(:x1 => zeros(3), :x2 => ones(3))
    @test size(df, 1) == 3
    @test size(df, 2) == 2
    @test isequal(df, DataFrame(x1 = [0.0, 0.0, 0.0], x2 = [1.0, 1.0, 1.0]))

    df = DataFrame(:type => [], :begin => [])
    @test names(df) == [:type, :begin]

    a=[1,2,3]
    df = DataFrame(:a=>a, :b=>1, :c=>1:3)
    @test names(df) == [:a, :b, :c]
    @test df.a == a
    @test df.a !== a
    df = DataFrame(:a=>a, :b=>1, :c=>1:3, copycols=false)
    @test names(df) == [:a, :b, :c]
    @test df.a === a
end

@testset "associative" begin
    df = DataFrame(Dict(:A => 1:3, :B => 4:6))
    @inferred DataFrame(Dict(:A => 1:3, :B => 4:6))
    @test df == DataFrame(A = 1:3, B = 4:6)
    @test eltypes(df) == [Int, Int]

    a=[1,2,3]
    df = DataFrame(Dict(:a=>a, :b=>1, :c=>1:3))
    @test names(df) == [:a, :b, :c]
    @test df.a == a
    @test df.a !== a
    df = DataFrame(Dict(:a=>a, :b=>1, :c=>1:3), copycols=false)
    @test names(df) == [:a, :b, :c]
    @test df.a === a
end

@testset "vector constructors" begin
    x = [1,2,3]
    y = [1,2,3]

    df = DataFrame([x, y])
    @test names(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], copycols=true)
    @test names(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], copycols=false)
    @test names(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame([x, y], [:x1, :x2])
    @test names(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], [:x1, :x2], copycols=true)
    @test names(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], [:x1, :x2], copycols=false)
    @test names(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame((x, y))
    @test names(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame((x, y), copycols=true)
    @test names(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame((x, y), copycols=false)
    @test names(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame((x, y), (:x1, :x2))
    @test names(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame((x, y), (:x1, :x2), copycols=true)
    @test names(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame((x, y), (:x1, :x2), copycols=false)
    @test names(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y
end

@testset "recyclers" begin
    @test DataFrame(a = 1:5, b = 1) == DataFrame(a = collect(1:5), b = fill(1, 5))
    @test DataFrame(a = 1, b = 1:5) == DataFrame(a = fill(1, 5), b = collect(1:5))
end

@testset "constructor errors" begin
    @test_throws DimensionMismatch DataFrame(a=1, b=[])
    @test_throws DimensionMismatch DataFrame(Any[collect(1:10)], DataFrames.Index([:A, :B]))
    @test_throws DimensionMismatch DataFrame(A = rand(2,2))
    @test_throws DimensionMismatch DataFrame(A = rand(2,1))
    @test_throws ArgumentError DataFrame([1, 2, 3])
end

@testset "column types" begin
    df = DataFrame(A = 1:3, B = 2:4, C = 3:5)
    answer = [Array{Int,1}, Array{Int,1}, Array{Int,1}]
    @test map(typeof, eachcol(df)) == answer
    df[:D] = [4, 5, missing]
    push!(answer, Vector{Union{Int, Missing}})
    @test map(typeof, eachcol(df)) == answer
    df[:E] = 'c'
    push!(answer, Vector{Char})
    @test map(typeof, eachcol(df)) == answer
end

@testset "categorical constructor" begin
    df = DataFrame([Int, String], [:a, :b], [false, true], 3)
    @test !(df[:a] isa CategoricalVector)
    @test df[:b] isa CategoricalVector
    @test_throws DimensionMismatch DataFrame([Int, String], [:a, :b], [true], 3)
end

end # module
