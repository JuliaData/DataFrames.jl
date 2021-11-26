module TestConstructors

using Test, DataFrames, CategoricalArrays, DataStructures
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
    @test size(DataFrame(copycols=false)) == (0, 0)

    vecvec = [CategoricalVector{Union{Float64, Missing}}(zeros(3)),
              CategoricalVector{Union{Float64, Missing}}(ones(3))]

    df = DataFrame(collect(Any, vecvec), Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    df2 = DataFrame(collect(Any, vecvec), Index([:x1, :x2]), copycols=false)
    @test size(df2, 1) == 3
    @test size(df2, 2) == 2
    @test df2.x1 === vecvec[1]
    @test df2.x2 === vecvec[2]

    @test_throws ArgumentError DataFrame([[1, 2]], :autos)
    @test_throws ArgumentError DataFrame([1 2], :autos)

    for copycolsarg in (true, false)
        @test df == DataFrame(vecvec, :auto, copycols=copycolsarg)
        @test df == DataFrame(collect(Any, vecvec), :auto, copycols=copycolsarg)
        @test df == DataFrame(collect(AbstractVector, vecvec), :auto, copycols=copycolsarg)
        @test df == DataFrame(x1=vecvec[1], x2=vecvec[2], copycols=copycolsarg)

        for cols in ([:x1, :x2], ["x1", "x2"])
            @test df == DataFrame(vecvec, cols, copycols=copycolsarg)
            @test df == DataFrame(collect(Any, vecvec), cols, copycols=copycolsarg)
            @test df == DataFrame(collect(AbstractVector, vecvec), cols, copycols=copycolsarg)
            @test df == DataFrame([col=>vect for (col, vect) in zip(cols, vecvec)], copycols=copycolsarg)
        end
    end

    @test DataFrame([1:3, 1:3], :auto) == DataFrame(Any[1:3, 1:3], :auto) ==
          DataFrame(UnitRange[1:3, 1:3], :auto) == DataFrame(AbstractVector[1:3, 1:3], :auto) ==
          DataFrame([[1, 2, 3], [1, 2, 3]], :auto) == DataFrame(Any[[1, 2, 3], [1, 2, 3]], :auto) ==
          DataFrame([1:3, [1, 2, 3]], :auto)
          DataFrame([:x1=>1:3, :x2=>[1, 2, 3]]) == DataFrame(["x1"=>1:3, "x2"=>[1, 2, 3]])

    @inferred DataFrame([1:3, 1:3], :auto)
    @inferred DataFrame([1:3, 1:3], [:a, :b])
    @inferred DataFrame([1:3, 1:3], ["a", "b"])

    @inferred DataFrame([:x1=>1:3, :x2=>[1, 2, 3]])
    @inferred DataFrame(["x1"=>1:3, "x2"=>[1, 2, 3]])

    @test df !== DataFrame(df)
    @test df == DataFrame(df)

    @test df == DataFrame(x1=Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2=Union{Float64, Missing}[1.0, 1.0, 1.0])
    @test df == DataFrame(x1=Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2=Union{Float64, Missing}[1.0, 1.0, 1.0],
                          x3=Union{Float64, Missing}[2.0, 2.0, 2.0])[:, [:x1, :x2]]
    @test df == DataFrame(x1=Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2=Union{Float64, Missing}[1.0, 1.0, 1.0],
                          x3=Union{Float64, Missing}[2.0, 2.0, 2.0])[:, ["x1", "x2"]]

    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0:0, :)
    @test_throws ArgumentError SubDataFrame(DataFrame(A=1), 0, :)
    @test_throws BoundsError DataFrame(A=1)[0, :]
    @test_throws BoundsError DataFrame(A=1)[0, 1:1]
    @test SubDataFrame(DataFrame(A=1), 1:1, :) == DataFrame(A=1)
    @test SubDataFrame(DataFrame(A=1:10), 1:4, :) == DataFrame(A=1:4)
    @test view(SubDataFrame(DataFrame(A=1:10), 1:4, :), 2:2, :) == DataFrame(A=2)
    @test view(SubDataFrame(DataFrame(A=1:10), 1:4, :), [true, true, false, false], :) == DataFrame(A=1:2)

    @test DataFrame(a=1, b=1:2) == DataFrame(a=[1, 1], b=[1, 2])

    @test_throws ArgumentError DataFrame(makeunique=true)
    @test_throws ArgumentError DataFrame(a=1, makeunique=true)
    @test_throws ArgumentError DataFrame(a=1, makeunique=true, copycols=false)
end

@testset "DataFrame keyword argument constructor" begin
    x = [1, 2, 3]
    y = [4, 5, 6]

    df = DataFrame(x=x, y=y)
    @test size(df) == (3, 2)
    @test propertynames(df) == [:x, :y]
    @test df.x == x
    @test df.y == y
    @test df.x !== x
    @test df.y !== y
    df = DataFrame(x=x, y=y, copycols=true)
    @test size(df) == (3, 2)
    @test propertynames(df) == [:x, :y]
    @test df.x == x
    @test df.y == y
    @test df.x !== x
    @test df.y !== y
    df = DataFrame(x=x, y=y, copycols=false)
    @test size(df) == (3, 2)
    @test propertynames(df) == [:x, :y]
    @test df.x === x
    @test df.y === y
    @test_throws ArgumentError DataFrame(x=x, y=y, copycols=1)

    df = DataFrame(x=x, y=y, copycols=false)
    @test size(df) == (3, 2)
    @test propertynames(df) == [:x, :y]
    @test df.x === x
    @test df.y === y
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
    @test DataFrame(:x1 => zeros(3), :x2 => ones(3)) ==
          DataFrame([:x1 => zeros(3), :x2 => ones(3)]) ==
          DataFrame("x1" => zeros(3), "x2" => ones(3)) ==
          DataFrame("x1" => zeros(3), "x2" => ones(3))

    @inferred DataFrame(:x1 => zeros(3), :x2 => ones(3))
    df = DataFrame([:x1 => zeros(3), :x2 => ones(3)])
    @test size(df, 1) == 3
    @test size(df, 2) == 2
    @test isequal(df, DataFrame(x1=[0.0, 0.0, 0.0], x2=[1.0, 1.0, 1.0]))

    df = DataFrame(:type => [], :begin => [])
    @test propertynames(df) == [:type, :begin]

    a=[1, 2, 3]
    df = DataFrame(:a=>a, :b=>1, :c=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df.a == a
    @test df.a !== a

    df = DataFrame(:a=>a, :b=>1, :c=>1:3, copycols=false)
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame("x1" => zeros(3), "x2" => ones(3))
    @inferred DataFrame("x1" => zeros(3), "x2" => ones(3))
    @test size(df, 1) == 3
    @test size(df, 2) == 2
    @test isequal(df, DataFrame(x1=[0.0, 0.0, 0.0], x2=[1.0, 1.0, 1.0]))

    df = DataFrame("type" => [], "begin" => [])
    @test propertynames(df) == [:type, :begin]

    a=[1, 2, 3]
    df = DataFrame("a"=>a, "b"=>1, "c"=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" == a
    @test df."a" !== a

    df = DataFrame("a"=>a, "b"=>1, "c"=>1:3, copycols=false)
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a

    @test_throws ArgumentError DataFrame(["type" => 1, :begin => 2])
end

@testset "associative" begin
    df = DataFrame(Dict(:A => 1:3, :B => 4:6))
    @inferred DataFrame(Dict(:A => 1:3, :B => 4:6))
    @test df == DataFrame(A=1:3, B=4:6)
    @test eltype.(eachcol(df)) == [Int, Int]

    a=[1, 2, 3]
    df = DataFrame(Dict(:a=>a, :b=>1, :c=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df.a == a
    @test df.a !== a

    df = DataFrame(Dict(:a=>a, :b=>1, :c=>1:3), copycols=false)
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame(Dict("A" => 1:3, "B" => 4:6))
    @inferred DataFrame(Dict("A" => 1:3, "B" => 4:6))
    @test df == DataFrame(A=1:3, B=4:6)
    @test eltype.(eachcol(df)) == [Int, Int]

    a=[1, 2, 3]
    df = DataFrame(Dict("a"=>a, "b"=>1, "c"=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" == a
    @test df."a" !== a
    df = DataFrame(Dict("a"=>a, "b"=>1, "c"=>1:3), copycols=false)
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a
end

@testset "vector constructors" begin
    x = [1, 2, 3]
    y = [1, 2, 3]

    df = DataFrame([x, y], :auto)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], :auto, copycols=true)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], :auto, copycols=false)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame([x, y], [:x1, :x2])
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], [:x1, :x2], copycols=true)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], [:x1, :x2], copycols=false)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame([x, y], ["x1", "x2"])
    @test names(df) == ["x1", "x2"]
    @test df."x1" == x
    @test df."x2" == y
    @test df."x1" !== x
    @test df."x2" !== y
    df = DataFrame([x, y], ["x1", "x2"], copycols=true)
    @test names(df) == ["x1", "x2"]
    @test df."x1" == x
    @test df."x2" == y
    @test df."x1" !== x
    @test df."x2" !== y
    df = DataFrame([x, y], ["x1", "x2"], copycols=false)
    @test names(df) == ["x1", "x2"]
    @test df."x1" === x
    @test df."x2" === y

    n = [:x1, :x2]
    v = AbstractVector[1:3, [1, 2, 3]]
    @test DataFrame(v, n).x1 isa Vector{Int}
    @test v[1] isa AbstractRange

    n = ["x1", "x2"]
    v = AbstractVector[1:3, [1, 2, 3]]
    @test DataFrame(v, n)."x1" isa Vector{Int}
    @test v[1] isa AbstractRange
end

@testset "recyclers" begin
    @test DataFrame(a=1:5, b=1) == DataFrame(a=collect(1:5), b=fill(1, 5))
    @test DataFrame(a=1, b=1:5) == DataFrame(a=fill(1, 5), b=collect(1:5))
    @test size(DataFrame(a=1, b=[])) == (0, 2)
    @test size(DataFrame(a=1, b=[], copycols=false)) == (0, 2)
end

@testset "constructor thrown exceptions" begin
    for copycolsarg in (true, false)
        @test_throws DimensionMismatch DataFrame(Any[collect(1:10)], DataFrames.Index([:A, :B]), copycols=copycolsarg)
        @test_throws ArgumentError DataFrame(A=rand(2, 2), copycols=copycolsarg)
        @test_throws ArgumentError DataFrame(A=rand(2, 1), copycols=copycolsarg)
        @test_throws ArgumentError DataFrame([1, 2, 3], :auto, copycols=copycolsarg)
        @test_throws DimensionMismatch DataFrame(AbstractVector[1:3, [1, 2]], :auto, copycols=copycolsarg)
        @test_throws ArgumentError DataFrame([1:3, 1], [:x1, :x2], copycols=copycolsarg)
        @test_throws ArgumentError DataFrame([1:3, 1], ["x1", "x2"], copycols=copycolsarg)
        @test_throws ErrorException DataFrame([1:3, 1], copycols=copycolsarg)
    end
end

@testset "column types" begin
    df = DataFrame(A=1:3, B=2:4, C=3:5)
    answer = [Array{Int, 1}, Array{Int, 1}, Array{Int, 1}]
    @test map(typeof, eachcol(df)) == answer
    df[!, :D] = [4, 5, missing]
    push!(answer, Vector{Union{Int, Missing}})
    @test map(typeof, eachcol(df)) == answer
    df[!, :E] .= 'c'
    push!(answer, Vector{Char})
    @test map(typeof, eachcol(df)) == answer
end

@testset "expansion of Ref and 0-dimensional arrays" begin
    @test DataFrame(a=Ref(1), b=fill(1)) == DataFrame(a=[1], b=[1])
    @test DataFrame(a=Ref(1), b=fill(1), c=1:3) ==
          DataFrame(a=[1, 1, 1], b=[1, 1, 1], c=1:3)
end

@testset "broadcasting into 0 rows" begin
    for df in [DataFrame(x1=1:0, x2=1), DataFrame(x1=1, x2=1:0)]
        @test size(df) == (0, 2)
        @test df.x1 isa Vector{Int}
        @test df.x2 isa Vector{Int}
    end
end

@testset "Dict constructor corner case" begin
    @test_throws ArgumentError DataFrame(Dict('a' => 1, true => 2))
    @test_throws ArgumentError DataFrame(Dict(:z => 1, "true" => 2))
    @test DataFrame(Dict("z" => 1, "true" => 2)) == DataFrame("true" => 2, "z" => 1)
    @test DataFrame(Dict([Symbol(c) => i for (i, c) in enumerate('a':'z')])) ==
          DataFrame(Dict([string(c) => i for (i, c) in enumerate('a':'z')])) ==
          DataFrame([Symbol(c) => i for (i, c) in enumerate('a':'z')])
    @test DataFrame(OrderedDict(:z => 1, :a => 2)) == DataFrame(z=1, a=2)

end

@testset "removed constructors" begin
    @test_throws ArgumentError DataFrame([1 2; 3 4])
    @test_throws ArgumentError DataFrame([[1, 2], [3, 4]])
    @test_throws ArgumentError DataFrame([Int, Float64], [:a, :b])
    @test_throws ArgumentError DataFrame([Int, Float64], [:a, :b], 2)
    @test_throws ArgumentError DataFrame([Int, Float64], ["a", "b"])
    @test_throws ArgumentError DataFrame([Int, Float64], ["a", "b"], 2)
    @test_throws ArgumentError DataFrame([Int, Float64], ["a", :b])
    @test_throws MethodError DataFrame([Int, Float64], ["a", :b], 2)
end

@testset "threading correctness tests" begin
    for x in (10, 2*10^6), y in 1:4
        df = DataFrame(rand(x, y), :auto)
        @test df == copy(df)
    end
end

@testset "non-specific vector of column names" begin
    ref = DataFrame(a=1:2, b=3:4)
    for x in ([1 3; 2 4], [[1, 2], [3, 4]], [1:2, 3:4], Any[[1, 2], [3, 4]], Any[1:2, 3:4])
        @test DataFrame(x, Any[:a, :b]) == ref
        @test DataFrame(x, Any["a", "b"]) == ref
        @test DataFrame(x, Union{String, Symbol}[:a, :b]) == ref
        @test DataFrame(x, Union{String, Symbol}["a", "b"]) == ref
        @test_throws ArgumentError DataFrame(x, Any["a", :b])
        @test_throws ArgumentError DataFrame(x, Union{String, Symbol}["a", :b])
    end
    @test DataFrame([], []) == DataFrame()
    @test DataFrame(fill(0, 0, 0), []) == DataFrame()
    @test_throws ArgumentError DataFrame(Type[], Symbol[])
    @test_throws ArgumentError DataFrame(Type[], String[])
    @test_throws MethodError DataFrame(Type[], [])
end

@testset "DataFrame matrix constructor copycols kwarg" begin
    m = [1 4; 2 5; 3 6]
    refdf = DataFrame(x1=1:3, x2=4:6)
    for cnames in ([:x1, :x2], ["x1", "x2"], Any[:x1, :x2], Any["x1", "x2"], :auto)
        df = DataFrame(m, cnames)
        @test df == refdf
        @test df.x1 isa Vector{Int}
        @test df.x2 isa Vector{Int}
        df = DataFrame(m, cnames, copycols=true)
        @test df == refdf
        @test df.x1 isa Vector{Int}
        @test df.x2 isa Vector{Int}
        df = DataFrame(m, cnames, copycols=false)
        @test df == refdf
        @test df.x1 isa SubArray{Int, 1, Matrix{Int}}
        @test df.x2 isa SubArray{Int, 1, Matrix{Int}}
        @test parent(df.x1) === m
        @test parent(df.x2) === m
    end
end

end # module
