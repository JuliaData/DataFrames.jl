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
    @test size(DataFrame!()) == (0,0)

    vecvec = [CategoricalVector{Union{Float64, Missing}}(zeros(3)),
              CategoricalVector{Union{Float64, Missing}}(ones(3))]

    df = DataFrame(collect(Any, vecvec), Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    df2 = DataFrame!(collect(Any, vecvec), Index([:x1, :x2]))
    @test size(df2, 1) == 3
    @test size(df2, 2) == 2
    @test df2.x1 === vecvec[1]
    @test df2.x2 === vecvec[2]

    for fun in (DataFrame, DataFrame!)
        @test df == fun(vecvec)
        @test df == fun(collect(Any, vecvec))
        @test df == fun(collect(AbstractVector, vecvec))
        @test df == fun(Tuple(vecvec))
        @test df == fun(x1 = vecvec[1], x2 = vecvec[2])

        for cols in ([:x1, :x2], ["x1", "x2"])
            @test df == fun(vecvec, cols)
            @test df == fun(collect(Any, vecvec), cols)
            @test df == fun(collect(AbstractVector, vecvec), cols)
            @test df == fun(Tuple(vecvec), Tuple(cols))
            @test df == fun([col=>vect for (col, vect) in zip(cols, vecvec)])
        end
    end

    @test DataFrame([1:3, 1:3]) == DataFrame(Any[1:3, 1:3]) ==
          DataFrame(UnitRange[1:3, 1:3]) == DataFrame(AbstractVector[1:3, 1:3]) ==
          DataFrame([[1,2,3], [1,2,3]]) == DataFrame(Any[[1,2,3], [1,2,3]]) ==
          DataFrame(([1,2,3], [1,2,3])) == DataFrame((1:3, 1:3)) ==
          DataFrame((1:3, [1,2,3])) == DataFrame([1:3, [1,2,3]])
          DataFrame((:x1=>1:3, :x2=>[1,2,3])) == DataFrame([:x1=>1:3, :x2=>[1,2,3]]) ==
          DataFrame(("x1"=>1:3, "x2"=>[1,2,3])) == DataFrame(["x1"=>1:3, "x2"=>[1,2,3]])

    @inferred DataFrame([1:3, 1:3])
    @inferred DataFrame((1:3, 1:3))
    @inferred DataFrame([1:3, 1:3], [:a, :b])
    @inferred DataFrame((1:3, 1:3), (:a, :b))
    @inferred DataFrame([1:3, 1:3], ["a", "b"])
    @inferred DataFrame((1:3, 1:3), ("a", "b"))

    @inferred DataFrame((:x1=>1:3, :x2=>[1,2,3]))
    @inferred DataFrame([:x1=>1:3, :x2=>[1,2,3]])
    @inferred DataFrame(("x1"=>1:3, "x2"=>[1,2,3]))
    @inferred DataFrame(["x1"=>1:3, "x2"=>[1,2,3]])

    @test df !== DataFrame(df)
    @test df == DataFrame(df)

    df2 = convert(DataFrame, Union{Float64, Missing}[0.0 1.0;
                                                     0.0 1.0;
                                                     0.0 1.0])
    rename!(df2, [:x1, :x2])
    @test df[!, :x1] == df2[!, :x1]
    @test df[!, :x2] == df2[!, :x2]


    df2 = convert(DataFrame, Union{Float64, Missing}[0.0 1.0;
                                                     0.0 1.0;
                                                     0.0 1.0])
    rename!(df2, ["x1", "x2"])
    @test df[!, "x1"] == df2[!, "x1"]
    @test df[!, "x2"] == df2[!, "x2"]

    df2 = DataFrame([0.0 1.0;
                     0.0 1.0;
                     0.0 1.0])
    rename!(df2, [:x1, :x2])
    @test df[!, :x1] == df2[!, :x1]
    @test df[!, :x2] == df2[!, :x2]

    df2 = DataFrame([0.0 1.0;
                     0.0 1.0;
                     0.0 1.0])
    rename!(df2, ["x1", "x2"])
    @test df[!, "x1"] == df2[!, "x1"]
    @test df[!, "x2"] == df2[!, "x2"]

    @test_throws ArgumentError DataFrame!([0.0 1.0;
                                           0.0 1.0;
                                           0.0 1.0])

    df2 = DataFrame([0.0 1.0;
                     0.0 1.0;
                     0.0 1.0], ["a", "b"])
    rename!(df2, ["a", "b"])
    @test df[!, "x1"] == df2[!, "a"]
    @test df[!, "x2"] == df2[!, "b"]

    df2 = DataFrame([0.0 1.0;
                     0.0 1.0;
                     0.0 1.0], [:a, :b])
    rename!(df2, [:a, :b])
    @test df[!, :x1] == df2[!, :a]
    @test df[!, :x2] == df2[!, :b]

    @test_throws ArgumentError DataFrame!([0.0 1.0;
                                           0.0 1.0;
                                           0.0 1.0], [:a, :b])

    df2 = DataFrame([0.0 1.0;
                     0.0 1.0;
                     0.0 1.0], ["a", "b"])
    rename!(df2, ["a", "b"])
    @test df[!, "x1"] == df2[!, "a"]
    @test df[!, "x2"] == df2[!, "b"]

    @test_throws ArgumentError DataFrame!([0.0 1.0;
                                           0.0 1.0;
                                           0.0 1.0], ["a", "b"])

    @test df == DataFrame(x1 = Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Missing}[1.0, 1.0, 1.0])
    @test df == DataFrame(x1 = Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Missing}[1.0, 1.0, 1.0],
                          x3 = Union{Float64, Missing}[2.0, 2.0, 2.0])[:, [:x1, :x2]]
    @test df == DataFrame(x1 = Union{Float64, Missing}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Missing}[1.0, 1.0, 1.0],
                          x3 = Union{Float64, Missing}[2.0, 2.0, 2.0])[:, ["x1", "x2"]]

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

    df = DataFrame!(x=x, y=y)
    @test size(df) == (3, 2)
    @test propertynames(df) == [:x, :y]
    @test df.x === x
    @test df.y === y
    @test_throws ArgumentError DataFrame!(x=x, y=y, copycols=true)
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

    df2 = DataFrame!(df1)
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
    @test propertynames(df) == [:type, :begin]

    a=[1,2,3]
    df = DataFrame(:a=>a, :b=>1, :c=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df.a == a
    @test df.a !== a
    df = DataFrame(:a=>a, :b=>1, :c=>1:3, copycols=false)
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame!(:a=>a, :b=>1, :c=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame("x1" => zeros(3), "x2" => ones(3))
    @inferred DataFrame("x1" => zeros(3), "x2" => ones(3))
    @test size(df, 1) == 3
    @test size(df, 2) == 2
    @test isequal(df, DataFrame(x1 = [0.0, 0.0, 0.0], x2 = [1.0, 1.0, 1.0]))

    df = DataFrame("type" => [], "begin" => [])
    @test propertynames(df) == [:type, :begin]

    a=[1,2,3]
    df = DataFrame("a"=>a, "b"=>1, "c"=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" == a
    @test df."a" !== a
    df = DataFrame("a"=>a, "b"=>1, "c"=>1:3, copycols=false)
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a

    df = DataFrame!("a"=>a, "b"=>1, "c"=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a
end

@testset "associative" begin
    df = DataFrame(Dict(:A => 1:3, :B => 4:6))
    @inferred DataFrame(Dict(:A => 1:3, :B => 4:6))
    @test df == DataFrame(A = 1:3, B = 4:6)
    @test eltype.(eachcol(df)) == [Int, Int]

    a=[1,2,3]
    df = DataFrame(Dict(:a=>a, :b=>1, :c=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df.a == a
    @test df.a !== a
    df = DataFrame(Dict(:a=>a, :b=>1, :c=>1:3), copycols=false)
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame!(Dict(:a=>a, :b=>1, :c=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame(Dict("A" => 1:3, "B" => 4:6))
    @inferred DataFrame(Dict("A" => 1:3, "B" => 4:6))
    @test df == DataFrame(A = 1:3, B = 4:6)
    @test eltype.(eachcol(df)) == [Int, Int]

    a=[1,2,3]
    df = DataFrame(Dict("a"=>a, "b"=>1, "c"=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" == a
    @test df."a" !== a
    df = DataFrame(Dict("a"=>a, "b"=>1, "c"=>1:3), copycols=false)
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a

    df = DataFrame!(Dict("a"=>a, "b"=>1, "c"=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a
end

@testset "vector constructors" begin
    x = [1,2,3]
    y = [1,2,3]

    df = DataFrame([x, y])
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], copycols=true)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame([x, y], copycols=false)
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

    df = DataFrame((x, y))
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame((x, y), copycols=true)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame((x, y), copycols=false)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame!((x, y))
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame((x, y), (:x1, :x2))
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame((x, y), (:x1, :x2), copycols=true)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 == x
    @test df.x2 == y
    @test df.x1 !== x
    @test df.x2 !== y
    df = DataFrame((x, y), (:x1, :x2), copycols=false)
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame((x, y), ("x1", "x2"))
    @test names(df) == ["x1", "x2"]
    @test df."x1" == x
    @test df."x2" == y
    @test df."x1" !== x
    @test df."x2" !== y
    df = DataFrame((x, y), ("x1", "x2"), copycols=true)
    @test names(df) == ["x1", "x2"]
    @test df."x1" == x
    @test df."x2" == y
    @test df."x1" !== x
    @test df."x2" !== y
    df = DataFrame((x, y), ("x1", "x2"), copycols=false)
    @test names(df) == ["x1", "x2"]
    @test df."x1" === x
    @test df."x2" === y

    df = DataFrame!((x, y), (:x1, :x2))
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    n = [:x1, :x2]
    v = AbstractVector[1:3, [1,2,3]]
    @test DataFrame(v, n).x1 isa Vector{Int}
    @test v[1] isa AbstractRange

    df = DataFrame!((x, y), ("x1", "x2"))
    @test names(df) == ["x1", "x2"]
    @test df."x1" === x
    @test df."x2" === y

    n = ["x1", "x2"]
    v = AbstractVector[1:3, [1,2,3]]
    @test DataFrame(v, n)."x1" isa Vector{Int}
    @test v[1] isa AbstractRange
end

@testset "recyclers" begin
    @test DataFrame(a = 1:5, b = 1) == DataFrame(a = collect(1:5), b = fill(1, 5))
    @test DataFrame(a = 1, b = 1:5) == DataFrame(a = fill(1, 5), b = collect(1:5))
    @test size(DataFrame(a=1, b=[])) == (0, 2)
    @test size(DataFrame!(a=1, b=[])) == (0, 2)
end

@testset "constructor thrown exceptions" begin
    for f in [DataFrame, DataFrame!]
        @test_throws DimensionMismatch f(Any[collect(1:10)], DataFrames.Index([:A, :B]))
        @test_throws ArgumentError f(A = rand(2,2))
        @test_throws ArgumentError f(A = rand(2,1))
        @test_throws ArgumentError f([1, 2, 3])
        @test_throws DimensionMismatch f(AbstractVector[1:3, [1,2]])
        @test_throws ArgumentError f([1:3, 1], [:x1, :x2])
        @test_throws ArgumentError f([1:3, 1], ["x1", "x2"])
        @test_throws ErrorException f([1:3, 1])
    end

    @test_throws MethodError DataFrame!([1 2; 3 4], copycols=false)
end

@testset "column types" begin
    df = DataFrame(A = 1:3, B = 2:4, C = 3:5)
    answer = [Array{Int,1}, Array{Int,1}, Array{Int,1}]
    @test map(typeof, eachcol(df)) == answer
    df[!, :D] = [4, 5, missing]
    push!(answer, Vector{Union{Int, Missing}})
    @test map(typeof, eachcol(df)) == answer
    df[!, :E] .= 'c'
    push!(answer, Vector{Char})
    @test map(typeof, eachcol(df)) == answer
end

@testset "Matrix constructor" begin
    df = DataFrame([1 2; 3 4])
    @test size(df) == (2, 2)
    @test df.x1 == [1, 3]
    @test df.x2 == [2, 4]
    @test_throws ArgumentError DataFrame!([1 2; 3 4])

end

@testset "constructor with types" begin
    df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}, Union{String, Missing}],
                   [:A, :B, :C], 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[!, 1]) == Vector{Union{Int, Missing}}
    @test typeof(df[!, 2]) == Vector{Union{Float64, Missing}}
    @test typeof(df[!, 3]) == Vector{Union{String, Missing}}
    @test all(ismissing, df[!, 1])
    @test all(ismissing, df[!, 2])
    @test all(ismissing, df[!, 3])

    df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}, Union{String, Missing}],
                   ["A", "B", "C"], 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[!, "A"]) == Vector{Union{Int, Missing}}
    @test typeof(df[!, "B"]) == Vector{Union{Float64, Missing}}
    @test typeof(df[!, "C"]) == Vector{Union{String, Missing}}
    @test all(ismissing, df[!, "A"])
    @test all(ismissing, df[!, "B"])
    @test all(ismissing, df[!, "C"])

    df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}], [:x1, :x2], 2)
    @test size(df) == (2, 2)
    @test eltype.(eachcol(df)) == [Union{Int, Missing}, Union{Float64, Missing}]

    @test_throws ArgumentError DataFrame!([Union{Int, Missing}, Union{Float64, Missing}],
                                          [:x1, :x2], 2)
    @test size(df) == (2, 2)
    @test eltype.(eachcol(df)) == [Union{Int, Missing}, Union{Float64, Missing}]

    df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}, Union{String, Missing}],
                   [:A, :B, :C])
    @test size(df, 1) == 0
    @test size(df, 2) == 3
    @test typeof(df[!, 1]) == Vector{Union{Int, Missing}}
    @test typeof(df[!, 2]) == Vector{Union{Float64, Missing}}
    @test typeof(df[!, 3]) == Vector{Union{String, Missing}}
    @test propertynames(df) == [:A, :B, :C]

    df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}, Union{String, Missing}],
                   ["A", "B", "C"])
    @test size(df, 1) == 0
    @test size(df, 2) == 3
    @test typeof(df[!, "A"]) == Vector{Union{Int, Missing}}
    @test typeof(df[!, "B"]) == Vector{Union{Float64, Missing}}
    @test typeof(df[!, "C"]) == Vector{Union{String, Missing}}
    @test names(df) == ["A", "B", "C"]
end

@testset "expansion of Ref and 0-dimensional arrays" begin
    @test DataFrame(a=Ref(1), b=fill(1)) == DataFrame(a=[1], b=[1])
    @test DataFrame(a=Ref(1), b=fill(1), c=1:3) ==
          DataFrame(a=[1,1,1], b=[1,1,1], c=1:3)
end

@testset "broadcasting into 0 rows" begin
    for df in [DataFrame(x1=1:0, x2=1), DataFrame(x1=1, x2=1:0)]
        @test size(df) == (0, 2)
        @test df.x1 isa Vector{Int}
        @test df.x2 isa Vector{Int}
    end
end

end # module
