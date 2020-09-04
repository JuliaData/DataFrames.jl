module TestDeprecated

using Test, DataFrames, CategoricalArrays

const ≅ = isequal

@testset "DataFrame!" begin
    x = [1,2,3]
    y = [4,5,6]
    @test DataFrame!(x=x, y=y, copycols=true) == DataFrame(x=x,y=y)
    df1 = DataFrame(x=x, y=y)
    df2 = DataFrame!(df1)
    @test df1 == df2
    @test df1.x === df2.x
    @test df1.y === df2.y

    a=[1,2,3]
    df = DataFrame!(:a=>a, :b=>1, :c=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame!("a"=>a, "b"=>1, "c"=>1:3)
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a

    df = DataFrame!(Dict(:a=>a, :b=>1, :c=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df.a === a

    df = DataFrame!(Dict("a"=>a, "b"=>1, "c"=>1:3))
    @test propertynames(df) == [:a, :b, :c]
    @test df."a" === a

    df = DataFrame!((x, y))
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame!((x, y), (:x1, :x2))
    @test propertynames(df) == [:x1, :x2]
    @test df.x1 === x
    @test df.x2 === y

    df = DataFrame!((x, y), ("x1", "x2"))
    @test names(df) == ["x1", "x2"]
    @test df."x1" === x
    @test df."x2" === y

    @test_throws MethodError DataFrame!([1 2; 3 4], copycols=false)
    @test_throws MethodError DataFrame!([1 2; 3 4])
    @test_throws MethodError DataFrame!([Union{Int, Missing}, Union{Float64, Missing}],
                                        [:x1, :x2], 2)
end

@testset "test categorical" begin
    df = DataFrame(x=["a", "b", "c"],
                   y=["a", "b", missing],
                   z=[1,2,3])
    for x in [df, view(df, :, :)]
        y = categorical(x)
        @test y isa DataFrame
        @test x ≅ y
        @test x.x !== y.x
        @test x.y !== y.y
        @test x.z !== y.z
        @test y.x isa CategoricalVector{String}
        @test y.y isa CategoricalVector{Union{Missing, String}}
        @test y.z isa Vector{Int}

        y = categorical(x, Int)
        @test y isa DataFrame
        @test x ≅ y
        @test x.x !== y.x
        @test x.y !== y.y
        @test x.z !== y.z
        @test y.x isa Vector{String}
        @test y.y isa Vector{Union{Missing, String}}
        @test y.z isa CategoricalVector{Int}

        for colsel in [:, names(x), [1,2,3], [true,true,true], r"", Not(r"a")]
            y = categorical(x, colsel)
            @test y isa DataFrame
            @test x ≅ y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test y.x isa CategoricalVector{String}
            @test y.y isa CategoricalVector{Union{Missing, String}}
            @test y.z isa CategoricalVector{Int}
        end

        for colsel in [:x, "x", 1, [:x], ["x"], [1], [true, false, false], r"x", Not(2:3)]
            y = categorical(x, colsel)
            @test y isa DataFrame
            @test x ≅ y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test y.x isa CategoricalVector{String}
            @test y.y isa Vector{Union{Missing, String}}
            @test y.z isa Vector{Int}
        end

        for colsel in [:z, "z", 3, [:z], ["z"], [3], [false, false, true], r"z", Not(1:2)]
            y = categorical(x, colsel)
            @test y isa DataFrame
            @test x ≅ y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test y.x isa Vector{String}
            @test y.y isa Vector{Union{Missing, String}}
            @test y.z isa CategoricalVector{Int}
        end

        for colsel in [Int[], Symbol[], [false, false, false], r"a", Not(:)]
            y = categorical(x, colsel)
            @test y isa DataFrame
            @test x ≅ y
            @test x.x !== y.x
            @test x.y !== y.y
            @test x.z !== y.z
            @test y.x isa Vector{String}
            @test y.y isa Vector{Union{Missing, String}}
            @test y.z isa Vector{Int}
        end
    end
end

@testset "test categorical!" begin
    using DataFrames: _columns
    df = DataFrame(A = Vector{Union{Int, Missing}}(1:3), B = Vector{Union{Int, Missing}}(4:6))
    DRT = CategoricalArrays.DefaultRefType
    @test all(c -> isa(c, Vector{Union{Int, Missing}}), eachcol(categorical!(deepcopy(df))))
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
              eachcol(categorical!(deepcopy(df), [1,2])))
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
              eachcol(categorical!(deepcopy(df), [:A,:B])))
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), [:A]))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), :A))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), [1]))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), 1))) == 1

    @test all(c -> isa(c, Vector{Union{Int, Missing}}), eachcol(categorical!(deepcopy(df))))
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
              eachcol(categorical!(deepcopy(df), Not(Not([1,2])))))
    @test all(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
              eachcol(categorical!(deepcopy(df), Not(Not([:A,:B])))))
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), Not(Not([:A]))))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), Not(Not(:A))))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), Not(Not([1]))))) == 1
    @test findfirst(c -> typeof(c) <: CategoricalVector{Union{Int, Missing}},
                    _columns(categorical!(deepcopy(df), Not(Not(1))))) == 1
end

@testset "categorical!" begin
    df = DataFrame([["a", "b"], ['a', 'b'], [true, false], 1:2, ["x", "y"]])
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df)))),
                  [CategoricalArrays.CategoricalValue{String,UInt32},
                   Char, Bool, Int,
                   CategoricalArrays.CategoricalValue{String,UInt32}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), :))),
                  [CategoricalArrays.CategoricalValue{String,UInt32},
                   CategoricalArrays.CategoricalValue{Char,UInt32},
                   CategoricalArrays.CategoricalValue{Bool,UInt32},
                   CategoricalArrays.CategoricalValue{Int,UInt32},
                   CategoricalArrays.CategoricalValue{String,UInt32}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), compress=true))),
                  [CategoricalArrays.CategoricalValue{String,UInt8},
                   Char, Bool, Int,
                   CategoricalArrays.CategoricalValue{String,UInt8}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), names(df)))),
                  [CategoricalArrays.CategoricalValue{String,UInt32},
                   CategoricalArrays.CategoricalValue{Char,UInt32},
                   CategoricalArrays.CategoricalValue{Bool,UInt32},
                   CategoricalArrays.CategoricalValue{Int,UInt32},
                   CategoricalArrays.CategoricalValue{String,UInt32}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), names(df), compress=true))),
                  [CategoricalArrays.CategoricalValue{String,UInt8},
                   CategoricalArrays.CategoricalValue{Char,UInt8},
                   CategoricalArrays.CategoricalValue{Bool,UInt8},
                   CategoricalArrays.CategoricalValue{Int,UInt8},
                   CategoricalArrays.CategoricalValue{String,UInt8}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), Not(1:0)))),
                  [CategoricalArrays.CategoricalValue{String,UInt32},
                   CategoricalArrays.CategoricalValue{Char,UInt32},
                   CategoricalArrays.CategoricalValue{Bool,UInt32},
                   CategoricalArrays.CategoricalValue{Int,UInt32},
                   CategoricalArrays.CategoricalValue{String,UInt32}]))
    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), Not(1:0), compress=true))),
                  [CategoricalArrays.CategoricalValue{String,UInt8},
                   CategoricalArrays.CategoricalValue{Char,UInt8},
                   CategoricalArrays.CategoricalValue{Bool,UInt8},
                   CategoricalArrays.CategoricalValue{Int,UInt8},
                   CategoricalArrays.CategoricalValue{String,UInt8}]))

    @test all(map(<:, eltype.(eachcol(categorical!(deepcopy(df), Integer))),
                  [String, Char,
                   CategoricalArrays.CategoricalValue{Bool,UInt32},
                   CategoricalArrays.CategoricalValue{Int,UInt32},
                   String]))

    df = DataFrame([["a", missing]])
    categorical!(df)
    @test df.x1 isa CategoricalVector{Union{Missing, String}}

    df = DataFrame(x1=[1, 2])
    categorical!(df)
    @test df.x1 isa Vector{Int}
    categorical!(df, :)
    @test df.x1 isa CategoricalVector{Int}
end

@testset "categorical with All and Between" begin
    df = DataFrame(x1=["a", "b"], y=[2, 3])
    categorical(df, All())
    categorical(df, Between(1, 2))
    categorical!(df, All())
    categorical!(df, Between(1,2))
end

end # module
