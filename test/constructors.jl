module TestConstructors
    using Base.Test
    using DataFrames, DataFrames.Index

    #
    # DataFrame
    #

    df = DataFrame()
    @test isequal(df.columns, Any[])
    @test isequal(df.colindex, Index())

    df = DataFrame(Any[NullableCategoricalVector(zeros(3)),
                       NullableCategoricalVector(ones(3))],
                   Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    @test isequal(df, DataFrame(Any[NullableCategoricalVector(zeros(3)),
                                    NullableCategoricalVector(ones(3))]))
    @test isequal(df, DataFrame(x1 = NullableArray([0.0, 0.0, 0.0]),
                                x2 = NullableArray([1.0, 1.0, 1.0])))

    df2 = convert(DataFrame, NullableArray([0.0 1.0;
                                            0.0 1.0;
                                            0.0 1.0]))
    names!(df2, [:x1, :x2])
    @test isequal(df[:x1], NullableArray(df2[:x1]))
    @test isequal(df[:x2], NullableArray(df2[:x2]))

    @test isequal(df, DataFrame(x1 = NullableArray([0.0, 0.0, 0.0]),
                                x2 = NullableArray([1.0, 1.0, 1.0])))
    @test isequal(df, DataFrame(x1 = NullableArray([0.0, 0.0, 0.0]),
                                x2 = NullableArray([1.0, 1.0, 1.0]),
                                x3 = NullableArray([2.0, 2.0, 2.0]))[[:x1, :x2]])

    df = DataFrame(Nullable{Int}, 2, 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Nullable{Int}, Nullable{Int}]

    df = DataFrame([Nullable{Int}, Nullable{Float64}], [:x1, :x2], 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Nullable{Int}, Nullable{Float64}]

    @test isequal(df, DataFrame([Nullable{Int}, Nullable{Float64}], 2))

    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0)
    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0)
    @test isequal(SubDataFrame(DataFrame(A=1), 1), DataFrame(A=1))
    @test isequal(SubDataFrame(DataFrame(A=1:10), 1:4), DataFrame(A=1:4))
    @test isequal(view(SubDataFrame(DataFrame(A=1:10), 1:4), 2), DataFrame(A=2))
    @test isequal(view(SubDataFrame(DataFrame(A=1:10), 1:4), [true, true, false, false]), DataFrame(A=1:2))

    @test DataFrame(a=1, b=1:2) == DataFrame(a=[1,1], b=[1,2])

    @testset "associative" begin
        df = DataFrame(Dict(:A => 1:3, :B => 4:6))
        @test df == DataFrame(A = 1:3, B = 4:6)
        @test eltypes(df) == [Int, Int]
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
    end

    @testset "column types" begin
        df = DataFrame(A = 1:3, B = 2:4, C = 3:5)
        answer = [Array{Int,1}, Array{Int,1}, Array{Int,1}]
        @test map(typeof, df.columns) == answer
        df[:D] = NullableArray([4, 5, Nullable()])
        push!(answer, NullableArray{Int,1})
        @test map(typeof, df.columns) == answer
        df[:E] = 'c'
        push!(answer, Array{Char,1})
        @test map(typeof, df.columns) == answer
    end
end
