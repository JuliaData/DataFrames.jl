module TestConstructors
    using Base.Test, DataFrames, DataFrames.Index

    #
    # DataFrame
    #

    df = DataFrame()
    @test df.columns == Any[]
    @test df.colindex == Index()

    df = DataFrame(Any[CategoricalVector{Union{Float64, Null}}(zeros(3)),
                       CategoricalVector{Union{Float64, Null}}(ones(3))],
                   Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    @test df == DataFrame(Any[CategoricalVector{Union{Float64, Null}}(zeros(3)),
                              CategoricalVector{Union{Float64, Null}}(ones(3))])
    @test df == DataFrame(x1 = Union{Int, Null}[0.0, 0.0, 0.0],
                          x2 = Union{Int, Null}[1.0, 1.0, 1.0])

    df2 = convert(DataFrame, Union{Float64, Null}[0.0 1.0;
                                                  0.0 1.0;
                                                  0.0 1.0])
    names!(df2, [:x1, :x2])
    @test df[:x1] == df2[:x1]
    @test df[:x2] == df2[:x2]

    @test df == DataFrame(x1 = Union{Float64, Null}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Null}[1.0, 1.0, 1.0])
    @test df == DataFrame(x1 = Union{Float64, Null}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Null}[1.0, 1.0, 1.0],
                          x3 = Union{Float64, Null}[2.0, 2.0, 2.0])[[:x1, :x2]]

    df = DataFrame(Union{Int, Null}, 2, 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Union{Int, Null}, Union{Int, Null}]

    df = DataFrame([Union{Int, Null}, Union{Float64, Null}], [:x1, :x2], 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Union{Int, Null}, Union{Float64, Null}]

    @test df == DataFrame([Union{Int, Null}, Union{Float64, Null}], 2)

    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0)
    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0)
    @test SubDataFrame(DataFrame(A=1), 1) == DataFrame(A=1)
    @test SubDataFrame(DataFrame(A=1:10), 1:4) == DataFrame(A=1:4)
    @test view(SubDataFrame(DataFrame(A=1:10), 1:4), 2) == DataFrame(A=2)
    @test view(SubDataFrame(DataFrame(A=1:10), 1:4), [true, true, false, false]) == DataFrame(A=1:2)

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
        df[:D] = [4, 5, null]
        push!(answer, Vector{Union{Int, Null}})
        @test map(typeof, df.columns) == answer
        df[:E] = 'c'
        push!(answer, Vector{Char})
        @test map(typeof, df.columns) == answer
    end
end
