module TestConstructors
    using Base.Test, DataFrames, DataFrames.Index

    #
    # DataFrame
    #

    dt = DataFrame()
    @test dt.columns == Any[]
    @test dt.colindex == Index()

    dt = DataFrame(Any[CategoricalVector{Union{Float64, Null}}(zeros(3)),
                       CategoricalVector{Union{Float64, Null}}(ones(3))],
                   Index([:x1, :x2]))
    @test size(dt, 1) == 3
    @test size(dt, 2) == 2

    @test dt == DataFrame(Any[CategoricalVector{Union{Float64, Null}}(zeros(3)),
                              CategoricalVector{Union{Float64, Null}}(ones(3))])
    @test dt == DataFrame(x1 = Union{Int, Null}[0.0, 0.0, 0.0],
                          x2 = Union{Int, Null}[1.0, 1.0, 1.0])

    dt2 = convert(DataFrame, Union{Float64, Null}[0.0 1.0;
                                                  0.0 1.0;
                                                  0.0 1.0])
    names!(dt2, [:x1, :x2])
    @test dt[:x1] == dt2[:x1]
    @test dt[:x2] == dt2[:x2]

    @test dt == DataFrame(x1 = Union{Float64, Null}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Null}[1.0, 1.0, 1.0])
    @test dt == DataFrame(x1 = Union{Float64, Null}[0.0, 0.0, 0.0],
                          x2 = Union{Float64, Null}[1.0, 1.0, 1.0],
                          x3 = Union{Float64, Null}[2.0, 2.0, 2.0])[[:x1, :x2]]

    dt = DataFrame(Union{Int, Null}, 2, 2)
    @test size(dt) == (2, 2)
    @test eltypes(dt) == [Union{Int, Null}, Union{Int, Null}]

    dt = DataFrame([Union{Int, Null}, Union{Float64, Null}], [:x1, :x2], 2)
    @test size(dt) == (2, 2)
    @test eltypes(dt) == [Union{Int, Null}, Union{Float64, Null}]

    @test dt == DataFrame([Union{Int, Null}, Union{Float64, Null}], 2)

    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0)
    @test_throws BoundsError SubDataFrame(DataFrame(A=1), 0)
    @test SubDataFrame(DataFrame(A=1), 1) == DataFrame(A=1)
    @test SubDataFrame(DataFrame(A=1:10), 1:4) == DataFrame(A=1:4)
    @test view(SubDataFrame(DataFrame(A=1:10), 1:4), 2) == DataFrame(A=2)
    @test view(SubDataFrame(DataFrame(A=1:10), 1:4), [true, true, false, false]) == DataFrame(A=1:2)

    @test DataFrame(a=1, b=1:2) == DataFrame(a=[1,1], b=[1,2])

    @testset "associative" begin
        dt = DataFrame(Dict(:A => 1:3, :B => 4:6))
        @test dt == DataFrame(A = 1:3, B = 4:6)
        @test eltypes(dt) == [Int, Int]
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
        dt = DataFrame(A = 1:3, B = 2:4, C = 3:5)
        answer = [Array{Int,1}, Array{Int,1}, Array{Int,1}]
        @test map(typeof, dt.columns) == answer
        dt[:D] = [4, 5, null]
        push!(answer, Vector{Union{Int, Null}})
        @test map(typeof, dt.columns) == answer
        dt[:E] = 'c'
        push!(answer, Vector{Char})
        @test map(typeof, dt.columns) == answer
    end
end
