module TestBroadcasting

using Test, DataFrames

refdf = DataFrame(ones(3, 5))

@testset "normal data frame and data frame row in broadcasted assignment - one column" begin
    df = copy(refdf)
    df[1] .+= 1
    @test df.x1 == [2, 2, 2]
    @test df[2:end] == refdf[2:end]

    dfv = @view df[1:2, 2:end]
    dfv[1] .+= 1
    @test dfv.x2 == [2, 2]
    @test dfv[2:end] == refdf[1:2, 3:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    dfr = df[1, 3:end]
    dfr[end-1:end] .= 10
    @test all(Vector(dfr) .== [1, 10, 10])
    @test df.x3[1] == 1 && df.x4[1] == 10 && df.x5[1] == 10

    df = copy(refdf)
    df[:, 1] .+= 1
    @test df.x1 == [2, 2, 2]
    @test df[2:end] == refdf[2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, 1] .+= 1
    @test dfv.x2 == [2, 2]
    @test dfv[2:end] == refdf[1:2, 3:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    df[1] .+= [1, 1, 1]
    @test df.x1 == [2, 2, 2]
    @test df[2:end] == refdf[2:end]

    dfv = @view df[1:2, 2:end]
    dfv[1] .+= [1, 1]
    @test dfv.x2 == [2, 2]
    @test dfv[2:end] == refdf[1:2, 3:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    dfr = df[1, 3:end]
    dfr[end-1:end] .= [10, 10]
    @test all(Vector(dfr) .== [1, 10, 10])
    @test df.x3[1] == 1 && df.x4[1] == 10 && df.x5[1] == 10

    df = copy(refdf)
    df[:, 1] .+= [1, 1, 1]
    @test df.x1 == [2, 2, 2]
    @test df[2:end] == refdf[2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, 1] .+= [1, 1]
    @test dfv.x2 == [2, 2]
    @test dfv[2:end] == refdf[1:2, 3:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    dfr = df[1, 3:end]
    @test_throws DimensionMismatch df[1] .= rand(3, 1)
    @test_throws DimensionMismatch dfv[1] .= rand(2, 1)
    @test_throws DimensionMismatch dfr[end-1:end] .= rand(3, 1)
    @test_throws DimensionMismatch df[:, 1] .= rand(3, 1)
    @test_throws DimensionMismatch dfv[:, 1] .= rand(2, 1)

    df = copy(refdf)
    df[:x1] .+= 1
    @test df.x1 == [2, 2, 2]
    @test df[2:end] == refdf[2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:x2] .+= 1
    @test dfv.x2 == [2, 2]
    @test dfv[2:end] == refdf[1:2, 3:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    dfr = df[1, 3:end]
    dfr[[:x4, :x5]] .= 10
    @test all(Vector(dfr) .== [1, 10, 10])
    @test df.x3[1] == 1 && df.x4[1] == 10 && df.x5[1] == 10

    df = copy(refdf)
    df[:, :x1] .+= 1
    @test df.x1 == [2, 2, 2]
    @test df[2:end] == refdf[2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, :x2] .+= 1
    @test dfv.x2 == [2, 2]
    @test dfv[2:end] == refdf[1:2, 3:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    df[:x1] .+= [1, 1, 1]
    @test df.x1 == [2, 2, 2]
    @test df[2:end] == refdf[2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:x2] .+= [1, 1]
    @test dfv.x2 == [2, 2]
    @test dfv[2:end] == refdf[1:2, 3:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    dfr = df[1, 3:end]
    dfr[[:x4, :x5]] .= [10, 10]
    @test all(Vector(dfr) .== [1, 10, 10])
    @test df.x3[1] == 1 && df.x4[1] == 10 && df.x5[1] == 10

    df = copy(refdf)
    df[:, :x1] .+= [1, 1, 1]
    @test df.x1 == [2, 2, 2]
    @test df[2:end] == refdf[2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, :x2] .+= [1, 1]
    @test dfv.x2 == [2, 2]
    @test dfv[2:end] == refdf[1:2, 3:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    dfr = df[1, 3:end]
    @test_throws DimensionMismatch df[:x1] .= rand(3, 1)
    @test_throws DimensionMismatch dfv[:x2] .= rand(2, 1)
    @test_throws DimensionMismatch dfr[[:x4, :x5]] .= rand(3, 1)
    @test_throws DimensionMismatch df[:, :x1] .= rand(3, 1)
    @test_throws DimensionMismatch dfv[:, :x2] .= rand(2, 1)
end

@testset "normal data frame and data frame row in broadcasted assignment - two columns" begin
    df = copy(refdf)
    df[[1,2]] .= Matrix(df[[1,2]]) .+ 1
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test df[3:end] == refdf[3:end]

    dfv = @view df[1:2, 3:end]
    dfv[[1,2]] .= Matrix(dfv[[1,2]]) .+ 1
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test dfv[3:end] == refdf[1:2, 5:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    df[:, [1,2]] .= Matrix(df[[1,2]]) .+ 1
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test df[3:end] == refdf[3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [1,2]] .= Matrix(dfv[[1,2]]) .+ 1
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test dfv[3:end] == refdf[1:2, 5:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    df[[1,2]] .= Matrix(df[[1,2]]) .+ [1 1
                                       1 1
                                       1 1]
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test df[3:end] == refdf[3:end]

    dfv = @view df[1:2, 3:end]
    dfv[[1,2]] .= Matrix(dfv[[1,2]]) .+ [1 1
                                         1 1]
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test dfv[3:end] == refdf[1:2, 5:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    df[:, [1,2]] .= Matrix(df[[1,2]]) .+ [1 1
                                          1 1
                                          1 1]
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test df[3:end] == refdf[3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [1,2]] .= Matrix(dfv[[1,2]]) .+ [1 1
                                            1 1]
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test dfv[3:end] == refdf[1:2, 5:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    @test_throws DimensionMismatch df[[1,2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[[1,2]] .= rand(2, 10)
    @test_throws DimensionMismatch df[:, [1,2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[:, [1,2]] .= rand(2, 10)

    df = copy(refdf)
    df[[:x1,:x2]] .= Matrix(df[[:x1,:x2]]) .+ 1
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test df[3:end] == refdf[3:end]

    dfv = @view df[1:2, 3:end]
    dfv[[:x3,:x4]] .= Matrix(dfv[[:x3,:x4]]) .+ 1
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test dfv[3:end] == refdf[1:2, 5:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    df[:, [:x1,:x2]] .= Matrix(df[[:x1,:x2]]) .+ 1
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test df[3:end] == refdf[3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [:x3,:x4]] .= Matrix(dfv[[:x3,:x4]]) .+ 1
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test dfv[3:end] == refdf[1:2, 5:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    df[[:x1,:x2]] .= Matrix(df[[:x1,:x2]]) .+ [1 1
                                               1 1
                                               1 1]
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test df[3:end] == refdf[3:end]

    dfv = @view df[1:2, 3:end]
    dfv[[:x3,:x4]] .= Matrix(dfv[[:x3,:x4]]) .+ [1 1
                                                 1 1]
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test dfv[3:end] == refdf[1:2, 5:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    df[:, [:x1,:x2]] .= Matrix(df[[:x1,:x2]]) .+ [1 1
                                                  1 1
                                                  1 1]
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test df[3:end] == refdf[3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [:x3,:x4]] .= Matrix(dfv[[:x3,:x4]]) .+ [1 1
                                                    1 1]
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test dfv[3:end] == refdf[1:2, 5:end]
    @test all(Matrix(df[1:2, 1:2]) .== 2)

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    @test_throws DimensionMismatch df[[:x1,:x2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[[:x3,:x4]] .= rand(2, 10)
    @test_throws DimensionMismatch df[:, [:x1,:x2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[:, [:x3,:x4]] .= rand(2, 10)
end

@testset "assignment to a whole data frame and data frame row" begin
    df = copy(refdf)
    df .= 10
    @test all(Matrix(df) .== 10)
    dfv = view(df, 1:2, 1:4)
    dfv .= 100
    @test (all(Matrix(df[1:2, 1:4]) .== 100))
    @test (all(Vector(df[3, 1:4]) .== 10))
    dfr = df[1, 1:2]
    dfr .= 1000
    @test (all(Vector(df[1, 1:2]) .== 1000))
    @test (all(Vector(df[3, :]) .!= 1000))

    df = copy(refdf)
    df[:] .= 10
    @test all(Matrix(df) .== 10)
    dfv = view(df, 1:2, 1:4)
    dfv[:] .= 100
    @test (all(Matrix(df[1:2, 1:4]) .== 100))
    @test (all(Vector(df[3, 1:4]) .== 10))
    dfr = df[1, 1:2]
    dfr[:] .= 1000
    @test (all(Vector(df[1, 1:2]) .== 1000))
    @test (all(Vector(df[3, :]) .!= 1000))

    df = copy(refdf)
    df[:,:] .= 10
    @test all(Matrix(df) .== 10)
    dfv = view(df, 1:2, 1:4)
    dfv[:, :] .= 100
    @test (all(Matrix(df[1:2, 1:4]) .== 100))
    @test (all(Vector(df[3, 1:4]) .== 10))
end

@testset "extending data frame in broadcasted assignment - one column" begin
    df = copy(refdf)
    df[:a] .= 1
    @test all(Matrix(df) .== 1)
    @test names(df)[end] == :a
    df[:b] .= [1, 1, 1]
    @test all(Matrix(df) .== 1)
    @test names(df)[end] == :b
    cdf = copy(df)
    @test_throws DimensionMismatch df[:c] .= ones(3, 1)
    @test df == cdf
    @test_throws DimensionMismatch df[:x] .= ones(4)
    @test df == cdf
    @test_throws BoundsError df[10] .= ones(3)
    @test df == cdf

    dfv = @view df[1:2, 2:end]
    @test_throws BoundsError dfv[10] .= ones(3)
    @test_throws ArgumentError dfv[:z] .= ones(3)
    @test df == cdf
    dfr = df[1, 3:end]
    @test_throws BoundsError dfr[10] .= ones(3)
    @test_throws ArgumentError dfr[:z] .= ones(3)
    @test df == cdf
end

@testset "empty data frame corner case" begin
    df = DataFrame()
    @test_throws ArgumentError df[1] .= 1
    @test_throws ArgumentError df[:a] .= [1]
    @test_throws ArgumentError df[[:a,:b]] .= [1]
    @test df == DataFrame()
    df .= 1
    @test df == DataFrame()
    df .= [1]
    @test df == DataFrame()
    df .= ones(1,1)
    @test df == DataFrame()
    @test_throws DimensionMismatch df .= ones(1,2)
    @test_throws DimensionMismatch df .= ones(1,1,1)

    @test_throws ArgumentError df[:a] .= 1
    @test_throws ArgumentError df[[:a, :b]] .= 1

    df = DataFrame(a=[])
    @test_throws ArgumentError df[:b] .= 1
end

@testset "test categorical values" begin
    df = copy(refdf)
    v = categorical([1,2,3])
    df[:c1] .= v
    @test df.c1 == v
    @test df.c1 !== v
    @test df.c1 isa CategoricalVector
    @test levels(df.c1) == levels(v)
    @test levels(df.c1) !== levels(v)
    df[:c2] .= v[1]
    @test df.c2 == [1,1,1]
    @test df.c2 isa CategoricalVector
    @test levels(df.c2) != levels(v)
end

end # module
