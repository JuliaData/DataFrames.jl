module TestBroadcasting

using Test, DataFrames

refdf = DataFrame(ones(3, 5))

@testset "data frame and data frame row in broadcasting" begin
    df = copy(refdf)
    dfv = view(df, 1:2, 1:4)
    dfr = df[1, 1:2]

    @test df .+ 1 == fill(2.0, 3, 5)
    @test all(df .== ones(3, 5))
    @test [i for i in 3:5, j in 1:5] == @. 2 * df + (1:3)
    @test df .- ones(3, 5) == zeros(3, 5)
    @test_throws DimensionMismatch df .- ones(3, 4)
    @test_throws DimensionMismatch df .- ones(4)
    @test_throws DimensionMismatch df .- ones(3, 4, 1)

    @test dfv .+ 1 == fill(2.0, 2, 4)
    @test all(dfv .== ones(2, 4))
    @test [i for i in 3:4, j in 1:4] == @. 2 * dfv + (1:2)
    @test dfv .- ones(2, 4) == zeros(2, 4)
    @test_throws DimensionMismatch dfv .- ones(3, 4)
    @test_throws DimensionMismatch dfv .- ones(4)
    @test_throws DimensionMismatch dfv .- ones(3, 4, 1)

    @test dfr .+ 1 == fill(2.0, 2)
    @test all(dfr .== ones(2))
    @test [3, 4] == @. 2 * dfr + (1:2)
    @test dfr .- ones(2) == zeros(2)
    @test_throws DimensionMismatch dfr .- ones(3, 4)
    @test_throws DimensionMismatch dfr .- ones(4)
    @test_throws DimensionMismatch dfr .- ones(3, 4, 1)
end

@testset "normal data frame and data frame row in broadcasted assignment - one column" begin
    df = copy(refdf)
    df[1] .+= 1
    @test df.x1 == [2, 2, 2]
    @test all(df[2:end] .== ones(size(df[2:end])...))

    dfv = @view df[1:2, 2:end]
    dfv[1] .+= 1
    @test dfv.x2 == [2, 2]
    @test all(dfv[2:end] .== ones(size(dfv[2:end])...))
    @test all(df[1:2, 1:2] .== 2)

    dfr = df[1, 3:end]
    dfr[end-1:end] .= 10
    @test all(dfr .== [1, 10, 10])
    @test df.x3[1] == 1 && df.x4[1] == 10 && df.x5[1] == 10

    df = copy(refdf)
    df[1] .+= [1, 1, 1]
    @test df.x1 == [2, 2, 2]
    @test all(df[2:end] .== ones(size(df[2:end])...))

    dfv = @view df[1:2, 2:end]
    dfv[1] .+= [1, 1]
    @test dfv.x2 == [2, 2]
    @test all(dfv[2:end] .== ones(size(dfv[2:end])...))
    @test all(df[1:2, 1:2] .== 2)

    dfr = df[1, 3:end]
    dfr[end-1:end] .= [10, 10]
    @test all(dfr .== [1, 10, 10])
    @test df.x3[1] == 1 && df.x4[1] == 10 && df.x5[1] == 10

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    dfr = df[1, 3:end]
    @test_throws DimensionMismatch df[1] .= rand(3, 1)
    @test_throws DimensionMismatch dfv[1] .= rand(2, 1)
    @test_throws DimensionMismatch dfr[end-1:end] .= rand(3, 1)

    df = copy(refdf)
    df[:x1] .+= 1
    @test df.x1 == [2, 2, 2]
    @test all(df[2:end] .== ones(size(df[2:end])...))

    dfv = @view df[1:2, 2:end]
    dfv[:x2] .+= 1
    @test dfv.x2 == [2, 2]
    @test all(dfv[2:end] .== ones(size(dfv[2:end])...))
    @test all(df[1:2, 1:2] .== 2)

    dfr = df[1, 3:end]
    dfr[[:x4, :x5]] .= 10
    @test all(dfr .== [1, 10, 10])
    @test df.x3[1] == 1 && df.x4[1] == 10 && df.x5[1] == 10

    df = copy(refdf)
    df[:x1] .+= [1, 1, 1]
    @test df.x1 == [2, 2, 2]
    @test all(df[2:end] .== ones(size(df[2:end])...))

    dfv = @view df[1:2, 2:end]
    dfv[:x2] .+= [1, 1]
    @test dfv.x2 == [2, 2]
    @test all(dfv[2:end] .== ones(size(dfv[2:end])...))
    @test all(df[1:2, 1:2] .== 2)

    dfr = df[1, 3:end]
    dfr[[:x4, :x5]] .= [10, 10]
    @test all(dfr .== [1, 10, 10])
    @test df.x3[1] == 1 && df.x4[1] == 10 && df.x5[1] == 10

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    dfr = df[1, 3:end]
    @test_throws DimensionMismatch df[:x1] .= rand(3, 1)
    @test_throws DimensionMismatch dfv[:x2] .= rand(2, 1)
    @test_throws DimensionMismatch dfr[[:x4, :x5]] .= rand(3, 1)
end

@testset "normal data frame and data frame row in broadcasted assignment - two columns" begin
    df = copy(refdf)
    df[[1,2]] .+= 1
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test all(df[3:end] .== ones(size(df[3:end])...))

    dfv = @view df[1:2, 3:end]
    dfv[[1,2]] .+= 1
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test all(dfv[3:end] .== ones(size(dfv[3:end])...))
    @test all(df[1:2, 1:2] .== 2)

    df = copy(refdf)
    df[[1,2]] .+= [1 1
                   1 1
                   1 1]
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test all(df[3:end] .== ones(size(df[3:end])...))

    dfv = @view df[1:2, 3:end]
    dfv[[1,2]] .+= [1 1
                    1 1]
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test all(dfv[3:end] .== ones(size(dfv[3:end])...))
    @test all(df[1:2, 1:2] .== 2)

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    dfr = df[1, 3:end]
    @test_throws DimensionMismatch df[[1,2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[[1,2]] .= rand(2, 10)

    df = copy(refdf)
    df[[:x1,:x2]] .+= 1
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test all(df[3:end] .== ones(size(df[3:end])...))

    dfv = @view df[1:2, 3:end]
    dfv[[:x3,:x4]] .+= 1
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test all(dfv[3:end] .== ones(size(dfv[3:end])...))
    @test all(df[1:2, 1:2] .== 2)

    df = copy(refdf)
    df[[:x1,:x2]] .+= [1 1
                       1 1
                       1 1]
    @test df.x1 == [2, 2, 2]
    @test df.x2 == [2, 2, 2]
    @test all(df[3:end] .== ones(size(df[3:end])...))

    dfv = @view df[1:2, 3:end]
    dfv[[:x3,:x4]] .+= [1 1
                        1 1]
    @test dfv.x3 == [2, 2]
    @test dfv.x4 == [2, 2]
    @test all(dfv[3:end] .== ones(size(dfv[3:end])...))
    @test all(df[1:2, 1:2] .== 2)

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    dfr = df[1, 3:end]
    @test_throws DimensionMismatch df[[:x1,:x2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[[:x3,:x4]] .= rand(2, 10)
end

@testset "assignment to a whole data frame and data frame row" begin
    df = copy(refdf)
    df .= 10
    @test all(df .== 10)
    dfv = view(df, 1:2, 1:4)
    dfv .= 100
    @test (all(df[1:2, 1:4] .== 100))
    @test (all(df[3, 1:4] .== 10))
    dfr = df[1, 1:2]
    dfr .= 1000
    @test (all(df[1, 1:2] .== 1000))
    @test (all(df[3, :] .!= 1000))
end

@testset "extending data frame in broadcasted assignment - one column" begin
    df = copy(refdf)
    df[:a] .= 1
    @test all(df .== 1)
    @test names(df)[end] == :a
    df[:b] .= [1, 1, 1]
    @test all(df .== 1)
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

@testset "extending data frame in broadcasted assignment - two columns" begin
    df = copy(refdf)
    df[[:x1, :a]] .= 10
    @test all(df.x1 .== 10) && all(df.a .== 10)
    @test names(df)[end] == :a
    df[[:b, :c]] .= [5 5
                     5 5
                     5 5]
    @test all(df[end-1:end] .== 5)
    @test names(df)[end-1:end] == [:b, :c]
    cdf = copy(df)
    @test_throws DimensionMismatch df[[:x1, :x]] .= ones(3, 10)
    @test df == cdf
    @test_throws DimensionMismatch df[[:x2, :x]] .= ones(4)
    @test df == cdf
    @test_throws BoundsError df[[1, 10]] .= ones(3)
    @test df == cdf

    dfv = @view df[1:2, 2:end]
    @test_throws BoundsError dfv[[10, 11]] .= ones(3)
    @test_throws ArgumentError dfv[[:x1, :z]] .= ones(3)
    @test df == cdf
    dfr = df[1, 3:end]
    @test_throws BoundsError dfr[[1,11]] .= ones(10)
    @test_throws ArgumentError dfr[[:x1,:z]] .= ones(10)
    @test df == cdf
end

@testset "empty data frame corner case" begin
    df = DataFrame()
    @test_throws BoundsError df[1] .= 1
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

    df[:a] .= 1
    @test nrow(df) == 0
    @test names(df) == [:a]
    @test eltypes(df) == [Int]
    df = DataFrame()
    df[[:a, :b]] .= 1
    @test nrow(df) == 0
    @test names(df) == [:a, :b]
    @test eltypes(df) == [Int, Int]
    df[:c] .= 1.0
    @test nrow(df) == 0
    @test names(df) == [:a, :b, :c]
    @test eltypes(df) == [Int, Int, Float64]
    df[[:c,:d]] .= 1.0
    @test nrow(df) == 0
    @test names(df) == [:a, :b, :c, :d]
    @test eltypes(df) == [Int, Int, Float64, Float64]
end

end # module
