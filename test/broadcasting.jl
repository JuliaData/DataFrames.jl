module TestBroadcasting

using Test, DataFrames, PooledArrays, Random, CategoricalArrays

const ≅ = isequal

refdf = DataFrame(reshape(1.5:15.5, (3, 5)), :auto)

@testset "CartesianIndex" begin
    df = DataFrame(rand(2, 3), :auto)
    for i in axes(df, 1), j in axes(df, 2)
        @test df[i, j] == df[CartesianIndex(i, j)]
        r = rand()
        df[CartesianIndex(i, j)] = r
        @test df[i, j] == r
    end
    @test_throws BoundsError df[CartesianIndex(0, 1)]
    @test_throws BoundsError df[CartesianIndex(0, 0)]
    @test_throws BoundsError df[CartesianIndex(1, 0)]
    @test_throws BoundsError df[CartesianIndex(5, 1)]
    @test_throws BoundsError df[CartesianIndex(5, 5)]
    @test_throws BoundsError df[CartesianIndex(1, 5)]

    @test_throws BoundsError df[CartesianIndex(0, 1)] = 1
    @test_throws ArgumentError df[CartesianIndex(0, 0)] = 1
    @test_throws ArgumentError df[CartesianIndex(1, 0)] = 1
    @test_throws BoundsError df[CartesianIndex(5, 1)] = 1
    @test_throws ArgumentError df[CartesianIndex(5, 5)] = 1
    @test_throws ArgumentError df[CartesianIndex(1, 5)] = 1
end

@testset "broadcasting of AbstractDataFrame objects" begin
    for df in (copy(refdf), view(copy(refdf), :, :))
        @test identity.(df) == refdf
        @test identity.(df) !== df
        @test (x->x).(df) == refdf
        @test (x->x).(df) !== df
        @test (df .+ df) ./ 2 == refdf
        @test (df .+ df) ./ 2 !== df
        @test df .+ Matrix(df) == 2 .* df
        @test Matrix(df) .+ df == 2 .* df
        @test (Matrix(df) .+ df .== 2 .* df) == DataFrame(trues(size(df)), names(df))
        @test df .+ 1 == df .+ ones(size(df))
        @test df .+ axes(df, 1) == DataFrame(Matrix(df) .+ axes(df, 1), names(df))
        @test df .+ permutedims(axes(df, 2)) == DataFrame(Matrix(df) .+ permutedims(axes(df, 2)), names(df))
    end

    df1 = copy(refdf)
    df2 = view(copy(refdf), :, :)
    @test (df1 .+ df2) ./ 2 == refdf
    @test (df1 .- df2) == DataFrame(zeros(size(refdf)), names(refdf))
    @test (df1 .* df2) == refdf .^ 2
    @test (df1 ./ df2) == DataFrame(ones(size(refdf)), names(refdf))
end

@testset "broadcasting of AbstractDataFrame objects thrown exceptions" begin
    df = copy(refdf)
    dfv = view(df, :, 2:ncol(df))

    @test_throws DimensionMismatch df .+ dfv
    @test_throws DimensionMismatch df .+ df[2:end, :]

    @test_throws DimensionMismatch df .+ [1, 2]
    @test_throws DimensionMismatch df .+ [1 2]
    @test_throws DimensionMismatch df .+ rand(2, 2)
    @test_throws DimensionMismatch dfv .+ [1, 2]
    @test_throws DimensionMismatch dfv .+ [1 2]
    @test_throws DimensionMismatch dfv .+ rand(2, 2)

    df2 = copy(df)
    rename!(df2, [:x1, :x2, :x3, :x4, :y])
    @test_throws ArgumentError df .+ df2
    @test_throws ArgumentError df .+ 1 .+ df2
end

@testset "broadcasting data frames" begin
    df1 = DataFrame(x=1, y=2)
    df2 = DataFrame(x=[1, 11], y=[2, 12])
    @test df1 .+ df2 == DataFrame(x=[2, 12], y=[4, 14])

    df1 = DataFrame(x=1, y=2)
    df2 = DataFrame(x=[1, 11], y=[2, 12])
    x = df2.x
    y = df2.y
    df2 .+= df1
    @test df2.x === x
    @test df2.y === y
    @test df2 == DataFrame(x=[2, 12], y=[4, 14])

    df = DataFrame(x=[1, 11], y=[2, 12])
    dfv = view(df, 1:1, 1:2)
    df .-= dfv
    @test df == DataFrame(x=[0, 10], y=[0, 10])

    @test DataFrame() .+ DataFrame() == DataFrame()
    @test_throws ArgumentError DataFrame(a=1, b=1) .+ DataFrame(b=1, a=1)

    df = DataFrame(a=1, b=2)
    @test_throws ArgumentError df .= DataFrame(b=1, a=2)
    @test_throws ArgumentError df .= DataFrame(a=1, c=2)
    @test_throws ArgumentError df[!, [:a, :b]] .= DataFrame(b=1, a=2)
    @test_throws ArgumentError df[!, [:a, :b]] .= DataFrame(a=1, c=2)
end

@testset "broadcasting of AbstractDataFrame objects corner cases" begin
    df = DataFrame(c11=categorical(["a", "b"]), c12=categorical([missing, "b"]), c13=categorical(["a", missing]),
                   c21=categorical([1, 2]), c22=categorical([missing, 2]), c23=categorical([1, missing]),
                   p11=PooledArray(["a", "b"]), p12=PooledArray([missing, "b"]), p13=PooledArray(["a", missing]),
                   p21=PooledArray([1, 2]), p22=PooledArray([missing, 2]), p23=PooledArray([1, missing]),
                   b1=[true, false], b2=[missing, false], b3=[true, missing],
                   f1=[1.0, 2.0], f2=[missing, 2.0], f3=[1.0, missing],
                   s1=["a", "b"], s2=[missing, "b"], s3=["a", missing])

    df2 = DataFrame(c11=categorical(["a", "b"]), c12=[nothing, "b"], c13=["a", nothing],
                    c21=categorical([1, 2]), c22=[nothing, 2], c23=[1, nothing],
                    p11=["a", "b"], p12=[nothing, "b"], p13=["a", nothing],
                    p21=[1, 2], p22=[nothing, 2], p23=[1, nothing],
                    b1=[true, false], b2=[nothing, false], b3=[true, nothing],
                    f1=[1.0, 2.0], f2=[nothing, 2.0], f3=[1.0, nothing],
                    s1=["a", "b"], s2=[nothing, "b"], s3=["a", nothing])

    @test df ≅ identity.(df)
    @test df ≅ (x->x).(df)
    df3 = coalesce.(df, nothing)
    @test df2 == df3
    @test eltype.(eachcol(df2)) == eltype.(eachcol(df3))
    for i in axes(df, 2)
        @test typeof(df2[!, i]) == typeof(df3[!, i])
    end
    df4 = (x -> df[1, 1]).(df)
    @test names(df4) == names(df)
    @test all(isa.(eachcol(df4), Ref(CategoricalArray)))
    @test all(eachcol(df4) .== Ref(categorical(["a", "a"])))

    df5 = DataFrame(x=Any[1, 2, 3], y=Any[1, 2.0, big(3)])
    @test identity.(df5) == df5
    @test (x->x).(df5) == df5
    @test df5 .+ 1 == DataFrame(Matrix(df5) .+ 1, names(df5))
    @test eltype.(eachcol(identity.(df5))) == [Int, BigFloat]
    @test eltype.(eachcol((x->x).(df5))) == [Int, BigFloat]
    @test eltype.(eachcol(df5 .+ 1)) == [Int, BigFloat]
end

@testset "normal data frame and data frame row in broadcasted assignment - one column" begin
    df = copy(refdf)
    df[!, 1] .+= 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[!, 1] .+= 100
    @test df.x2 == [104.5, 105.5, 6.5]
    # reverse the performed operations
    df.x1 -= [1, 1, 1]
    df.x2 -= [100, 100, 0]
    @test df == DataFrame(reshape(1.5:15.5, (3, 5)), :auto)

    df = copy(refdf)
    df[:, 1] .+= 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv.x2 .+= 1
    @test dfv.x2 == [5.5, 6.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    dfr = df[1, 3:end]
    dfr[end-1:end] .= 10
    @test Vector(dfr) == [7.5, 10.0, 10.0]
    @test Matrix(df) == [2.5  5.5  7.5  10.0  10.0
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[!, 1] .+= [1, 2, 3]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    df = copy(refdf)
    df.x1 .+= [1, 2, 3]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv.x2 .+= [1, 2]
    @test dfv.x2 == [5.5, 7.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    dfr = df[1, 3:end]
    dfr[end-1:end] .= [10, 11]
    @test Vector(dfr) == [7.5, 10.0, 11.0]
    @test Matrix(df) == [2.5  5.5  7.5  10.0  11.0
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, 1] .+= [1, 2, 3]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, 1] .+= [1, 2]
    @test dfv.x2 == [5.5, 7.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    # test a more complex broadcasting pattern
    df = copy(refdf)
    df[!, 1] .+= [0, 1, 2] .+ 1
    @test df.x1 == df[!, 1] == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    df = copy(refdf)
    df[!, "x1"] .+= [0, 1, 2] .+ 1
    @test df."x1" == df[!, 1] == [2.5, 4.5, 6.5]
    @test df[:, Not("x1")] == refdf[:, 2:end]

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    dfv[!, 1] .+= [0, 1] .+ 1
    @test df == DataFrame([1.5  5.5  7.5  10.5  13.5
                           2.5  7.5  8.5  11.5  14.5
                           3.5  6.5  9.5  12.5  15.5], :auto)

    dfv = @view df[1:2, 2:end]
    @test_throws ArgumentError dfv[!, "x1"] .+= [0, 1] .+ 1
    @test df == DataFrame([1.5  5.5  7.5  10.5  13.5
                           2.5  7.5  8.5  11.5  14.5
                           3.5  6.5  9.5  12.5  15.5], :auto)

    df = copy(refdf)
    df.x1 .+= [0, 1, 2] .+ 1
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv.x2 .+= [0, 1] .+ 1
    @test dfv.x2 == [5.5, 7.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df."x1" .+= [0, 1, 2] .+ 1
    @test df."x1" == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv."x2" .+= [0, 1] .+ 1
    @test dfv."x2" == [5.5, 7.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    dfr = df[1, 3:end]
    dfr[end-1:end] .= [9, 10] .+ 1
    @test Vector(dfr) == [7.5, 10.0, 11.0]
    @test Matrix(df) == [2.5  5.5  7.5  10.0  11.0
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, 1] .+= [0, 1, 2] .+ 1
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, 1] .+= [0, 1] .+ 1
    @test dfv.x2 == [5.5, 7.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, "x1"] .+= [0, 1, 2] .+ 1
    @test df."x1" == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, "x2"] .+= [0, 1] .+ 1
    @test dfv."x2" == [5.5, 7.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    dfr = df[1, 3:end]
    @test_throws DimensionMismatch dfv[!, 1] .= fill(100, 2, 2)
    @test_throws DimensionMismatch dfv[!, 1] .= reshape(fill(200, 4), :, 2)
    @test_throws DimensionMismatch df[!, 1] .= rand(1, 2)
    @test_throws DimensionMismatch dfr[end-1:end] .= rand(3, 1)
    @test_throws DimensionMismatch df[:, 1] .= rand(1, 3)
    @test_throws DimensionMismatch dfv[:, 1] .= rand(1, 2)
    @test_throws DimensionMismatch df[!, 1] .= reshape(rand(3), 1, :)
    @test_throws DimensionMismatch dfr[end-1:end] .= reshape(rand(3), :, 1)
    @test_throws DimensionMismatch df[:, 1] .= reshape(rand(3), 1, :, 1)
    @test_throws DimensionMismatch dfv[:, 1] .= reshape(rand(2), 1, :, 1)

    df = copy(refdf)
    df[!, :x1] .+= 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[!, :x2] .+= 1

    dfr = df[1, 3:end]
    dfr[[:x4, :x5]] .= 10
    @test Vector(dfr) == [7.5, 10.0, 10.0]
    @test Matrix(df) == [2.5  5.5  7.5  10.0  10.0
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[!, "x1"] .+= 1
    @test df."x1" == [2.5, 3.5, 4.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[!, "x2"] .+= 1

    dfr = df[1, 3:end]
    dfr[["x4", "x5"]] .= 10
    @test Vector(dfr) == [7.5, 10.0, 10.0]
    @test Matrix(df) == [2.5  5.5  7.5  10.0  10.0
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, :x1] .+= 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, :x2] .+= 1
    @test dfv.x2 == [5.5, 6.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, "x1"] .+= 1
    @test df."x1" == [2.5, 3.5, 4.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, "x2"] .+= 1
    @test dfv."x2" == [5.5, 6.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         3.5  6.5  8.5  11.5  14.5
                         4.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[!, :x1] .+= [1, 2, 3]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[!, :x2] .+= [1, 2]

    dfr = df[1, 3:end]
    dfr[[:x4, :x5]] .= [10, 11]
    @test Vector(dfr) == [7.5, 10.0, 11.0]
    @test Matrix(df) == [2.5  5.5  7.5  10.0  11.0
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[!, "x1"] .+= [1, 2, 3]
    @test df."x1" == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[!, :x2] .+= [1, 2]

    dfr = df[1, 3:end]
    dfr[["x4", "x5"]] .= [10, 11]
    @test Vector(dfr) == [7.5, 10.0, 11.0]
    @test Matrix(df) == [2.5  5.5  7.5  10.0  11.0
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, :x1] .+= [1, 2, 3]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, :x2] .+= [1, 2]
    @test dfv.x2 == [5.5, 7.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, "x1"] .+= [1, 2, 3]
    @test df."x1" == [2.5, 4.5, 6.5]
    @test df[:, 2:end] == refdf[:, 2:end]

    dfv = @view df[1:2, 2:end]
    dfv[:, "x2"] .+= [1, 2]
    @test dfv."x2" == [5.5, 7.5]
    @test dfv[:, 2:end] == refdf[1:2, 3:end]
    @test Matrix(df) == [2.5  5.5  7.5  10.5  13.5
                         4.5  7.5  8.5  11.5  14.5
                         6.5  6.5  9.5  12.5  15.5]

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    dfr = df[1, 3:end]

    @test_throws DimensionMismatch dfv[!, :x2] .= fill(100, 2, 2)
    @test_throws DimensionMismatch dfv[!, 1] .= reshape(fill(200, 4), :, 2)
    @test_throws DimensionMismatch dfv[!, "x2"] .= fill(100, 2, 2)
    @test_throws DimensionMismatch df[!, :x1] .= rand(1, 3)
    @test_throws DimensionMismatch dfr[[:x4, :x5]] .= rand(3, 1)
    @test_throws DimensionMismatch df[:, :x1] .= rand(1, 3)
    @test_throws DimensionMismatch dfv[:, :x2] .= rand(1, 2)
    @test_throws DimensionMismatch df[!, 1] .= reshape(rand(3), 1, :)
    @test_throws DimensionMismatch dfr[end-1:end] .= reshape(rand(3), :, 1)
    @test_throws DimensionMismatch df[:, 1] .= reshape(rand(3), 1, :)
    @test_throws DimensionMismatch dfv[:, 1] .= reshape(rand(2), 1, :)
    @test_throws DimensionMismatch df[!, "x1"] .= rand(1, 3)
    @test_throws DimensionMismatch dfr[["x4", "x5"]] .= rand(3, 1)
    @test_throws DimensionMismatch df[:, "x1"] .= rand(1, 3)
    @test_throws DimensionMismatch dfv[:, "x2"] .= rand(1, 2)
end

@testset "normal data frame and data frame view in broadcasted assignment - two columns" begin
    df = copy(refdf)
    df[:, [1, 2]] .= Matrix(df[:, [1, 2]]) .+ 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df.x2 == [5.5, 6.5, 7.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [1, 2]] .= Matrix(dfv[:, [1, 2]]) .+ 1
    @test dfv.x3 == [8.5, 9.5]
    @test dfv.x4 == [11.5, 12.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, [1, 2]] .= Matrix(df[:, [1, 2]]) .+ 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df.x2 == [5.5, 6.5, 7.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [1, 2]] .= Matrix(dfv[:, [1, 2]]) .+ 1
    @test dfv.x3 == [8.5, 9.5]
    @test dfv.x4 == [11.5, 12.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, [1, 2]] .= Matrix(df[:, [1, 2]]) .+ [1 4
                                               2 5
                                               3 6]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df.x2 == [8.5, 10.5, 12.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [1, 2]] .= Matrix(dfv[:, [1, 2]]) .+ [1 3
                                               2 4]
    @test dfv.x3 == [8.5, 10.5]
    @test dfv.x4 == [13.5, 15.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    df = copy(refdf)
    df[:, [1, 2]] .= Matrix(df[:, [1, 2]]) .+ [1 4
                                               2 5
                                               3 6]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df.x2 == [8.5, 10.5, 12.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [1, 2]] .= Matrix(dfv[:, [1, 2]]) .+ [1 3
                                               2 4]
    @test dfv.x3 == [8.5, 10.5]
    @test dfv.x4 == [13.5, 15.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    @test_throws DimensionMismatch df[:, [1, 2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[:, [1, 2]] .= rand(2, 10)
    @test_throws DimensionMismatch df[:, [1, 2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[:, [1, 2]] .= rand(2, 10)

    df = copy(refdf)
    df[:, [:x1, :x2]] .= Matrix(df[:, [:x1, :x2]]) .+ 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df.x2 == [5.5, 6.5, 7.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [:x3, :x4]] .= Matrix(dfv[:, [:x3, :x4]]) .+ 1
    @test dfv.x3 == [8.5, 9.5]
    @test dfv.x4 == [11.5, 12.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, ["x1", "x2"]] .= Matrix(df[:, [:x1, :x2]]) .+ 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df.x2 == [5.5, 6.5, 7.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, ["x3", "x4"]] .= Matrix(dfv[:, [:x3, :x4]]) .+ 1
    @test dfv.x3 == [8.5, 9.5]
    @test dfv.x4 == [11.5, 12.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, [:x1, :x2]] .= Matrix(df[:, [:x1, :x2]]) .+ 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df.x2 == [5.5, 6.5, 7.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [:x3, :x4]] .= Matrix(dfv[:, [:x3, :x4]]) .+ 1
    @test dfv.x3 == [8.5, 9.5]
    @test dfv.x4 == [11.5, 12.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, ["x1", "x2"]] .= Matrix(df[:, [:x1, :x2]]) .+ 1
    @test df.x1 == [2.5, 3.5, 4.5]
    @test df.x2 == [5.5, 6.5, 7.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, ["x3", "x4"]] .= Matrix(dfv[:, [:x3, :x4]]) .+ 1
    @test dfv.x3 == [8.5, 9.5]
    @test dfv.x4 == [11.5, 12.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5  5.5  8.5  11.5  13.5
                         3.5  6.5  9.5  12.5  14.5
                         4.5  7.5  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, [:x1, :x2]] .= Matrix(df[:, [:x1, :x2]]) .+ [1 4
                                                       2 5
                                                       3 6]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df.x2 == [8.5, 10.5, 12.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [:x3, :x4]] .= Matrix(dfv[:, [:x3, :x4]]) .+ [1 3
                                                         2 4]
    @test dfv.x3 == [8.5, 10.5]
    @test dfv.x4 == [13.5, 15.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    df = copy(refdf)
    df[:, ["x1", "x2"]] .= Matrix(df[:, ["x1", "x2"]]) .+ [1 4
                                                           2 5
                                                           3 6]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df.x2 == [8.5, 10.5, 12.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, ["x3", "x4"]] .= Matrix(dfv[:, ["x3", "x4"]]) .+ [1 3
                                                             2 4]
    @test dfv.x3 == [8.5, 10.5]
    @test dfv.x4 == [13.5, 15.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    df = copy(refdf)
    df[:, [:x1, :x2]] .= Matrix(df[:, [:x1, :x2]]) .+ [1 4
                                                       2 5
                                                       3 6]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df.x2 == [8.5, 10.5, 12.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, [:x3, :x4]] .= Matrix(dfv[:, [:x3, :x4]]) .+ [1 3
                                                         2 4]
    @test dfv.x3 == [8.5, 10.5]
    @test dfv.x4 == [13.5, 15.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    df = copy(refdf)
    df[:, ["x1", "x2"]] .= Matrix(df[:, ["x1", "x2"]]) .+ [1 4
                                                           2 5
                                                           3 6]
    @test df.x1 == [2.5, 4.5, 6.5]
    @test df.x2 == [8.5, 10.5, 12.5]
    @test df[:, 3:end] == refdf[:, 3:end]

    dfv = @view df[1:2, 3:end]
    dfv[:, ["x3", "x4"]] .= Matrix(dfv[:, ["x3", "x4"]]) .+ [1 3
                                                             2 4]
    @test dfv.x3 == [8.5, 10.5]
    @test dfv.x4 == [13.5, 15.5]
    @test dfv[:, 3:end] == refdf[1:2, 5:end]
    @test Matrix(df) == [2.5   8.5   8.5  13.5  13.5
                         4.5  10.5  10.5  15.5  14.5
                         6.5  12.5   9.5  12.5  15.5]

    df = copy(refdf)
    dfv = @view df[1:2, 2:end]
    @test_throws DimensionMismatch df[:, [:x1, :x2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[:, [:x3, :x4]] .= rand(2, 10)
    @test_throws DimensionMismatch df[:, [:x1, :x2]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[:, [:x3, :x4]] .= rand(2, 10)
    @test_throws DimensionMismatch df[:, ["x1", "x2"]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[:, ["x3", "x4"]] .= rand(2, 10)
    @test_throws DimensionMismatch df[:, ["x1", "x2"]] .= rand(3, 10)
    @test_throws DimensionMismatch dfv[:, ["x3", "x4"]] .= rand(2, 10)

    df = copy(refdf)
    df[:, [1, 2]] .= [1 2
                      3 4
                      5 6]
    @test Matrix(df) == [1.0  2.0  7.5  10.5  13.5
                         3.0  4.0  8.5  11.5  14.5
                         5.0  6.0  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, [1, 2]] .= [1, 3, 5]
    @test Matrix(df) == [1.0  1.0  7.5  10.5  13.5
                         3.0  3.0  8.5  11.5  14.5
                         5.0  5.0  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, [1, 2]] .= reshape([1, 3, 5], 3, 1)
    @test Matrix(df) == [1.0  1.0  7.5  10.5  13.5
                         3.0  3.0  8.5  11.5  14.5
                         5.0  5.0  9.5  12.5  15.5]

    df = copy(refdf)
    df[:, [1, 2]] .= 1
    @test Matrix(df) == [1.0  1.0  7.5  10.5  13.5
                         1.0  1.0  8.5  11.5  14.5
                         1.0  1.0  9.5  12.5  15.5]

    df = copy(refdf)
    dfv = view(df, 2:3, 2:4)
    dfv[:, [1, 2]] .= [1 2
                       3 4]
    @test Matrix(df) == [1.5  4.5  7.5  10.5  13.5
                         2.5  1.0  2.0  11.5  14.5
                         3.5  3.0  4.0  12.5  15.5]

    df = copy(refdf)
    dfv = view(df, 2:3, 2:4)
    dfv[:, [1, 2]] .= [1, 3]
    @test Matrix(df) == [1.5  4.5  7.5  10.5  13.5
                         2.5  1.0  1.0  11.5  14.5
                         3.5  3.0  3.0  12.5  15.5]

    df = copy(refdf)
    dfv = view(df, 2:3, 2:4)
    dfv[:, [1, 2]] .= reshape([1, 3], 2, 1)
    @test Matrix(df) == [1.5  4.5  7.5  10.5  13.5
                         2.5  1.0  1.0  11.5  14.5
                         3.5  3.0  3.0  12.5  15.5]

    df = copy(refdf)
    dfv = view(df, 2:3, 2:4)
    dfv[:, [1, 2]] .= 1
    @test Matrix(df) == [1.5  4.5  7.5  10.5  13.5
                         2.5  1.0  1.0  11.5  14.5
                         3.5  1.0  1.0  12.5  15.5]
end

@testset "assignment to a whole data frame and data frame row" begin
    df = copy(refdf)
    df .= 10
    @test all(Matrix(df) .== 10)
    dfv = view(df, 1:2, 1:4)
    dfv .= 100
    @test Matrix(df) == [100.0  100.0  100.0  100.0  10.0
                        100.0  100.0  100.0  100.0  10.0
                         10.0   10.0   10.0   10.0  10.0]
    dfr = df[1, 1:2]
    dfr .= 1000
    @test Matrix(df) == [1000.0  1000.0  100.0  100.0  10.0
                         100.0   100.0  100.0  100.0  10.0
                         10.0    10.0   10.0   10.0  10.0]

    df = copy(refdf)
    df[:, :] .= 10
    @test all(Matrix(df) .== 10)
    dfv = view(df, 1:2, 1:4)
    dfv[:, :] .= 100
    @test Matrix(df) == [100.0  100.0  100.0  100.0  10.0
                         100.0  100.0  100.0  100.0  10.0
                         10.0   10.0   10.0   10.0  10.0]
    dfr = df[1, 1:2]
    dfr[:] .= 1000
    @test Matrix(df) == [1000.0  1000.0  100.0  100.0  10.0
                         100.0   100.0   100.0  100.0  10.0
                         10.0    10.0    10.0   10.0   10.0]

    df = copy(refdf)
    df[:, :] .= 10
    @test all(Matrix(df) .== 10)
    dfv = view(df, 1:2, 1:4)
    dfv[:, :] .= 100
    @test Matrix(df) == [100.0  100.0  100.0  100.0  10.0
                         100.0  100.0  100.0  100.0  10.0
                         10.0   10.0   10.0   10.0  10.0]
end

@testset "extending data frame in broadcasted assignment - one column" begin
    df = copy(refdf)
    df[!, :a] .= 1
    @test Matrix(df) == [1.5  4.5  7.5  10.5  13.5  1.0
                         2.5  5.5  8.5  11.5  14.5  1.0
                         3.5  6.5  9.5  12.5  15.5  1.0]
    @test names(df)[end] == "a"
    @test df[:, 1:end-1] == refdf
    df[!, :b] .= [1, 2, 3]
    @test Matrix(df) == [1.5  4.5  7.5  10.5  13.5  1.0 1.0
                         2.5  5.5  8.5  11.5  14.5  1.0 2.0
                         3.5  6.5  9.5  12.5  15.5  1.0 3.0]
    @test names(df)[end] == "b"
    @test df[:, 1:end-2] == refdf
    cdf = copy(df)
    @test_throws DimensionMismatch df[!, :c] .= ones(1, 3)
    @test df == cdf
    @test_throws DimensionMismatch df[!, :x] .= ones(4)
    @test df == cdf
    @test_throws ArgumentError df[!, 10] .= ones(3)
    @test df == cdf

    dfv = @view df[1:2, 2:end]
    @test_throws ArgumentError dfv[!, 10] .= ones(3)
    @test_throws ArgumentError dfv[!, :z] .= ones(3)
    @test df == cdf
    dfr = df[1, 3:end]
    @test_throws BoundsError dfr[10] .= ones(3)
    @test_throws ArgumentError dfr[:z] .= ones(3)
    @test df == cdf

    df = DataFrame()
    @test_throws DimensionMismatch df[!, :a] .= sin.(1:3)
    df[!, :b] .= sin.(1)
    df[!, :c] .= sin(1) .+ 1
    @test df == DataFrame(b=Float64[], c=Float64[])

    df = copy(refdf)
    df[!, "a"] .= 1
    @test Matrix(df) == [1.5  4.5  7.5  10.5  13.5  1.0
                         2.5  5.5  8.5  11.5  14.5  1.0
                         3.5  6.5  9.5  12.5  15.5  1.0]
    @test names(df)[end] == "a"
    @test df[:, 1:end-1] == refdf
    df[!, "b"] .= [1, 2, 3]
    @test Matrix(df) == [1.5  4.5  7.5  10.5  13.5  1.0 1.0
                         2.5  5.5  8.5  11.5  14.5  1.0 2.0
                         3.5  6.5  9.5  12.5  15.5  1.0 3.0]
    @test names(df)[end] == "b"
    @test df[:, 1:end-2] == refdf
    cdf = copy(df)
    @test_throws DimensionMismatch df[!, "c"] .= ones(1, 3)
    @test df == cdf
    @test_throws DimensionMismatch df[!, "x"] .= ones(4)
    @test df == cdf
    @test_throws ArgumentError df[!, 10] .= ones(3)
    @test df == cdf

    dfv = @view df[1:2, 2:end]
    @test_throws ArgumentError dfv[!, 10] .= ones(3)
    @test_throws ArgumentError dfv[!, "z"] .= ones(3)
    @test df == cdf
    dfr = df[1, 3:end]
    @test_throws BoundsError dfr[10] .= ones(3)
    @test_throws ArgumentError dfr["z"] .= ones(3)
    @test df == cdf

    df = DataFrame()
    @test_throws DimensionMismatch df[!, "a"] .= sin.(1:3)
    df[!, "b"] .= sin.(1)
    df[!, "c"] .= sin(1) .+ 1
    @test df == DataFrame(b=Float64[], c=Float64[])
end

@testset "empty data frame corner case" begin
    df = DataFrame()
    @test_throws ArgumentError df[!, 1] .= 1
    @test_throws ArgumentError df[!, 2] .= 1
    @test_throws ArgumentError df[!, [:a, :b]] .= [1]
    @test_throws ArgumentError df[!, [:a, :b]] .= 1
    @test_throws DimensionMismatch df[!, :a] .= [1 2]
    @test_throws DimensionMismatch df[!, :a] .= [1, 2]
    @test_throws DimensionMismatch df[!, :a] .= sin.(1) .+ [1, 2]
    @test_throws ArgumentError df[!, ["a", "b"]] .= [1]
    @test_throws ArgumentError df[!, ["a", "b"]] .= 1
    @test_throws DimensionMismatch df[!, "a"] .= [1 2]
    @test_throws DimensionMismatch df[!, "a"] .= [1, 2]
    @test_throws DimensionMismatch df[!, "a"] .= sin.(1) .+ [1, 2]

    for rhs in [1, [1], Int[], "abc", ["abc"]]
        df = DataFrame()
        df[!, :a] .= rhs
        @test size(df) == (0, 1)
        @test eltype(df[!, 1]) == (rhs isa AbstractVector ? eltype(rhs) : typeof(rhs))

        df = DataFrame()
        df[!, :a] .= length.(rhs)
        @test size(df) == (0, 1)
        @test eltype(df[!, 1]) == Int

        df = DataFrame()
        df[!, :a] .= length.(rhs) .+ 1
        @test size(df) == (0, 1)
        @test eltype(df[!, 1]) == Int

        df = DataFrame()
        @. df[!, :a] = length(rhs) + 1
        @test size(df) == (0, 1)
        @test eltype(df[!, 1]) == Int

        df = DataFrame(x=Int[])
        df[!, :a] .= rhs
        @test size(df) == (0, 2)
        @test eltype(df[!, 2]) == (rhs isa AbstractVector ? eltype(rhs) : typeof(rhs))

        df = DataFrame(x=Int[])
        df[!, :a] .= length.(rhs)
        @test size(df) == (0, 2)
        @test eltype(df[!, 2]) == Int

        df = DataFrame(x=Int[])
        df[!, :a] .= length.(rhs) .+ 1
        @test size(df) == (0, 2)
        @test eltype(df[!, 2]) == Int

        df = DataFrame(x=Int[])
        @. df[!, :a] = length(rhs) + 1
        @test size(df) == (0, 2)
        @test eltype(df[!, 2]) == Int

        df = DataFrame()
        df[!, "a"] .= rhs
        @test size(df) == (0, 1)
        @test eltype(df[!, 1]) == (rhs isa AbstractVector ? eltype(rhs) : typeof(rhs))

        df = DataFrame()
        df[!, "a"] .= length.(rhs)
        @test size(df) == (0, 1)
        @test eltype(df[!, 1]) == Int

        df = DataFrame()
        df[!, "a"] .= length.(rhs) .+ 1
        @test size(df) == (0, 1)
        @test eltype(df[!, 1]) == Int

        df = DataFrame()
        @. df[!, "a"] = length(rhs) + 1
        @test size(df) == (0, 1)
        @test eltype(df[!, 1]) == Int

        df = DataFrame(x=Int[])
        df[!, "a"] .= rhs
        @test size(df) == (0, 2)
        @test eltype(df[!, 2]) == (rhs isa AbstractVector ? eltype(rhs) : typeof(rhs))

        df = DataFrame(x=Int[])
        df[!, "a"] .= length.(rhs)
        @test size(df) == (0, 2)
        @test eltype(df[!, 2]) == Int

        df = DataFrame(x=Int[])
        df[!, "a"] .= length.(rhs) .+ 1
        @test size(df) == (0, 2)
        @test eltype(df[!, 2]) == Int

        df = DataFrame(x=Int[])
        @. df[!, "a"] = length(rhs) + 1
        @test size(df) == (0, 2)
        @test eltype(df[!, 2]) == Int
    end

    df = DataFrame()
    df .= 1
    @test df == DataFrame()
    df .= [1]
    @test df == DataFrame()
    df .= ones(1, 1)
    @test df == DataFrame()
    @test_throws DimensionMismatch df .= ones(1, 2)
    @test_throws DimensionMismatch df .= ones(1, 2, 1)

    df = DataFrame(a=[])
    df[!, :b] .= sin.(1)
    @test eltype(df.b) == Float64
    df[!, :b] .= [1]
    @test eltype(df.b) == Int
    df[!, :b] .= 'a'
    @test eltype(df.b) == Char
    @test names(df) == ["a", "b"]

    c = categorical(["a", "b", "c"])
    df = DataFrame()
    @test_throws DimensionMismatch df[!, :a] .= c

    df[!, :b] .= c[1]
    @test nrow(df) == 0
    @test df.b isa CategoricalVector{String}

    df = DataFrame(a=[])
    df[!, "b"] .= sin.(1)
    @test eltype(df."b") == Float64
    df[!, "b"] .= [1]
    @test eltype(df."b") == Int
    df[!, "b"] .= 'a'
    @test eltype(df."b") == Char
    @test names(df) == ["a", "b"]

    c = categorical(["a", "b", "c"])
    df = DataFrame()
    @test_throws DimensionMismatch df[!, "a"] .= c

    df[!, "b"] .= c[1]
    @test nrow(df) == 0
    @test df."b" isa CategoricalVector{String}
end

@testset "test categorical values" begin
    for v in Any[categorical([1, 2, 3]), categorical([1, 2, missing]),
                 categorical([missing, 1, 2]),
                 categorical(["1", "2", "3"]), categorical(["1", "2", missing]),
                 categorical([missing, "1", "2"])]
        df = copy(refdf)
        df[!, :c1] .= v
        @test df.c1 ≅ v
        @test df.c1 !== v
        @test df.c1 isa CategoricalVector
        @test levels(df.c1) == levels(v)
        @test levels(df.c1) !== levels(v)
        df[!, :c2] .= v[2]
        @test df.c2 == fill(v[2], 3)
        @test df.c2 isa CategoricalVector
        @test levels(df.c2) == levels(v)
        df[!, :c3] .= (x->x).(v)
        @test df.c3 ≅ v
        @test df.c3 !== v
        @test df.c3 isa CategoricalVector
        @test levels(df.c3) == levels(v)
        @test levels(df.c3) !== levels(v)
        df[!, :c4] .= identity.(v)
        @test df.c4 ≅ v
        @test df.c4 !== v
        @test df.c4 isa CategoricalVector
        @test levels(df.c4) == levels(v)
        @test levels(df.c4) !== levels(v)
        df[!, :c5] .= (x->v[2]).(v)
        @test unique(df.c5) == [unwrap(v[2])]
        @test df.c5 isa CategoricalVector
        @test levels(df.c5) == levels(v)
    end
end

@testset "scalar broadcasting" begin
    a = DataFrame(x=zeros(2))
    a .= 1 ./ (1 + 2)
    @test a.x == [1/3, 1/3]
    a .= 1 ./ (1 .+ 3)
    @test a.x == [1/4, 1/4]
    a .= sqrt.(1 ./ 2)
    @test a.x == [sqrt(1/2), sqrt(1/2)]
end

@testset "tuple broadcasting" begin
    X = DataFrame(zeros(2, 3), :auto)
    X .= (1, 2)
    @test X == DataFrame([1 1 1; 2 2 2], :auto)

    X = DataFrame(zeros(2, 3), :auto)
    X .= (1, 2) .+ 10 .- X
    @test X == DataFrame([11 11 11; 12 12 12], :auto)

    X = DataFrame(zeros(2, 3), :auto)
    X .+= (1, 2) .+ 10
    @test X == DataFrame([11 11 11; 12 12 12], :auto)

    df = DataFrame(rand(2, 3), :auto)
    @test floor.(Int, df ./ (1,)) == DataFrame(zeros(Int, 2, 3), :auto)
    df .= floor.(Int, df ./ (1,))
    @test df == DataFrame(zeros(2, 3), :auto)

    df = DataFrame(rand(2, 3), :auto)
    @test_throws InexactError convert.(Int, df)
    df2 = convert.(Int, floor.(df))
    @test df2 == DataFrame(zeros(Int, 2, 3), :auto)
    @test eltype.(eachcol(df2)) == [Int, Int, Int]
end

@testset "scalar on assignment side" begin
    df = DataFrame(rand(2, 3), :auto)
    @test_throws MethodError df[1, 1] .= df[1, 1] .- df[1, 1]
    df[1, 1:1] .= df[1, 1] .- df[1, 1]
    @test df[1, 1] == 0
    @test_throws MethodError df[1, 2] .-= df[1, 2]
    df[1:1, 2] .-= df[1, 2]
    @test df[1, 2] == 0
end

@testset "nothing test" begin
    X = DataFrame(Any[1 2; 3 4], :auto)
    X .= nothing
    @test (X .== nothing) == DataFrame(trues(2, 2), :auto)

    X = DataFrame([1 2; 3 4], :auto)
    @test_throws MethodError X .= nothing
    @test X == DataFrame([1 2; 3 4], :auto)

    X = DataFrame([1 2; 3 4], :auto)
    foreach(i -> X[!, i] .= nothing, axes(X, 2))
    @test (X .== nothing) == DataFrame(trues(2, 2), :auto)
end

@testset "aliasing test" begin
    df = DataFrame(x=[1, 2])
    y = view(df.x, [2, 1])
    df .= y
    @test df.x == [2, 1]

    df = DataFrame(x=[1, 2])
    y = view(df.x, [2, 1])
    dfv = view(df, :, :)
    dfv .= y
    @test df.x == [2, 1]

    df = DataFrame(x=2, y=1, z=1)
    dfr = df[1, :]
    y = view(df.x, 1)
    dfr .= 2 .* y
    @test Vector(dfr) == [4, 4, 4]

    df = DataFrame(x=[1, 2], y=[11, 12])
    df2 = DataFrame()
    df2.x = [-1, -2]
    df2.y = df.x
    df3 = copy(df2)
    df .= df2
    @test df == df3

    Random.seed!(1234)
    for i in 1:10
        df1 = DataFrame(rand(100, 100), :auto)
        df2 = copy(df1)
        for i in 1:100
            df2[!, rand(1:100)] = df1[!, i]
        end
        df3 = copy(df2)
        df1 .= df2
        @test df1 == df3
        @test df2 != df3
    end

    for i in 1:10
        df1 = DataFrame(rand(100, 100), :auto)
        df2 = copy(df1)
        for i in 1:100
            df2[!, rand(1:100)] = df1[!, i]
        end
        df3 = copy(df2)
        df1 .= view(df2, :, :)
        @test df1 == df3
        @test df2 != df3
    end

    for i in 1:10
        df1 = DataFrame(rand(100, 100), :auto)
        df2 = copy(df1)
        for i in 1:100
            df2[!, rand(1:100)] = df1[!, i]
        end
        df3 = copy(df2)
        view(df1, :, :) .= df2
        @test df1 == df3
        @test df2 != df3
    end

    for i in 1:10
        df1 = DataFrame(rand(100, 100), :auto)
        df2 = copy(df1)
        df3 = copy(df1)
        for i in 1:100
            df2[!, rand(1:100)] = df1[!, i]
            df3[!, rand(1:100)] = df1[!, i]
        end
        df6 = copy(df2)
        df7 = copy(df3)
        df4 = DataFrame(sin.(df1[1, 1] .+ copy(df1[!, 1]) .+ Matrix(df2) ./ Matrix(df3)), names(df3))
        df5 = sin.(view(df1, 1, 1) .+ df1[!, 1] .+ df2 ./ df3)
        df1 .= sin.(view(df1, 1, 1) .+ df1[!, 1] .+ df2 ./ df3)
        @test df1 == df4 == df5
        @test df2 != df6
        @test df3 != df7
    end

    for i in 1:10
        df1 = DataFrame(rand(100, 100), :auto)
        df2 = copy(df1)
        df3 = copy(df1)
        for i in 1:100
            df2[!, rand(1:100)] = df1[!, i]
            df3[!, rand(1:100)] = df1[!, i]
        end
        df6 = copy(df2)
        df7 = copy(df3)
        df4 = DataFrame(sin.(df1[1, 1] .+ copy(df1[!, 1]) .+ Matrix(df2) ./ Matrix(df3)), names(df3))
        df5 = sin.(view(df1, 1, 1) .+ df1[!, 1] .+ view(df2, :, :) ./ df3)
        df1 .= sin.(view(df1[!, 1], 1) .+ view(df1[!, 1], :) .+ df2 ./ view(df3, :, :))
        @test df1 == df4 == df5
        @test df2 != df6
        @test df3 != df7
    end

    for i in 1:10
        df1 = DataFrame(rand(100, 100), :auto)
        df2 = copy(df1)
        df3 = copy(df1)
        for i in 1:100
            df2[!, rand(1:100)] = df1[!, i]
            df3[!, rand(1:100)] = df1[!, i]
        end
        df6 = copy(df2)
        df7 = copy(df3)
        df4 = DataFrame(sin.(df1[1, 1] .+ copy(df1[!, 1]) .+ Matrix(df2) ./ Matrix(df3)), names(df3))
        df5 = sin.(view(df1, 1, 1) .+ df1[!, 1] .+ view(df2, :, :) ./ df3)
        view(df1, :, :) .= sin.(view(df1[!, 1], 1) .+ view(df1[!, 1], :) .+ df2 ./ view(df3, :, :))
        @test df1 == df4 == df5
        @test df2 != df6
        @test df3 != df7
    end
end

@testset "@. test" begin
    df = DataFrame(rand(2, 3), :auto)
    sdf = view(df, 1:1, :)
    dfm = Matrix(df)
    sdfm = Matrix(sdf)

    r1 = @. (df + sdf + 5) / sdf
    @test r1 isa DataFrame

    @. df = sin(sdf / (df + 1))
    @. dfm = sin(sdfm / (dfm + 1))
    @test df == DataFrame(dfm, names(df))
end

@testset "test common cases" begin
    m = rand(1000, 10)
    df = DataFrame(m, :auto)
    @test df .+ 1 == DataFrame(m .+ 1, names(df))
    @test df .+ transpose(1:10) == DataFrame(m .+ transpose(1:10), names(df))
    @test df .+ (1:1000) == DataFrame(m .+ (1:1000), names(df))
    @test df .+ m == DataFrame(m .+ m, names(df))
    @test m .+ df == DataFrame(m .+ m, names(df))
    @test df .+ df == DataFrame(m .+ m, names(df))

    df .+= 1
    m .+= 1
    @test df == DataFrame(m, names(df))
    df .+= transpose(1:10)
    m .+= transpose(1:10)
    @test df == DataFrame(m, names(df))
    df .+= (1:1000)
    m .+= (1:1000)
    @test df == DataFrame(m, names(df))
    df .+= df
    m .+= m
    @test df == DataFrame(m, names(df))
    df2 = copy(df)
    m2 = copy(m)
    df .+= df .+ df2 .+ m2 .+ 1
    m .+= m .+ df2 .+ m2 .+ 1
    @test df == DataFrame(m, names(df))
end

@testset "data frame only on left hand side broadcasting assignment" begin
    Random.seed!(1234)

    m = rand(3, 4);
    m2 = copy(m);
    m3 = copy(m);
    df = DataFrame(a=view(m, :, 1), b=view(m, :, 1),
                   c=view(m, :, 1), d=view(m, :, 1), copycols=false);
    df2 = copy(df)
    mdf = Matrix(df)

    @test m .+ df == m2 .+ df
    @test Matrix(m .+ df) == m .+ mdf
    @test sin.(m .+ df) .+ 1 .+ m2 == sin.(m2 .+ df) .+ 1 .+ m
    @test Matrix(m .+ df ./ 2 .* df2) == m .+ mdf ./ 2 .* mdf

    m2 .+= df .+ 1 ./ df2
    m .+= df .+ 1 ./ df2
    @test m2 == m
    for col in eachcol(df)
        @test col == m[:, 1]
    end
    for col in eachcol(df2)
        @test col == m3[:, 1]
    end

    m = rand(3, 4);
    m2 = copy(m);
    m3 = copy(m);
    df = view(DataFrame(a=view(m, :, 1), b=view(m, :, 1),
                        c=view(m, :, 1), d=view(m, :, 1), copycols=false),
              [3, 2, 1], :)
    df2 = copy(df)
    mdf = Matrix(df)

    @test m .+ df == m2 .+ df
    @test Matrix(m .+ df) == m .+ mdf
    @test sin.(m .+ df) .+ 1 .+ m2 == sin.(m2 .+ df) .+ 1 .+ m
    @test Matrix(m .+ df ./ 2 .* df2) == m .+ mdf ./ 2 .* mdf

    m2 .+= df .+ 1 ./ df2
    m .+= df .+ 1 ./ df2
    @test m2 == m
    for col in eachcol(df)
        @test col == m[3:-1:1, 1]
    end
    for col in eachcol(df2)
        @test col == m3[3:-1:1, 1]
    end
end

@testset "broadcasting with 3-dimensional object" begin
    y = zeros(4, 3, 2)
    df = DataFrame(ones(4, 3), :auto)
    @test_throws DimensionMismatch df .+ y
    @test_throws DimensionMismatch y .+ df
    @test_throws DimensionMismatch df .+= y
    y .+= df
    @test y == ones(4, 3, 2)
end

@testset "additional checks of post-! broadcasting rules" begin
    df = copy(refdf)
    v1 = df[!, 1]
    @test_throws MethodError df[CartesianIndex(1, 1)] .= 1
    @test_throws MethodError df[CartesianIndex(1, 1)] .= "d"
    @test_throws DimensionMismatch df[CartesianIndex(1, 1)] .= [1, 2]

    df = copy(refdf)
    v1 = df[!, 1]
    @test_throws MethodError df[1, 1] .= 1
    @test_throws MethodError df[1, 1] .= "d"
    @test_throws DimensionMismatch df[1, 1] .= [1, 2]

    df = copy(refdf)
    v1 = df[!, 1]
    @test_throws MethodError df[1, :x1] .= 1
    @test_throws MethodError df[1, :x1] .= "d"
    @test_throws DimensionMismatch df[1, :x1] .= [1, 2]

    df = copy(refdf)
    v1 = df[!, 1]
    @test_throws MethodError df[1, "x1"] .= 1
    @test_throws MethodError df[1, "x1"] .= "d"
    @test_throws DimensionMismatch df[1, "x1"] .= [1, 2]

    df = copy(refdf)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[1, 1:2] .= 'd'
    @test v1 == [100.0, 2.5, 3.5]
    @test v2 == [100.0, 5.5, 6.5]
    @test_throws MethodError df[1, 1:2] .= "d"
    @test v1 == [100.0, 2.5, 3.5]
    @test v2 == [100.0, 5.5, 6.5]
    df[1, 1:2] .= 'e':'f'
    @test v1 == [101.0, 2.5, 3.5]
    @test v2 == [102.0, 5.5, 6.5]
    @test_throws DimensionMismatch df[1, 1:2] .= ['d' 'd']
    @test v1 == [101.0, 2.5, 3.5]
    @test v2 == [102.0, 5.5, 6.5]

    df = copy(refdf)
    v1 = df[!, 1]
    df[:, 1] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws MethodError df[:, 1] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch df[:, 1] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]

    df = copy(refdf)
    v1 = df[!, 1]
    df[:, :x1] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws MethodError df[:, :x1] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch df[:, :x1] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]

    df = copy(refdf)
    v1 = df[!, 1]
    df[:, 1] .= 'd':'f'
    @test v1 == [100.0, 101.0, 102.0]
    @test_throws MethodError df[:, 1] .= ["d", "e", "f"]
    @test v1 == [100.0, 101.0, 102.0]

    df = copy(refdf)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[:, 1:2] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]
    @test_throws MethodError df[:, 1:2] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch df[:, 1:2] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]

    df = copy(refdf)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[:, 1:2] .= 'd':'f'
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [100.0, 101.0, 102.0]
    @test_throws MethodError df[:, 1:2] .= ["d", "e", "f"]
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [100.0, 101.0, 102.0]

    df = copy(refdf)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[:, 1:2] .= permutedims('d':'e')
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [101.0, 101.0, 101.0]

    df = copy(refdf)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[:, 1:2] .= reshape('d':'i', 3, :)
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [103.0, 104.0, 105.0]
    @test_throws DimensionMismatch df[:, 1:2] .= reshape('d':'i', 1, :, 3)
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [103.0, 104.0, 105.0]

    df = copy(refdf)
    v1 = df[!, 1]
    v1′ = df[:, 1]
    df[!, 1] .= 100.0
    @test df.x1 == [100.0, 100.0, 100.0]
    @test v1 == v1′
    df[!, 1] .= 'd'
    @test df.x1 == ['d', 'd', 'd']
    @test v1 == v1′
    @test_throws DimensionMismatch df[!, 1] .= [1 2 3]
    @test df.x1 == ['d', 'd', 'd']
    @test v1 == v1′

    df = copy(refdf)
    v1 = df[!, 1]
    v1′ = df[:, 1]
    df[!, :x1] .= 100.0
    @test df.x1 == [100.0, 100.0, 100.0]
    @test v1 == v1′
    df[!, :x1] .= 'd'
    @test df.x1 == ['d', 'd', 'd']
    @test v1 == v1′
    @test_throws DimensionMismatch df[!, :x1] .= [1 2 3]
    @test df.x1 == ['d', 'd', 'd']
    @test v1 == v1′

    df = copy(refdf)
    df[!, :newcol] .= 100.0
    @test df.newcol == [100.0, 100.0, 100.0]
    @test df[:, 1:end-1] == refdf

    df = copy(refdf)
    df[!, "newcol"] .= 100.0
    @test df.newcol == [100.0, 100.0, 100.0]
    @test df[:, 1:end-1] == refdf

    df = copy(refdf)
    df[!, :newcol] .= 'd'
    @test df.newcol == ['d', 'd', 'd']
    @test df[:, 1:end-1] == refdf

    df = copy(refdf)
    df[!, "newcol"] .= 'd'
    @test df.newcol == ['d', 'd', 'd']
    @test df[:, 1:end-1] == refdf

    df = copy(refdf)
    @test_throws DimensionMismatch df[!, :newcol] .= [1 2 3]
    @test df == refdf

    df = copy(refdf)
    @test_throws DimensionMismatch df[!, "newcol"] .= [1 2 3]
    @test df == refdf

    df = copy(refdf)
    @test_throws ArgumentError df[!, 10] .= 'a'
    @test df == refdf
    @test_throws ArgumentError df[!, 10] .= [1, 2, 3]
    @test df == refdf
    @test_throws ArgumentError df[!, 10] .= [1 2 3]
    @test df == refdf

    df = copy(refdf)
    df[!, 1:2] .= 'a'
    @test Matrix(df) == ['a'  'a'  7.5  10.5  13.5
                         'a'  'a'  8.5  11.5  14.5
                         'a'  'a'  9.5  12.5  15.5]

    df = copy(refdf)
    v1 = df[!, 1]
    if isdefined(Base, :dotgetproperty) # Introduced in Julia 1.7
        df.x1 .= 'd'
        @test df.x1 == ['d', 'd', 'd']
        @test eltype(df.x1) === Char
        @test_throws MethodError df[:, 1] .= "d"
        @test_throws DimensionMismatch df[:, 1] .= [1 2 3]
        @test v1 == [1.5, 2.5, 3.5]
    else
        df.x1 .= 'd'
        @test v1 == [100.0, 100.0, 100.0]
        @test_throws MethodError df[:, 1] .= "d"
        @test v1 == [100.0, 100.0, 100.0]
        @test_throws DimensionMismatch df[:, 1] .= [1 2 3]
        @test v1 == [100.0, 100.0, 100.0]
    end

    if isdefined(Base, :dotgetproperty) # Introduced in Julia 1.7
        df = DataFrame(a=1:4, b=1, c=2)
        df.a .= 'a':'d'
        @test df == DataFrame(a='a':'d', b=1, c=2)
        dfv = view(df, 2:3, 2:3)
        x = df.b
        dfv.b .= 0
        @test df.b == [1, 0, 0, 1]
        @test x == [1, 1, 1, 1]
    else
        df = DataFrame(a=1:4, b=1, c=2)
        df.a .= 'a':'d'
        @test df == DataFrame(a=97:100, b=1, c=2)
        dfv = view(df, 2:3, 2:3)
        x = df.b
        dfv.b .= 0
        @test df.b == [1, 0, 0, 1]
        @test x === df.b
    end

    df = copy(refdf)
    if isdefined(Base, :dotgetproperty) # Introduced in Julia 1.7
        df.newcol .= 'd'
        @test df == [refdf DataFrame(newcol=fill('d', 3))]
    else
        @test_throws ArgumentError df.newcol .= 'd'
        @test df == refdf
    end

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    @test_throws MethodError df[CartesianIndex(1, 1)] .= 1
    @test_throws MethodError df[CartesianIndex(1, 1)] .= "d"
    @test_throws DimensionMismatch df[CartesianIndex(1, 1)] .= [1, 2]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    @test_throws MethodError df[1, 1] .= 1
    @test_throws MethodError df[1, 1] .= "d"
    @test_throws DimensionMismatch df[1, 1] .= [1, 2]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    @test_throws MethodError df[1, :x1] .= 1
    @test_throws MethodError df[1, :x1] .= "d"
    @test_throws DimensionMismatch df[1, :x1] .= [1, 2]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[1, 1:2] .= 'd'
    @test v1 == [100.0, 2.5, 3.5]
    @test v2 == [100.0, 5.5, 6.5]
    @test_throws MethodError df[1, 1:2] .= "d"
    @test v1 == [100.0, 2.5, 3.5]
    @test v2 == [100.0, 5.5, 6.5]
    df[1, 1:2] .= 'e':'f'
    @test v1 == [101.0, 2.5, 3.5]
    @test v2 == [102.0, 5.5, 6.5]
    @test_throws DimensionMismatch df[1, 1:2] .= ['d' 'd']
    @test v1 == [101.0, 2.5, 3.5]
    @test v2 == [102.0, 5.5, 6.5]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    df[:, 1] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws MethodError df[:, 1] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch df[:, 1] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    df[:, :x1] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws MethodError df[:, :x1] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch df[:, :x1] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    df[:, 1] .= 'd':'f'
    @test v1 == [100.0, 101.0, 102.0]
    @test_throws MethodError df[:, 1] .= ["d", "e", "f"]
    @test v1 == [100.0, 101.0, 102.0]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[:, 1:2] .= 'd'
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]
    @test_throws MethodError df[:, 1:2] .= "d"
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]
    @test_throws DimensionMismatch df[:, 1:2] .= [1 2 3]
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [100.0, 100.0, 100.0]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[:, 1:2] .= 'd':'f'
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [100.0, 101.0, 102.0]
    @test_throws MethodError df[:, 1:2] .= ["d", "e", "f"]
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [100.0, 101.0, 102.0]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[:, 1:2] .= permutedims('d':'e')
    @test v1 == [100.0, 100.0, 100.0]
    @test v2 == [101.0, 101.0, 101.0]

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    v2 = df[!, 2]
    df[:, 1:2] .= reshape('d':'i', 3, :)
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [103.0, 104.0, 105.0]
    @test_throws DimensionMismatch df[:, 1:2] .= reshape('d':'i', 1, :, 3)
    @test v1 == [100.0, 101.0, 102.0]
    @test v2 == [103.0, 104.0, 105.0]

    df = view(copy(refdf), :, :)
    df[!, 1] .= 100
    @test parent(df).x1 == [100, 100, 100]
    @test eltype(parent(df).x1) == Float64

    df = view(copy(refdf), :, :)
    df[!, :x1] .= 100.0
    @test parent(df).x1 == [100, 100, 100]
    @test eltype(parent(df).x1) == Float64

    df = view(copy(refdf), :, :)
    df[!, :newcol] .= 100.0
    @test parent(df).newcol == [100, 100, 100]
    @test eltype(parent(df).newcol) == Union{Float64, Missing}

    df = view(copy(refdf), :, :)
    @test_throws ArgumentError df[!, 10] .= 'a'
    @test df == refdf
    @test_throws ArgumentError df[!, 10] .= [1, 2, 3]
    @test df == refdf
    @test_throws ArgumentError df[!, 10] .= [1 2 3]
    @test df == refdf

    df = view(copy(refdf), :, :)
    df[!, 1:2] .= 'a'
    @test parent(df).x1 == parent(df).x2 == ['a', 'a', 'a']
    @test eltype(parent(df).x1) === Any

    df = view(copy(refdf), :, :)
    v1 = df[!, 1]
    if isdefined(Base, :dotgetproperty) # Introduced in Julia 1.7
        df.x1 .= 'd'
        @test df.x1 == ['d', 'd', 'd']
        @test eltype(df.x1) === Any
        df[:, 1] .= "d"
        @test df.x1 == ["d", "d", "d"]
        @test_throws DimensionMismatch df[:, 1] .= [1 2 3]
        @test v1 == [1.5, 2.5, 3.5]
    else
        df.x1 .= 'd'
        @test v1 == [100.0, 100.0, 100.0]
        @test_throws MethodError df[:, 1] .= "d"
        @test v1 == [100.0, 100.0, 100.0]
        @test_throws DimensionMismatch df[:, 1] .= [1 2 3]
        @test v1 == [100.0, 100.0, 100.0]
    end

    df = view(copy(refdf), :, :)
    if VERSION >= v"1.7"
        df.newcol .= 'd'
        @test df.newcol == fill('d', 3)
    else
        @test_throws ArgumentError df.newcol .= 'd'
        @test df == refdf
    end
end

@testset "DataFrameRow getproperty broadcasted assignment" begin
    df = DataFrame(a=[[1, 2], [3, 4]], b=[[5, 6], [7, 8]])
    dfr = df[1, :]
    dfr.a .= 10
    @test df == DataFrame(a=[[10, 10], [3, 4]], b=[[5, 6], [7, 8]])
    @test_throws MethodError dfr.a .= ["a", "b"]

    df = DataFrame(a=[[1, 2], [3, 4]], b=[[5, 6], [7, 8]])
    dfr = df[1, 1:1]
    dfr.a .= 10
    @test df == DataFrame(a=[[10, 10], [3, 4]], b=[[5, 6], [7, 8]])
    @test_throws MethodError dfr.a .= ["a", "b"]

    df = DataFrame(a=[[1, 2], [3, 4]], b=[[5, 6], [7, 8]])
    dfr = df[1, :]
    dfr."a" .= 10
    @test df == DataFrame(a=[[10, 10], [3, 4]], b=[[5, 6], [7, 8]])
    @test_throws MethodError dfr."a" .= ["a", "b"]

    df = DataFrame(a=[[1, 2], [3, 4]], b=[[5, 6], [7, 8]])
    dfr = df[1, 1:1]
    dfr."a" .= 10
    @test df == DataFrame(a=[[10, 10], [3, 4]], b=[[5, 6], [7, 8]])
    @test_throws MethodError dfr."a" .= ["a", "b"]
end

@testset "make sure that : is in place and ! allocates" begin
    df = DataFrame(a=[1, 2, 3])
    a = df.a
    df[:, :a] .+= 1
    @test a == [2, 3, 4]
    @test df.a === a
    df[!, :a] .+= 1
    @test a == [2, 3, 4]
    @test df.a == [3, 4, 5]
    @test df.a !== a

    df = DataFrame(a=[1, 2, 3])
    a = df.a
    df[:, "a"] .+= 1
    @test a == [2, 3, 4]
    @test df.a === a
    df[!, "a"] .+= 1
    @test a == [2, 3, 4]
    @test df.a == [3, 4, 5]
    @test df.a !== a
end

@testset "add new correct rules for df[row, col] .= v broadcasting" begin
    for v in [:a, "a"]
        df = DataFrame(a=1)
        @test_throws MethodError df[1, 1] .= 10
        @test_throws MethodError df[1, v] .= 10
        @test_throws MethodError df[CartesianIndex(1, 1)] .= 10
        df = DataFrame(a=[[1, 2, 3]])
        df[1, 1] .= 10
        @test df == DataFrame(a=[[10, 10, 10]])
        df[1, v] .= 100
        @test df == DataFrame(a=[[100, 100, 100]])
        df[CartesianIndex(1, 1)] .= 1000
        @test df == DataFrame(a=[[1000, 1000, 1000]])
    end
end

@testset "broadcasting into df[!, cols]" begin
    for selector in [1:2, Between(:x1, :x2), Not(r"x3"), [:x1, :x2],
                     ["x1", "x2"], Between("x1", "x2")]
        df = DataFrame(x1=1:3, x2=4:6)
        df[!, selector] .= "a"
        @test df == DataFrame(fill("a", 3, 2), :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6)
        df[!, selector] .= Ref((a=1, b=2))
        @test df == DataFrame(fill((a=1, b=2), 3, 2), :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6)
        df[!, selector] .= ["a" "b"]
        @test df == DataFrame(["a" "b"
                               "a" "b"
                               "a" "b"], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6)
        df[!, selector] .= ["a", "b", "c"]
        @test df == DataFrame(["a" "a"
                               "b" "b"
                               "c" "c"], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6)
        df[!, selector] .= categorical(["a"])
        @test df == DataFrame(["a" "a"
                               "a" "a"
                               "a" "a"], :auto)
        @test df.x1 isa CategoricalVector
        @test df.x2 isa CategoricalVector
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6)
        df[!, selector] .= DataFrame(["a" "b"], :auto)
        @test df == DataFrame(["a" "b"
                               "a" "b"
                               "a" "b"], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6)
        df[!, selector] .= DataFrame(["a" "d"
                                      "b" "e"
                                      "c" "f"], :auto)
        @test df == DataFrame(["a" "d"
                               "b" "e"
                               "c" "f"], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6)
        df[!, selector] .= ["a" "d"
                            "b" "e"
                            "c" "f"]
        @test df == DataFrame(["a" "d"
                               "b" "e"
                               "c" "f"], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6, x3=1)
        df[!, selector] .= "a"
        @test df == DataFrame(["a" "a" 1
                               "a" "a" 1
                               "a" "a" 1], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6, x3=1)
        df[!, selector] .= Ref((a=1, b=2))
        @test df[:, 1:2] == DataFrame(fill((a=1, b=2), 3, 2), :auto)
        @test df[:, 3] == [1, 1, 1]
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6, x3=1)
        df[!, selector] .= ["a" "b"]
        @test df == DataFrame(["a" "b" 1
                               "a" "b" 1
                               "a" "b" 1], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6, x3=1)
        df[!, selector] .= ["a", "b", "c"]
        @test df == DataFrame(["a" "a" 1
                               "b" "b" 1
                               "c" "c" 1], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6, x3=1)
        df[!, selector] .= categorical(["a"])
        @test df == DataFrame(["a" "a" 1
                               "a" "a" 1
                               "a" "a" 1], :auto)
        @test df.x1 isa CategoricalVector
        @test df.x2 isa CategoricalVector
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6, x3=1)
        df[!, selector] .= DataFrame(["a" "b"], :auto)
        @test df == DataFrame(["a" "b" 1
                               "a" "b" 1
                               "a" "b" 1], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6, x3=1)
        df[!, selector] .= DataFrame(["a" "d"
                                      "b" "e"
                                      "c" "f"], :auto)
        @test df == DataFrame(["a" "d" 1
                               "b" "e" 1
                               "c" "f" 1], :auto)
        @test df.x1 !== df.x2

        df = DataFrame(x1=1:3, x2=4:6, x3=1)
        df[!, selector] .= ["a" "d"
                            "b" "e"
                            "c" "f"]
        @test df == DataFrame(["a" "d" 1
                               "b" "e" 1
                               "c" "f" 1], :auto)
        @test df.x1 !== df.x2
    end

    df = DataFrame(x1=1:3, x2=4:6)
    @test_throws ArgumentError df[!, [:x1, :x3]] .= "a"
end

@testset "broadcasting over heterogeneous columns" begin
    df = DataFrame(x=[1, 1.0, big(1), "1"])
    f_identity(x) = x
    @test df == f_identity.(df)
end

@testset "@views on df[!, col]" begin
    df = DataFrame(ones(3, 4), :auto)
    @views df[!, 1] .+= 1
    @test df[!, 1] == [2.0, 2.0, 2.0]
    @views df[:, 2] .= df[!, 4] .+ df[!, 3]
    @test df[!, 2] == [2.0, 2.0, 2.0]

    # make sure we do not mess with maybeview
    @test @views typeof(df[!, 1:2]) <: SubDataFrame
end

@testset "broadcasting of df[:, col] = value" begin
    df = DataFrame(ones(3, 4), :auto)
    z = ["a", "b", "c"]
    df[:, :z] .= z
    @test df.z == z
    @test df.z !== z
    @test_throws ArgumentError df[:, 6] .= z
    @test_throws MethodError df[:, 1] .= z

    df = DataFrame(ones(3, 4), :auto)
    z = "abc"
    df[:, :z] .= z
    @test df.z == fill("abc", 3)
    @test_throws ArgumentError df[:, 6] .= z
    @test_throws MethodError df[:, 1] .= z

    df = DataFrame(ones(3, 4), :auto)
    z = fill("abc", 1, 1, 2)
    @test_throws DimensionMismatch df[:, :z] .= z

    df = DataFrame(ones(3, 4), :auto)
    z = ["a", "b", "c"]
    df[:, "z"] .= z
    @test df.z == z
    @test df.z !== z

    df = DataFrame(ones(3, 4), :auto)
    z = "abc"
    df[:, "z"] .= z
    @test df.z == fill("abc", 3)

    df = DataFrame(ones(3, 4), :auto)
    z = fill("abc", 1, 1, 2)
    @test_throws DimensionMismatch df[:, "z"] .= z
end

@testset "broadcasting of getproperty" begin
    df = DataFrame(a=1:4)
    if isdefined(Base, :dotgetproperty) # Introduced in Julia 1.7
        df.b .= 1
        x = df.b
        df.c .= 4:-1:1
        df.a .= 'a':'d'
        @test df.a isa Vector{Char}
        @test df == DataFrame(a='a':'d', b=1, c=4:-1:1)

        # in views also column replacement is performed
        dfv = view(df, 2:3, 2:3)
        dfv.b .= 0
        @test x == [1, 1, 1, 1]
        @test df.b !== x
        @test df == DataFrame(a='a':'d', b=[1, 0, 0, 1], c=4:-1:1)
        dfv.c .= ["p", "q"]
        @test df == DataFrame(a='a':'d', b=[1, 0, 0, 1], c=[4, "p", "q", 1])
    else
        # Julia older than 1.7
        df[!, :b] .= 1
        x = df.b
        df[!, :c] .= 4:-1:1
        df.a .= 'a':'d'
        dfv = view(df, 2:3, 2:3)
        dfv.b .= 0
        @test x == [1, 0, 0, 1]
        @test df.b === x
        @test df == DataFrame(a=97:100, b=[1, 0, 0, 1], c=4:-1:1)
        @test_throws MethodError dfv.c .= ["p", "q"]
        @test df == DataFrame(a=97:100, b=[1, 0, 0, 1], c=[4, 3, 2, 1])
    end
end

@testset "dotgetproperty on SubDataFrame" begin
    df = DataFrame(a=1:3, b=4:6)
    dfv = @view df[[3, 1], :]
    if isdefined(Base, :dotgetproperty) # Introduced in Julia 1.7
        dfv.c .= [1, 2]
        @test df ≅ DataFrame(a=1:3, b=4:6, c=[2, missing, 1])
    else
        @test_throws ArgumentError dfv.c .= [1, 2]
    end

    df = DataFrame(a=1:3, b=4:6)
    dfv = @view df[[3, 1], 1:2]
    @test_throws ArgumentError dfv.c .= [1, 2]
end

@testset "test coverage for corner cases that are not normally called" begin
    @test Base.Broadcast.BroadcastStyle(DataFrames.DataFrameStyle(),
                                        Base.Broadcast.Unknown()) isa DataFrames.DataFrameStyle
    df = DataFrame(a=1)
    @test Base.Broadcast.broadcast_unalias(nothing, df) === df
    @test Base.Broadcast.broadcast_unalias(df[1, :], df) == df
    @test Base.Broadcast.broadcast_unalias(df[1, :], df) !== df
    @test Base.Broadcast.broadcast_unalias(copy(df)[1, :], df) === df
end

end # module
