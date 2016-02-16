module TestDataFrameRow
    using Base.Test
    using DataFrames, Compat

    df = DataFrame(a=@data([1,   2,   3,   1,   2,   2 ]),
                   b=@data([2.0, NA,  1.2, 2.0, NA,  NA]),
                   c=@data(["A", "B", "C", "A", "B", NA]),
                   d=PooledDataArray(
                     @data([:A,  NA,  :C,  :A,  NA,  :C])))
    df2 = DataFrame(a = @data([1, 2, 3]))

    df3 = DataFrame(a = @data([1,1,1]), b = @data([2,2,2]))

    #
    # Equality
    #
    @test_throws ArgumentError isequal(DataFrameRow(df, 1), DataFrameRow(df2, 1))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df, 2))
    @test !isequal(DataFrameRow(df, 1), DataFrameRow(df, 3))
    @test isequal(DataFrameRow(df, 1), DataFrameRow(df, 4))
    @test isequal(DataFrameRow(df, 2), DataFrameRow(df, 5))
    @test !isequal(DataFrameRow(df, 2), DataFrameRow(df, 6))

    # hashing
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 2)))
    @test !isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 3)))
    @test isequal(hash(DataFrameRow(df, 1)), hash(DataFrameRow(df, 4)))
    @test isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df, 5)))
    @test !isequal(hash(DataFrameRow(df, 2)), hash(DataFrameRow(df, 6)))

    # similar
    let dfsim1=similar(DataFrameRow(df3,1))
        @test isa(dfsim1, DataFrameRow)
        @test length(dfsim1)==size(df3,2)
    end
    let dfsim2=similar(DataFrameRow(df3,1),4)
        @test isa(dfsim2, DataFrame)
        @test size(dfsim2)==(4,size(df3,2))
    end

    # setindex!
    let df4 = DataFrame(a = @data([1,3,5]), b = @data([2,4,6]))
        df4[2,:] = DataFrameRow(df3,1)
        df4[3,:] = DataFrameRow(df3,1)
        @test isequal(df4,df3)
    end
    let df5 = DataFrame(a = @data(rand(1:100,3)), b = @data(rand(1:100,3)))
        df5[:,:] = DataFrameRow(df3,1)
        @test isequal(df5,df3[1,:])
    end

end
