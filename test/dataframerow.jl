module TestDataFrameRow
    using Base.Test, DataFrames

    df = DataFrame(a=Union{Int, Null}[1, 2, 3, 1, 2, 2],
                   b=[2.0, null, 1.2, 2.0, null, null],
                   c=["A", "B", "C", "A", "B", null],
                   d=CategoricalArray([:A, null, :C, :A, null, :C]))
    df2 = DataFrame(a = [1, 2, 3])

    #
    # Equality
    #
    @test_throws ArgumentError DataFrameRow(df, 1) == DataFrameRow(df2, 1)
    @test DataFrameRow(df, 1) != DataFrameRow(df, 2)
    @test DataFrameRow(df, 1) != DataFrameRow(df, 3)
    @test DataFrameRow(df, 1) == DataFrameRow(df, 4)
    @test DataFrameRow(df, 2) == DataFrameRow(df, 5)
    @test DataFrameRow(df, 2) != DataFrameRow(df, 6)

    # isless()
    df4 = DataFrame(a=[1, 1, 2, 2, 2, 2, null, null],
                    b=Union{Float64, Null}[2.0, 3.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0],
                    c=[:B, null, :A, :C, :D, :D, :A, :A])
    @test DataFrameRow(df4, 1) < DataFrameRow(df4, 2)
    @test DataFrameRow(df4, 2) > DataFrameRow(df4, 1)
    @test DataFrameRow(df4, 1) == DataFrameRow(df4, 1)
    @test DataFrameRow(df4, 1) < DataFrameRow(df4, 3)
    @test DataFrameRow(df4, 3) > DataFrameRow(df4, 1)
    @test DataFrameRow(df4, 3) < DataFrameRow(df4, 4)
    @test DataFrameRow(df4, 4) > DataFrameRow(df4, 3)
    @test DataFrameRow(df4, 4) < DataFrameRow(df4, 5)
    @test DataFrameRow(df4, 5) > DataFrameRow(df4, 4)
    @test DataFrameRow(df4, 6) == DataFrameRow(df4, 5)
    @test DataFrameRow(df4, 5) == DataFrameRow(df4, 6)
    @test DataFrameRow(df4, 7) < DataFrameRow(df4, 8)
    @test DataFrameRow(df4, 8) > DataFrameRow(df4, 7)

    # hashing
    @test hash(DataFrameRow(df, 1)) != hash(DataFrameRow(df, 2))
    @test hash(DataFrameRow(df, 1)) != hash(DataFrameRow(df, 3))
    @test hash(DataFrameRow(df, 1)) == hash(DataFrameRow(df, 4))
    @test hash(DataFrameRow(df, 2)) == hash(DataFrameRow(df, 5))
    @test hash(DataFrameRow(df, 2)) != hash(DataFrameRow(df, 6))


    # check that hashrows() function generates the same hashes as DataFrameRow
    df_rowhashes = DataFrames.hashrows(df)
    @test df_rowhashes == [hash(dr) for dr in eachrow(df)]

    # test incompatible frames
    @test_throws UndefVarError DataFrameRow(df, 1) == DataFrameRow(df3, 1)

    # test RowGroupDict
    N = 20
    d1 = rand(map(Int64, 1:2), N)
    df5 = DataFrame(Any[d1], [:d1])
    df6 = DataFrame(d1 = [2,3])

    # test_group("groupby")
    gd = DataFrames.group_rows(df5)
    @test gd.ngroups == 2

    # getting groups for the rows of the other frames
    @test length(gd[DataFrameRow(df6, 1)]) > 0
    @test_throws KeyError gd[DataFrameRow(df6, 2)]
    @test isempty(DataFrames.findrows(gd, df6, (gd.df[1],), (df6[1],), 2))

    # grouping empty frame
    gd = DataFrames.group_rows(DataFrame(x=Int[]))
    @test gd.ngroups == 0

    # grouping single row
    gd = DataFrames.group_rows(df5[1,:])
    @test gd.ngroups == 1
end
