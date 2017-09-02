module TestDataFrameRow
    using Base.Test, DataFrames

    dt = DataFrame(a=Union{Int, Null}[1, 2, 3, 1, 2, 2],
                   b=[2.0, null, 1.2, 2.0, null, null],
                   c=["A", "B", "C", "A", "B", null],
                   d=CategoricalArray([:A, null, :C, :A, null, :C]))
    dt2 = DataFrame(a = [1, 2, 3])

    #
    # Equality
    #
    @test_throws ArgumentError DataFrameRow(dt, 1) == DataFrameRow(dt2, 1)
    @test DataFrameRow(dt, 1) != DataFrameRow(dt, 2)
    @test DataFrameRow(dt, 1) != DataFrameRow(dt, 3)
    @test DataFrameRow(dt, 1) == DataFrameRow(dt, 4)
    @test DataFrameRow(dt, 2) == DataFrameRow(dt, 5)
    @test DataFrameRow(dt, 2) != DataFrameRow(dt, 6)

    # isless()
    dt4 = DataFrame(a=[1, 1, 2, 2, 2, 2, null, null],
                    b=Union{Float64, Null}[2.0, 3.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0],
                    c=[:B, null, :A, :C, :D, :D, :A, :A])
    @test DataFrameRow(dt4, 1) < DataFrameRow(dt4, 2)
    @test DataFrameRow(dt4, 2) > DataFrameRow(dt4, 1)
    @test DataFrameRow(dt4, 1) == DataFrameRow(dt4, 1)
    @test DataFrameRow(dt4, 1) < DataFrameRow(dt4, 3)
    @test DataFrameRow(dt4, 3) > DataFrameRow(dt4, 1)
    @test DataFrameRow(dt4, 3) < DataFrameRow(dt4, 4)
    @test DataFrameRow(dt4, 4) > DataFrameRow(dt4, 3)
    @test DataFrameRow(dt4, 4) < DataFrameRow(dt4, 5)
    @test DataFrameRow(dt4, 5) > DataFrameRow(dt4, 4)
    @test DataFrameRow(dt4, 6) == DataFrameRow(dt4, 5)
    @test DataFrameRow(dt4, 5) == DataFrameRow(dt4, 6)
    @test DataFrameRow(dt4, 7) < DataFrameRow(dt4, 8)
    @test DataFrameRow(dt4, 8) > DataFrameRow(dt4, 7)

    # hashing
    @test hash(DataFrameRow(dt, 1)) != hash(DataFrameRow(dt, 2))
    @test hash(DataFrameRow(dt, 1)) != hash(DataFrameRow(dt, 3))
    @test hash(DataFrameRow(dt, 1)) == hash(DataFrameRow(dt, 4))
    @test hash(DataFrameRow(dt, 2)) == hash(DataFrameRow(dt, 5))
    @test hash(DataFrameRow(dt, 2)) != hash(DataFrameRow(dt, 6))


    # check that hashrows() function generates the same hashes as DataFrameRow
    dt_rowhashes = DataFrames.hashrows(dt)
    @test dt_rowhashes == [hash(dr) for dr in eachrow(dt)]

    # test incompatible frames
    @test_throws UndefVarError DataFrameRow(dt, 1) == DataFrameRow(dt3, 1)

    # test RowGroupDict
    N = 20
    d1 = rand(map(Int64, 1:2), N)
    dt5 = DataFrame(Any[d1], [:d1])
    dt6 = DataFrame(d1 = [2,3])

    # test_group("groupby")
    gd = DataFrames.group_rows(dt5)
    @test gd.ngroups == 2

    # getting groups for the rows of the other frames
    @test length(gd[DataFrameRow(dt6, 1)]) > 0
    @test_throws KeyError gd[DataFrameRow(dt6, 2)]
    @test isempty(DataFrames.findrows(gd, dt6, (gd.dt[1],), (dt6[1],), 2))

    # grouping empty frame
    gd = DataFrames.group_rows(DataFrame(x=Int[]))
    @test gd.ngroups == 0

    # grouping single row
    gd = DataFrames.group_rows(dt5[1,:])
    @test gd.ngroups == 1
end
