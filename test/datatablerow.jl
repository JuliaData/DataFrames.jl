module TestDataTableRow
    using Base.Test, DataTables

    dt = DataTable(a=Union{Int, Null}[1, 2, 3, 1, 2, 2],
                   b=[2.0, null, 1.2, 2.0, null, null],
                   c=["A", "B", "C", "A", "B", null],
                   d=CategoricalArray([:A, null, :C, :A, null, :C]))
    dt2 = DataTable(a = [1, 2, 3])

    #
    # Equality
    #
    @test_throws ArgumentError DataTableRow(dt, 1) == DataTableRow(dt2, 1)
    @test DataTableRow(dt, 1) != DataTableRow(dt, 2)
    @test DataTableRow(dt, 1) != DataTableRow(dt, 3)
    @test DataTableRow(dt, 1) == DataTableRow(dt, 4)
    @test DataTableRow(dt, 2) == DataTableRow(dt, 5)
    @test DataTableRow(dt, 2) != DataTableRow(dt, 6)

    # isless()
    dt4 = DataTable(a=[1, 1, 2, 2, 2, 2, null, null],
                    b=Union{Float64, Null}[2.0, 3.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0],
                    c=[:B, null, :A, :C, :D, :D, :A, :A])
    @test DataTableRow(dt4, 1) < DataTableRow(dt4, 2)
    @test DataTableRow(dt4, 2) > DataTableRow(dt4, 1)
    @test DataTableRow(dt4, 1) == DataTableRow(dt4, 1)
    @test DataTableRow(dt4, 1) < DataTableRow(dt4, 3)
    @test DataTableRow(dt4, 3) > DataTableRow(dt4, 1)
    @test DataTableRow(dt4, 3) < DataTableRow(dt4, 4)
    @test DataTableRow(dt4, 4) > DataTableRow(dt4, 3)
    @test DataTableRow(dt4, 4) < DataTableRow(dt4, 5)
    @test DataTableRow(dt4, 5) > DataTableRow(dt4, 4)
    @test DataTableRow(dt4, 6) == DataTableRow(dt4, 5)
    @test DataTableRow(dt4, 5) == DataTableRow(dt4, 6)
    @test DataTableRow(dt4, 7) < DataTableRow(dt4, 8)
    @test DataTableRow(dt4, 8) > DataTableRow(dt4, 7)

    # hashing
    @test hash(DataTableRow(dt, 1)) != hash(DataTableRow(dt, 2))
    @test hash(DataTableRow(dt, 1)) != hash(DataTableRow(dt, 3))
    @test hash(DataTableRow(dt, 1)) == hash(DataTableRow(dt, 4))
    @test hash(DataTableRow(dt, 2)) == hash(DataTableRow(dt, 5))
    @test hash(DataTableRow(dt, 2)) != hash(DataTableRow(dt, 6))


    # check that hashrows() function generates the same hashes as DataTableRow
    dt_rowhashes = DataTables.hashrows(dt)
    @test dt_rowhashes == [hash(dr) for dr in eachrow(dt)]

    # test incompatible frames
    @test_throws UndefVarError DataTableRow(dt, 1) == DataTableRow(dt3, 1)

    # test RowGroupDict
    N = 20
    d1 = rand(map(Int64, 1:2), N)
    dt5 = DataTable(Any[d1], [:d1])
    dt6 = DataTable(d1 = [2,3])

    # test_group("groupby")
    gd = DataTables.group_rows(dt5)
    @test gd.ngroups == 2

    # getting groups for the rows of the other frames
    @test length(gd[DataTableRow(dt6, 1)]) > 0
    @test_throws KeyError gd[DataTableRow(dt6, 2)]
    @test isempty(DataTables.findrows(gd, dt6, (gd.dt[1],), (dt6[1],), 2))

    # grouping empty frame
    gd = DataTables.group_rows(DataTable(x=Int[]))
    @test gd.ngroups == 0

    # grouping single row
    gd = DataTables.group_rows(dt5[1,:])
    @test gd.ngroups == 1
end
