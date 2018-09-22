module TestDataFrameRow
    using Test, DataFrames

    df = DataFrame(a=Union{Int, Missing}[1, 2, 3, 1, 2, 2],
                   b=[2.0, missing, 1.2, 2.0, missing, missing],
                   c=["A", "B", "C", "A", "B", missing],
                   d=CategoricalArray([:A, missing, :C, :A, missing, :C]))
    df2 = DataFrame(a = [1, 2, 3])

    @test names(DataFrameRow(df, 1)) == [:a, :b, :c, :d]

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
    df4 = DataFrame(a=[1, 1, 2, 2, 2, 2, missing, missing],
                    b=Union{Float64, Missing}[2.0, 3.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0],
                    c=[:B, missing, :A, :C, :D, :D, :A, :A])
    @test isless(DataFrameRow(df4, 1), DataFrameRow(df4, 2))
    @test !isless(DataFrameRow(df4, 2), DataFrameRow(df4, 1))
    @test !isless(DataFrameRow(df4, 1), DataFrameRow(df4, 1))
    @test isless(DataFrameRow(df4, 1), DataFrameRow(df4, 3))
    @test !isless(DataFrameRow(df4, 3), DataFrameRow(df4, 1))
    @test isless(DataFrameRow(df4, 3), DataFrameRow(df4, 4))
    @test !isless(DataFrameRow(df4, 4), DataFrameRow(df4, 3))
    @test isless(DataFrameRow(df4, 4), DataFrameRow(df4, 5))
    @test !isless(DataFrameRow(df4, 5), DataFrameRow(df4, 4))
    @test !isless(DataFrameRow(df4, 6), DataFrameRow(df4, 5))
    @test !isless(DataFrameRow(df4, 5), DataFrameRow(df4, 6))
    @test isless(DataFrameRow(df4, 7), DataFrameRow(df4, 8))
    @test !isless(DataFrameRow(df4, 8), DataFrameRow(df4, 7))

    # hashing
    @test hash(DataFrameRow(df, 1)) != hash(DataFrameRow(df, 2))
    @test hash(DataFrameRow(df, 1)) != hash(DataFrameRow(df, 3))
    @test hash(DataFrameRow(df, 1)) == hash(DataFrameRow(df, 4))
    @test hash(DataFrameRow(df, 2)) == hash(DataFrameRow(df, 5))
    @test hash(DataFrameRow(df, 2)) != hash(DataFrameRow(df, 6))

    # check that hashrows() function generates the same hashes as DataFrameRow
    df_rowhashes, _ = DataFrames.hashrows(df, false)
    @test df_rowhashes == [hash(dr) for dr in eachrow(df)]

    # test incompatible frames
    @test_throws UndefVarError DataFrameRow(df, 1) == DataFrameRow(df3, 1)

    # test RowGroupDict
    N = 20
    d1 = rand(map(Int64, 1:2), N)
    df5 = DataFrame([d1], [:d1])
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

    # getproperty, setproperty! and propertynames
    r = DataFrameRow(df, 1)
    @test Base.propertynames(r) == names(df)
    @test r.a === 1
    @test r.b === 2.0
    r.a = 2
    @test r.a === 2
    r.b = 1
    @test r.b === 1.0

    # getindex
    r = DataFrameRow(df, 1)
    @test r[:] == r

    # keys, values and iteration
    @test keys(r) == names(df)
    @test values(r) == (df[1, 1], df[1, 2], df[1, 3], df[1, 4])
    @test collect(pairs(r)) == [:a=>df[1, 1], :b=>df[1, 2], :c=>df[1, 3], :d=>df[1, 4]]

    df = DataFrame(a=nothing, b=1)
    io = IOBuffer()
    show(io, DataFrameRow(df, 1))
    @test String(take!(io)) == "DataFrameRow (row 1)\na  \nb  1"
end

