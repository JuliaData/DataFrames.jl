module TestDataFrameRow
    using Test, DataFrames
    using DataFrames: columns

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
    df_rowhashes, _ = DataFrames.hashrows(Tuple(columns(df)), false)
    @test df_rowhashes == [hash(dr) for dr in eachrow(df)]

    # test incompatible frames
    @test_throws UndefVarError DataFrameRow(df, 1) == DataFrameRow(df3, 1)

    # test RowGroupDict
    N = 20
    d1 = rand(map(Int64, 1:2), N)
    df5 = DataFrame([d1], [:d1])
    df6 = DataFrame(d1 = [2,3])

    # test_group("group_rows")
    gd = DataFrames.group_rows(df5)
    @test length(unique(gd.groups)) == 2

    # getting groups for the rows of the other frames
    @test length(gd[DataFrameRow(df6, 1)]) > 0
    @test_throws KeyError gd[DataFrameRow(df6, 2)]
    @test isempty(DataFrames.findrows(gd, df6, (gd.df[1],), (df6[1],), 2))

    # grouping empty frame
    gd = DataFrames.group_rows(DataFrame(x=Int[]))
    @test length(unique(gd.groups)) == 0

    # grouping single row
    gd = DataFrames.group_rows(df5[1:1,:])
    @test length(unique(gd.groups)) == 1

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

    # keys, values and iteration, size
    @test keys(r) == names(df)
    @test values(r) == (df[1, 1], df[1, 2], df[1, 3], df[1, 4])
    @test collect(pairs(r)) == [:a=>df[1, 1], :b=>df[1, 2], :c=>df[1, 3], :d=>df[1, 4]]

    @test haskey(r, :a)
    @test !haskey(r, :zzz)

    @test length(r) == 4
    @test ndims(r) == 1
    @test ndims(typeof(r)) == 1
    @test size(r) == (4,)
    @test size(r, 1) == 4
    @test_throws BoundsError size(r, 2)

    df = DataFrame(a=nothing, b=1)
    io = IOBuffer()
    show(io, DataFrameRow(df, 1))
    @test String(take!(io)) == "DataFrameRow (row 1)\na  \nb  1"

    # convert
    df = DataFrame(a=[1, missing], b=[2.0, 3.0])
    dfr = DataFrameRow(df, 1)
    @test convert(Vector, dfr)::Vector{Union{Float64, Missing}} == [1.0, 2.0]
    @test convert(Vector{Int}, dfr)::Vector{Int} == [1, 2]
    @test Vector(dfr)::Vector{Union{Float64, Missing}} == [1.0, 2.0]
    @test Vector{Int}(dfr)::Vector{Int} == [1, 2]

    # copy
    df = DataFrame(a=Union{Int, Missing}[1, 2, 3, 1, 2, 2],
                   b=[2.0, missing, 1.2, 2.0, missing, missing],
                   c=["A", "B", "C", "A", "B", missing])
    @test copy(DataFrameRow(df, 1)) == (a = 1, b = 2.0, c = "A")
    @test isequal(copy(DataFrameRow(df, 2)), (a = 2, b = missing, c = "B"))

    # parent and parentindices
    @test parent(df[1, :]) === df
    @test parentindices(df[1, :]) == (1, Base.OneTo(3))
    @test parent(df[1, 1:3]) !== df
    @test parentindices(df[1, 1:3]) == (1, Base.OneTo(3))

    # iteration and collect
    ref = ["a", "b", "c"]
    df = DataFrame(permutedims(ref))
    dfr = df[1, :]
    @test collect(dfr) == ref
    @test eltype(collect(dfr)) === String
    for (v1, v2) in zip(ref, dfr)
        @test v1 == v2
    end
    for (i, v) in enumerate(dfr)
        @test v == ref[i]
    end
    dfr = DataFrame(a=1, b=true, c=1.0)[1,:]
    @test eltype(collect(dfr)) === Real
end
