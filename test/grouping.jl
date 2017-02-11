module TestGrouping
    using Base.Test
    using DataTables

    df = DataTable(a = repeat([1, 2, 3, 4], outer=[2]),
                   b = repeat([2, 1], outer=[4]),
                   c = randn(8))
    #df[6, :a] = Nullable()
    #df[7, :b] = Nullable()

    cols = [:a, :b]

    f(df) = DataTable(cmax = maximum(df[:c]))

    sdf = sort(df, cols=cols)
    bdf = by(df, cols, f)

    @test isequal(bdf[cols], unique(sdf[cols]))

    byf = by(df, :a, df -> DataTable(bsum = sum(df[:b])))

    @test all(T -> T <: AbstractVector, map(typeof, colwise([sum], df)))
    @test all(T -> T <: AbstractVector, map(typeof, colwise(sum, df)))

    gd = groupby(df, cols)
    ga = map(f, gd)

    @test isequal(bdf, combine(ga))

    g(df) = DataTable(cmax1 = Vector(df[:cmax]) + 1)
    h(df) = g(f(df))

    @test isequal(combine(map(h, gd)), combine(map(g, ga)))

    # testing pool overflow
    df2 = DataTable(v1 = categorical(collect(1:1000)), v2 = categorical(fill(1, 1000)))
    @test groupby(df2, [:v1, :v2]).starts == collect(1:1000)
    @test groupby(df2, [:v2, :v1]).starts == collect(1:1000)

    # grouping empty frame
    @test groupby(DataTable(A=Int[]), :A).starts == Int[]
    # grouping single row
    @test groupby(DataTable(A=Int[1]), :A).starts == Int[1]

    # issue #960
    x = CategoricalArray(collect(1:20))
    df = DataTable(v1=x, v2=x)
    groupby(df, [:v1, :v2])

    df2 = by(e->1, DataTable(x=Int64[]), :x)
    @test size(df2) == (0,1)
    @test isequal(sum(df2[:x]), Nullable(0))

    # Check that reordering levels does not confuse groupby
    df = DataTable(Key1 = CategoricalArray(["A", "A", "B", "B"]),
                   Key2 = CategoricalArray(["A", "B", "A", "B"]),
                   Value = 1:4)
    gd = groupby(df, :Key1)
    @test isequal(gd[1], DataTable(Key1=["A", "A"], Key2=["A", "B"], Value=1:2))
    @test isequal(gd[2], DataTable(Key1=["B", "B"], Key2=["A", "B"], Value=3:4))
    gd = groupby(df, [:Key1, :Key2])
    @test isequal(gd[1], DataTable(Key1="A", Key2="A", Value=1))
    @test isequal(gd[2], DataTable(Key1="A", Key2="B", Value=2))
    @test isequal(gd[3], DataTable(Key1="B", Key2="A", Value=3))
    @test isequal(gd[4], DataTable(Key1="B", Key2="B", Value=4))
    # Reorder levels, add unused level
    levels!(df[:Key1], ["Z", "B", "A"])
    levels!(df[:Key2], ["Z", "B", "A"])
    gd = groupby(df, :Key1)
    @test isequal(gd[1], DataTable(Key1=["B", "B"], Key2=["A", "B"], Value=3:4))
    @test isequal(gd[2], DataTable(Key1=["A", "A"], Key2=["A", "B"], Value=1:2))
    gd = groupby(df, [:Key1, :Key2])
    @test isequal(gd[1], DataTable(Key1="B", Key2="B", Value=4))
    @test isequal(gd[2], DataTable(Key1="B", Key2="A", Value=3))
    @test isequal(gd[3], DataTable(Key1="A", Key2="B", Value=2))
    @test isequal(gd[4], DataTable(Key1="A", Key2="A", Value=1))
end
