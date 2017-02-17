module TestGrouping
    using Base.Test
    using DataTables

    dt = DataTable(a = repeat([1, 2, 3, 4], outer=[2]),
                   b = repeat([2, 1], outer=[4]),
                   c = randn(8))
    #dt[6, :a] = Nullable()
    #dt[7, :b] = Nullable()

    cols = [:a, :b]

    f(dt) = DataTable(cmax = maximum(dt[:c]))

    sdt = sort(dt, cols=cols)
    bdt = by(dt, cols, f)

    @test isequal(bdt[cols], unique(sdt[cols]))

    byf = by(dt, :a, dt -> DataTable(bsum = sum(dt[:b])))

    @test all(T -> T <: AbstractVector, map(typeof, colwise([sum], dt)))
    @test all(T -> T <: AbstractVector, map(typeof, colwise(sum, dt)))

    gd = groupby(dt, cols)
    ga = map(f, gd)

    @test isequal(bdt, combine(ga))

    g(dt) = DataTable(cmax1 = Vector(dt[:cmax]) + 1)
    h(dt) = g(f(dt))

    @test isequal(combine(map(h, gd)), combine(map(g, ga)))

    # testing pool overflow
    dt2 = DataTable(v1 = categorical(collect(1:1000)), v2 = categorical(fill(1, 1000)))
    @test groupby(dt2, [:v1, :v2]).starts == collect(1:1000)
    @test groupby(dt2, [:v2, :v1]).starts == collect(1:1000)

    # grouping empty frame
    @test groupby(DataTable(A=Int[]), :A).starts == Int[]
    # grouping single row
    @test groupby(DataTable(A=Int[1]), :A).starts == Int[1]

    # issue #960
    x = CategoricalArray(collect(1:20))
    dt = DataTable(v1=x, v2=x)
    groupby(dt, [:v1, :v2])

    dt2 = by(e->1, DataTable(x=Int64[]), :x)
    @test size(dt2) == (0,1)
    @test isequal(sum(dt2[:x]), Nullable(0))

    # Check that reordering levels does not confuse groupby
    dt = DataTable(Key1 = CategoricalArray(["A", "A", "B", "B"]),
                   Key2 = CategoricalArray(["A", "B", "A", "B"]),
                   Value = 1:4)
    gd = groupby(dt, :Key1)
    @test isequal(gd[1], DataTable(Key1=["A", "A"], Key2=["A", "B"], Value=1:2))
    @test isequal(gd[2], DataTable(Key1=["B", "B"], Key2=["A", "B"], Value=3:4))
    gd = groupby(dt, [:Key1, :Key2])
    @test isequal(gd[1], DataTable(Key1="A", Key2="A", Value=1))
    @test isequal(gd[2], DataTable(Key1="A", Key2="B", Value=2))
    @test isequal(gd[3], DataTable(Key1="B", Key2="A", Value=3))
    @test isequal(gd[4], DataTable(Key1="B", Key2="B", Value=4))
    # Reorder levels, add unused level
    levels!(dt[:Key1], ["Z", "B", "A"])
    levels!(dt[:Key2], ["Z", "B", "A"])
    gd = groupby(dt, :Key1)
    @test isequal(gd[1], DataTable(Key1=["B", "B"], Key2=["A", "B"], Value=3:4))
    @test isequal(gd[2], DataTable(Key1=["A", "A"], Key2=["A", "B"], Value=1:2))
    gd = groupby(dt, [:Key1, :Key2])
    @test isequal(gd[1], DataTable(Key1="B", Key2="B", Value=4))
    @test isequal(gd[2], DataTable(Key1="B", Key2="A", Value=3))
    @test isequal(gd[3], DataTable(Key1="A", Key2="B", Value=2))
    @test isequal(gd[4], DataTable(Key1="A", Key2="A", Value=1))
end
