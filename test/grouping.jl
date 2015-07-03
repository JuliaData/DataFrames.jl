module TestGrouping
    using Base.Test
    using DataFrames

    df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                   b = repeat([2, 1], outer=[4]),
                   c = randn(8))
    #df[6, :a] = Nullable()
    #df[7, :b] = Nullable()

    cols = [:a, :b]

    f(df) = DataFrame(cmax = maximum(df[:c]))

    sdf = sort(df, cols=cols)
    bdf = by(df, cols, f)

    @test isequal(bdf[cols], unique(sdf[cols]))

    byf = by(df, :a, df -> DataFrame(bsum = sum(df[:b])))

    @test all(T -> T <: AbstractVector, map(typeof, colwise([sum], df)))
    @test all(T -> T <: AbstractVector, map(typeof, colwise(sum, df)))

    gd = groupby(df, cols)
    ga = map(f, gd)

    @test isequal(bdf, combine(ga))

    g(df) = DataFrame(cmax1 = Vector(df[:cmax]) + 1)
    h(df) = g(f(df))

    @test isequal(combine(map(h, gd)), combine(map(g, ga)))

    # issue #960
    x = CategoricalArray(collect(1:20))
    df = DataFrame(v1=x, v2=x)
    groupby(df, [:v1, :v2])

    df2 = by(e->1, DataFrame(x=Int64[]), :x)
    @test size(df2) == (0,1)
    @test isequal(sum(df2[:x]), Nullable(0))

    # Check that reordering levels does not confuse groupby
    df = DataFrame(Key1 = CategoricalArray(["A", "A", "B", "B"]),
                   Key2 = CategoricalArray(["A", "B", "A", "B"]),
                   Value = 1:4)
    gd = groupby(df, :Key1)
    @test isequal(gd[1], DataFrame(Key1=["A", "A"], Key2=["A", "B"], Value=1:2))
    @test isequal(gd[2], DataFrame(Key1=["B", "B"], Key2=["A", "B"], Value=3:4))
    gd = groupby(df, [:Key1, :Key2])
    @test isequal(gd[1], DataFrame(Key1="A", Key2="A", Value=1))
    @test isequal(gd[2], DataFrame(Key1="A", Key2="B", Value=2))
    @test isequal(gd[3], DataFrame(Key1="B", Key2="A", Value=3))
    @test isequal(gd[4], DataFrame(Key1="B", Key2="B", Value=4))
    # Reorder levels, add unused level
    levels!(df[:Key1], ["Z", "B", "A"])
    levels!(df[:Key2], ["Z", "B", "A"])
    gd = groupby(df, :Key1)
    @test isequal(gd[1], DataFrame(Key1=["B", "B"], Key2=["A", "B"], Value=3:4))
    @test isequal(gd[2], DataFrame(Key1=["A", "A"], Key2=["A", "B"], Value=1:2))
    gd = groupby(df, [:Key1, :Key2])
    @test isequal(gd[1], DataFrame(Key1="B", Key2="B", Value=4))
    @test isequal(gd[2], DataFrame(Key1="B", Key2="A", Value=3))
    @test isequal(gd[3], DataFrame(Key1="A", Key2="B", Value=2))
    @test isequal(gd[4], DataFrame(Key1="A", Key2="A", Value=1))

    # test conversion of SubDataFrame to DataFrame
    sub_df = SubDataFrame(df, [1, 3, 4])
    sub_df_copy = convert(DataFrame, sub_df)
    @test isa(sub_df_copy, DataFrame)
    @test size(sub_df_copy) == (3, size(df, 2))

    # test mixing of SubDataFrame and DataFrame in combine()
    gix = 1
    df2 = map(gd) do sub_gd
        iseven(gix) ? sub_gd : convert(DataFrame, sub_gd)
    end |> combine
    @test isa(df2, DataFrame)
    delete!(df2, [:Key1_1, :Key2_1]) # grouping columns are re-added
    @test size(df2) == size(df)

    # test promotion and conversion of SubDataFrame to DataFrame when modifying
    append_inc_col(df) = hcat(df, DataFrame(Value2 = df[:Value] + 1))
    @test isequal(delete!(combine(map(append_inc_col, groupby(df, :Key1))), [:Key1_1]),
                  append_inc_col(sdf))

    @test isequal(delete!(combine(map(append_inc_col, groupby(df, [:Key1, :Key2]))), [:Key1_1, :Key2_1]),
                  append_inc_col(sdf))
end
