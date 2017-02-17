module TestData
    importall Base # so that we get warnings for conflicts
    using Base.Test
    using DataFrames
    using Compat

    #test_group("NullableArray creation")
    nvint = NullableArray(Nullable{Int}[1, 2, Nullable(), 4])
    nvint2 = NullableArray(5:8)
    nvint3 = NullableArray(5:8)
    nvflt = NullableArray(Nullable{Float64}[1.0, 2.0, Nullable(), 4.0])
    nvstr = NullableArray(Nullable{Compat.ASCIIString}["one", "two", Nullable(), "four"])
    dvdict = NullableArray(Dict, 4)    # for issue #199

    #test_group("constructors")
    df1 = DataFrame(Any[nvint, nvstr], [:Ints, :Strs])
    df2 = DataFrame(Any[nvint, nvstr])
    df3 = DataFrame(Any[nvint])
    df4 = convert(DataFrame, [1:4 1:4])
    df5 = DataFrame(Any[NullableArray([1,2,3,4]), nvstr])
    df6 = DataFrame(Any[nvint, nvint, nvstr], [:A, :B, :C])
    df7 = DataFrame(x = nvint, y = nvstr)
    @test size(df7) == (4, 2)
    @test isequal(df7[:x], nvint)

    #test_group("description functions")
    @test size(df6, 1) == 4
    @test size(df6, 2) == 3
    @test names(df6) == [:A, :B, :C]
    @test names(df2) == [:x1, :x2]
    @test names(df7) == [:x, :y]

    #test_group("ref")
    @test isequal(df6[2, 3], Nullable("two"))
    @test isnull(df6[3, 3])
    @test isequal(df6[2, :C], Nullable("two"))
    @test isequal(df6[:B], nvint)
    @test size(df6[[2,3]], 2) == 2
    @test size(df6[2,:], 1) == 1
    @test size(df6[[1, 3], [1, 3]]) == (2, 2)
    @test size(df6[1:2, 1:2]) == (2, 2)
    @test size(head(df6,2)) == (2, 3)
    # lots more to do

    #test_group("assign")
    df6[3] = NullableArray(["un", "deux", "troix", "quatre"])
    @test isequal(df6[1, 3], Nullable("un"))
    df6[:B] = [4, 3, 2, 1]
    @test isequal(df6[1,2], Nullable(4))
    df6[:D] = [true, false, true, false]
    @test isequal(df6[1,4], Nullable(true))
    delete!(df6, :D)
    @test names(df6) == [:A, :B, :C]
    @test size(df6, 2) == 3

    #test_group("null handling")
    @test nrow(df5[completecases(df5), :]) == 3
    @test nrow(dropnull(df5)) == 3
    returned = dropnull(df4)
    @test df4 == returned && df4 !== returned
    @test nrow(dropnull!(df5)) == 3
    returned = dropnull!(df4)
    @test df4 == returned && df4 === returned

    #test_context("SubDataFrames")

    #test_group("constructors")
    # single index is rows
    sdf6a = view(df6, 1)
    sdf6b = view(df6, 2:3)
    sdf6c = view(df6, [true, false, true, false])
    @test size(sdf6a) == (1,3)
    sdf6d = view(df6, [1,3], :B)
    @test size(sdf6d) == (2,1)

    #test_group("ref")
    @test isequal(sdf6a[1,2], Nullable(4))

    #test_context("Within")
    #test_group("Associative")

    #test_group("DataFrame")
    srand(1)
    N = 20
    #Cast to Int64 as rand() behavior differs between Int32/64
    d1 = NullableArray(rand(map(Int64, 1:2), N))
    d2 = NullableCategoricalArray(Nullable{String}["A", "B", Nullable()])[rand(map(Int64, 1:3), N)]
    d3 = NullableArray(randn(N))
    d4 = NullableArray(randn(N))
    df7 = DataFrame(Any[d1, d2, d3], [:d1, :d2, :d3])

    #test_group("groupby")
    gd = groupby(df7, :d1)
    @test length(gd) == 2
    # @test isequal(gd[2]["d2"], CategoricalVector["A", "B", Nullable(), "A", Nullable(), Nullable(), Nullable(), Nullable()])
    @test isequal(sum(gd[2][:d3]), sum(df7[:d3][Vector(df7[:d1]) .== 2]))

    g1 = groupby(df7, [:d1, :d2])
    g2 = groupby(df7, [:d2, :d1])
    @test isequal(sum(g1[1][:d3]), sum(g2[1][:d3]))

    res = Nullable(0.0)
    for x in g1
        res += sum(x[:d1])
    end
    @test isequal(res, sum(df7[:d1]))

    @test aggregate(DataFrame(a=1), identity) == DataFrame(a_identity=1)

    df8 = aggregate(df7[[1, 3]], sum)
    @test isequal(df8[1, :d1_sum], sum(df7[:d1]))

    df8 = aggregate(df7, :d2, [sum, length])
    @test size(df8, 1) == 3
    @test size(df8, 2) == 5
    @test isequal(df8[2, :d1_length], Nullable(4))
    @test isequal(df8, aggregate(groupby(df7, :d2), [sum, length]))

    df9 = df7 |> groupby([:d2]) |> [sum, length]
    @test isequal(df9, df8)
    df9 = aggregate(df7, :d2, [sum, length])
    @test isequal(df9, df8)

    df10 = DataFrame(
        Any[[1:4;], [2:5;], ["a", "a", "a", "b" ], ["c", "d", "c", "d"]],
        [:d1, :d2, :d3, :d4]
    )

    gd = groupby(df10, [:d3])
    ggd = groupby(gd[1], [:d3, :d4]) # make sure we can groupby subdataframes
    @test ggd[1][1, :d3] == "a"
    @test ggd[1][1, :d4] == "c"
    @test ggd[1][2, :d3] == "a"
    @test ggd[1][2, :d4] == "c"
    @test ggd[2][1, :d3] == "a"
    @test ggd[2][1, :d4] == "d"

    #test_group("reshape")
    d1 = DataFrame(a = repeat([1:3;], inner = [4]),
                   b = repeat([1:4;], inner = [3]),
                   c = randn(12),
                   d = randn(12),
                   e = map(string, 'a':'l'))

    stack(d1, :a)
    d1s = stack(d1, [:a, :b])
    d1s2 = stack(d1, [:c, :d])
    d1s3 = stack(d1)
    d1m = melt(d1, [:c, :d, :e])
    @test isequal(d1s[1:12, :c], d1[:c])
    @test isequal(d1s[13:24, :c], d1[:c])
    @test isequal(d1s2, d1s3)
    @test names(d1s) == [:variable, :value, :c, :d, :e]
    @test isequal(d1s, d1m)
    d1m = melt(d1[[1,3,4]], :a)
    @test names(d1m) == [:variable, :value, :a]

    # Test naming of measure/value columns
    d1s_named = stack(d1, [:a, :b], variable_name=:letter, value_name=:someval)
    @test names(d1s_named) == [:letter, :someval, :c, :d, :e]
    d1m_named = melt(d1[[1,3,4]], :a, variable_name=:letter, value_name=:someval)
    @test names(d1m_named) == [:letter, :someval, :a]

    stackdf(d1, :a)
    d1s = stackdf(d1, [:a, :b])
    d1s2 = stackdf(d1, [:c, :d])
    d1s3 = stackdf(d1)
    d1m = meltdf(d1, [:c, :d, :e])
    @test isequal(d1s[1:12, :c], d1[:c])
    @test isequal(d1s[13:24, :c], d1[:c])
    @test isequal(d1s2, d1s3)
    @test names(d1s) == [:variable, :value, :c, :d, :e]
    @test isequal(d1s, d1m)
    d1m = meltdf(d1[[1,3,4]], :a)
    @test names(d1m) == [:variable, :value, :a]

    d1s_named = stackdf(d1, [:a, :b], variable_name=:letter, value_name=:someval)
    @test names(d1s_named) == [:letter, :someval, :c, :d, :e]
    d1m_named = meltdf(d1, [:c, :d, :e], variable_name=:letter, value_name=:someval)
    @test names(d1m_named) == [:letter, :someval, :c, :d, :e]

    d1s[:id] = [1:12; 1:12]
    d1s2[:id] = [1:12; 1:12]
    d1us = unstack(d1s, :id, :variable, :value)
    d1us2 = unstack(d1s2)
    d1us3 = unstack(d1s2, :variable, :value)
    @test isequal(d1us[:a], d1[:a])
    @test isequal(d1us2[:d], d1[:d])
    @test isequal(d1us2[:3], d1[:d])



    d2 = DataFrame(id1 = [:a, :a, :a, :b],
                   id2 = [:A, :B, :B, :B],
                   id3 = [:t, :f, :t, :f],
                   val = [.1, .2, .3, .4])


    #test_group("merge")

    srand(1)
    df1 = DataFrame(a = shuffle!([1:10;]),
                    b = [:A,:B][rand(1:2, 10)],
                    v1 = randn(10))

    df2 = DataFrame(a = shuffle!(reverse([1:5;])),
                    b2 = [:A,:B,:C][rand(1:3, 5)],
                    v2 = randn(5))

    m1 = join(df1, df2, on = :a)
    @test isequal(m1[:a], NullableArray([1, 2, 3, 4, 5]))
    # TODO: Re-enable
    m2 = join(df1, df2, on = :a, kind = :outer)
    # @test isequal(m2[:b2],
    #               NullableArray(Nullable{String}["A", "B", "B", "B", "B",
    #                                              Nullable(), Nullable(),
    #                                              Nullable(), Nullable(), Nullable()]))
    # @test isequal(m2[:b2],
    #               NullableArray(Nullable{String}["B", "B", "B", "C", "B",
    #                                              Nullable(), Nullable(),
    #                                              Nullable(), Nullable(), Nullable()]))

    df1 = DataFrame(a = [1, 2, 3],
                    b = ["America", "Europe", "Africa"])
    df2 = DataFrame(a = [1, 2, 4],
                    c = ["New World", "Old World", "New World"])

    m1 = join(df1, df2, on = :a, kind = :inner)
    @test isequal(m1[:a], NullableArray([1, 2]))

    m2 = join(df1, df2, on = :a, kind = :left)
    @test isequal(m2[:a], NullableArray([1, 2, 3]))

    m3 = join(df1, df2, on = :a, kind = :right)
    @test isequal(m3[:a], NullableArray([1, 2, 4]))

    m4 = join(df1, df2, on = :a, kind = :outer)
    @test isequal(m4[:a], NullableArray([1, 2, 3, 4]))

    # test with nulls (issue #185)
    df1 = DataFrame()
    df1[:A] = NullableArray(Nullable{Compat.ASCIIString}["a", "b", "a", Nullable()])
    df1[:B] = NullableArray([1, 2, 1, 3])

    df2 = DataFrame()
    df2[:A] = NullableArray(Nullable{Compat.ASCIIString}["a", Nullable(), "c"])
    df2[:C] = NullableArray([1, 2, 4])

    m1 = join(df1, df2, on = :A)
    @test size(m1) == (3,3)
    @test isequal(m1[:A], NullableArray(Nullable{Compat.ASCIIString}[Nullable(),"a","a"]))

    m2 = join(df1, df2, on = :A, kind = :outer)
    @test size(m2) == (5,3)
    @test isequal(m2[:A], NullableArray(Nullable{Compat.ASCIIString}[Nullable(),"a","a","b","c"]))

    srand(1)
    df1 = DataFrame(
        a = [:x,:y][rand(1:2, 10)],
        b = [:A,:B][rand(1:2, 10)],
        v1 = randn(10)
    )

    df2 = DataFrame(
        a = [:x,:y][[1,2,1,1,2]],
        b = [:A,:B,:C][[1,1,1,2,3]],
        v2 = randn(5)
    )
    df2[1,:a] = Nullable()

    # # TODO: Restore this functionality
    # m1 = join(df1, df2, on = [:a,:b])
    # @test isequal(m1[:a], NullableArray(["x", "x", "y", "y", fill("x", 5)]))
    # m2 = join(df1, df2, on = ["a","b"], kind = :outer)
    # @test isequal(m2[10,:v2], Nullable())
    # @test isequal(m2[:a],
    #               NullableArray(Nullable{String}["x", "x", "y", "y",
    #                                              "x", "x", "x", "x", "x", "y",
    #                                              Nullable(), "y"])

    srand(1)
    function spltdf(d)
        d[:x1] = map(x -> x[1], d[:a])
        d[:x2] = map(x -> x[2], d[:a])
        d[:x3] = map(x -> x[3], d[:a])
        d
    end
    df1 = DataFrame(
        a = ["abc", "abx", "axz", "def", "dfr"],
        v1 = randn(5)
    )
    df1 = spltdf(df1)
    df2 = DataFrame(
        a = ["def", "abc","abx", "axz", "xyz"],
        v2 = randn(5)
    )
    df2 = spltdf(df2)

    # m1 = join(df1, df2, on = :a)
    # m2 = join(df1, df2, on = [:x1, :x2, :x3])
    # @test isequal(sort(m1[:a]), sort(m2[:a]))

    # test nonunique() with extra argument
    df1 = DataFrame(a = ["a", "b", "a", "b", "a", "b"], b = 1:6, c = [1:3;1:3])
    df = vcat(df1, df1)
    @test find(nonunique(df)) == collect(7:12)
    @test find(nonunique(df, :)) == collect(7:12)
    @test find(nonunique(df, Colon())) == collect(7:12)
    @test find(nonunique(df, :a)) == collect(3:12)
    @test find(nonunique(df, [:a, :c])) == collect(7:12)
    @test find(nonunique(df, [1, 3])) == collect(7:12)
    @test find(nonunique(df, 1)) == collect(3:12)

    # Test unique() with extra argument
    @test isequal(unique(df), df1)
    @test isequal(unique(df, :), df1)
    @test isequal(unique(df, Colon()), df1)
    @test isequal(unique(df, 2:3), df1)
    @test isequal(unique(df, 3), df1[1:3,:])
    @test isequal(unique(df, [1, 3]), df1)
    @test isequal(unique(df, [:a, :c]), df1)
    @test isequal(unique(df, :a), df1[1:2,:])

    #test unique!() with extra argument
    unique!(df, [1, 3])
    @test isequal(df, df1)
end
