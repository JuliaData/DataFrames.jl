module TestData
    using Base.Test, DataFrames
    importall Base # so that we get warnings for conflicts

    #test_group("constructors")
    df1 = DataFrame(Any[[1, 2, null, 4], ["one", "two", null, "four"]], [:Ints, :Strs])
    df2 = DataFrame(Any[[1, 2, null, 4], ["one", "two", null, "four"]])
    df3 = DataFrame(Any[[1, 2, null, 4]])
    df4 = DataFrame(Any[Vector{Union{Int, Null}}(1:4), Vector{Union{Int, Null}}(1:4)])
    df5 = DataFrame(Any[Union{Int, Null}[1, 2, 3, 4], ["one", "two", null, "four"]])
    df6 = DataFrame(Any[[1, 2, null, 4], [1, 2, null, 4], ["one", "two", null, "four"]],
                    [:A, :B, :C])
    df7 = DataFrame(x = [1, 2, null, 4], y = ["one", "two", null, "four"])
    @test size(df7) == (4, 2)
    @test df7[:x] == [1, 2, null, 4]

    #test_group("description functions")
    @test size(df6, 1) == 4
    @test size(df6, 2) == 3
    @test names(df6) == [:A, :B, :C]
    @test names(df2) == [:x1, :x2]
    @test names(df7) == [:x, :y]

    #test_group("ref")
    @test df6[2, 3] == "two"
    @test isnull(df6[3, 3])
    @test df6[2, :C] == "two"
    @test df6[:B] == [1, 2, null, 4]
    @test size(df6[[2,3]], 2) == 2
    @test size(df6[2,:], 1) == 1
    @test size(df6[[1, 3], [1, 3]]) == (2, 2)
    @test size(df6[1:2, 1:2]) == (2, 2)
    @test size(head(df6,2)) == (2, 3)
    # lots more to do

    #test_group("assign")
    df6[3] = ["un", "deux", "trois", "quatre"]
    @test df6[1, 3] == "un"
    df6[:B] = [4, 3, 2, 1]
    @test df6[1,2] == 4
    df6[:D] = [true, false, true, false]
    @test df6[1,4]
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
    @test sdf6a[1,2] == 4

    #test_context("Within")
    #test_group("Associative")

    #test_group("DataFrame")
    srand(1)
    N = 20
    #Cast to Int64 as rand() behavior differs between Int32/64
    d1 = Vector{Union{Int64, Null}}(rand(map(Int64, 1:2), N))
    d2 = CategoricalArray(["A", "B", null])[rand(map(Int64, 1:3), N)]
    d3 = randn(N)
    d4 = randn(N)
    df7 = DataFrame(Any[d1, d2, d3], [:d1, :d2, :d3])

    #test_group("groupby")
    gd = groupby(df7, :d1)
    @test length(gd) == 2
    @test gd[2][:d2] == CategoricalVector(["B", null, "A", null, null, null, null, null, "A"])
    @test sum(gd[2][:d3]) == sum(df7[:d3][df7[:d1] .== 2])

    g1 = groupby(df7, [:d1, :d2])
    g2 = groupby(df7, [:d2, :d1])
    @test sum(g1[1][:d3]) == sum(g2[1][:d3])

    res = 0.0
    for x in g1
        res += sum(x[:d1])
    end
    @test res == sum(df7[:d1])

    @test aggregate(DataFrame(a=1), identity) == DataFrame(a_identity=1)

    df8 = aggregate(df7[[1, 3]], sum)
    @test df8[1, :d1_sum] == sum(df7[:d1])

    df8 = aggregate(df7, :d2, [sum, length], sort=true)
    @test df8[1:2, :d2] == ["A", "B"]
    @test size(df8, 1) == 3
    @test size(df8, 2) == 5
    @test sum(df8[:d1_length]) == N
    @test all(df8[:d1_length] .> 0)
    @test df8[:d1_length] == [4, 5, 11]
    @test df8 == aggregate(groupby(df7, :d2, sort=true), [sum, length])
    @test df8[1, :d1_length] == 4
    @test df8[2, :d1_length] == 5
    @test df8[3, :d1_length] == 11
    @test df8 == aggregate(groupby(df7, :d2), [sum, length], sort=true)

    df9 = df7 |> groupby([:d2], sort=true) |> [sum, length]
    @test df9 == df8
    df9 = aggregate(df7, :d2, [sum, length], sort=true)
    @test df9 == df8

    df10 = DataFrame(
        Any[[1:4;], [2:5;], ["a", "a", "a", "b" ], ["c", "d", "c", "d"]],
        [:d1, :d2, :d3, :d4]
    )

    gd = groupby(df10, [:d3], sort=true)
    ggd = groupby(gd[1], [:d3, :d4], sort=true) # make sure we can groupby subDataFrames
    @test ggd[1][1, :d3] == "a"
    @test ggd[1][1, :d4] == "c"
    @test ggd[1][2, :d3] == "a"
    @test ggd[1][2, :d4] == "c"
    @test ggd[2][1, :d3] == "a"
    @test ggd[2][1, :d4] == "d"

    #test_group("reshape")
    d1 = DataFrame(a = Array{Union{Int, Null}}(repeat([1:3;], inner = [4])),
                   b = Array{Union{Int, Null}}(repeat([1:4;], inner = [3])),
                   c = Array{Union{Float64, Null}}(randn(12)),
                   d = Array{Union{Float64, Null}}(randn(12)),
                   e = Array{Union{String, Null}}(map(string, 'a':'l')))

    stack(d1, :a)
    d1s = stack(d1, [:a, :b])
    d1s2 = stack(d1, [:c, :d])
    d1s3 = stack(d1)
    d1m = melt(d1, [:c, :d, :e])
    @test d1s[1:12, :c] == d1[:c]
    @test d1s[13:24, :c] == d1[:c]
    @test d1s2 == d1s3
    @test names(d1s) == [:variable, :value, :c, :d, :e]
    @test d1s == d1m
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
    @test d1s[1:12, :c] == d1[:c]
    @test d1s[13:24, :c] == d1[:c]
    @test d1s2 == d1s3
    @test names(d1s) == [:variable, :value, :c, :d, :e]
    @test d1s == d1m
    d1m = meltdf(d1[[1,3,4]], :a)
    @test names(d1m) == [:variable, :value, :a]

    d1s_named = stackdf(d1, [:a, :b], variable_name=:letter, value_name=:someval)
    @test names(d1s_named) == [:letter, :someval, :c, :d, :e]
    d1m_named = meltdf(d1, [:c, :d, :e], variable_name=:letter, value_name=:someval)
    @test names(d1m_named) == [:letter, :someval, :c, :d, :e]

    d1s[:id] = Union{Int, Null}[1:12; 1:12]
    d1s2[:id] =  Union{Int, Null}[1:12; 1:12]
    d1us = unstack(d1s, :id, :variable, :value)
    d1us2 = unstack(d1s2)
    d1us3 = unstack(d1s2, :variable, :value)
    @test d1us[:a] == d1[:a]
    @test d1us2[:d] == d1[:d]
    @test d1us2[:3] == d1[:d]

    #test_group("merge")

    srand(1)
    df1 = DataFrame(a = shuffle!(Vector{Union{Int, Null}}(1:10)),
                    b = rand(Union{Symbol, Null}[:A,:B], 10),
                    v1 = Vector{Union{Float64, Null}}(randn(10)))

    df2 = DataFrame(a = shuffle!(Vector{Union{Int, Null}}(1:5)),
                    b2 = rand(Union{Symbol, Null}[:A,:B,:C], 5),
                    v2 = Vector{Union{Float64, Null}}(randn(5)))

    m1 = join(df1, df2, on = :a, kind=:inner)
    @test m1[:a] == df1[:a][df1[:a] .<= 5] # preserves df1 order
    m2 = join(df1, df2, on = :a, kind = :outer)
    @test m2[:a] == df1[:a] # preserves df1 order
    @test m2[:b] == df1[:b] # preserves df1 order
    m2 = join(df1, df2, on = :a, kind = :outer)
    @test m2[:b2] == [null, :A, :A, null, :C, null, null, :B, null, :A]

    df1 = DataFrame(a = Union{Int, Null}[1, 2, 3],
                    b = Union{String, Null}["America", "Europe", "Africa"])
    df2 = DataFrame(a = Union{Int, Null}[1, 2, 4],
                    c = Union{String, Null}["New World", "Old World", "New World"])

    m1 = join(df1, df2, on = :a, kind = :inner)
    @test m1[:a] == [1, 2]

    m2 = join(df1, df2, on = :a, kind = :left)
    @test m2[:a] == [1, 2, 3]

    m3 = join(df1, df2, on = :a, kind = :right)
    @test m3[:a] == [1, 2, 4]

    m4 = join(df1, df2, on = :a, kind = :outer)
    @test m4[:a] == [1, 2, 3, 4]

    # test with nulls (issue #185)
    df1 = DataFrame()
    df1[:A] = ["a", "b", "a", null]
    df1[:B] = Union{Int, Null}[1, 2, 1, 3]

    df2 = DataFrame()
    df2[:A] = ["a", null, "c"]
    df2[:C] = Union{Int, Null}[1, 2, 4]

    m1 = join(df1, df2, on = :A)
    @test size(m1) == (3,3)
    @test m1[:A] == ["a","a", null]

    m2 = join(df1, df2, on = :A, kind = :outer)
    @test size(m2) == (5,3)
    @test m2[:A] == ["a", "b", "a", null, "c"]

    srand(1)
    df1 = DataFrame(
        a = rand(Union{Symbol, Null}[:x,:y], 10),
        b = rand(Union{Symbol, Null}[:A,:B], 10),
        v1 = Vector{Union{Float64, Null}}(randn(10))
    )

    df2 = DataFrame(
        a = Union{Symbol, Null}[:x,:y][[1,2,1,1,2]],
        b = Union{Symbol, Null}[:A,:B,:C][[1,1,1,2,3]],
        v2 = Vector{Union{Float64, Null}}(randn(5))
    )
    df2[1,:a] = null

    m1 = join(df1, df2, on = [:a,:b])
    @test m1[:a] == Union{Nulls.Null, Symbol}[:x, :x, :y, :y, :y, :x, :x, :x]
    m2 = join(df1, df2, on = [:a,:b], kind = :outer)
    @test m2[10,:v2] == null
    @test m2[:a] == [:x, :x, :y, :y, :y, :x, :x, :y, :x, :y, null, :y]

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

    m1 = join(df1, df2, on = :a)
    m2 = join(df1, df2, on = [:x1, :x2, :x3])
    @test sort(m1[:a]) == sort(m2[:a])

    # test nonunique() with extra argument
    df1 = DataFrame(a = Union{String, Null}["a", "b", "a", "b", "a", "b"],
                    b = Vector{Union{Int, Null}}(1:6),
                    c = Union{Int, Null}[1:3;1:3])
    df = vcat(df1, df1)
    @test find(nonunique(df)) == collect(7:12)
    @test find(nonunique(df, :)) == collect(7:12)
    @test find(nonunique(df, Colon())) == collect(7:12)
    @test find(nonunique(df, :a)) == collect(3:12)
    @test find(nonunique(df, [:a, :c])) == collect(7:12)
    @test find(nonunique(df, [1, 3])) == collect(7:12)
    @test find(nonunique(df, 1)) == collect(3:12)

    # Test unique() with extra argument
    @test unique(df) == df1
    @test unique(df, :) == df1
    @test unique(df, Colon()) == df1
    @test unique(df, 2:3) == df1
    @test unique(df, 3) == df1[1:3,:]
    @test unique(df, [1, 3]) == df1
    @test unique(df, [:a, :c]) == df1
    @test unique(df, :a) == df1[1:2,:]

    #test unique!() with extra argument
    unique!(df, [1, 3])
    @test df == df1
end
