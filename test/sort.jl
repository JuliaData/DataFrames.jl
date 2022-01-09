module TestSort

using DataFrames, Random, Test, CategoricalArrays

@testset "standard tests" begin
    dv1 = [9, 1, 8, missing, 3, 3, 7, missing]
    dv2 = [9, 1, 8, missing, 3, 3, 7, missing]
    dv3 = Vector{Union{Int, Missing}}(1:8)
    cv1 = CategoricalArray(dv1, ordered=true)

    d = DataFrame(dv1=dv1, dv2=dv2, dv3=dv3, cv1=cv1)

    @test sortperm(d) == sortperm(dv1)
    @test sortperm(d[:, [:dv3, :dv1]]) == sortperm(dv3)
    @test sort(d, :dv1)[!, :dv3] == sort(d, "dv1")[!, "dv3"] == sortperm(dv1)
    @test sort(d, :dv2)[!, :dv3] == sortperm(dv1)
    @test sort(d, :cv1)[!, :dv3] == sortperm(dv1)
    @test sort(d, [:dv1, :cv1])[!, :dv3] == sortperm(dv1)
    @test sort(d, [:dv1, :dv3])[!, :dv3] == sortperm(dv1)

    df = DataFrame(rank=rand(1:12, 1000),
                   chrom=rand(1:24, 1000),
                   pos=rand(1:100000, 1000))

    @test issorted(sort(df))
    @test issorted(sort(df, rev=true), rev=true)
    @test issorted(sort(df, [:chrom, :pos])[:, [:chrom, :pos]])
    @test issorted(sort(df, ["chrom", "pos"])[:, ["chrom", "pos"]])

    ds = sort(df, [order(:rank, rev=true), :chrom, :pos])
    @test issorted(ds, [order(:rank, rev=true), :chrom, :pos])
    @test issorted(ds, rev=[true, false, false])

    ds = sort(df, [order("rank", rev=true), "chrom", "pos"])
    @test issorted(ds, [order("rank", rev=true), "chrom", "pos"])
    @test issorted(ds, rev=[true, false, false])

    ds2 = sort(df, [:rank, :chrom, :pos], rev=[true, false, false])
    @test issorted(ds2, [order(:rank, rev=true), :chrom, :pos])
    @test issorted(ds2, rev=[true, false, false])

    @test ds2 == ds

    ds2 = sort(df, ["rank", "chrom", "pos"], rev=[true, false, false])
    @test issorted(ds2, [order("rank", rev=true), "chrom", "pos"])
    @test issorted(ds2, rev=[true, false, false])

    @test ds2 == ds

    sort!(df, [:rank, :chrom, :pos], rev=[true, false, false])
    @test issorted(df, [order(:rank, rev=true), :chrom, :pos])
    @test issorted(df, rev=[true, false, false])

    @test df == ds

    sort!(df, ["rank", "chrom", "pos"], rev=[true, false, false])
    @test issorted(df, [order("rank", rev=true), "chrom", "pos"])
    @test issorted(df, rev=[true, false, false])

    @test df == ds

    @test_throws ArgumentError sort(df, (:rank, :chrom, :pos))

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test !issorted(df, :x)
    @test issorted(sort(df, :x), :x)

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test !issorted(df, "x")
    @test issorted(sort(df, "x"), "x")

    x = DataFrame(a=1:3, b=3:-1:1, c=3:-1:1)
    @test issorted(x)
    @test !issorted(x, [:b, :c])
    @test !issorted(x[:, 2:3], [:b, :c])
    @test issorted(sort(x, [2, 3]), [:b, :c])
    @test issorted(sort(x[:, 2:3]), [:b, :c])

    x = DataFrame(a=1:3, b=3:-1:1, c=3:-1:1)
    @test issorted(x)
    @test !issorted(x, ["b", "c"])
    @test !issorted(x[:, 2:3], ["b", "c"])
    @test issorted(sort(x, [2, 3]), ["b", "c"])
    @test issorted(sort(x[:, 2:3]), ["b", "c"])

    # Check that columns that shares the same underlying array are only permuted once PR#1072
    df = DataFrame(a=[2, 1])
    df.b = df.a
    sort!(df, :a)
    @test df == DataFrame(a=[1, 2], b=[1, 2])

    x = DataFrame(x=[1, 2, 3, 4], y=[1, 3, 2, 4])
    sort!(x, :y)
    @test x.y == [1, 2, 3, 4]
    @test x.x == [1, 3, 2, 4]

    @test_throws TypeError sort(x, by=:x)

    Random.seed!(1)
    # here there will be probably no ties
    df_rand1 = DataFrame(rand(100, 4), :auto)
    # but here we know we will have ties
    df_rand2 = copy(df_rand1)
    df_rand2.x1 = shuffle([fill(1, 50); fill(2, 50)])
    df_rand2.x4 = shuffle([fill(1, 50); fill(2, 50)])

    # test sorting by 1 column
    for df_rand in [df_rand1, df_rand2]
        # testing sort
        for n1 in names(df_rand)
            # passing column name
            @test sort(df_rand, n1) == df_rand[sortperm(df_rand[:, n1]), :]
            # passing vector with one column name
            @test sort(df_rand, [n1]) == df_rand[sortperm(df_rand[:, n1]), :]
            # passing vector with two column names
            for n2 in setdiff(names(df_rand), [n1])
                @test sort(df_rand, [n1, n2]) ==
                      df_rand[sortperm(collect(zip(df_rand[:, n1],
                                                   df_rand[:, n2]))), :]
            end
        end
        # testing if sort! is consistent with issorted and sort
        ref_df = df_rand
        for n1 in names(df_rand)
            df_rand = copy(ref_df)
            @test sort!(df_rand, n1) == sort(ref_df, n1)
            @test issorted(df_rand, n1)
            df_rand = copy(ref_df)
            @test sort!(df_rand, [n1]) == sort(ref_df, [n1])
            @test issorted(df_rand, [n1])
            for n2 in setdiff(names(df_rand), [n1])
                df_rand = copy(ref_df)
                @test sort!(df_rand, [n1, n2]) == sort(ref_df, [n1, n2])
                @test issorted(df_rand, n1)
                @test issorted(df_rand, [n1, n2])
            end
        end
    end
end

@testset "non standard selectors" begin
    Random.seed!(1234)
    df = DataFrame(rand(1:2, 1000, 4), :auto)
    for f in [sort, sort!, sortperm, issorted]
        @test f(df) == f(df, :) == f(df, All()) == f(df, Cols(:)) == f(df, r"x") ==
              f(df, Between(1, 4)) == f(df, Not([]))
    end
end

@testset "view kwarg test" begin
    df = DataFrame(rand(3, 4), :auto)
    @test sort(df) isa DataFrame
    @inferred sort(df)
    @test sort(view(df, 1:2, 1:2)) isa DataFrame
    @test sort(df, view=false) isa DataFrame
    @test sort(view(df, 1:2, 1:2), view=false) isa DataFrame
    @test sort(df, view=true) isa SubDataFrame
    @test sort(df, view=true) == sort(df)
    @test sort(view(df, 1:2, 1:2), view=true) isa SubDataFrame
    @test sort(view(df, 1:2, 1:2), view=true) == sort(view(df, 1:2, 1:2))
end

@testset "hard tests of different sorting orders" begin
    Random.seed!(1234)
    df = DataFrame(rand([0, 1], 10^5, 3), :auto)
    @test sortperm(df, :x1) == sortperm(df.x1)
    @test sortperm(df, [:x1, :x2]) == sortperm(tuple.(df.x1, df.x2))
    @test sortperm(df, [:x1, :x2, :x3]) == sortperm(tuple.(df.x1, df.x2, df.x3))
    @test sortperm(df, :x1, rev=true) == sortperm(df.x1, rev=true)
    @test sortperm(df, [:x1, :x2], rev=true) ==
          sortperm(tuple.(df.x1, df.x2), rev=true)
    @test sortperm(df, [:x1, :x2, :x3], rev=true) ==
          sortperm(tuple.(df.x1, df.x2, df.x3), rev=true)

    @test issorted(sort(df, :x1), :x1)
    @test issorted(sort(df, [:x1, :x2]), [:x1, :x2])
    @test issorted(sort(df, [:x1, :x2, :x3]), [:x1, :x2, :x3])
    @test issorted(sort(df, :x1, rev=true), :x1, rev=true)
    @test issorted(sort(df, [:x1, :x2], rev=true), [:x1, :x2], rev=true)
    @test issorted(sort(df, [:x1, :x2, :x3], rev=true), [:x1, :x2, :x3], rev=true)

    for r1 in (true, false)
        @test sortperm(df, order(:x1, rev=r1)) == sortperm((1 - 2*r1) * df.x1)
        @test issorted(sort(df, order(:x1, rev=r1)), order(:x1, rev=r1))
        for r2 in (true, false)
            @test sortperm(df, [order(:x1, rev=r1), order(:x2, rev=r2)]) ==
                  sortperm(tuple.((1 - 2*r1) * df.x1, (1 - 2*r2) * df.x2))
            @test issorted(sort(df, [order(:x1, rev=r1), order(:x2, rev=r2)]),
                           [order(:x1, rev=r1), order(:x2, rev=r2)])
            for r3 in (true, false)
                @test sortperm(df, [order(:x1, rev=r1), order(:x2, rev=r2), order(:x3, rev=r3)]) ==
                      sortperm(tuple.((1 - 2*r1) * df.x1, (1 - 2*r2) * df.x2, (1 - 2*r3) * df.x3))
                @test issorted(sort(df, [order(:x1, rev=r1), order(:x2, rev=r2), order(:x3, rev=r3)]),
                               [order(:x1, rev=r1), order(:x2, rev=r2), order(:x3, rev=r3)])
            end
        end
    end

    for i in 2:20
        df = DataFrame(ones(10, i), :auto)
        df[!, end] = randperm(10)
        @test sortperm(df) == sortperm(df[!, end])
        @test sortperm(df, [1:i-1; order(i, rev=true)]) == sortperm(df[!, end], rev=true)
        df[!, 1] = randperm(10)
        @test sortperm(df) == sortperm(df[!, 1])
        @test sortperm(df, [order(1, rev=true); 2:i-1; order(i, rev=true)]) ==
              sortperm(df[!, 1], rev=true)
    end
end

@testset "sort! tests" begin
    # safe aliasing test
    # this works because 2:5 is immutable
    df = DataFrame()
    x = [10:-1:1;]
    df.x1 = view(x, 2:5)
    df.x2 = view(x, 2:5)
    sort!(df)
    @test issorted(df)
    @test x == [10, 6, 7, 8, 9, 5, 4, 3, 2, 1]
    @test df == DataFrame(x1=6:9, x2=6:9)

    df = DataFrame()
    x = [10:-1:1;]
    df.x1 = view(x, 2:5)
    df.x2 = view(x, [2:5;])
    sort!(df)
    @test_broken issorted(df)
    @test_broken x == [10, 6, 7, 8, 9, 5, 4, 3, 2, 1]
    @test_broken df == DataFrame(x1=6:9, x2=6:9)

    df = DataFrame()
    x = [10:-1:1;]
    df.x1 = view(x, 2:5)
    df.x2 = view(x, 2:5)
    dfv = view(df, 2:4, :)
    sort!(dfv)
    @test issorted(dfv)
    @test x == [10, 9, 6, 7, 8, 5, 4, 3, 2, 1]
    @test df == DataFrame(x1=[9; 6:8], x2=[9; 6:8])

    df = DataFrame()
    x = [10:-1:1;]
    df.x1 = view(x, [2:5;])
    df.x2 = view(x, 2:5)
    dfv = view(df, 2:4, :)
    sort!(dfv)
    @test_broken issorted(dfv)
    @test_broken x == [10, 9, 6, 7, 8, 5, 4, 3, 2, 1]
    @test_broken df == DataFrame(x1=[9; 6:8], x2=[9; 6:8])

    # unsafe aliasing test
    df = DataFrame()
    x = [10:-1:1;]
    df.x1 = view(x, 2:5)
    df.x2 = view(x, 3:6)
    sort!(df)
    @test_broken x == 10:-1:1
    @test_broken df == DataFrame(x1=9:-1:6, x2=8:-1:5)

    df = DataFrame()
    x = [10:-1:1;]
    df.x1 = view(x, 2:5)
    df.x2 = view(x, 3:6)
    dfv = view(df, 2:4, :)
    sort!(dfv)
    @test_broken x == 10:-1:1
    @test_broken df == DataFrame(x1=9:-1:6, x2=8:-1:5)

    # complex view sort test
    Random.seed!(1234)
    df = DataFrame(rand(100, 10), :auto)
    insertcols!(df, 1, :id => axes(df, 1))
    df2 = copy(df)
    dfv = view(df, 100:-2:1, [5, 3, 1])
    sort!(dfv, :id)
    @test issorted(dfv, :id)
    @test select(df, Not([1, 3, 5])) == select(df2, Not([1, 3, 5]))
    @test sort(select(df, [1, 3, 5]), :id) == select(df2, [1, 3, 5])
    @test select(df, [1, 3, 5]) ==
          select(df2, [1, 3, 5])[[isodd(i) ? i : (102 - i) for i in 1:100], :]
end

@testset "check sorting kwarg argument correctness" begin
    for df in (DataFrame(x=1:3), DataFrame(x=1:3, y=1:3)), fun in (issorted, sort, sortperm, sort!)
        @test_throws TypeError fun(df, by=Int)
        @test_throws TypeError fun(df, lt=Int)
        @test_throws TypeError fun(df, rev=Int)
        @test_throws TypeError fun(df, order=Int)
        @test_throws TypeError fun(df, by=1)
        @test_throws TypeError fun(df, lt=1)
        @test_throws TypeError fun(df, rev=1)
        @test_throws TypeError fun(df, order=1)
        @test_throws TypeError fun(df, by=(identity,))
        @test_throws TypeError fun(df, lt=(isless,))
        @test_throws TypeError fun(df, rev=(true,))
        @test_throws TypeError fun(df, order=(Base.Forward,))
        @test_throws TypeError fun(df, by=(identity, identity))
        @test_throws TypeError fun(df, lt=(isless, isless))
        @test_throws TypeError fun(df, rev=(true, true))
        @test_throws TypeError fun(df, order=(Base.Forward, Base.Forward))
    end

    for df in (DataFrame(x=1:3), DataFrame(x=1:3, y=1:3)), fun in (sort, sort!)
        dfc = copy(df)
        @test fun(df, by=identity) == dfc
        @test fun(df, by=fill(identity, ncol(df))) == dfc
        @test fun(df, lt=isless) == dfc
        @test fun(df, lt=fill(isless, ncol(df))) == dfc
        @test fun(df, rev=false) == dfc
        @test fun(df, rev=fill(false, ncol(df))) == dfc
        @test fun(df, order=Base.Forward) == dfc
        @test fun(df, order=fill(Base.Forward, ncol(df))) == dfc
    end

    for df in (DataFrame(x=1:3), DataFrame(x=1:3, y=1:3))
        @test issorted(df, by=identity)
        @test issorted(df, by=fill(identity, ncol(df)))
        @test issorted(df, lt=isless)
        @test issorted(df, lt=fill(isless, ncol(df)))
        @test issorted(df, rev=false)
        @test issorted(df, rev=fill(false, ncol(df)))
        @test issorted(df, order=Base.Forward)
        @test issorted(df, order=fill(Base.Forward, ncol(df)))

        @test issorted(df, :x, by=identity)
        @test issorted(df, :x, by=[identity])
        @test issorted(df, :x, lt=isless)
        @test issorted(df, :x, lt=[isless])
        @test issorted(df, :x, rev=false)
        @test issorted(df, :x, rev=[false])
        @test issorted(df, :x, order=Base.Forward)
        @test issorted(df, :x, order=[Base.Forward])

        @test sortperm(df, by=identity) == 1:3
        @test sortperm(df, by=fill(identity, ncol(df))) == 1:3
        @test sortperm(df, lt=isless) == 1:3
        @test sortperm(df, lt=fill(isless, ncol(df))) == 1:3
        @test sortperm(df, rev=false) == 1:3
        @test sortperm(df, rev=fill(false, ncol(df))) == 1:3
        @test sortperm(df, order=Base.Forward) == 1:3
        @test sortperm(df, order=fill(Base.Forward, ncol(df))) == 1:3
    end
end

@testset "correct aliasing detection" begin
    # make sure sorting a view does not produce an error
    for sel in ([1, 2, 3], 1:3)
        df = DataFrame(a=1:5, b=11:15, c=21:25)
        dfc = copy(df)
        sdf = view(df, sel, :)
        sort!(sdf)
        @test df[:, 1:3] == dfc
    end

    Random.seed!(1234)
    for sel in ([1:100;], 1:100)
        df = DataFrame(a=rand(100), b=rand(100), c=rand(100), id=1:100)
        dfc = sort(df)
        sdf = view(df, sel, :)
        sort!(sdf)
        @test df == dfc
        sort!(df, :id)
        df.d = df.a
        sort!(sdf)
        @test df[:, 1:4] == dfc
    end

    # this is a test that different views are incorrectly handled
    x = [1:6;]
    df = DataFrame(a=view(x, 1:5), b=view(x, 6:-1:2), copycols=false)
    dfv = view(df, 1:3, :)
    sort!(dfv, :b)
    @test df == DataFrame(a=[3, 2, 1, 6, 5], b=[4, 5, 6, 1, 2])
    # this is the "correct" result if we had no aliasing
    x = [1:6;]
    df = DataFrame(a=view(x, 1:5), b=view(x, 6:-1:2))
    dfv = view(df, 1:3, :)
    sort!(dfv, :b)
    @test df == DataFrame(a=[3, 2, 1, 4, 5], b=[4, 5, 6, 3, 2])

    df = DataFrame(x1=rand(10))
    df.x2 = df.x1
    df.x3 = df.x1
    df.x4 = df.x1
    df.x5 = df.x1
    sort!(df, :x4)
    @test issorted(df)

    df = DataFrame(x1=rand(10))
    df.x2 = df.x1
    df.x3 = df.x1
    df.x4 = df.x1
    df.x5 = df.x1
    dfv = @view df[1:5, 1:4]
    sort!(dfv, :x4)
    @test issorted(dfv)
    @test issorted(df[1:5, :])
end

end # module
