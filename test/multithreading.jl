module TestMultithreading

using Test, DataFrames

@testset "split_indices" begin
    for len in 1:100, basesize in 1:10
        x = DataFrames.split_indices(len, basesize)

        @test length(x) == max(1, div(len, basesize))
        @test reduce(vcat, x) == 1:len
        vmin, vmax = extrema(length(v) for v in x)
        @test vmin + 1 == vmax || vmin == vmax
        @test len < basesize || vmin >= basesize
    end

    @test_throws AssertionError DataFrames.split_indices(0, 10)
    @test_throws AssertionError DataFrames.split_indices(10, 0)

    # Check overflow on 32-bit
    len = typemax(Int32)
    basesize = 100_000_000
    x = collect(DataFrames.split_indices(len, basesize))
    @test length(x) == div(len, basesize)
    @test x[1][1] === 1
    @test x[end][end] === Int(len)
    vmin, vmax = extrema(length(v) for v in x)
    @test vmin + 1 == vmax || vmin == vmax
    @test len < basesize || vmin >= basesize
end

@testset "split_to_chunks" begin
    for lg in 1:100, nt in 1:11
        if lg < nt
            @test_throws AssertionError DataFrames.split_to_chunks(lg, nt)
            continue
        end
        x = collect(DataFrames.split_to_chunks(lg, nt))
        @test reduce(vcat, x) == 1:lg
        @test sum(length, x) == lg
        @test first(x[1]) == 1
        @test last(x[end]) == lg
        @test length(x) == nt
        for i in 1:nt-1
            @test first(x[i+1])-last(x[i]) == 1
        end
    end

    @test_throws AssertionError DataFrames.split_to_chunks(0, 10)
    @test_throws AssertionError DataFrames.split_to_chunks(10, 0)
    @test_throws AssertionError DataFrames.split_to_chunks(10, 11)
end

@testset "@spawn_or_run_task and @spawn_or_run" begin
    for threads in (true, false)
        t = DataFrames.@spawn_or_run_task threads 1
        @test fetch(t) === 1

        x = Ref(false)
        @sync begin
            t = DataFrames.@spawn_or_run_task threads begin
                sleep(0.1)
                x[] = true
            end
        end
        @test x[]

        x = Ref(false)
        @sync begin
            res = DataFrames.@spawn_or_run threads begin
                sleep(0.1)
                x[] = true
            end
            @test res === nothing
        end
        @test x[]
    end
end

@testset "disabling multithreading via keyword argument" begin
    refdf = DataFrame(x=1:1000, y=rand(1:4, 1000))

    # On DataFrame
    df = copy(refdf)
    n = Ref(0)
    @test combine(df, [] => (() -> n[] += 1) => :n1,
                  [] => (() -> n[] += 1) => :n2,
                  threads=false) ==
        DataFrame(n1=1, n2=2)
    n = Ref(0)
    @test combine(df, [] => ByRow(() -> n[] += 1) => :n1,
                  [] => ByRow(() -> n[] += 1) => :n2,
                  threads=false) ==
        DataFrame(n1=1:1000, n2=1001:2000)

    df = copy(refdf)
    m = Ref(0)
    n = Ref(0)
    @test select(df, [] => (() -> m[] += 1) => :n1,
                  [] => (() -> m[] += 1) => :n2,
                  threads=false) ==
        select!(df, [] => (() -> n[] += 1) => :n1,
                  [] => (() -> n[] += 1) => :n2,
                  threads=false) ==
        DataFrame(n1=fill(1, 1000), n2=fill(2, 1000))
    df = copy(refdf)
    m = Ref(0)
    n = Ref(0)
    @test select(df, [] => ByRow(() -> m[] += 1) => :n1,
                  [] => ByRow(() -> m[] += 1) => :n2,
                  threads=false) ==
        select!(df, [] => ByRow(() -> n[] += 1) => :n1,
                  [] => ByRow(() -> n[] += 1) => :n2,
                  threads=false) ==
        DataFrame(n1=1:1000, n2=1001:2000)

    df = copy(refdf)
    m = Ref(0)
    n = Ref(0)
    @test transform(df, [] => (() -> m[] += 1) => :n1,
                    [] => (() -> m[] += 1) => :n2,
                    threads=false) ==
        transform!(df, [] => (() -> n[] += 1) => :n1,
                   [] => (() -> n[] += 1) => :n2,
                   threads=false) ==
        [refdf DataFrame(n1=fill(1, 1000), n2=fill(2, 1000))]
    df = copy(refdf)
    m = Ref(0)
    n = Ref(0)
    @test transform(df, [] => ByRow(() -> m[] += 1) => :n1,
                    [] => ByRow(() -> m[] += 1) => :n2,
                    threads=false) ==
        transform(df, [] => ByRow(() -> n[] += 1) => :n1,
                  [] => ByRow(() -> n[] += 1) => :n2,
                  threads=false) ==
        [refdf DataFrame(n1=1:1000, n2=1001:2000)]

    df = copy(refdf)
    m = Ref(0)
    n = Ref(0)
    @test df[1:100,:] ==
        subset(df, [] => ByRow(() -> (m[] += 1; m[] <= 100)),
                 [] => ByRow(() -> (m[] += 1; m[] <= 1100)),
                 threads=false) ==
        subset(df, [] => ByRow(() -> (n[] += 1; n[] <= 100)),
                 [] => ByRow(() -> (n[] += 1; n[] <= 1100)),
                 threads=false)

    # On GroupedDataFrame
    df = copy(refdf)
    gd = groupby(df, :y)
    n = Ref(0)
    @test combine(gd, [] => (() -> n[] += 1) => :n1,
                  [] => (() -> n[] += 1) => :n2,
                  threads=false) ==
        DataFrame(y=1:4, n1=1:4, n2=5:8)
    if Threads.nthreads() > 1
        @test combine(gd, [] => (() -> Threads.threadid()) => :id1,
                      [] => (() -> Threads.threadid()) => :id2,
                      threads=true) !=
            DataFrame(y=1:4, id1=1, id2=1)
    end

    df = copy(refdf)
    gd = groupby(df, :y)
    m = Ref(0)
    n = Ref(0)
    @test select(gd, [] => (() -> m[] += 1) => :n1,
                 [] => (() -> m[] += 1) => :n2,
                 threads=false) ==
        select!(gd, [] => (() -> n[] += 1) => :n1,
                [] => (() -> n[] += 1) => :n2,
                threads=false) ==
        select(leftjoin(refdf, DataFrame(y=1:4, n1=1:4, n2=5:8), on=:y), :y, :n1, :n2)
    if Threads.nthreads() > 1
        df = copy(refdf)
        gd = groupby(df, :y)
        @test select(gd, [] => (() -> Threads.threadid()) => :id1,
                     [] => (() -> Threads.threadid()) => :id2,
                     threads=true) !=
            DataFrame(y=refdf.y, id1=1, id2=1)
        @test select!(gd, [] => (() -> Threads.threadid()) => :id1,
                      [] => (() -> Threads.threadid()) => :id2,
                      threads=true) !=
            DataFrame(y=refdf.y, id1=1, id2=1)
    end

    df = copy(refdf)
    gd = groupby(df, :y)
    m = Ref(0)
    n = Ref(0)
    @test transform(gd, [] => (() -> m[] += 1) => :n1,
                    [] => (() -> m[] += 1) => :n2,
                    threads=false) ==
        transform!(gd, [] => (() -> n[] += 1) => :n1,
                [] => (() -> n[] += 1) => :n2,
                    threads=false) ==
        leftjoin(refdf, DataFrame(y=1:4, n1=1:4, n2=5:8), on=:y)
    if Threads.nthreads() > 1
        df = copy(refdf)
        gd = groupby(df, :y)
        @test transform(gd, [] => (() -> Threads.threadid()) => :id1,
                        [] => (() -> Threads.threadid()) => :id2,
                        threads=true) !=
            [refdf DataFrame(id1=fill(1, nrow(refdf)), id2=1)]
        @test transform!(gd, [] => (() -> Threads.threadid()) => :id1,
                         [] => (() -> Threads.threadid()) => :id2,
                         threads=true) !=
            [refdf DataFrame(id1=fill(1, nrow(refdf)), id2=1)]
    end

    df = copy(refdf)
    gd = groupby(df, :y)
    m = Ref(0)
    n = Ref(0)
    @test df[in.(df.y, Ref((keys(gd)[1].y, keys(gd)[2].y))),:] ==
        subset(gd, [:y] => (y -> (m[] += 1; fill(m[] <= 2, length(y)))),
               [:y] => (y -> (m[] += 1; fill(m[] <= 6, length(y)))),
               threads=false) ==
        subset!(gd, [:y] => (y -> (n[] += 1; fill(n[] <= 2, length(y)))),
                [:y] => (y -> (n[] += 1; fill(n[] <= 6, length(y)))),
                threads=false)

    # unstack
    df = DataFrame(id=[1, 1, 1, 2, 2, 3, 3],
                   variable=[:a, :a, :b, :a, :b, :a, :b],
                   value=1:7)
    l = Ref(0)
    m = Ref(0)
    n = Ref(0)
    unstack(df, combine=x -> (l[] += 1),
            threads=false) ==
            DataFrame(id=1:3, a=[1, 3, 5], b=[2, 4, 6]) ==
    unstack(df, :variable, :value, combine=x -> (m[] += 1),
            threads=false) ==
            DataFrame(id=1:3, a=[1, 3, 5], b=[2, 4, 6]) ==
    unstack(df, :id, :variable, :value, combine=x -> (n[] += 1),
            threads=false) ==
            DataFrame(id=1:3, a=[1, 3, 5], b=[2, 4, 6])

    # describe
    df = DataFrame(x=1:10, y=2:11)
    n = Ref(0)
    @test describe(df, cols=All() .=> (x -> (n[] += 1))) ==
        describe(DataFrame(x_function=1, y_function=2))
    n = Ref(0)
    @test describe(df, cols=All() .=> ByRow(x -> (n[] += 1))) ==
        describe(DataFrame(x_function=1:10, y_function=11:20))

    # nonunique
    df = DataFrame(x=1:10, y=2:11)
    n = Ref(0)
    @test nonunique(df, [:x => (x -> (@assert(n[] == 0); n[] += 1)),
                         :y => (y -> (@assert(n[] == 1); n[] += 1))]) ==
        [false, true, true, true, true, true, true, true, true, true]
    n = Ref(0)
    @test nonunique(df, [:x => ByRow(x -> (n[] == 2 ? n[] = 1 : n[] += 1)),
                         :y => ByRow(x -> (n[] == 4 ? n[] = 1 : n[] += 1))]) ==
        [false, false, false, false, true, true, true, true, true, true]

end

end # module
