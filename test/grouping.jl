module TestGrouping
    using Base.Test
    using DataFrames

    df = DataFrame(a=rep(1:4, 2), b=rep(2:-1:1, 4), c=randn(8))
    #df[6, :a] = NA
    #df[7, :b] = NA

    cols = [:a, :b]

    f(df) = DataFrame(cmax = maximum(df[:c]))

    sdf = sort(df, cols=cols)
    bdf = by(df, cols, f)

    @test bdf[cols] == unique(sdf[cols])

    byf = by(df, :a, df -> DataFrame(bsum = sum(df[:b])))

    @test all(T -> T <: AbstractVector, map(typeof, colwise([sum], df)))
    @test all(T -> T <: AbstractVector, map(typeof, colwise(sum, df)))

    gd = groupby(df, cols)
    ga = map(f, gd)

    @test bdf == combine(ga)

    g(df) = DataFrame(cmax1 = df[:cmax] + 1)
    h(df) = g(f(df))

    @test combine(map(h, gd)) == combine(map(g, ga))


    df = DataFrame(v1 = @data([1, NA, 2, 1]), v2 = @data([1, 3, 2, 1]), v3 = @data([NA, 1, 1, NA]))
    
    @test group(df, skipna = false).pool == [1, 2, 3]
    @test group(df, skipna = false).refs == [1, 2, 3, 1]
    @test group(df).pool == [1]
    @test group(df).refs == [0, 0, 1, 0]
    
    # test that type of refs of last column is promoted
    df = DataFrame(v1 = pool(1:1000), v2 = pool(fill(1, 1000)))
    @test groupby(df, [:v1, :v2]).starts == [1:1000;]
    


end
