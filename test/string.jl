module TestStringIndexing

using Test, DataFrames

@testset "iteration" begin
    df = DataFrame(a=1:2, b=3:4)
    er = eachrow(df)
    ec = eachcol(df)
    @test er.a == er."a" == ec.a == ec."a" == df.a == df."a"
    @test_throws ArgumentError er.c
    @test_throws ArgumentError er."c"
    @test_throws ArgumentError ec.c
    @test_throws ArgumentError ec."c"
    @test_throws ArgumentError df.c
    @test_throws ArgumentError df."c"
    @test hasproperty(er, :a) == hasproperty(er, "a") ==
          hasproperty(ec, :a) == hasproperty(ec, "a") ==
          hasproperty(df, :a) == hasproperty(df, "a") == true
    @test hasproperty(er, :c) == hasproperty(er, "c") ==
          hasproperty(ec, :c) == hasproperty(ec, "c") ==
          hasproperty(df, :c) == hasproperty(df, "c") == false

    @test keys(er) == 1:2
    @test propertynames(er) == propertynames(ec) == propertynames(df) ==
          keys(ec) == [:a, :b]
    @test_throws MethodError keys(df)
end

@testset "joins" begin
    df1 = DataFrame(a=1, b=2)
    df2 = DataFrame(a=1, c=2)
    df3 = DataFrame(a=1, d=2)

    # only check if the output is the same in all cases
    for f in (innerjoin, leftjoin, rightjoin, outerjoin, antijoin, semijoin)
        @test f(df1, df2, on=:a) == f(df1, df2, on="a") ==
              f(df1, df2, on=[:a]) == f(df1, df2, on=["a"]) ==
              f(df1, df2, on=:a => :a) == f(df1, df2, on="a" => "a") ==
              f(df1, df2, on=[:a => :a]) == f(df1, df2, on=["a" => "a"])
        @test_throws TypeError f(df1, df2, on = :a => "a")
        @test_throws ArgumentError f(df1, df2, on = [:a => "a"])

        if f === innerjoin || f === outerjoin
            @test f(df1, df2, df3, on=:a) == f(df1, df2, df3, on="a") ==
                  f(df1, df2, df3, on=[:a]) == f(df1, df2, df3, on=["a"]) ==
                  f(df1, df2, df3, on=:a => :a) == f(df1, df2, df3, on="a" => "a") ==
                  f(df1, df2, df3, on=[:a => :a]) == f(df1, df2, df3, on=["a" => "a"])
            @test_throws TypeError f(df1, df2, df3, on = :a => "a")
            @test_throws ArgumentError f(df1, df2, df3, on = [:a => "a"])
        end
    end
end

@testset "reshape" begin
    df = DataFrame(a=repeat([1:3;], inner=[4]),
                   b=repeat([1:4;], inner=[3]),
                   c=1:12, d=1.0:12.0,
                   e=map(string, 'a':'l'))

    # only check if the output is the same in all cases
    for v in (true, false)
        @test stack(df, [:c, :d], [:a], variable_name=:varn, value_name=:valn, view=v) ==
              stack(df, ["c", "d"], ["a"], variable_name="varn", value_name="valn", view=v)
    end

    wide = DataFrame(id=1:12,
                     a=repeat([1:3;], inner=[4]),
                     b=repeat([1:4;], inner=[3]),
                     c=randn(12),
                     d=randn(12))

    long = stack(wide)
    @test unstack(long, :variable, :value) == unstack(long, "variable", "value")
    @test unstack(long, :id, :variable, :value) ==
          unstack(long, "id", "variable", "value")
    @test unstack(long, [:id, :a], :variable, :value) ==
          unstack(long, ["id", "a"], "variable", "value")
    @test unstack(long, :id, :variable, :value, renamecols=x->Symbol(:_, x)) ==
          unstack(long, "id", "variable", "value", renamecols=x->"_"*x)
end

@testset "selection" begin
    df = DataFrame(a=1:2, b=3:4)
    # only check if the output is the same in all cases
    @test select(df, :a, :b => :d, :b => identity => :d2, :b => identity,
                 [:a, :a] => (+), [:a, :a] => (+) => :e, AsTable(:a) => ByRow(first),
                 nrow => :xxx) ==
          select(df, "a", "b" => "d", "b" => identity => "d2", "b" => identity,
                 ["a", "a"] => (+), ["a", "a"] => (+) => "e", AsTable("a") => ByRow(first),
                 nrow => "xxx")
    @test transform(df, :a, :b => :d, :b => identity => :d2, :b => identity,
                    [:a, :a] => (+), [:a, :a] => (+) => :e, AsTable(:a) => ByRow(first),
                    nrow => :xxx) ==
          transform(df, "a", "b" => "d", "b" => identity => "d2", "b" => identity,
                    ["a", "a"] => (+), ["a", "a"] => (+) => "e", AsTable("a") => ByRow(first),
                    nrow => "xxx")
    @test select(df, [:a]) == select(df, ["a"]) ==
          select(df, :a) == select(df, "a")
    @test transform(df, [:a]) == transform(df, ["a"]) ==
          transform(df, :a) == transform(df, "a")

    df2 = copy(df)
    @test select!(df, :a, :b => :d, :b => identity => :d2, :b => identity,
                  [:a, :a] => (+), [:a, :a] => (+) => :e, AsTable(:a) => ByRow(first),
                  nrow => :xxx) ==
          select!(df2, "a", "b" => "d", "b" => identity => "d2", "b" => identity,
                  ["a", "a"] => (+), ["a", "a"] => (+) => "e", AsTable("a") => ByRow(first),
                  nrow => "xxx")

    df = DataFrame(a=1:2, b=3:4)
    df2 = copy(df)
    @test select!(df, [:a]) == select!(df2, ["a"])

    df = DataFrame(a=1:2, b=3:4)
    df2 = copy(df)
    @test select!(df, :a) == select!(df2, "a")

    df = DataFrame(a=1:2, b=3:4)
    df2 = copy(df)
    @test transform!(df, :a, :b => :d, :b => identity => :d2, :b => identity,
                    [:a, :a] => (+), [:a, :a] => (+) => :e, AsTable(:a) => ByRow(first),
                    nrow => :xxx) ==
          transform!(df2, "a", "b" => "d", "b" => identity => "d2", "b" => identity,
                    ["a", "a"] => (+), ["a", "a"] => (+) => "e", AsTable("a") => ByRow(first),
                    nrow => "xxx")

    df = DataFrame(a=1:2, b=3:4)
    df2 = copy(df)
    @test transform!(df, [:a]) == transform!(df2, ["a"])

    df = DataFrame(a=1:2, b=3:4)
    df2 = copy(df)
    @test transform!(df, :a) == transform!(df2, "a")

    df = DataFrame(a=1:2, b=3:4)
    @test_throws ArgumentError select(df, [:a, "b"])
    @test_throws ArgumentError select(df, ["a", :b])
    @test_throws ArgumentError select(df, ["a", "b", "a"])
end

@testset "tables" begin
    df = DataFrame(a=1:2, b=3:4)

    @test columnindex(df, :a) == columnindex(df, "a")
    @test columnindex(df, :c) == columnindex(df, "c")

    @test Tables.schema(df) == Tables.schema(Tables.columntable(df))
    @test [:a, :b] == Tables.columnnames(df) == Tables.columnnames(df[1, :]) ==
          Tables.columnnames(eachrow(df)) == Tables.columnnames(eachcol(df))
end

@testset "split-apply-combine" begin
    df = DataFrame(g=[1, 1, 1, 2, 2], a=1:5)

    # only check if the output is the same in all cases
    gdf = groupby(df, :g)
    @test gdf == groupby(df, "g")
    @test groupby(df, [:g, :a]) == groupby(df, ["g", "a"])

    @test names(gdf) == ["g", "a"]

    k = keys(gdf)
    @test names(k[1]) == ["g"]
    @test propertynames(k[1]) == keys(k[1]) == [:g]
    @test haskey(k[1], :g) == haskey(k[1], "g") == true
    @test haskey(k[1], :a) == haskey(k[1], "a") == false
    @test  k[1].g == k[1]."g" == k[1][:g] == k[1]["g"]

    @test combine(gdf, :a) == combine(gdf, "a") ==
          combine(gdf, [:a]) == combine(gdf, ["a"])

    @test combine(gdf, "a" => identity, ungroup=false) ==
          combine(gdf, :a => identity, ungroup=false)
    @test combine(gdf, ["a"] => identity, ungroup=false) ==
          combine(gdf, [:a] => identity, ungroup=false)
    @test combine(gdf, nrow => :n, ungroup=false) ==
          combine(gdf, nrow => "n", ungroup=false)

    @test combine(gdf, "a" => identity) == combine(gdf, :a => identity)
    @test combine(gdf, ["a"] => identity) == combine(gdf, [:a] => identity)
    @test combine(gdf, nrow => :n) == combine(gdf, nrow => "n")
end

@testset "DataFrameRow" begin
    dfr = DataFrame(a=1:2, b=3:4, c=5:6)[2, ["c", "a"]]
    @test names(dfr) == ["c", "a"]
    @test names(dfr, "a") == names(dfr, :a) == names(dfr, 2) == names(dfr, Not("c")) ==
          names(dfr, ["a"]) == names(dfr, Cols("a")) == names(dfr, Between("a", "a")) == ["a"]
    @test keys(dfr) == propertynames(dfr) == [:c, :a]
    @test haskey(dfr, :a) == haskey(dfr, "a") == true
    @test haskey(dfr, :z) == haskey(dfr, "z") == false
    @test hasproperty(dfr, :a) == hasproperty(dfr, "a") == true
    @test hasproperty(dfr, :z) == hasproperty(dfr, "z") == false
    @test dfr["a"] == dfr[:a] == dfr[2]
    dfr["a"] = 1000
    @test dfr."a" == 1000
    dfr[1:2] = Dict("a" => 100, "c" => 500)
    @test dfr.a == 100
    @test dfr.c == 500
    @test_throws ArgumentError dfr[1:2] = Dict("a" => 100, "f" => 500)
    @test_throws ArgumentError dfr[1:2] = Dict("a" => 100, :c => 500)
end

@testset "names, propertynames and hasproperty" begin
    df = DataFrame(a=1, x1=2, x2=3, x3=4)
    sdf = view(df, 1:1, 1:3)
    dfr = view(df, 1:1, 1:3)
    er = eachrow(df)
    ec = eachcol(df)
    gdf = groupby(df, :a)

    for v in [df, er, ec]
        @test names(v) == ["a", "x1", "x2", "x3"]
        @test propertynames(v) == [:a, :x1, :x2, :x3]
        @test hasproperty(v, :a)
        @test !hasproperty(v, :x)
        @test hasproperty(v, "a")
        @test !hasproperty(v, "x")
        @test names(v, 1) == names(v, :a) == names(v, "a") == ["a"]
        @test names(v, Not(:a)) == names(v, Not("a")) == names(v, Not(1)) ==
              names(v, r"x") == ["x1", "x2", "x3"]
    end

    @test names(gdf) == ["a", "x1", "x2", "x3"]
    @test names(gdf, 1) == names(gdf, :a) == names(gdf, "a") == ["a"]
    @test names(gdf, Not(:a)) == names(gdf, Not("a")) == names(gdf, Not(1)) ==
          names(gdf, r"x") == ["x1", "x2", "x3"]


    for v in [sdf, dfr]
        @test names(v) == ["a", "x1", "x2"]
        @test propertynames(v) == [:a, :x1, :x2]
        @test names(v, 1) == names(v, :a) == names(v, "a") == ["a"]
        @test names(v, Not(:a)) == names(v, Not("a")) == names(v, Not(1)) ==
              names(v, r"x") == ["x1", "x2"]
        @test hasproperty(v, :a)
        @test !hasproperty(v, :x)
        @test hasproperty(v, "a")
        @test !hasproperty(v, "x")
    end
end

@testset "append! and push!" begin
    df = DataFrame(a=1, b=2)
    append!(df, Dict("a" => 2, "b" => 3))
    @test df == DataFrame(a=1:2, b=2:3)

    df = DataFrame()
    append!(df, Dict("a" => 2, "b" => 3))
    @test df == DataFrame(a=2, b=3)

    df = DataFrame(a=1, b=2)
    push!(df, Dict("a" => 2, "b" => 3))
    @test df == DataFrame(a=1:2, b=2:3)
    @test_throws ArgumentError push!(df, Dict("a" => 2, "b" => 3), cols=:orderequal)
    push!(df, Dict("b" => 4, "c" => 0), cols=:union)
    @test isequal(df, DataFrame(a=[1:2; missing], b=2:4, c=[missing, missing, 0]))
end

end # module
