module TestGrouping
    using Test, DataFrames, Random, Statistics
    const ≅ = isequal

    @testset "parent" begin
        df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8])
        gd = groupby(df, :a)
        @test parent(gd) === df
    end

    @testset "colwise" begin
        Random.seed!(1)
        df = DataFrame(a = repeat(Union{Int, Missing}[1, 3, 2, 4], outer=[2]),
                       b = repeat(Union{Int, Missing}[2, 1], outer=[4]),
                       c = Vector{Union{Float64, Missing}}(randn(8)))

        missingfree = DataFrame([collect(1:10)], [:x1])

        @testset "::Function, ::AbstractDataFrame" begin
            cw = colwise(sum, df)
            answer = [20, 12, -0.4283098098931877]
            @test isa(cw, Vector{Real})
            @test size(cw) == (ncol(df),)
            @test cw == answer

            cw = colwise(sum, missingfree)
            answer = [55]
            @test isa(cw, Array{Int, 1})
            @test size(cw) == (ncol(missingfree),)
            @test cw == answer
        end

        @testset "::Function, ::GroupedDataFrame" begin
            gd = groupby(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
            @test colwise(length, gd) == [[2,2], [2,2]]
        end

        @testset "::Vector, ::AbstractDataFrame" begin
            cw = colwise([sum], df)
            answer = [20 12 -0.4283098098931877]
            @test isa(cw, Array{Real, 2})
            @test size(cw) == (length([sum]),ncol(df))
            @test cw == answer

            cw = colwise([sum, minimum], missingfree)
            answer = reshape([55, 1], (2,1))
            @test isa(cw, Array{Int, 2})
            @test size(cw) == (length([sum, minimum]), ncol(missingfree))
            @test cw == answer

            cw = colwise([Vector{Union{Int, Missing}}], missingfree)
            answer = reshape([Vector{Union{Int, Missing}}(1:10)], (1,1))
            @test isa(cw, Array{Vector{Union{Int, Missing}},2})
            @test size(cw) == (1, ncol(missingfree))
            @test cw == answer

            @test_throws MethodError colwise(["Bob", :Susie], DataFrame(A = 1:10, B = 11:20))
        end

        @testset "::Vector, ::GroupedDataFrame" begin
            gd = groupby(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
            @test colwise([length], gd) == [[2 2], [2 2]]
        end

        @testset "::Tuple, ::AbstractDataFrame" begin
            cw = colwise((sum, length), df)
            answer = Any[20 12 -0.4283098098931877; 8 8 8]
            @test isa(cw, Array{Real, 2})
            @test size(cw) == (length((sum, length)), ncol(df))
            @test cw == answer

            cw = colwise((sum, length), missingfree)
            answer = reshape([55, 10], (2,1))
            @test isa(cw, Array{Int, 2})
            @test size(cw) == (length((sum, length)), ncol(missingfree))
            @test cw == answer

            cw = colwise((CategoricalArray, Vector{Union{Int, Missing}}), missingfree)
            answer = reshape([CategoricalArray(1:10), Vector{Union{Int, Missing}}(1:10)],
                             (2, ncol(missingfree)))
            @test typeof(cw) == Array{AbstractVector,2}
            @test size(cw) == (2, ncol(missingfree))
            @test cw == answer

            @test_throws MethodError colwise(("Bob", :Susie), DataFrame(A = 1:10, B = 11:20))
        end

        @testset "::Tuple, ::GroupedDataFrame" begin
            gd = groupby(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
            @test colwise((length), gd) == [[2,2],[2,2]]
        end
    end

    @testset "by, groupby and map(::Function, ::GroupedDataFrame)" begin
        Random.seed!(1)
        df = DataFrame(a = repeat(Union{Int, Missing}[1, 3, 2, 4], outer=[2]),
                       b = repeat(Union{Int, Missing}[2, 1], outer=[4]),
                       c = Vector{Union{Float64, Missing}}(randn(8)))

        cols = [:a, :b]

        f1(df) = DataFrame(cmax = maximum(df[:c]))
        f2(df) = (cmax = maximum(df[:c]),)
        f3(df) = maximum(df[:c])
        f4(df) = [maximum(df[:c]), minimum(df[:c])]
        f5(df) = reshape([maximum(df[:c]), minimum(df[:c])], 2, 1)
        f6(df) = [maximum(df[:c]) minimum(df[:c])]
        f7(df) = (c2 = df[:c].^2,)
        f8(df) = DataFrame(c2 = df[:c].^2)

        res = unique(df[cols])
        res.cmax = [maximum(df[(df.a .== a) .& (df.b .== b), :c])
                    for (a, b) in zip(res.a, res.b)]
        res2 = unique(df[cols])[repeat(1:4, inner=2), :]
        res2.x1 = collect(Iterators.flatten([[maximum(df[(df.a .== a) .& (df.b .== b), :c]),
                                              minimum(df[(df.a .== a) .& (df.b .== b), :c])]
                                              for (a, b) in zip(res.a, res.b)]))
        res3 = unique(df[cols])
        res3.x1 = [maximum(df[(df.a .== a) .& (df.b .== b), :c])
                   for (a, b) in zip(res.a, res.b)]
        res3.x2 = [minimum(df[(df.a .== a) .& (df.b .== b), :c])
                   for (a, b) in zip(res.a, res.b)]
        res4 = df[cols]
        res4.c2 = df.c.^2
        sres = sort(res, cols)
        sres2 = sort(res2, cols)
        sres3 = sort(res3, cols)
        sres4 = sort(res4, cols)

        # by() without groups sorting
        @test sort(by(df, cols, identity)) ==
            sort(hcat(df, df[cols], makeunique=true))[[:a, :b, :a_1, :b_1, :c]]
        @test sort(by(df, cols, df -> DataFrameRow(df, 1, :))[[:a, :b, :c]]) ==
            sort(df[.!nonunique(df, cols), :])
        @test by(df, cols, f1) == res
        @test by(df, cols, f2) == res
        @test rename(by(df, cols, f3), :x1 => :cmax) == res
        @test by(df, cols, f4) == res2
        @test by(df, cols, f5) == res2
        @test by(df, cols, f6) == res3
        @test sort(by(df, cols, f7)) == sort(res4)
        @test sort(by(df, cols, f8)) == sort(res4)

        # by() with groups sorting
        @test by(df, cols, identity, sort=true) ==
            sort(hcat(df, df[cols], makeunique=true), cols)[[:a, :b, :a_1, :b_1, :c]]
        @test by(df, cols, df -> DataFrameRow(df, 1, :), sort=true)[[:a, :b, :c]] ==
            sort(df[.!nonunique(df, cols), :])
        @test by(df, cols, f1, sort=true) == sres
        @test by(df, cols, f2, sort=true) == sres
        @test rename(by(df, cols, f3, sort=true), :x1 => :cmax) == sres
        @test by(df, cols, f4, sort=true) == sres2
        @test by(df, cols, f5, sort=true) == sres2
        @test by(df, cols, f6, sort=true) == sres3
        @test by(df, cols, f7, sort=true) == sres4
        @test by(df, cols, f8, sort=true) == sres4

        @test by(df, [:a], f1) == by(df, :a, f1)
        @test by(df, [:a], f1, sort=true) == by(df, :a, f1, sort=true)

        # groupby() without groups sorting
        gd = groupby(df, cols)
        @test sort(combine(identity, gd)) ==
            sort(combine(gd)) ==
            sort(hcat(df, df[cols], makeunique=true))[[:a, :b, :a_1, :b_1, :c]]
        @test combine(f1, gd) == res
        @test combine(f2, gd) == res
        @test rename(combine(f3, gd), :x1 => :cmax) == res
        @test combine(f4, gd) == res2
        @test combine(f5, gd) == res2
        @test combine(f6, gd) == res3
        @test sort(combine(f7, gd)) == sort(res4)
        @test sort(combine(f8, gd)) == sort(res4)

        # groupby() with groups sorting
        gd = groupby(df, cols, sort=true)
        for i in 1:length(gd)
            @test all(gd[i].a .== sres.a[i])
            @test all(gd[i].b .== sres.b[i])
        end
        @test combine(identity, gd) ==
            combine(gd) ==
            sort(hcat(df, df[cols], makeunique=true), cols)[[:a, :b, :a_1, :b_1, :c]]
        @test combine(f1, gd) == sres
        @test combine(f2, gd) == sres
        @test rename(combine(f3, gd), :x1 => :cmax) == sres
        @test combine(f4, gd) == sres2
        @test combine(f5, gd) == sres2
        @test combine(f6, gd) == sres3
        @test combine(f7, gd) == sres4
        @test combine(f8, gd) == sres4

        # map() without and with groups sorting
        for sort in (false, true)
            gd = groupby(df, cols, sort=sort)
            v = map(d -> d[[:c]], gd)
            @test length(gd) == length(v)
            @test v[1] == gd[1] && v[2] == gd[2] && v[3] == gd[3] && v[4] == gd[4]
            v = map(f1, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f1, df, cols, sort=sort)
            v = map(f2, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f2, df, cols, sort=sort)
            v = map(f3, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f3, df, cols, sort=sort)
            v = map(f4, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f4, df, cols, sort=sort)
            v = map(f5, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f5, df, cols, sort=sort)
            v = map(f5, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f5, df, cols, sort=sort)
            v = map(f6, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f6, df, cols, sort=sort)
            v = map(f7, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f7, df, cols, sort=sort)
            v = map(f8, gd)
            @test vcat(v[1], v[2], v[3], v[4]) == by(f8, df, cols, sort=sort)
        end

        # testing pool overflow
        df2 = DataFrame(v1 = categorical(collect(1:1000)), v2 = categorical(fill(1, 1000)))
        @test groupby(df2, [:v1, :v2]).starts == collect(1:1000)
        @test groupby(df2, [:v2, :v1]).starts == collect(1:1000)

        # grouping empty table
        @test groupby(DataFrame(A=Int[]), :A).starts == Int[]
        # grouping single row
        @test groupby(DataFrame(A=Int[1]), :A).starts == Int[1]

        # issue #960
        x = CategoricalArray(collect(1:20))
        df = DataFrame(v1=x, v2=x)
        groupby(df, [:v1, :v2])

        df2 = by(e->1, DataFrame(x=Int64[]), :x)
        @test size(df2) == (0, 1)
        @test sum(df2[:x]) == 0

        # Check that reordering levels does not confuse groupby
        df = DataFrame(Key1 = CategoricalArray(["A", "A", "B", "B"]),
                       Key2 = CategoricalArray(["A", "B", "A", "B"]),
                       Value = 1:4)
        gd = groupby(df, :Key1)
        @test length(gd) == 2
        @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["A", "B"], Value=1:2)
        @test gd[2] == DataFrame(Key1=["B", "B"], Key2=["A", "B"], Value=3:4)
        gd = groupby(df, [:Key1, :Key2])
        @test length(gd) == 4
        @test gd[1] == DataFrame(Key1="A", Key2="A", Value=1)
        @test gd[2] == DataFrame(Key1="A", Key2="B", Value=2)
        @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
        @test gd[4] == DataFrame(Key1="B", Key2="B", Value=4)
        # Reorder levels, add unused level
        levels!(df[:Key1], ["Z", "B", "A"])
        levels!(df[:Key2], ["Z", "B", "A"])
        gd = groupby(df, :Key1)
        @test gd == groupby(df, :Key1, skipmissing=true)
        @test length(gd) == 2
        @test gd[1] == DataFrame(Key1=["B", "B"], Key2=["A", "B"], Value=3:4)
        @test gd[2] == DataFrame(Key1=["A", "A"], Key2=["A", "B"], Value=1:2)
        gd = groupby(df, [:Key1, :Key2])
        @test gd == groupby(df, [:Key1, :Key2], skipmissing=true)
        @test length(gd) == 4
        @test gd[1] == DataFrame(Key1="A", Key2="A", Value=1)
        @test gd[2] == DataFrame(Key1="A", Key2="B", Value=2)
        @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
        @test gd[4] == DataFrame(Key1="B", Key2="B", Value=4)
        # Make first level unused too
        df.Key1[1:2] .= "B"
        gd = groupby(df, :Key1)
        @test length(gd) == 1
        @test gd[1] == DataFrame(Key1="B", Key2=["A", "B", "A", "B"], Value=1:4)
        gd = groupby(df, [:Key1, :Key2])
        @test gd == groupby(df, [:Key1, :Key2])
        @test length(gd) == 2
        @test gd[1] == DataFrame(Key1="B", Key2="A", Value=[1, 3])
        @test gd[2] == DataFrame(Key1="B", Key2="B", Value=[2, 4])

        # Check that CategoricalArray column is preserved when returning a value...
        res = combine(d -> DataFrame(x=d[1, :Key2]), groupby(df, :Key1))
        @test res.x isa CategoricalVector{String}
        res = combine(d -> (x=d[1, :Key2],), groupby(df, :Key1))
        @test res.x isa CategoricalVector{String}
        # ...and when returning an array
        res = combine(d -> DataFrame(x=d[:Key1]), groupby(df, :Key1))
        @test res.x isa CategoricalVector{String}

        # Check that CategoricalArray and String give a String...
        res = combine(d -> d.Key1 == ["A", "A"] ? DataFrame(x=d[1, :Key1]) : DataFrame(x="C"),
                     groupby(df, :Key1))
        @test res.x isa Vector{String}
        res = combine(d -> d.Key1 == ["A", "A"] ? (x=d[1, :Key1],) : (x="C",),
                      groupby(df, :Key1))
        @test res.x isa Vector{String}
        # ...even when CategoricalString comes second
        res = combine(d -> d.Key1 == ["B", "B"] ? DataFrame(x=d[1, :Key1]) : DataFrame(x="C"),
                      groupby(df, :Key1))
        @test res.x isa Vector{String}
        res = combine(d -> d.Key1 == ["B", "B"] ? (x=d[1, :Key1],) : (x="C",),
                      groupby(df, :Key1))
        @test res.x isa Vector{String}

        df = DataFrame(x = [1, 2, 3], y = [2, 3, 1])

        # Test function returning DataFrameRow
        res = by(d -> DataFrameRow(d, 1, :), df, :x)
        @test res == DataFrame(x=df.x, x_1=df.x, y=df.y)

        # Test function returning Tuple
        res = by(d -> (sum(d.y),), df, :x)
        @test res == DataFrame(x=df.x, x1=tuple.([2, 3, 1]))

        # Test with some groups returning empty data frames
        @test by(d -> d.x == [1] ? DataFrame(z=[]) : DataFrame(z=1), df, :x) ==
            DataFrame(x=[2, 3], z=[1, 1])
        v = map(d -> d.x == [1] ? DataFrame(z=[]) : DataFrame(z=1), groupby(df, :x))
        @test length(v) == 2
        @test vcat(v[1], v[2]) == DataFrame(x=[2, 3], z=[1, 1])

        # Test that returning values of different types works with NamedTuple
        res = by(d -> d.x == [1] ? 1 : 2.0, df, :x)
        @test res.x1 isa Vector{Float64}
        @test res.x1 == [1, 2, 2]
        # Two columns need to be widened at different times
        res = by(d -> (a=d.x == [1] ? 1 : 2.0, b=d.x == [3] ? missing : "a"), df, :x)
        @test res.a isa Vector{Float64}
        @test res.a == [1, 2, 2]
        @test res.b isa Vector{Union{String,Missing}}
        @test res.b ≅ ["a", "a", missing]
        # Corner case: two columns need to be widened at the same time
        res = by(d -> (a=d.x == [1] ? 1 : 2.0, b=d.x == [1] ? missing : "a"), df, :x)
        @test res.a isa Vector{Float64}
        @test res.a == [1, 2, 2]
        @test res.b isa Vector{Union{String,Missing}}
        @test res.b ≅ [missing, "a", "a"]

        # Test that returning values of different types works with DataFrame
        res = by(d -> DataFrame(x1 = d.x == [1] ? 1 : 2.0), df, :x)
        @test res.x1 isa Vector{Float64}
        @test res.x1 == [1, 2, 2]
        # Two columns need to be widened at different times
        res = by(d -> DataFrame(a=d.x == [1] ? 1 : 2.0, b=d.x == [3] ? missing : "a"), df, :x)
        @test res.a isa Vector{Float64}
        @test res.a == [1, 2, 2]
        @test res.b isa Vector{Union{String,Missing}}
        @test res.b ≅ ["a", "a", missing]
        # Corner case: two columns need to be widened at the same time
        res = by(d -> DataFrame(a=d.x == [1] ? 1 : 2.0, b=d.x == [1] ? missing : "a"), df, :x)
        @test res.a isa Vector{Float64}
        @test res.a == [1, 2, 2]
        @test res.b isa Vector{Union{String,Missing}}
        @test res.b ≅ [missing, "a", "a"]

        # Test return values with columns in different orders
        @test by(d -> d.x == [1] ? (x1=1, x2=3) : (x2=2, x1=4), df, :x) ==
            DataFrame(x=1:3, x1=[1, 4, 4], x2=[3, 2, 2])
        @test by(d -> d.x == [1] ? DataFrame(x1=1, x2=3) : DataFrame(x2=2, x1=4), df, :x) ==
            DataFrame(x=1:3, x1=[1, 4, 4], x2=[3, 2, 2])

        # Test with NamedTuple with columns of incompatible lengths
        @test_throws DimensionMismatch by(d -> (x1=[1], x2=[3, 4]), df, :x)
        @test_throws DimensionMismatch by(d -> d.x == [1] ? (x1=[1], x2=[3]) :
                                                            (x1=[1], x2=[3, 4]), df, :x)

        # Test with incompatible return values
        @test_throws ArgumentError by(d -> d.x == [1] ? (x1=1,) : DataFrame(x1=1), df, :x)
        @test_throws ArgumentError by(d -> d.x == [1] ? NamedTuple() : (x1=1), df, :x)
        @test_throws ArgumentError by(d -> d.x == [1] ? 1 : DataFrame(x1=1), df, :x)
        @test_throws ArgumentError by(d -> d.x == [1] ? DataFrame() : DataFrame(x1=1), df, :x)
        # Special case allowed due to how implementation works
        @test by(d -> d.x == [1] ? 1 : (x1=1), df, :x) == by(d -> 1, df, :x)

        # Test that columns names and types are respected for empty input
        df = DataFrame(x=Int[], y=String[])
        res = by(d -> 1, df, :x)
        @test size(res) == (0, 1)
        @test res.x isa Vector{Int}

        # Test with empty data frame
        df = DataFrame(x=[], y=[])
        gd = groupby(df, :x)
        @test combine(df -> sum(df.x), gd) == DataFrame(x=[])
        res = map(df -> sum(df.x), gd)
        @test length(res) == 0
        @test res.parent == DataFrame(x=[])
    end

    @testset "grouping with missings" for df in
        (DataFrame(Key1 = ["A", missing, "B", "B", "A"],
                   Key2 = ["B", "A", "A", missing, "A"],
                   Value = 1:5),
         DataFrame(Key1 = categorical(["A", missing, "B", "B", "A"]),
                   Key2 = ["B", "A", "A", missing, "A"],
                   Value = 1:5),
         DataFrame(Key1 = ["A", missing, "B", "B", "A"],
                   Key2 = categorical(["B", "A", "A", missing, "A"]),
                   Value = 1:5),
         DataFrame(Key1 = ["A", missing, "B", "B", "A"],
                   Key2 = levels!(categorical(["B", "A", "A", missing, "A"]), ["X", "A", "B"]),
                   Value = 1:5),
         DataFrame(Key1 = ["A", missing, "B", "B", "A"],
                   Key2 = levels!(categorical(["B", "A", "A", missing, "A"]), ["A", "B", "X"]),
                   Value = 1:5),
         DataFrame(Key1 = categorical(["A", missing, "B", "B", "A"]),
                   Key2 = categorical(["B", "A", "A", missing, "A"]),
                   Value = 1:5))

        @testset "sort=false, skipmissing=false" begin
            gd = groupby(df, :Key1)
            @test length(gd) == 3
            if df.Key1 isa CategoricalVector # Order of missing changes
                @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["B", "A"], Value=[1, 5])
                @test gd[2] ≅ DataFrame(Key1=["B", "B"], Key2=["A", missing], Value=3:4)
                @test gd[3] ≅ DataFrame(Key1=missing, Key2="A", Value=2)
            else
                @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["B", "A"], Value=[1, 5])
                @test gd[2] ≅ DataFrame(Key1=missing, Key2="A", Value=2)
                @test gd[3] ≅ DataFrame(Key1=["B", "B"], Key2=["A", missing], Value=3:4)
            end

            gd = groupby(df, [:Key1, :Key2])
            @test length(gd) == 5
            @test gd[1] == DataFrame(Key1="A", Key2="B", Value=1)
            @test gd[2] ≅ DataFrame(Key1=missing, Key2="A", Value=2)
            @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
            @test gd[4] ≅ DataFrame(Key1="B", Key2=missing, Value=4)
            @test gd[5] ≅ DataFrame(Key1="A", Key2="A", Value=5)
        end

        @testset "sort=false, skipmissing=true" begin
            gd = groupby(df, :Key1, skipmissing=true)
            @test length(gd) == 2
            @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["B", "A"], Value=[1, 5])
            @test gd[2] ≅ DataFrame(Key1=["B", "B"], Key2=["A", missing], Value=3:4)

            gd = groupby(df, [:Key1, :Key2], skipmissing=true)
            @test length(gd) == 3
            @test gd[1] == DataFrame(Key1="A", Key2="B", Value=1)
            @test gd[2] == DataFrame(Key1="B", Key2="A", Value=3)
            @test gd[3] == DataFrame(Key1="A", Key2="A", Value=5)
        end

        @testset "sort=true, skipmissing=false" begin
            gd = groupby(df, :Key1, sort=true)
            @test length(gd) == 3
            @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["B", "A"], Value=[1, 5])
            @test gd[2] ≅ DataFrame(Key1=["B", "B"], Key2=["A", missing], Value=3:4)
            @test gd[3] ≅ DataFrame(Key1=missing, Key2="A", Value=2)

            gd = groupby(df, [:Key1, :Key2], sort=true)
            @test length(gd) == 5
            @test gd[1] ≅ DataFrame(Key1="A", Key2="A", Value=5)
            @test gd[2] == DataFrame(Key1="A", Key2="B", Value=1)
            @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
            @test gd[4] ≅ DataFrame(Key1="B", Key2=missing, Value=4)
            @test gd[5] ≅ DataFrame(Key1=missing, Key2="A", Value=2)
        end

        @testset "sort=true, skipmissing=true" begin
            gd = groupby(df, :Key1, sort=true, skipmissing=true)
            @test length(gd) == 2
            @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["B", "A"], Value=[1, 5])
            @test gd[2] ≅ DataFrame(Key1=["B", "B"], Key2=["A", missing], Value=3:4)

            gd = groupby(df, [:Key1, :Key2], sort=true, skipmissing=true)
            @test length(gd) == 3
            @test gd[1] == DataFrame(Key1="A", Key2="A", Value=5)
            @test gd[2] == DataFrame(Key1="A", Key2="B", Value=1)
            @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
        end
    end

    @testset "by, combine and map with pair interface" begin
        vexp = x -> exp.(x)
        Random.seed!(1)
        df = DataFrame(a = repeat([1, 3, 2, 4], outer=[2]),
                       b = repeat([2, 1], outer=[4]),
                       c = rand(Int, 8))

        # Only test that different by syntaxes work,
        # and rely on tests below for deeper checks
        @test by(:c => sum, df, :a) ==
            by(df, :a, :c => sum) ==
            by(df, :a, (:c => sum,)) ==
            by(df, :a, [:c => sum]) ==
            by(df, :a, c_sum = :c => sum) ==
            by(d -> (c_sum=sum(d.c),), df, :a)
            by(df, :a, d -> (c_sum=sum(d.c),))

        @test by(:c => vexp, df, :a) ==
            by(df, :a, :c => vexp) ==
            by(df, :a, (:c => vexp,)) ==
            by(df, :a, [:c => vexp]) ==
            by(df, :a, c_function = :c => vexp) ==
            by(d -> (c_function=vexp(d.c),), df, :a)
            by(df, :a, d -> (c_function=vexp(d.c),))

        @test by(df, :a, :b => sum, :c => sum) ==
            by(df, :a, (:b => sum, :c => sum,)) ==
            by(df, :a, [:b => sum, :c => sum]) ==
            by(df, :a, b_sum = :b => sum, c_sum = :c => sum) ==
            by(d -> (b_sum=sum(d.b), c_sum=sum(d.c)), df, :a)
            by(df, :a, d -> (b_sum=sum(d.b), c_sum=sum(d.c)))

        @test by(df, :a, :b => vexp, :c => identity) ==
            by(df, :a, (:b => vexp, :c => identity,)) ==
            by(df, :a, [:b => vexp, :c => identity]) ==
            by(df, :a, b_function = :b => vexp, c_identity = :c => identity) ==
            by(d -> (b_function=vexp(d.b), c_identity=identity(d.c)), df, :a)
            by(df, :a, d -> (b_function=vexp(d.b), c_identity=identity(d.c)))

        gd = groupby(df, :a)

        # Only test that different combine syntaxes work,
        # and rely on tests below for deeper checks
        @test combine(:c => sum, gd) ==
            combine(gd, :c => sum) ==
            combine(gd, (:c => sum,)) ==
            combine(gd, [:c => sum]) ==
            combine(gd, c_sum = :c => sum) ==
            combine(:c => x -> (c_sum=sum(x),), gd) ==
            combine(gd, :c => x -> (c_sum=sum(x),)) ==
            combine(d -> (c_sum=sum(d.c),), gd) ==
            combine(gd, d -> (c_sum=sum(d.c),))

        @test combine(:c => vexp, gd) ==
            combine(gd, :c => vexp) ==
            combine(gd, (:c => vexp,)) ==
            combine(gd, [:c => vexp]) ==
            combine(gd, c_function = :c => vexp) ==
            combine(:c => x -> (c_function=exp.(x),), gd) ==
            combine(gd, :c => x -> (c_function=exp.(x),)) ==
            combine(d -> (c_function=exp.(d.c),), gd)
            combine(gd, d -> (c_function=exp.(d.c),))

        @test combine(gd, :b => sum, :c => sum) ==
            combine(gd, (:b => sum, :c => sum,)) ==
            combine(gd, [:b => sum, :c => sum]) ==
            combine(gd, b_sum = :b => sum, c_sum = :c => sum) ==
            combine((:b, :c) => x -> (b_sum=sum(x.b), c_sum=sum(x.c)), gd) ==
            combine(gd, (:b, :c) => x -> (b_sum=sum(x.b), c_sum=sum(x.c))) ==
            combine(d -> (b_sum=sum(d.b), c_sum=sum(d.c)), gd) ==
            combine(gd, d -> (b_sum=sum(d.b), c_sum=sum(d.c)))

        @test combine(gd, :b => vexp, :c => identity) ==
            combine(gd, (:b => vexp, :c => identity,)) ==
            combine(gd, [:b => vexp, :c => identity]) ==
            combine(gd, b_function = :b => vexp, c_identity = :c => identity) ==
            combine((:b, :c) => x -> (b_function=vexp(x.b), c_identity=x.c), gd) ==
            combine(gd, (:b, :c) => x -> (b_function=vexp(x.b), c_identity=x.c)) ==
            combine(d -> (b_function=vexp(d.b), c_identity=d.c), gd) ==
            combine(gd, d -> (b_function=vexp(d.b), c_identity=d.c))

        for f in (map, combine)
            for col in (:c, 3)
                @test f(col => sum, gd) == f(d -> (c_sum=sum(d.c),), gd)
                @test f(col => x -> sum(x), gd) == f(d -> (c_function=sum(d.c),), gd)
                @test f(col => x -> (z=sum(x),), gd) == f(d -> (z=sum(d.c),), gd)
                @test f(col => x -> DataFrame(z=sum(x),), gd) == f(d -> (z=sum(d.c),), gd)
                @test f(col => identity, gd) == f(d -> (c_identity=d.c,), gd)
                @test f(col => x -> (z=x,), gd) == f(d -> (z=d.c,), gd)

                @test f((xyz = col => sum,), gd) ==
                    f(d -> (xyz=sum(d.c),), gd)
                @test f((xyz = col => x -> sum(x),), gd) ==
                    f(d -> (xyz=sum(d.c),), gd)
                @test f((xyz = col => x -> (sum(x),),), gd) ==
                    f(d -> (xyz=(sum(d.c),),), gd)
                @test_throws ArgumentError f((xyz = col => x -> (z=sum(x),),), gd)
                @test_throws ArgumentError f((xyz = col => x -> DataFrame(z=sum(x),),), gd)
                @test_throws ArgumentError f((xyz = col => x -> (z=x,),), gd)
                @test_throws ArgumentError f(col => x -> (z=1, xzz=[1]), gd)

                for wrap in (vcat, tuple)
                    @test f(wrap(col => sum), gd) ==
                        f(d -> (c_sum=sum(d.c),), gd)
                    @test f(wrap(col => x -> sum(x)), gd) ==
                        f(d -> (c_function=sum(d.c),), gd)
                    @test f(wrap(col => x -> (sum(x),)), gd) ==
                        f(d -> (c_function=(sum(d.c),),), gd)
                    @test_throws ArgumentError f(wrap(col => x -> (z=sum(x),)), gd)
                    @test_throws ArgumentError f(wrap(col => x -> DataFrame(z=sum(x),)), gd)
                    @test_throws ArgumentError f(wrap(col => x -> (z=x,)), gd)
                    @test_throws ArgumentError f(wrap(col => x -> (z=1, xzz=[1])), gd)
                end
            end
            for cols in ((:b, :c), [:b, :c], (2, 3), 2:3, [2, 3], [false, true, true])
                @test f(cols => x -> (y=exp.(x.b), z=x.c), gd) ==
                    f(d -> (y=exp.(d.b), z=d.c), gd)
                @test f(cols => x -> [exp.(x.b) x.c], gd) ==
                    f(d -> [exp.(d.b) d.c], gd)

                @test f((xyz = cols => x -> sum(x.b) + sum(x.c),), gd) ==
                    f(d -> (xyz=sum(d.b) + sum(d.c),), gd)
                if eltype(cols) === Bool
                    cols2 = [[false, true, false], [false, false, true]]
                    @test_throws MethodError f((xyz = cols[1] => sum, xzz = cols2[2] => sum), gd)
                    @test_throws MethodError f((xyz = cols[1] => sum, xzz = cols2[1] => sum), gd)
                    @test_throws MethodError f((xyz = cols[1] => sum, xzz = cols2[2] => x -> first(x)), gd)
                else
                    cols2 = cols
                    @test f((xyz = cols2[1] => sum, xzz = cols2[2] => sum), gd) ==
                        f(d -> (xyz=sum(d.b), xzz=sum(d.c)), gd)
                    @test f((xyz = cols2[1] => sum, xzz = cols2[1] => sum), gd) ==
                        f(d -> (xyz=sum(d.b), xzz=sum(d.b)), gd)
                    @test f((xyz = cols2[1] => sum, xzz = cols2[2] => x -> first(x)), gd) ==
                        f(d -> (xyz=sum(d.b), xzz=first(d.c)), gd)
                    @test_throws ArgumentError f((xyz = cols2[1] => vexp, xzz = cols2[2] => sum), gd)
                end

                @test_throws ArgumentError f(cols => x -> (y=exp.(x.b), z=sum(x.c)), gd)
                @test_throws ArgumentError f((xyz = cols2 => x -> DataFrame(y=exp.(x.b), z=sum(x.c)),), gd)
                @test_throws ArgumentError f((xyz = cols2 => x -> [exp.(x.b) x.c],), gd)

                for wrap in (vcat, tuple)
                    @test f(wrap(cols => x -> sum(x.b) + sum(x.c)), gd) ==
                        f(d -> sum(d.b) + sum(d.c), gd)

                    if eltype(cols) === Bool
                        cols2 = [[false, true, false], [false, false, true]]
                        @test f(wrap(cols2[1] => x -> sum(x.b), cols2[2] => x -> sum(x.c)), gd) ==
                            f(d -> (x1=sum(d.b), x2=sum(d.c)), gd)
                        @test f(wrap(cols2[1] => x -> sum(x.b), cols2[2] => x -> first(x.c)), gd) ==
                            f(d -> (x1=sum(d.b), x2=first(d.c)), gd)
                    else
                        cols2 = cols
                        @test f(wrap(cols[1] => sum, cols[2] => sum), gd) ==
                            f(d -> (b_sum=sum(d.b), c_sum=sum(d.c)), gd)
                        @test f(wrap(cols[1] => sum, cols[2] => x -> first(x)), gd) ==
                            f(d -> (b_sum=sum(d.b), c_function=first(d.c)), gd)
                        @test_throws ArgumentError f(wrap(cols2[1] => vexp, cols2[2] => sum), gd)
                    end

                    @test_throws ArgumentError f(wrap(cols => x -> DataFrame(y=exp.(x.b), z=sum(x.c))), gd)
                    @test_throws ArgumentError f(wrap(cols => x -> [exp.(x.b) x.c]), gd)
                end
            end
        end
    end

    struct TestType end
    Base.isless(::TestType, ::Int) = true
    Base.isless(::Int, ::TestType) = false
    Base.isless(::TestType, ::TestType) = false

    @testset "combine with aggregation functions" begin
        Random.seed!(1)
        df = DataFrame(a = rand(1:5, 20), x1 = rand(Int, 20), x2 = rand(Complex{Int}, 20))

        for f in (sum, prod, maximum, minimum, mean, var, std, first, last, length)
            gd = groupby(df, :a)

            res = combine(gd, y = :x1 => f)
            expected = combine(gd, y = :x1 => x -> f(x))
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)

            for T in (Union{Missing, Int}, Union{Int, Int8},
                      Union{Missing, Int, Int8})
                df.x3 = Vector{T}(df.x1)
                gd = groupby(df, :a)
                res = combine(gd, y = :x3 => f)
                expected = combine(gd, y = :x3 => x -> f(x))
                @test res ≅ expected
                @test typeof(res.y) == typeof(expected.y)
            end

            f === length && continue

            df.x3 = allowmissing(df.x1)
            df.x3[1] = missing
            gd = groupby(df, :a)
            res = combine(gd, y = :x3 => f)
            expected = combine(gd, y = :x3 => x -> f(x))
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
            res = combine(gd, y = :x3 => f∘skipmissing)
            expected = combine(gd, y = :x3 => x -> f(collect(skipmissing(x))))
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
        end
        # Test complex numbers
        for f in (sum, prod, mean, var, std, first, last, length)
            gd = groupby(df, :a)

            res = combine(gd, y = :x2 => f)
            expected = combine(gd, y = :x2 => x -> f(x))
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
        end
        # Test CategoricalArray
        for f in (maximum, minimum, first, last, length),
            (T, m) in ((Int, false),
                       (Union{Missing, Int}, false), (Union{Missing, Int}, true))
            df.x3 = CategoricalVector{T}(df.x1)
            m && (df.x3[1] = missing)
            gd = groupby(df, :a)
            res = combine(gd, y = :x3 => f)
            expected = combine(gd, y = :x3 => x -> f(x))
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)

            f === length && continue

            res = combine(gd, y = :x3 => f∘skipmissing)
            expected = combine(gd, y = :x3 => x -> f(collect(skipmissing(x))))
            @test res == expected
            @test typeof(res.y) == typeof(expected.y)
            if m
                gd[1].x3 = missing
                @test_throws ArgumentError combine(gd, y = :x3 => f∘skipmissing)
            end
        end
        @test combine(gd, y = :x1 => maximum, z = :x2 => sum) ==
            combine(gd, y = :x1 => x -> maximum(x), z = :x2 => x -> sum(x))
        # Test floating point corner cases
        df = DataFrame(a = [1, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6],
                       x1 = [0.0, 1.0, 2.0, NaN, NaN, NaN, Inf, Inf, Inf, 1.0, NaN, 0.0, -0.0])

        for f in (sum, prod, maximum, minimum, mean, var, std, first, last, length)
            gd = groupby(df, :a)

            res = combine(gd, y = :x1 => f)
            expected = combine(gd, y = :x1 => x -> f(x))
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)

            f === length && continue

            df.x3 = allowmissing(df.x1)
            df.x3[1] = missing
            gd = groupby(df, :a)
            res = combine(gd, y = :x3 => f)
            expected = combine(gd, y = :x3 => x -> f(x))
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
            res = combine(gd, y = :x3 => f∘skipmissing)
            expected = combine(gd, y = :x3 => x -> f(collect(skipmissing(x))))
            @test res ≅ expected
            @test typeof(res.y) == typeof(expected.y)
        end

        df = DataFrame(x = [1, 1, 2, 2], y = Any[1, 2.0, 3.0, 4.0])
        res = by(df, :x, z = :y => maximum)
        @test res.z isa Vector{Float64}
        @test res.z == by(df, :x, z = :y => x -> maximum(x)).z

        # Test maximum when no promotion rule exists
        df = DataFrame(x = [1, 1, 2, 2], y = [1, TestType(), TestType(), TestType()])
        gd = groupby(df, :x)
        for f in (maximum, minimum)
            res = combine(gd, z = :y => maximum)
            @test res.z isa Vector{Any}
            @test res.z == by(df, :x, z = :y => x -> maximum(x)).z
        end
    end

    @testset "iteration protocol" begin
        gd = groupby(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
        for v in gd
            @test size(v) == (2,2)
        end
    end

    @testset "getindex" begin
        df = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                       b = 1:8)
        gd = groupby(df, :a)
        @test gd[1] isa SubDataFrame
        @test gd[1] == view(df, [1, 5], :)
        @test_throws BoundsError gd[5]
        if VERSION < v"1.0.0-"
            @test gd[true] == gd[1]
        else
            @test_throws ArgumentError gd[true]
        end
        @test_throws MethodError gd["a"]
        gd2 = gd[[true, false, false, false]]
        @test length(gd2) == 1
        @test gd2[1] == gd[1]
        @test_throws BoundsError gd[[true, false]]
        gd3 = gd[:]
        @test gd3 isa GroupedDataFrame
        @test length(gd3) == 4
        @test gd3 == gd
        for i in 1:4
            @test gd3[i] == gd[i]
        end
        gd4 = gd[[1,2]]
        @test gd4 isa GroupedDataFrame
        @test length(gd4) == 2
        for i in 1:2
            @test gd4[i] == gd[i]
        end
        @test_throws BoundsError gd[1:5]
    end

    @testset "== and isequal" begin
        df1 = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                        b = 1:8)
        df2 = DataFrame(a = repeat([1, 2, 3, 4], outer=[2]),
                        b = [1:7;missing])
        gd1 = groupby(df1, :a)
        gd2 = groupby(df2, :a)
        @test gd1 == gd1
        @test isequal(gd1, gd1)
        @test ismissing(gd1 == gd2)
        @test !isequal(gd1, gd2)
        @test ismissing(gd2 == gd2)
        @test isequal(gd2, gd2)
        df1.c = df1.a
        df2.c = df2.a
        @test gd1 != groupby(df2, :c)
        df2[7, :b] = 10
        @test gd1 != gd2
        df3 = DataFrame(a = repeat([1, 2, 3, missing], outer=[2]),
                        b = 1:8)
        df4 = DataFrame(a = repeat([1, 2, 3, missing], outer=[2]),
                        b = [1:7;missing])
        gd3 = groupby(df3, :a)
        gd4 = groupby(df4, :a)
        @test ismissing(gd3 == gd4)
        @test !isequal(gd3, gd4)
        gd3 = groupby(df3, :a, skipmissing = true)
        gd4 = groupby(df4, :a, skipmissing = true)
        @test gd3 == gd4
        @test isequal(gd3, gd4)
    end

    @testset "show" begin
        function capture_stdout(f::Function)
            oldstdout = stdout
            rd, wr = redirect_stdout()
            f()
            str = String(readavailable(rd))
            redirect_stdout(oldstdout)
            size = displaysize(rd)
            close(rd)
            close(wr)
            str, size
        end

        df = DataFrame(A = Int64[1:4;], B = ["x\"", "∀ε>0: x+ε>x", "z\$", "A\nC"],
                       C = Float32[1.0, 2.0, 3.0, 4.0])
        gd = groupby(df, :A)
        io = IOContext(IOBuffer(), :limit=>true)
        show(io, gd)
        str = String(take!(io.io))
        @test str == """
        GroupedDataFrame with 4 groups based on key: :A
        First Group (1 row): :A = 1
        │ Row │ A     │ B      │ C       │
        │     │ Int64 │ String │ Float32 │
        ├─────┼───────┼────────┼─────────┤
        │ 1   │ 1     │ x"     │ 1.0     │
        ⋮
        Last Group (1 row): :A = 4
        │ Row │ A     │ B      │ C       │
        │     │ Int64 │ String │ Float32 │
        ├─────┼───────┼────────┼─────────┤
        │ 1   │ 4     │ A\\nC   │ 4.0     │"""
        show(io, gd, allgroups=true)
        str = String(take!(io.io))
        @test str == """
        GroupedDataFrame with 4 groups based on key: :A
        Group 1 (1 row): :A = 1
        │ Row │ A     │ B      │ C       │
        │     │ Int64 │ String │ Float32 │
        ├─────┼───────┼────────┼─────────┤
        │ 1   │ 1     │ x\"     │ 1.0     │
        Group 2 (1 row): :A = 2
        │ Row │ A     │ B           │ C       │
        │     │ Int64 │ String      │ Float32 │
        ├─────┼───────┼─────────────┼─────────┤
        │ 1   │ 2     │ ∀ε>0: x+ε>x │ 2.0     │
        Group 3 (1 row): :A = 3
        │ Row │ A     │ B      │ C       │
        │     │ Int64 │ String │ Float32 │
        ├─────┼───────┼────────┼─────────┤
        │ 1   │ 3     │ z\$     │ 3.0     │
        Group 4 (1 row): :A = 4
        │ Row │ A     │ B      │ C       │
        │     │ Int64 │ String │ Float32 │
        ├─────┼───────┼────────┼─────────┤
        │ 1   │ 4     │ A\\nC   │ 4.0     │"""

        # Test two-argument show
        str1, dsize = capture_stdout() do
            show(gd)
        end
        io = IOContext(IOBuffer(), :limit=>true, :displaysize=>dsize)
        show(io, gd)
        str2 = String(take!(io.io))
        @test str1 == str2


        @test sprint(show, "text/html", gd) ==
            "<p><b>GroupedDataFrame with 4 groups based on key: :A</b></p>" *
            "<p><i>First Group (1 row): :A = 1</i></p><table class=\"data-frame\">" *
            "<thead><tr><th></th><th>A</th><th>B</th><th>C</th></tr><tr><th></th>" *
            "<th>Int64</th><th>String</th><th>Float32</th></tr></thead>" *
            "<tbody><tr><th>1</th><td>1</td><td>x\"</td><td>1.0</td></tr></tbody>" *
            "</table><p>&vellip;</p><p><i>Last Group (1 row): :A = 4</i></p>" *
            "<table class=\"data-frame\"><thead><tr><th></th><th>A</th><th>B</th><th>C</th></tr>" *
            "<tr><th></th><th>Int64</th><th>String</th><th>Float32</th></tr></thead>" *
            "<tbody><tr><th>1</th><td>4</td><td>A\\nC</td><td>4.0</td></tr></tbody></table>"

        @test sprint(show, "text/latex", gd) == """
            GroupedDataFrame with 4 groups based on key: :A

            First Group (1 row): :A = 1

            \\begin{tabular}{r|ccc}
            \t& A & B & C\\\\
            \t\\hline
            \t& Int64 & String & Float32\\\\
            \t\\hline
            \t1 & 1 & x" & 1.0 \\\\
            \\end{tabular}

            \$\\dots\$

            Last Group (1 row): :A = 4

            \\begin{tabular}{r|ccc}
            \t& A & B & C\\\\
            \t\\hline
            \t& Int64 & String & Float32\\\\
            \t\\hline
            \t1 & 4 & A\\textbackslash{}nC & 4.0 \\\\
            \\end{tabular}
            """
    end
end
