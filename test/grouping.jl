module TestGrouping
    using Test, DataFrames, Random
    const ≅ = isequal

    Random.seed!(1)
    df = DataFrame(a = repeat(Union{Int, Missing}[1, 2, 3, 4], outer=[2]),
                   b = repeat(Union{Int, Missing}[2, 1], outer=[4]),
                   c = Vector{Union{Float64, Missing}}(randn(8)))
    #df[6, :a] = missing
    #df[7, :b] = missing

    missingfree = DataFrame(Any[collect(1:10)], [:x1])
    @testset "colwise" begin
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

    cols = [:a, :b]
    f(df) = DataFrame(cmax = maximum(df[:c]))

    sdf = unique(df[cols])

    # by() without groups sorting
    bdf = by(df, cols, f)
    @test bdf[cols] == sdf

    # by() with groups sorting
    sbdf = by(df, cols, f, sort=true)
    @test sbdf[cols] == sort(sdf)

    byf = by(df, :a, df -> DataFrame(bsum = sum(df[:b])))

    # groupby() without groups sorting
    gd = groupby(df, cols)
    ga = map(f, gd)

    @test bdf == combine(ga)

    # groupby() with groups sorting
    gd = groupby(df, cols, sort=true)
    ga = map(f, gd)
    @test sbdf == combine(ga)

    g(df) = DataFrame(cmax1 = [c + 1 for c in df[:cmax]])
    h(df) = g(f(df))

    @test combine(map(h, gd)) == combine(map(g, ga))

    # testing pool overflow
    df2 = DataFrame(v1 = categorical(collect(1:1000)), v2 = categorical(fill(1, 1000)))
    @test groupby(df2, [:v1, :v2]).starts == collect(1:1000)
    @test groupby(df2, [:v2, :v1]).starts == collect(1:1000)

    # grouping empty table
    @test groupby(DataFrame(A=Int[]), :A).starts == Int[]
    # grouping single row
    @test groupby(DataFrame(A=Int[1]), :A).starts == Int[1]

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
    @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["A", "B"], Value=1:2)
    @test gd[2] == DataFrame(Key1=["B", "B"], Key2=["A", "B"], Value=3:4)
    gd = groupby(df, [:Key1, :Key2])
    @test gd[1] == DataFrame(Key1="A", Key2="A", Value=1)
    @test gd[2] == DataFrame(Key1="A", Key2="B", Value=2)
    @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
    @test gd[4] == DataFrame(Key1="B", Key2="B", Value=4)
    # Reorder levels, add unused level
    levels!(df[:Key1], ["Z", "B", "A"])
    levels!(df[:Key2], ["Z", "B", "A"])
    gd = groupby(df, :Key1)
    @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["A", "B"], Value=1:2)
    @test gd[2] == DataFrame(Key1=["B", "B"], Key2=["A", "B"], Value=3:4)
    gd = groupby(df, [:Key1, :Key2])
    @test gd[1] == DataFrame(Key1="A", Key2="A", Value=1)
    @test gd[2] == DataFrame(Key1="A", Key2="B", Value=2)
    @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
    @test gd[4] == DataFrame(Key1="B", Key2="B", Value=4)

    @testset "grouping with missings" begin
        global df = DataFrame(Key1 = ["A", missing, "B", "B", "A"],
                              Key2 = CategoricalArray(["B", "A", "A", missing, "A"]),
                              Value = 1:5)

        @testset "sort=false, skipmissing=false" begin
            global gd = groupby(df, :Key1)
            @test length(gd) == 3
            @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["B", "A"], Value=[1, 5])
            @test gd[2] ≅ DataFrame(Key1=missing, Key2="A", Value=2)
            @test gd[3] ≅ DataFrame(Key1=["B", "B"], Key2=["A", missing], Value=3:4)

            global gd = groupby(df, [:Key1, :Key2])
            @test length(gd) == 5
            @test gd[1] == DataFrame(Key1="A", Key2="B", Value=1)
            @test gd[2] ≅ DataFrame(Key1=missing, Key2="A", Value=2)
            @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
            @test gd[4] ≅ DataFrame(Key1="B", Key2=missing, Value=4)
            @test gd[5] ≅ DataFrame(Key1="A", Key2="A", Value=5)
        end

        @testset "sort=false, skipmissing=true" begin
            global gd = groupby(df, :Key1, skipmissing=true)
            @test length(gd) == 2
            @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["B", "A"], Value=[1, 5])
            @test gd[2] ≅ DataFrame(Key1=["B", "B"], Key2=["A", missing], Value=3:4)

            global gd = groupby(df, [:Key1, :Key2], skipmissing=true)
            @test length(gd) == 3
            @test gd[1] == DataFrame(Key1="A", Key2="B", Value=1)
            @test gd[2] == DataFrame(Key1="B", Key2="A", Value=3)
            @test gd[3] == DataFrame(Key1="A", Key2="A", Value=5)
        end

        @testset "sort=true, skipmissing=false" begin
            global gd = groupby(df, :Key1, sort=true)
            @test length(gd) == 3
            @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["B", "A"], Value=[1, 5])
            @test gd[2] ≅ DataFrame(Key1=["B", "B"], Key2=["A", missing], Value=3:4)
            @test gd[3] ≅ DataFrame(Key1=missing, Key2="A", Value=2)

            global gd = groupby(df, [:Key1, :Key2], sort=true)
            @test length(gd) == 5
            @test gd[1] ≅ DataFrame(Key1="A", Key2="A", Value=5)            
            @test gd[2] == DataFrame(Key1="A", Key2="B", Value=1)
            @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
            @test gd[4] ≅ DataFrame(Key1="B", Key2=missing, Value=4)
            @test gd[5] ≅ DataFrame(Key1=missing, Key2="A", Value=2)        
        end

        @testset "sort=false, skipmissing=true" begin
            global gd = groupby(df, :Key1, sort=true, skipmissing=true)
            @test length(gd) == 2
            @test gd[1] == DataFrame(Key1=["A", "A"], Key2=["B", "A"], Value=[1, 5])
            @test gd[2] ≅ DataFrame(Key1=["B", "B"], Key2=["A", missing], Value=3:4)

            global gd = groupby(df, [:Key1, :Key2], sort=true, skipmissing=true)
            @test length(gd) == 3
            @test gd[1] == DataFrame(Key1="A", Key2="A", Value=5)
            @test gd[2] == DataFrame(Key1="A", Key2="B", Value=1)
            @test gd[3] == DataFrame(Key1="B", Key2="A", Value=3)
        end
    end

    @testset "iteration protocol" begin
        global gd = groupby(DataFrame(A = [:A, :A, :B, :B], B = 1:4), :A)
        for v in gd
            @test size(v) == (2,2)
        end
    end
end
