module TestData

using Test, DataFrames, Random, Statistics, CategoricalArrays
const ≅ = isequal

"""Check if passed data frames are `isequal` and have the same types of columns"""
isequal_coltyped(df1::AbstractDataFrame, df2::AbstractDataFrame) =
    isequal(df1, df2) && typeof.(eachcol(df1)) == typeof.(eachcol(df2))

@testset "constructors" begin
    df1 = DataFrame([[1, 2, missing, 4], ["one", "two", missing, "four"]], [:Ints, :Strs])
    df2 = DataFrame([[1, 2, missing, 4], ["one", "two", missing, "four"]], :auto)
    df3 = DataFrame([[1, 2, missing, 4]], :auto)
    df6 = DataFrame([[1, 2, missing, 4], [1, 2, missing, 4], ["one", "two", missing, "four"]],
                    [:A, :B, :C])
    df7 = DataFrame(x=[1, 2, missing, 4], y=["one", "two", missing, "four"])
    @test size(df7) == (4, 2)
    @test df7[!, :x] ≅ [1, 2, missing, 4]

    #test_group("description functions")
    @test size(df6, 1) == 4
    @test size(df6, 2) == 3
    @test propertynames(df6) == [:A, :B, :C]
    @test propertynames(df2) == [:x1, :x2]
    @test propertynames(df7) == [:x, :y]

    #test_group("ref")
    @test df6[2, 3] == "two"
    @test ismissing(df6[3, 3])
    @test df6[2, :C] == "two"
    @test df6[!, :B] ≅ [1, 2, missing, 4]
    @test size(df6[:, [2, 3]], 2) == 2
    @test size(df6[2, :], 1) == ncol(df6) # this is a DataFrameRow
    @test size(df6[2:2, :], 1) == 1
    @test size(df6[[1, 3], [1, 3]]) == (2, 2)
    @test size(df6[1:2, 1:2]) == (2, 2)
    @test size(first(df6, 2)) == (2, 3)
    # lots more to do

    #test_group("assign")
    df6[!, 3] = ["un", "deux", "trois", "quatre"]
    @test df6[1, 3] == "un"
    df6[!, :B] = [4, 3, 2, 1]
    @test df6[1, 2] == 4
    df6[!, :D] = [true, false, true, false]
    @test df6[1, 4]
    select!(df6, Not(:D))
    @test propertynames(df6) == [:A, :B, :C]
    @test size(df6, 2) == 3

    #test_context("SubDataFrames")

    #test_group("constructors")
    # single index is rows
    sdf6a = view(df6, 1:1, :)
    sdf6b = view(df6, 2:3, :)
    sdf6c = view(df6, [true, false, true, false], :)
    @test size(sdf6a) == (1, 3)
    sdf6d = view(df6, [1, 3], [:B])
    @test size(sdf6d) == (2, 1)
    sdf6e = view(df6, [0x01], :)
    @test size(sdf6e) == (1, 3)
    sdf6f = view(df6, UInt64[1, 2], :)
    @test size(sdf6f) == (2, 3)
    sdf6g = view(df6, [1, 3], :B)
    sdf6h = view(df6, 1, :B)
    sdf6i = view(df6, 1, [:B])

    #test_group("ref")
    @test sdf6a[1, 2] == 4

    #test_context("Within")
    #test_group("Associative")

    #test_group("DataFrame")
    Random.seed!(1)
    N = 20
    d1 = Vector{Union{Int64, Missing}}(rand(1:2, N))
    d2 = CategoricalArray(["A", "B", missing])[rand(1:3, N)]
    d3 = randn(N)
    d4 = randn(N)
    df7 = DataFrame([d1, d2, d3], [:d1, :d2, :d3])

    #test_group("groupby")
    gd = groupby(df7, :d1)
    @test length(gd) == 2
    @test gd[1][:, :d2] ≅ d2[d1 .== 1]
    @test gd[2][:, :d2] ≅ d2[d1 .== 2]
    @test gd[1][:, :d3] == d3[d1 .== 1]
    @test gd[2][:, :d3] == d3[d1 .== 2]

    g1 = groupby(df7, [:d1, :d2])
    g2 = groupby(df7, [:d2, :d1])
    @test g1[1][:, :d3] == g2[1][:, :d3]

    res = 0.0
    for x in g1
        res += sum(x[:, :d1])
    end
    @test res == sum(df7[!, :d1])

    df10 = DataFrame([[1:4;], [2:5;],
                      ["a", "a", "a", "b" ], ["c", "d", "c", "d"]],
                     [:d1, :d2, :d3, :d4])

    gd = groupby(df10, [:d3], sort=true)
    ggd = groupby(gd[1], [:d3, :d4], sort=true) # make sure we can groupby SubDataFrames
    @test ggd[1][1, :d3] == "a"
    @test ggd[1][1, :d4] == "c"
    @test ggd[1][2, :d3] == "a"
    @test ggd[1][2, :d4] == "c"
    @test ggd[2][1, :d3] == "a"
    @test ggd[2][1, :d4] == "d"
end

@testset "completecases and dropmissing" begin
    df1 = DataFrame([Vector{Union{Int, Missing}}(1:4), Vector{Union{Int, Missing}}(1:4)],
                    :auto)
    df2 = DataFrame([Union{Int, Missing}[1, 2, 3, 4], ["one", "two", missing, "four"]],
                    :auto)
    df3 = DataFrame(x=Int[1, 2, 3, 4], y=Union{Int, Missing}[1, missing, 2, 3],
                    z=Missing[missing, missing, missing, missing])

    @test completecases(df2) == .!ismissing.(df2.x2)
    @test @inferred(completecases(df3, :x)) == trues(nrow(df3))
    @test completecases(df3, :y) == .!ismissing.(df3.y)
    @test completecases(df3, :z) == completecases(df3, [:z, :x]) ==
          completecases(df3, [:x, :z]) == completecases(df3, [:y, :x, :z]) ==
          falses(nrow(df3))
    @test @inferred(completecases(df3, [:y, :x])) ==
          completecases(df3, [:x, :y]) == .!ismissing.(df3.y)
    @test dropmissing(df2) == df2[[1, 2, 4], :]
    returned = dropmissing(df1)
    @test df1 == returned && df1 !== returned
    df2b = copy(df2)
    @test dropmissing!(df2b) === df2b
    @test df2b == df2[[1, 2, 4], :]
    df1b = copy(df1)
    @test dropmissing!(df1b) === df1b
    @test df1b == df1

    @test isempty(completecases(DataFrame())) && completecases(DataFrame()) isa BitVector

    @test_throws ArgumentError completecases(DataFrame(a=1), Not(:a))
    @test_throws ArgumentError completecases(DataFrame(a=1), [])

    @test completecases(DataFrame(x=1:3, y=1:3), [:x]) == trues(3)
    @test completecases(DataFrame(x=[1, missing, 3], y=1:3), [:x]) == [true, false, true]
    @test_throws ArgumentError completecases(DataFrame(x=1:3), Cols())
    @test_throws MethodError completecases(DataFrame(x=1), true)
    @test_throws ArgumentError completecases(df3, :a)

    for cols in (:x2, "x2", [:x2], ["x2"], [:x1, :x2], ["x1", "x2"], 2, [2], 1:2,
                 [true, true], [false, true], :,
                 r"x2", r"x", Not(1), Not([1]), Not(Int[]), Not([]), Not(Symbol[]),
                 Not(1:0), Not([true, false]), Not(:x1), Not([:x1]))
        @test df2[completecases(df2, cols), :] == df2[[1, 2, 4], :]
        @test dropmissing(df2, cols) == df2[[1, 2, 4], :]
        returned = dropmissing(df1, cols)
        @test df1 == returned && df1 !== returned
        df2b = copy(df2)
        @test dropmissing!(df2b, cols) === df2b
        @test df2b == df2[[1, 2, 4], :]
        @test dropmissing(df2, cols) == df2b
        @test df2 != df2b
        df1b = copy(df1)
        @test dropmissing!(df1b, cols) === df1b
        @test df1b == df1
    end

    # Zero column case
    @test isempty(dropmissing(DataFrame())) && dropmissing(DataFrame()) isa DataFrame
    @test isempty(dropmissing!(DataFrame())) && dropmissing!(DataFrame()) isa DataFrame
    df = DataFrame(a=1:3, b=4:6)
    dfv = @view df[:, 2:1]
    @test isempty(dropmissing(dfv)) && dropmissing(dfv) isa DataFrame
    @test_throws ArgumentError dropmissing!(dfv)
    @test_throws ArgumentError dropmissing(df1, [])
    @test_throws ArgumentError dropmissing!(df1, [])

    df = DataFrame(a=[1, missing, 3])
    sdf = view(df, :, :)
    @test dropmissing(sdf) == DataFrame(a=[1, 3])
    @test eltype(dropmissing(df, disallowmissing=false).a) == Union{Int, Missing}
    @test eltype(dropmissing(df, disallowmissing=true).a) == Int
    @test eltype(dropmissing(sdf, disallowmissing=false).a) == Union{Int, Missing}
    @test eltype(dropmissing(sdf, disallowmissing=true).a) == Int
    @test df ≅ DataFrame(a=[1, missing, 3]) # make sure we did not mutate df

    @test_throws ArgumentError dropmissing!(sdf)

    df2 = copy(df)
    @test dropmissing!(df, disallowmissing=true) === df
    @test dropmissing!(df2, disallowmissing=false) === df2
    @test eltype(df.a) == Int
    @test eltype(df2.a) == Union{Int, Missing}
    @test df.a == df2.a == [1, 3]

    # view=true
    df = DataFrame(a=[1, missing, 3])
    @test dropmissing(df, view=false) == DataFrame(a=[1, 3])
    @test dropmissing(df, view=true) == view(df, [1, 3], :)
    @test typeof(dropmissing(df, view=true)) <: SubDataFrame
    @test eltype(dropmissing(df, view=true, disallowmissing=false).a) == Union{Int, Missing}
    @test_throws ArgumentError dropmissing(df, view=true, disallowmissing=true)
    @test eltype(dropmissing(df, view=false, disallowmissing=false).a) == Union{Int, Missing}
    @test eltype(dropmissing(df, view=false, disallowmissing=true).a) == Int

    a = [1, 2]
    df = DataFrame(a=a, copycols=false)
    @test dropmissing!(df) === df
    @test a === df.a
    dfx = dropmissing(df)
    @test df == df
    @test dfx !== df
    @test dfx.a !== df.a
    @test a === df.a # we did not touch df

    b = Union{Int, Missing}[1, 2]
    df = DataFrame(b=b)
    @test eltype(dropmissing(df).b) == Int
    @test eltype(dropmissing!(df).b) == Int

    # disallowmissing argument
    a = Union{Int, Missing}[3, 4]
    b = Union{Int, Missing}[1, 2]
    df = DataFrame(;a,b)
    @test eltype(dropmissing(df, disallowmissing=false).a) == Union{Int, Missing}
    @test eltype(dropmissing!(copy(df), disallowmissing=false).a) == Union{Int, Missing}
    @test eltype(dropmissing(df, disallowmissing=true).a) == Int
    @test eltype(dropmissing!(copy(df), disallowmissing=true).a) == Int
    @test eltype(dropmissing(df, :a, disallowmissing=true).a) == Int
    @test eltype(dropmissing!(copy(df), :a, disallowmissing=true).a) == Int
    @test eltype(dropmissing(df, :b, disallowmissing=true).a) == Union{Int, Missing}
    @test eltype(dropmissing!(copy(df), :b, disallowmissing=true).a) == Union{Int, Missing}

    # CategoricalArrays
    c = categorical([1, 2, 1, missing])
    df = DataFrame(c=c)
    @test dropmissing(df) == DataFrame(c=categorical([1, 2, 1]))
    @test eltype(dropmissing(df).c) == CategoricalValue{Int, UInt32}
    @test eltype(dropmissing!(df).c) == CategoricalValue{Int, UInt32}

    # Multithreaded execution test (must be at least ncol > 1, nrow > 100_000)
    N_rows, N_cols = 110_000, 3
    df = DataFrame([rand(N_rows) for i in 1:N_cols], :auto) |> allowmissing
    # Deterministic drop mask: IF remainder of index position divided by 10 == column index THEN missing
    for i in 1:ncol(df)
        missing_mask = (eachindex(df[!, i]) .% 10) .== i
        df[missing_mask, i] .= missing
    end

    notmissing_rows = [i for i in 1:N_rows if i % 10 == 0 || i % 10 > ncol(df)]
    @test dropmissing(df) ≅ df[notmissing_rows, :]

    cols = [:x1, :x2]
    notmissing_rows = [i for i in 1:N_rows if i % 10 == 0 || i % 10 > length(cols)]
    returned = dropmissing(df, cols)
    @test returned ≅ df[notmissing_rows, :]
    @test eltype(returned[:, cols[1]]) == nonmissingtype(eltype(df[:, cols[1]]))
    @test eltype(returned[:, cols[2]]) == nonmissingtype(eltype(df[:, cols[2]]))
    @test eltype(returned[:, ncol(df)]) == eltype(df[:, ncol(df)])

    # correct handling of not propagating views
    df = DataFrame(a=1:3, b=Any[11, missing, 13])
    df2 = dropmissing(df)
    @test df2 == DataFrame(a=[1, 3], b=[11, 13])
    @test df2.a isa Vector{Int}
    @test df2.b isa Vector{Any}
end

@testset "deleteat! https://github.com/JuliaLang/julia/pull/41646 bug workaround" begin
    # these tests will crash Julia if they are not correct
    df = DataFrame(a= Vector{Union{Bool,Missing}}(missing, 10^4));
    deleteat!(df, 2:(nrow(df) - 5))
    @test nrow(df) == 6

    df = DataFrame(a= Vector{Union{Bool,Missing}}(missing, 10^4));
    deleteat!(df, [false; trues(nrow(df) - 6); falses(5)])
    @test nrow(df) == 6
end

@testset "dropmissing and unique view kwarg test" begin
    df = DataFrame(rand(3, 4), :auto)
    for fun in (dropmissing, unique)
        @test fun(df) isa DataFrame
        @inferred fun(df)
        @test fun(view(df, 1:2, 1:2)) isa DataFrame
        @test fun(df, view=false) isa DataFrame
        @test fun(view(df, 1:2, 1:2), view=false) isa DataFrame
        @test fun(df, view=true) isa SubDataFrame
        @test fun(df, view=true) == fun(df)
        @test fun(view(df, 1:2, 1:2), view=true) isa SubDataFrame
        @test fun(view(df, 1:2, 1:2), view=true) == fun(view(df, 1:2, 1:2))
    end
    @test_throws ArgumentError dropmissing(df, view=true, disallowmissing=true)
end

@testset "filter() and filter!()" begin
    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter(r -> r[:x] > 1, df) == DataFrame(x=[3, 2], y=["b", "a"])
    @test filter!(r -> r[:x] > 1, df) === df == DataFrame(x=[3, 2], y=["b", "a"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter(:x => x -> x > 1, df) == DataFrame(x=[3, 2], y=["b", "a"])
    @test filter!(:x => x -> x > 1, df) === df == DataFrame(x=[3, 2], y=["b", "a"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter("x" => x -> x > 1, df) == DataFrame(x=[3, 2], y=["b", "a"])
    @test filter!("x" => x -> x > 1, df) === df == DataFrame(x=[3, 2], y=["b", "a"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter(1 => x -> x > 1, df) == DataFrame(x=[3, 2], y=["b", "a"])
    @test filter!(1 => x -> x > 1, df) === df == DataFrame(x=[3, 2], y=["b", "a"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter([:x] => x -> x > 1, df) == DataFrame(x=[3, 2], y=["b", "a"])
    @test filter!([:x] => x -> x > 1, df) === df == DataFrame(x=[3, 2], y=["b", "a"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter(["x"] => x -> x > 1, df) == DataFrame(x=[3, 2], y=["b", "a"])
    @test filter!(["x"] => x -> x > 1, df) === df == DataFrame(x=[3, 2], y=["b", "a"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter((:) => (r...) -> r[1] > 1, df) == DataFrame(x=[3, 2], y=["b", "a"])
    @test filter!((:) => (r...) -> r[1] > 1, df) === df == DataFrame(x=[3, 2], y=["b", "a"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter([:x, :x] => ==, df) == df
    @test filter!([:x, :x] => ==, df) === df == DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter(["x", "x"] => ==, df) == df
    @test filter!(["x", "x"] => ==, df) === df == DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
    @test filter([2, 2] => !=, df) == DataFrame(x=Int[], y=String[])
    @test filter!([2, 2] => !=, df) === df == DataFrame(x=Int[], y=String[])

    for sel in [r"x", [1, 2], [:x1, :x2], ["x1", "x2"], :, Not(r"y")]
        df = DataFrame(x1=[3, 1, 2, 1], x2=["b", "c", "aa", "bbb"])
        @test filter(sel => (a, b) -> a == length(b), df) ==
              DataFrame(x1=[1, 2], x2=["c", "aa"])
        @test filter!(sel => (a, b) -> a == length(b), df) === df ==
              DataFrame(x1=[1, 2], x2=["c", "aa"])
    end

    df = DataFrame(x=[3, 1, 2, 1, missing], y=["b", "c", "a", "b", "c"])
    @test_throws TypeError filter(r -> r[:x] > 1, df)
    @test_throws TypeError filter!(r -> r[:x] > 1, df)
    @test_throws TypeError filter(:x => x -> x > 1, df)
    @test_throws TypeError filter("x" => x -> x > 1, df)
    @test_throws TypeError filter!(:x => x -> x > 1, df)
    @test_throws TypeError filter!("x" => x -> x > 1, df)
    @test_throws TypeError filter(1 => x -> x > 1, df)
    @test_throws TypeError filter!(1 => x -> x > 1, df)
    @test_throws TypeError filter([:x] => x -> x > 1, df)
    @test_throws TypeError filter(["x"] => x -> x > 1, df)
    @test_throws TypeError filter!([:x] => x -> x > 1, df)
    @test_throws TypeError filter!(["x"] => x -> x > 1, df)
    @test_throws TypeError filter((:) => (r...) -> r[1] > 1, df)
    @test_throws TypeError filter!((:) => (r...) -> r[1] > 1, df)
end

@testset "filter view kwarg test" begin
    df = DataFrame(rand(3, 4), :auto)
    for fun in (row -> row.x1 > 0, :x1 => x -> x > 0, "x1" => x -> x > 0,
                [:x1] => x -> x > 0, ["x1"] => x -> x > 0,
                r"1" => x -> x > 0, AsTable(:) => x -> x.x1 > 0)
        @test filter(fun, df) isa DataFrame
        @inferred filter(fun, df)
        @test filter(fun, view(df, 1:2, 1:2)) isa DataFrame
        @test filter(fun, df, view=false) isa DataFrame
        @test filter(fun, view(df, 1:2, 1:2), view=false) isa DataFrame
        @test filter(fun, df, view=true) isa SubDataFrame
        @test filter(fun, df, view=true) == filter(fun, df)
        @test filter(fun, view(df, 1:2, 1:2), view=true) isa SubDataFrame
        @test filter(fun, view(df, 1:2, 1:2), view=true) == filter(fun, view(df, 1:2, 1:2))
    end
end

@testset "filter and filter! with SubDataFrame" begin
    dfv = view(DataFrame(x=[0, 0, 3, 1, 3, 1], y=1:6), 3:6, 1:1)

    @test filter(:x => x -> x > 2, dfv) == DataFrame(x=[3, 3])
    @test filter(:x => x -> x > 2, dfv, view=true) == DataFrame(x=[3, 3])
    @test parent(filter(:x => x -> x > 2, dfv, view=true)) === parent(dfv)

    @test_throws ArgumentError filter!(:x => x -> x > 2, dfv)
end

@testset "filter and filter! with AsTable" begin
    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])

    function testfun(x)
        @assert x isa NamedTuple
        @assert propertynames(x) == (:x,)
        return x.x > 1
    end

    @test filter(AsTable(:x) => testfun, df) == DataFrame(x=[3, 2], y=["b", "a"])
    filter!(AsTable(:x) => testfun, df)
    @test df == DataFrame(x=[3, 2], y=["b", "a"])

    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])

    @test filter(AsTable("x") => testfun, df) == DataFrame(x=[3, 2], y=["b", "a"])
    filter!(AsTable("x") => testfun, df)
    @test df == DataFrame(x=[3, 2], y=["b", "a"])
end

@testset "empty arg to filter and filter!" begin
    df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])

    @test filter([] => () -> true, df) == df
    @test filter(AsTable(r"z") => x -> true, df) == df
    @test filter!([] => () -> true, copy(df)) == df
    @test filter!(AsTable(r"z") => x -> true, copy(df)) == df

    flipflop0 = let
        state = false
        () -> (state = !state)
    end

    flipflop1 = let
        state = false
        x -> (state = !state)
    end

    @test filter([] => flipflop0, df) == df[[1, 3], :]
    @test filter(Int[] => flipflop0, df) == df[[1, 3], :]
    @test filter(String[] => flipflop0, df) == df[[1, 3], :]
    @test filter(Symbol[] => flipflop0, df) == df[[1, 3], :]
    @test filter(r"z" => flipflop0, df) == df[[1, 3], :]
    @test filter(Not(All()) => flipflop0, df) == df[[1, 3], :]
    @test filter(Cols() => flipflop0, df) == df[[1, 3], :]
    @test filter(AsTable(r"z") => flipflop1, df) == df[[1, 3], :]
    @test filter(AsTable([]) => flipflop1, df) == df[[1, 3], :]
    @test filter!([] => flipflop0, copy(df)) == df[[1, 3], :]
    @test filter!(Int[] => flipflop0, copy(df)) == df[[1, 3], :]
    @test filter!(String[] => flipflop0, copy(df)) == df[[1, 3], :]
    @test filter!(Symbol[] => flipflop0, copy(df)) == df[[1, 3], :]
    @test filter!(r"z" => flipflop0, copy(df)) == df[[1, 3], :]
    @test filter!(Not(All()) => flipflop0, copy(df)) == df[[1, 3], :]
    @test filter!(Cols() => flipflop0, copy(df)) == df[[1, 3], :]
    @test filter!(AsTable(r"z") => flipflop1, copy(df)) == df[[1, 3], :]
    @test filter!(AsTable([]) => flipflop1, copy(df)) == df[[1, 3], :]

    @test_throws MethodError filter([] => flipflop1, df)
    @test_throws MethodError filter(AsTable([]) => flipflop0, df)
end

@testset "names with cols" begin
    df = DataFrame(a=1, x1=2, x2=3, x3=4, x4=5)

    for v in [df, groupby(df, :a)]
        @test names(v, All()) == names(v, :) == names(v) == ["a", "x1", "x2", "x3", "x4"]
        @test names(v, Between(:x1, :x3)) == ["x1", "x2", "x3"]
        @test names(v, Not(:a)) == names(v, r"x") == ["x1", "x2", "x3", "x4"]
        @test names(v, :x1) == names(v, 2) == ["x1"]
        @test names(v, Cols()) == names(v, Cols()) == []
    end

    for v in [view(df, :, [4, 3, 2, 1]), groupby(view(df, :, [4, 3, 2, 1]), 1), view(df, 1, [4, 3, 2, 1])]
        @test names(v, All()) == names(v, :) == names(v) ==  ["x3", "x2", "x1", "a"]
        @test names(v, Between(:x2, :x1)) == ["x2", "x1"]
        @test names(v, Not(:a)) == names(v, r"x") == ["x3", "x2", "x1"]
        @test names(v, :x1) == names(v, 3) == ["x1"]
        @test names(v, Cols()) == names(v, Cols()) == []
    end
end

@testset "empty and empty!" begin
    df = DataFrame(a=1, b="x")
    df1 = empty(df)
    @test df == DataFrame(a=1, b="x")
    @test names(df1) == ["a", "b"]
    @test nrow(df1) == 0
    @test eltype(df1.a) <: Int
    @test eltype(df1.b) <: String
    @test empty!(df) === df
    @test names(df) == ["a", "b"]
    @test nrow(df) == 0
    @test eltype(df.a) <: Int
    @test eltype(df.b) <: String
end

@testset "isapprox" begin
    df = DataFrame([:x1 => zeros(3), :x2 => ones(3)])
    @test isapprox(df, DataFrame(x1=[0.0, 0.0, 0.0], x2=[1.0, 1.0, 1.0]))
    @test isapprox(df, DataFrame(x1=[0.0, 0.0, 0.0], x2=[1.000000010000, 1.0, 1.0]))
    @test_throws DimensionMismatch isapprox(DataFrame(a=1), DataFrame(a=[1,2]))
    @test_throws ArgumentError isapprox(DataFrame(a=1), DataFrame(b=1))
    @test !isapprox(df, DataFrame(x1=[0.0, 0.0, 0.0], x2=[1.1, 1.0, 1.0]))
    @test !isapprox(df, DataFrame(x1=[0.0, 0.0, 0.0], x2=[1.1, 1.0, 1.0]), atol=0.09)
    @test isapprox(df, DataFrame(x1=[0.0, 0.0, 0.0], x2=[1.1, 1.0, 1.0]), atol=0.11)
end

@testset "fillcombinations"  begin
    df1 = DataFrame(a=[1, 2, missing], b=[1, 1, 2],
                    c=categorical([11, 12, missing]), d=111:113)
    levels!(df1.c, [12, 11, 10])

    for ad in (true, false)
        res = fillcombinations(df1, [:a, :b], allowduplicates=ad)
        @test levels(res.c) == levels(df1.c)
        @test isequal_coltyped(res,
                               DataFrame(a=[1, 2, missing, 1, 2, missing],
                                         b=[1, 1, 1, 2, 2, 2],
                                         c=categorical([11, 12, missing, missing, missing, missing]),
                                         d=[111, 112, missing, missing, missing, 113]))

        # fill in the pool - column :c becomes Vector
        res = fillcombinations(df1, [:a, :b], fill=12, allowduplicates=ad)
        @test isequal_coltyped(res,
                               DataFrame(a=[1, 2, missing, 1, 2, missing],
                                         b=[1, 1, 1, 2, 2, 2],
                                         c=[11, 12, 12, 12, 12, missing],
                                         d=Union{Int, Missing}[111, 112, 12, 12, 12, 113]))
        # fill not in the pool - column :c becomes Vector
        res = fillcombinations(df1, [:a, :b], fill=-100, allowduplicates=ad)
        @test isequal_coltyped(res,
                               DataFrame(a=[1, 2, missing, 1, 2, missing],
                                         b=[1, 1, 1, 2, 2, 2],
                                         c=[11, 12, -100, -100, -100, missing],
                                         d=Union{Int, Missing}[111, 112, -100, -100, -100, 113]))
        # fill is CategoricalValue - in column :d it gets unwrapped to its integer value
        res = fillcombinations(df1, [:a, :b], fill=CategoricalValue(12, df1.c), allowduplicates=ad)
        @test levels(res.c) == levels(df1.c)
        @test isequal_coltyped(res,
                               DataFrame(a=[1, 2, missing, 1, 2, missing],
                                         b=[1, 1, 1, 2, 2, 2],
                                         c=categorical([11, 12, 12, 12, 12, missing]),
                                         d=Union{Int, Missing}[111, 112, 12, 12, 12, 113]))
        res = fillcombinations(df1, [:a, :b], fill="X", allowduplicates=ad)

        @test isequal_coltyped(res,
                               DataFrame(a=[1, 2, missing, 1, 2, missing],
                                         b=[1, 1, 1, 2, 2, 2],
                                         c=[11, 12, "X", "X", "X", missing],
                                         d=[111, 112, "X", "X", "X", 113]))
        res = fillcombinations(df1, :c, allowduplicates=ad)
        @test levels(res.c) == levels(df1.c)
        @test isequal_coltyped(res,
                               DataFrame(a=[2, 1, missing, missing],
                                         b=[1, 1, missing, 2],
                                         c=categorical([12, 11, 10, missing]),
                                         d=[112, 111, missing, 113]))
        res = fillcombinations(df1, :c, fill="X", allowduplicates=ad)
        @test levels(res.c) == levels(df1.c)
        @test isequal_coltyped(res,
                               DataFrame(a=[2, 1, "X", missing],
                                         b=[1, 1, "X", 2],
                                         c=categorical([12, 11, 10, missing]),
                                         d=[112, 111, "X", 113]))
        res = fillcombinations(df1, [:c, :b], allowduplicates=ad)
        @test levels(res.c) == levels(df1.c)
        @test isequal_coltyped(res,
                               DataFrame(a=[2; 1; fill(missing, 6)],
                                         b=[1, 1, 1, 1, 2, 2, 2, 2],
                                         c=categorical([12, 11, 10, missing, 12, 11, 10, missing]),
                                         d=[112; 111; fill(missing, 5); 113]))
    end

    df2 = DataFrame(a=[1, missing, 2, 1], b=[3, 1, 2, 2],
                    c=categorical([11, 12, missing, 11]), d=111:114)
    levels!(df2.c, [12, 11, 10])
    @test_throws ArgumentError fillcombinations(df2, :a)
    @test_throws ArgumentError fillcombinations(df2, :b)
    @test_throws ArgumentError fillcombinations(df2, [:a, :c])
    @test isequal_coltyped(fillcombinations(df2, [:a, :c], allowduplicates=true, fill=0),
                           DataFrame(a=[1, 2, missing, 1, 1, 2, missing, 1, 2, missing, 1, 2, missing],
                                     b=Union{Int,Missing}[0, 0, 1, 3, 2, 0, 0, 0, 0, 0, 0, 2, 0],
                                     c=categorical([12, 12, 12, 11, 11, 11, 11, 10, 10, 10, missing, missing, missing]),
                                     d=Union{Int,Missing}[0, 0, 112, 111, 114, 0, 0, 0, 0, 0, 0, 113, 0]))
    @test isequal_coltyped(fillcombinations(df2, [:a, :c], allowduplicates=true),
                           DataFrame(a=[1, 2, missing, 1, 1, 2, missing, 1, 2, missing, 1, 2, missing],
                                     b=[missing, missing, 1, 3, 2, missing, missing, missing, missing, missing, missing, 2, missing],
                                     c=categorical([12, 12, 12, 11, 11, 11, 11, 10, 10, 10, missing, missing, missing]),
                                     d=[missing, missing, 112, 111, 114, missing, missing, missing, missing, missing, missing, 113, missing]))

    # test of a larger scenario
    Random.seed!(1234)
    df3 = DataFrame(a=rand(1:10, 100), b=rand(1:10, 100), c=1:100)
    @test_throws ArgumentError fillcombinations(df3, [:a, :b])
    large_res = fillcombinations(df3, [:a, :b], allowduplicates=true)
    @test issorted(large_res.b)
    @test levels(large_res.b) == levels(large_res.b) == 1:10
    for (i, sdf) in enumerate(groupby(large_res, :b))
        @test first(sdf.b) == i
        @test issorted(sdf.a)
        @test levels(sdf.a) == 1:10
    end
    gdf3 = groupby(df3, [:a, :b])
    glarge_res = groupby(large_res, [:a, :b])
    @test length(glarge_res) == 100
    kt_gdf3 = Tuple.(keys(gdf3))
    kt_glarge_res = Tuple.(keys(glarge_res))
    @test isempty(setdiff(kt_gdf3, kt_glarge_res))
    for t in kt_glarge_res
        if t in kt_gdf3
            @test gdf3[t] == glarge_res[t]
        else
            @test DataFrame(a=t[1], b=t[2], c=missing) ≅ glarge_res[t]
        end
    end

    # empty indexcols
    @test_throws ArgumentError fillcombinations(DataFrame(a=1), [])

    # empty data frame case
    df = DataFrame(a=Int[], b=categorical(Int[]), c=String[])
    levels!(df.b, [1, 2])
    @test isequal_coltyped(fillcombinations(df, :a), df)
    @test isequal_coltyped(fillcombinations(df, :b),
                           DataFrame(a=missings(Int, 2), b=categorical([1, 2]), c=missings(String, 2)))
    @test isequal_coltyped(fillcombinations(df, :c), df)
    @test isequal_coltyped(fillcombinations(df, [:c, :b]), df)

    df = DataFrame(order94270=[1,1], source72490=1:2)
    @test fillcombinations(df, 1, allowduplicates=true) == df

    df = DataFrame(a=[[1, 1], [2, 2], missing], b=[[1, 1, 1], [1, 1, 1], [2, 2, 2]])
    @test fillcombinations(df, 1:2) ≅
        DataFrame(a=[[1, 1], [2, 2], missing, [1, 1], [2, 2], missing],
                  b=[[1, 1, 1], [1, 1, 1], [1, 1, 1], [2, 2, 2], [2, 2, 2], [2, 2, 2]])

    df = DataFrame(a=[1, 1, 2, 3], b=["a", "a", "a", "b"])
    @test fillcombinations(df, 1:2, allowduplicates=true) ==
          DataFrame(a=[1, 1, 2, 3, 1, 2, 3],
                    b=["a", "a", "a", "a", "b", "b", "b"])
    @test_throws ArgumentError fillcombinations(df, 1:2)
    @test_throws ArgumentError fillcombinations(df, 1)
    @test_throws ArgumentError fillcombinations(df, 2)
end

@testset "allcombinations" begin
    @test allcombinations(DataFrame) == DataFrame()
    @test allcombinations(DataFrame, a=1:2, b=3:4) ==
          allcombinations(DataFrame, "a" => 1:2, "b" => 3:4) ==
          allcombinations(DataFrame, :a => 1:2, :b => 3:4) ==
          DataFrame(a=[1, 2, 1, 2], b=[3, 3, 4, 4])
    @test_throws MethodError allcombinations(DataFrame, "a" => 1:2, :b => 3:4)
    @test_throws ArgumentError allcombinations(DataFrame, "a" => 1:2, "a" => 3:4)

    res = allcombinations(DataFrame, a=categorical(["a", "b", "a"], levels=["c", "b", "a"]))
    @test res == DataFrame(a=["a", "b", "a"])
    @test res.a isa CategoricalVector
    @test levels(res.a) == ["c", "b", "a"]

    @test allcombinations(DataFrame, a=categorical(["a", "b", "a"]),
                          b=Ref([1, 2]),
                          c=fill(1:2),
                          d=DataFrame(p=1, q=2)) ==
          DataFrame(a=categorical(["a", "b", "a"]),
                    b=Ref([1, 2]),
                    c=fill(1:2),
                    d=DataFrame(p=1, q=2))
    @test allcombinations(DataFrame, a=categorical(["a", "b", "a"]),
                          b=Ref([1, 2]),
                          c=fill(1:2),
                          d=DataFrame(p=1, q=2),
                          e=1:2) ==
          DataFrame(a=categorical(["a", "b", "a", "a", "b", "a"]),
                    b=Ref([1, 2]),
                    c=fill(1:2),
                    d=DataFrame(p=1, q=2),
                    e=[1, 1, 1, 2, 2, 2])
    @test_throws ArgumentError allcombinations(DataFrame, a=[1 2; 3 4])

    @test allcombinations(DataFrame, a=[1, 1, 1], b=[2, 2, 2]) ==
          DataFrame(a=fill(1, 9), b=fill(2, 9))
    @test allcombinations(DataFrame, a=[1, 1, 1], b='a':'b', c=[2, 2, 2]) ==
          DataFrame(a=fill(1, 18), b=repeat('a':'b', inner=3, outer=3), c=fill(2, 18))

    res = allcombinations(DataFrame, b=categorical(String[], levels=["a"]))
    @test nrow(res) == 0
    @test names(res) == ["b"]
    @test typeof(res.b) <: CategoricalVector{String}
    @test levels(res.b) == ["a"]

    res = allcombinations(DataFrame, b=categorical(String[], levels=["a"]), c='a':'b')
    @test nrow(res) == 0
    @test names(res) == ["b", "c"]
    @test typeof(res.b) <: CategoricalVector{String}
    @test levels(res.b) == ["a"]
    @test typeof(res.c) === Vector{Char}

    res = allcombinations(DataFrame, a=1:3, b=categorical(String[], levels=["a"]))
    @test nrow(res) == 0
    @test names(res) == ["a", "b"]
    @test typeof(res.a) === Vector{Int}
    @test typeof(res.b) <: CategoricalVector{String}
    @test levels(res.b) == ["a"]

    res = allcombinations(DataFrame, a=1:3, b=categorical(String[], levels=["a"]), c='a':'b')
    @test nrow(res) == 0
    @test names(res) == ["a", "b", "c"]
    @test typeof(res.a) === Vector{Int}
    @test typeof(res.b) <: CategoricalVector{String}
    @test levels(res.b) == ["a"]
    @test typeof(res.c) === Vector{Char}
end

end # module
