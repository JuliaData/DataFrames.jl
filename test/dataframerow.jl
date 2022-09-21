module TestDataFrameRow

using Test, DataFrames, Random, Logging, CategoricalArrays
const ≅ = isequal
const ≇ = !isequal

ref_df = DataFrame(a=Union{Int, Missing}[1, 2, 3, 1, 2, 2],
                   b=[2.0, missing, 1.2, 2.0, missing, missing],
                   c=["A", "B", "C", "A", "B", missing],
                   d=CategoricalArray(["A", missing, "C", "A", missing, "C"]))

@testset "constructors" begin
    df = deepcopy(ref_df)
    sdf = view(df, [5, 3], [3, 1, 2])

    @test names(DataFrameRow(df, 1, :)) == ["a", "b", "c", "d"]
    @test DataFrameRow(df, 1) == DataFrameRow(df, 1, :)
    @test DataFrameRow(df, 1) == DataFrameRow(df, 1, r"")
    @test names(DataFrameRow(df, 3, [3, 2])) == ["c", "b"]
    @test copy(DataFrameRow(df, 3, [3, 2])) == (c="C", b=1.2)
    @test copy(DataFrameRow(df, 3, r"[bc]")) == (b=1.2, c="C")
    @test copy(DataFrameRow(sdf, 2, [3, 2])) == (b=1.2, a=3)
    @test copy(DataFrameRow(sdf, 2, r"[bc]")) == (c="C", b=1.2)
    @test copy(DataFrameRow(sdf, 2, :)) == (c="C", a=3, b=1.2)
    @test DataFrameRow(sdf, 2) == DataFrameRow(sdf, 2, :)
    @test DataFrameRow(df, 3, [3, 2]) == df[3, [3, 2]] == view(df, 3, [3, 2])
    @test DataFrameRow(df, 3, r"[bc]") == df[3, [2, 3]] == df[3, r"[bc]"]
    @test DataFrameRow(sdf, 2, [3, 2]) == sdf[2, [3, 2]] == view(sdf, 2, [3, 2])
    @test DataFrameRow(sdf, 2, r"[bc]") == sdf[2, [1, 3]] == sdf[2, r"[bc]"]
    @test DataFrameRow(sdf, 2, :) == sdf[2, :] == view(sdf, 2, :)
    @test DataFrameRow(df, 3, 2:3) === df[3, 2:3]
    @test view(df, 3, 2:3) === df[3, 2:3]
    for x in (df, sdf)
        @test_throws ArgumentError DataFrameRow(x, 1, :a)
        @test_throws ArgumentError DataFrameRow(x, 1, 1)
        @test_throws BoundsError DataFrameRow(x, 1, 1:10)
        @test_throws BoundsError DataFrameRow(x, 1, [1:10;])
        @test_throws BoundsError DataFrameRow(x, 100, 1:2)
        @test_throws BoundsError DataFrameRow(x, 100, [1:2;])
        @test_throws BoundsError DataFrameRow(x, 100, :)
        @test_throws BoundsError DataFrameRow(x, 100)
        @test_throws ArgumentError DataFrameRow(x, true, 1:2)
        @test_throws ArgumentError DataFrameRow(x, true)
    end
    @test_throws ArgumentError DataFrameRow(sdf, true, 1:2)
end

@testset "getindex and setindex!" begin
    df = deepcopy(ref_df)
    sdf = view(df, [5, 3], [3, 1, 2])

    r = DataFrameRow(df, 2, :)
    @test r[:] === r
    @test r[r""] ≅ r
    @test view(r, :) === r
    @test view(r, r"") ≅ r
    @test r[3] == "B"
    @test r[r"c"] == r[[3]]
    @test_throws BoundsError r[5]
    @test view(r, 3)[] == "B"
    @test view(r, r"c")[1] == "B"
    @test view(r, r"c") == view(r, [3])
    view(r, 3)[] = "BB"
    @test df.c[2] == "BB"
    @test_throws MethodError r[true]
    @test_throws MethodError view(r, true)
    @test copy(r[[:c, :a]]) == (c = "BB", a = 2)
    @test copy(r[r"[ac]"]) == (a = 2, c = "BB")
    @test copy(r[r"x"]) == NamedTuple()
    @test copy(r[2:1]) == NamedTuple()
    @test copy(r[Symbol[]]) == NamedTuple()
    @test copy(view(r, [:c, :a])) == (c = "BB", a = 2)
    @test copy(view(r, r"[ac]")) == (a = 2, c = "BB")
    @test copy(view(r, r"x")) == NamedTuple()
    @test copy(view(r, 2:1)) == NamedTuple()
    @test copy(view(r, Symbol[])) == NamedTuple()
    @test copy(r[[true, false, true, false]]) == (a = 2, c = "BB")
    r.c = "B"
    @test df.c[2] == "B"
    @test_throws BoundsError copy(r[[true, false, true]])

    r = DataFrameRow(sdf, 2, [3, 1])
    @test r[:] == r
    @test r[r""] == r
    @test view(r, :) === r
    @test view(r, r"") == r
    @test r[2] == "C"
    @test r[r"c"] == r[[2]]
    @test r[2:1] == r[r"x"]
    @test_throws BoundsError r[4]
    @test view(r, 2)[] == "C"
    @test view(r, r"c") == view(r, [2])
    view(r, 2)[] = "CC"
    @test df.c[3] == "CC"
    @test_throws MethodError r[true]
    @test_throws MethodError view(r, true)
    @test copy(r[[:c, :b]]) == (c = "CC", b = 1.2)
    @test copy(view(r, [:c, :b])) == (c = "CC", b = 1.2)
    @test copy(view(r, [:c, :b])) == (c = "CC", b = 1.2)
    @test copy(r[[false, true]]) == (c = "CC",)
    @test copy(r[r"[cb]"]) == (b = 1.2, c = "CC")
    @test copy(view(r, r"[cb]")) == (b = 1.2, c = "CC")
    @test copy(view(r, r"[cb]")) == (b = 1.2, c = "CC")
    @test copy(r[r"b"]) == (b = 1.2,)
    @test copy(view(r, r"b")) == (b = 1.2,)
    @test copy(view(r, r"b")) == (b = 1.2,)
    r.c = "C"
    @test df.c[3] == "C"

    df = DataFrame([1 2 3 4
                    5 6 7 8], :auto)
    r = df[1, r"[1-3]"]
    @test names(r) == ["x1", "x2", "x3"]
    r[:] .= 10
    @test df == DataFrame([10 10 10 4
                            5  6  7 8], :auto)
    r[r"[2-3]"] .= 20
    @test df == DataFrame([10 20 20 4
                            5  6  7 8], :auto)
end

@testset "equality" begin
    df = deepcopy(ref_df)
    df2 = DataFrame(a=[1, 2, 3])

    @test !isequal(DataFrameRow(df, 1, :), DataFrameRow(df2, 1, :))
    @test isequal(DataFrame(a=missing)[1, :], DataFrame(a=missing)[1, :])
    @test DataFrameRow(df, 1, :) != DataFrameRow(df2, 1, :)
    @test DataFrameRow(df, 1, [:a]) == DataFrameRow(df2, 1, :)
    @test DataFrameRow(df, 1, [:a]) == DataFrameRow(df2, big(1), :)
    @test DataFrameRow(df, 1, :) != DataFrameRow(df, 2, :)
    @test DataFrameRow(df, 1, :) != DataFrameRow(df, 3, :)
    @test DataFrameRow(df, 1, :) == DataFrameRow(df, 4, :)
    @test ismissing(DataFrameRow(df, 2, :) == DataFrameRow(df, 5, :))
    @test DataFrameRow(df, 2, :) ≅ DataFrameRow(df, 5, :)
    @test ismissing(DataFrameRow(df, 2, :) != DataFrameRow(df, 6, :))
    @test DataFrameRow(df, 2, :) ≇ DataFrameRow(df, 6, :)

    dc_df = deepcopy(df)
    @test DataFrameRow(df, 1, :) == DataFrameRow(dc_df, 1, :)
    @test DataFrameRow(df, 1, 1:2) == DataFrameRow(dc_df, 1, 1:2)
    @test DataFrameRow(df, 1, [1, 2]) != DataFrameRow(dc_df, 1, [2, 1])
    dc_df[1, 1] = 2
    @test DataFrameRow(df, 1, :) != DataFrameRow(dc_df, 1, :)
    @test DataFrameRow(df, 1, 1:2) != DataFrameRow(dc_df, 1, 1:2)
    @test DataFrameRow(df, 1, [2]) == DataFrameRow(dc_df, 1, [2])
    rename!(df, :b=>:z)
    @test DataFrameRow(df, 1, [2]) != DataFrameRow(dc_df, 1, [2])
end

@testset "isless" begin
    df = DataFrame(a=[1, 1, 2, 2, 2, 2, missing, missing],
                   b=Union{Float64, Missing}[2.0, 3.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0],
                   c=[:B, missing, :A, :C, :D, :D, :A, :A])

    @test isless(DataFrameRow(df, 1, :), DataFrameRow(df, 2, :))
    @test !isless(DataFrameRow(df, 2, :), DataFrameRow(df, 1, :))
    @test !isless(DataFrameRow(df, 1, :), DataFrameRow(df, 1, :))
    @test isless(DataFrameRow(df, 1, :), DataFrameRow(df, 3, :))
    @test !isless(DataFrameRow(df, 3, :), DataFrameRow(df, 1, :))
    @test isless(DataFrameRow(df, 3, :), DataFrameRow(df, 4, :))
    @test !isless(DataFrameRow(df, 4, :), DataFrameRow(df, 3, :))
    @test isless(DataFrameRow(df, 4, :), DataFrameRow(df, 5, :))
    @test !isless(DataFrameRow(df, 5, :), DataFrameRow(df, 4, :))
    @test !isless(DataFrameRow(df, 6, :), DataFrameRow(df, 5, :))
    @test !isless(DataFrameRow(df, 5, :), DataFrameRow(df, 6, :))
    @test isless(DataFrameRow(df, 7, :), DataFrameRow(df, 8, :))
    @test !isless(DataFrameRow(df, 8, :), DataFrameRow(df, 7, :))

    @test_throws ArgumentError df[1, 1:2] < df[1, 1:3]
    @test_throws ArgumentError df[1, 1:2] < df[1, 2:3]
end

@testset "hashing of DataFrameRow and GroupKey" begin
    df = deepcopy(ref_df)

    @test hash(DataFrameRow(df, 1, :)) != hash(DataFrameRow(df, 2, :))
    @test hash(DataFrameRow(df, 1, :)) != hash(DataFrameRow(df, 3, :))
    @test hash(DataFrameRow(df, 1, :)) == hash(DataFrameRow(df, 4, :))
    @test hash(DataFrameRow(df, 2, :)) == hash(DataFrameRow(df, 5, :))
    @test hash(DataFrameRow(df, 2, :)) != hash(DataFrameRow(df, 6, :))

    df = DataFrame(reshape(1:24, 6, 4), :auto)
    df.x2 = string.(df.x2)
    df.x3 = categorical(df.x3)
    df.x4 = Float64.(df.x4)
    gks = keys(groupby(df, :))

    for i in axes(df, 1), h in UInt(0):UInt(10)
        @test hash(DataFrameRow(df, i, :), h) ==
              hash(gks[i], h) ==
              hash(NamedTuple(DataFrameRow(df, i, :)), h)
    end
end

@testset "getproperty, setproperty! and propertynames" begin
    df = deepcopy(ref_df)

    r = DataFrameRow(df, 1, :)
    @test propertynames(r) == keys(r) == Symbol.(names(df))
    @test r.a === 1
    @test r.b === 2.0
    @test copy(r[[:a, :b]]) == (a=1, b=2.0)

    r.a = 2
    @test r.a === 2
    r.b = 1
    @test r.b === 1.0
end

@testset "keys, values and iteration, size" begin
    df = deepcopy(ref_df)
    r = DataFrameRow(df, 1, :)

    @test keys(r) == propertynames(df)
    @test values(r) == (df[1, 1], df[1, 2], df[1, 3], df[1, 4])
    @test collect(pairs(r)) == [:a=>df[1, 1], :b=>df[1, 2], :c=>df[1, 3], :d=>df[1, 4]]

    @test haskey(r, :a)
    @test haskey(r, 1)
    @test !haskey(r, :zzz)
    @test !haskey(r, 0)
    @test !haskey(r, 1000)
    @test_throws ArgumentError haskey(r, true)

    x = DataFrame(ones(5, 4), :auto)
    dfr = view(x, 2, 2:3)
    @test names(dfr) == names(x)[2:3]
    dfr = view(x, 2, [4, 2])
    @test names(dfr) == names(x)[[4, 2]]

    x = DataFrame(ones(10, 10), :auto)
    r = x[3, [8, 5, 1, 3]]
    @test length(r) == 4
    @test lastindex(r) == 4
    @test ndims(r) == 1
    @test ndims(typeof(r)) == 1
    @test size(r) == (4,)
    @test size(r, 1) == 4
    @test_throws BoundsError size(r, 2)
    @test keys(r) == [:x8, :x5, :x1, :x3]
    r[:] .= 0.0
    r[1:2] .= 2.0
    @test values(r) == (2.0, 2.0, 0.0, 0.0)
    @test collect(pairs(r)) == [:x8 => 2.0, :x5 => 2.0, :x1 => 0.0, :x3 => 0.0]

    r = deepcopy(ref_df)[1, :]
    @test map(identity, r[1:3]) == (a = 1, b = 2.0, c = "A")
    @test map((a, b) -> (a, b), r[1:3], r[1:3]) == (a = (1, 1), b = (2.0, 2.0), c = ("A", "A"))
    @test get(r, 1, 100) == 1
    @test get(r, :a, 100) == 1
    @test get(r, 10, 100) == 100
    @test get(r, :z, 100) == 100
    @test get(() -> 100, r, 1) == 1
    @test get(() -> 100, r, :a) == 1
    @test get(() -> 100, r, 10) == 100
    @test get(() -> 100, r, :z) == 100
end

@testset "convert, copy and merge" begin
    df = DataFrame(a=[1, missing, missing], b=[2.0, 3.0, 0.0])
    dfr = DataFrameRow(df, 1, :)
    nt = first(Tables.namedtupleiterator(df))
    @test copy(dfr) === nt === NamedTuple{(:a, :b), Tuple{Union{Missing, Int}, Float64}}((1, 2.0))
    @test sum(skipmissing(copy(df[3, :]))) == 0
    @test Vector(dfr)::Vector{Union{Float64, Missing}} == [1.0, 2.0]
    @test Vector{Int}(dfr)::Vector{Int} == [1, 2]

    @test Array(dfr)::Vector{Union{Float64, Missing}} == [1.0, 2.0]
    @test Array{Int}(dfr)::Vector{Int} == [1, 2]

    df = ref_df[:, 1:3]
    @test copy(DataFrameRow(df, 1, :)) == (a=1, b=2.0, c="A")
    @test copy(DataFrameRow(df, 2, :)) ≅ (a=2, b=missing, c="B")

    dfr = DataFrame(a=1, b=2)[1, :]
    dfr2 = DataFrame(c=3, d=4)[1, :]
    @test NamedTuple(dfr) == (a=1, b=2)
    @test convert(NamedTuple, dfr) == (a=1, b=2)
    @test merge(dfr) == (a=1, b=2)
    @test merge(dfr, (c=3, d=4)) == (a=1, b=2, c=3, d=4)
    @test merge((c=3, d=4), dfr) == (c=3, d=4, a=1, b=2)
    @test merge(dfr, dfr2) == (a=1, b=2, c=3, d=4)
    @test merge(dfr, pairs((c=3, d=4))) == (a=1, b=2, c=3, d=4)
    @test merge(dfr, pairs(dfr2)) == (a=1, b=2, c=3, d=4)

    testkwargs(;kw...) = collect(kw)
    @test testkwargs(;dfr...) == [:a=>1, :b=>2]
    @test testkwargs(;dfr..., dfr2...) == [:a=>1, :b=>2, :c=>3, :d=>4]
    @test testkwargs(;x=0, dfr..., dfr2..., z=5) == [:x=>0, :a=>1, :b=>2, :c=>3, :d=>4, :z=>5]
end

@testset "parent and parentindices" begin
    df = ref_df[:, 1:3]

    @test parent(df[2, :]) === df
    @test parentindices(df[2, []]) == (2, Int[])
    @test parentindices(df[2, :]) == (2, Base.OneTo(3))
    @test parentindices(df[2, r""]) == (2, [1, 2, 3])
    @test parentindices(df[2, r"[ab]"]) == (2, [1, 2])
    @test parentindices(df[2, r"x"]) == (2, Int[])
    @test parent(df[1, 1:3]) === df
    @test parentindices(df[1, [3, 2]]) == (1, [3, 2])
    sdf = view(df, [4, 3], [:c, :a])
    @test parent(sdf[2, :]) === df
    @test parentindices(sdf[2, :]) == (3, [3, 1])
    @test parentindices(sdf[2, r""]) == (3, [3, 1])
    @test parentindices(sdf[2, r"a"]) == (3, [1])
    @test parentindices(sdf[2, r"x"]) == (3, Int[])
    @test parent(sdf[1, 1:2]) === df
    @test_throws ArgumentError parentindices(sdf[1, [2, 2]])
    @test parent(df[2, r""]) === df
    @test parent(df[2, r"a"]) === df
    @test parent(df[2, r"x"]) === df
    @test parentindices(df[2, :]) == (2, Base.OneTo(3))
    @test parentindices(df[2, r""]) == (2, [1, 2, 3])
    @test parentindices(df[2, r"a"]) == (2, [1])
    @test parentindices(df[2, r"x"]) == (2, Int[])
    @test parent(df[1, 1:3]) === df
    @test parentindices(df[1, [3, 2]]) == (1, [3, 2])
    sdf = view(df, [4, 3], [:c, :a])
    @test parent(sdf[2, :]) === df
    @test parent(sdf[2, r""]) === df
    @test parent(sdf[2, r"a"]) === df
    @test parent(sdf[2, r"x"]) === df
    @test parentindices(sdf[2, :]) == (3, [3, 1])
    @test parentindices(sdf[2, r""]) == (3, [3, 1])
    @test parentindices(sdf[2, r"a"]) == (3, [1])
    @test parentindices(sdf[2, r"x"]) == (3, Int[])
    @test parent(sdf[1, 1:2]) === df
    @test_throws ArgumentError parentindices(sdf[1, [2, 2]])
end

@testset "iteration and collect" begin
    ref = ["a", "b", "c"]
    df = DataFrame(permutedims(ref), :auto)
    dfr = df[1, :]
    @test Base.IteratorEltype(dfr) == Base.EltypeUnknown()
    @test collect(dfr) == ref
    @test eltype(collect(dfr)) === String
    for (v1, v2) in zip(ref, dfr)
        @test v1 == v2
    end
    for (i, v) in enumerate(dfr)
        @test v == ref[i]
    end
    dfr = DataFrame(a=1, b=true, c=1.0)[1, :]
    @test eltype(collect(dfr)) === Real
end

@testset "duplicate column" begin
    df = DataFrame([11:16 21:26 31:36 41:46], :auto)
    sdf = view(df, [3, 1, 4], [3, 1, 4])
    @test_throws ArgumentError df[2, [2, 2, 2]]
    @test_throws ArgumentError sdf[2, [2, 2, 2]]
end

@testset "conversion and push!" begin
    buf = IOBuffer()
    sl = SimpleLogger(buf)

    df = DataFrame(x=1, y=2)

    @test df == DataFrame(df[1, :])
    @test df[1:1, [2, 1]] == DataFrame(df[1, [2, 1]])
    @test df[1:1, 1:1] == DataFrame(df[1, 1:1])
    @test_throws ArgumentError DataFrame(df[1, [1, 1]])

    @test_throws ArgumentError push!(df, df[1, 1:1])
    @test df == DataFrame(x=1, y=2)

    with_logger(sl) do
        @test_throws ArgumentError push!(df, df[1, [2, 2]])
    end
    @test df == DataFrame(x=1, y=2)

    @test_throws ArgumentError push!(df, df[1, [2, 1]], cols=:orderequal)
    @test df == DataFrame(x=1, y=2)

    @test push!(df, df[1, :]) == DataFrame(x=[1, 1], y=[2, 2])
    @test push!(df, df[1, [2, 1]], cols=:setequal) == DataFrame(x=[1, 1, 1], y=[2, 2, 2])
    @test_throws ArgumentError push!(df, df[1, [2, 1]], cols=:setequals)

    push!(df, df[1, [2, 1]], cols=:intersect)
    @test df == DataFrame(x=[1, 1, 1, 1], y=[2, 2, 2, 2])

    df2 = DataFrame()
    @test push!(df2, df[1, :]) === df2
    @test df2 == df[1:1, :]
end

@testset "show" begin
    function capture_stdout(f::Function)
        oldstdout = stdout
        rd, wr = redirect_stdout()
        f()
        redirect_stdout(oldstdout)
        size = displaysize(rd)
        close(wr)
        str = read(rd, String)
        close(rd)
        str, size
    end

    df = DataFrame(a=nothing, b=1)

    @test sprint(show, DataFrameRow(df, 1, :)) == """
        DataFrameRow
         Row │ a        b
             │ Nothing  $(Int)
        ─────┼────────────────
           1 │              1"""


    df = DataFrame(a=1:3, b=["a", "b", "c"], c=Int64[1, 0, 1])
    dfr = df[2, 2:3]

    @test sprint(show, dfr) == """
        DataFrameRow
         Row │ b       c
             │ String  Int64
        ─────┼───────────────
           2 │ b           0"""

    # Test two-argument show
    str1, size = capture_stdout() do
        show(dfr)
    end
    io = IOContext(IOBuffer(), :limit=>true, :displaysize=>size)
    show(io, dfr)
    str2 = String(take!(io.io))
    @test str1 == str2

    # Test error when invalid keyword arguments are passed in text backend.
    @test_throws ArgumentError show(stdout, dfr, max_column_width="100px")
    @test_throws ArgumentError show(stdout, MIME("text/plain"), dfr, max_column_width="100px")

    str = sprint(show, "text/html", dfr)
    @test str == "<div>" *
                 "<div style = \"float: left;\">" *
                 "<span>DataFrameRow (2 columns)</span>" *
                 "</div>" *
                 "<div style = \"clear: both;\">" *
                 "</div>" *
                 "</div>" *
                 "<div class = \"data-frame\" style = \"overflow-x: scroll;\">" *
                 "<table class = \"data-frame\" style = \"margin-bottom: 6px;\">" *
                 "<thead>" *
                 "<tr class = \"header\">" *
                 "<th class = \"rowLabel\" style = \"font-weight: bold; text-align: right;\">Row</th>" *
                 "<th style = \"text-align: left;\">b</th>" *
                 "<th style = \"text-align: left;\">c</th>" *
                 "</tr>" *
                 "<tr class = \"subheader headerLastRow\">" *
                 "<th class = \"rowLabel\" style = \"font-weight: bold; text-align: right;\">" *
                 "</th>" *
                 "<th title = \"String\" style = \"text-align: left;\">String</th>" *
                 "<th title = \"Int64\" style = \"text-align: left;\">Int64</th>" *
                 "</tr>" *
                 "</thead>" *
                 "<tbody>" *
                 "<tr>" *
                 "<td class = \"rowLabel\" style = \"font-weight: bold; text-align: right;\">2</td>" *
                 "<td style = \"text-align: left;\">b</td>" *
                 "<td style = \"text-align: right;\">0</td>" *
                 "</tr>" *
                 "</tbody>" *
                 "</table>" *
                 "</div>"

    @test sprint(show, "text/latex", dfr) == """
        \\begin{tabular}{r|cc}
        \t& b & c\\\\
        \t\\hline
        \t& String & Int64\\\\
        \t\\hline
        \t2 & b & 0 \\\\
        \\end{tabular}
        """

    @test sprint(show, "text/csv", dfr) == """
        \"b\",\"c\"
        \"b\",0
        """

    @test sprint(show, "text/tab-separated-values", dfr) == """
        \"b\"\t\"c\"
        \"b\"\t0
        """

    io = IOBuffer()
    show(io, dfr, eltypes=false)
    str = String(take!(io))
    @test str == """
    DataFrameRow
     Row │ b  c
    ─────┼──────
       2 │ b  0"""

    io = IOBuffer()
    show(io, MIME("text/plain"), dfr, eltypes=false)
    str = String(take!(io))
    @test str == """
    DataFrameRow
     Row │ b  c
    ─────┼──────
       2 │ b  0"""

    io = IOBuffer()
    show(io, MIME("text/html"), dfr, eltypes=false)
    str = String(take!(io))
    @test str == "<div>" *
                 "<div style = \"float: left;\">" *
                 "<span>DataFrameRow (2 columns)</span>" *
                 "</div>" *
                 "<div style = \"clear: both;\">" *
                 "</div>" *
                 "</div>" *
                 "<div class = \"data-frame\" style = \"overflow-x: scroll;\">" *
                 "<table class = \"data-frame\" style = \"margin-bottom: 6px;\">" *
                 "<thead>" *
                 "<tr class = \"header headerLastRow\">" *
                 "<th class = \"rowLabel\" style = \"font-weight: bold; text-align: right;\">Row</th>" *
                 "<th style = \"text-align: left;\">b</th>" *
                 "<th style = \"text-align: left;\">c</th>" *
                 "</tr>" *
                 "</thead>" *
                 "<tbody>" *
                 "<tr>" *
                 "<td class = \"rowLabel\" style = \"font-weight: bold; text-align: right;\">2</td>" *
                 "<td style = \"text-align: left;\">b</td>" *
                 "<td style = \"text-align: right;\">0</td>" *
                 "</tr>" *
                 "</tbody>" *
                 "</table>" *
                 "</div>"

    io = IOBuffer()
    show(io, MIME("text/latex"), dfr, eltypes=false)
    str = String(take!(io))
    @test str == "\\begin{tabular}{r|cc}\n\t& b & c\\\\\n\t\\hline\n\t2 & b & 0 \\\\\n\\end{tabular}\n"
end

@testset "check Vector type" begin
    df = DataFrame(a=1, b=1.0, c=1)
    dfr = df[1, [1, 3]]
    @test eltype(Vector(dfr)) == Int
end

@testset "DataFrameRow" begin
    df = DataFrame(A=Vector{Union{Int, Missing}}(1:2), B=Vector{Union{Int, Missing}}(2:3))
    row = DataFrameRow(df, 1, :)

    row[:A] = 100
    @test df[1, :A] == 100

    row[1] = 101
    @test df[1, :A] == 101
end

@testset "rownumber" begin
    df = DataFrame(reshape(1:12, 3, 4), :auto)
    dfr = df[2, :]
    @test rownumber(dfr) == 2
    @test parentindices(dfr) == (2, 1:4)
    @test parent(dfr) === df

    dfr = @view df[2, :]
    @test rownumber(dfr) == 2
    @test parentindices(dfr) == (2, 1:4)
    @test parent(dfr) === df

    dfr = dfr[1:2]
    @test rownumber(dfr) == 2
    @test parentindices(dfr) == (2, 1:2)
    @test parent(dfr) === df

    dfr = @view dfr[1:2]
    @test rownumber(dfr) == 2
    @test parentindices(dfr) == (2, 1:2)
    @test parent(dfr) === df

    for (i, r) in enumerate(eachrow(df))
        @test rownumber(r) == i
        @test parentindices(r) == (i, 1:4)
        @test parent(r) === df
    end

    dfv = @view df[2:3, 1:3]
    dfrv = dfv[2, :]
    @test rownumber(dfrv) == 2
    @test parentindices(dfrv) == (3, 1:3)
    @test parent(dfrv) == df

    dfrv = @view dfv[2, :]
    @test rownumber(dfrv) == 2
    @test parentindices(dfrv) == (3, 1:3)
    @test parent(dfrv) == df

    dfrv = dfrv[1:2]
    @test rownumber(dfrv) == 2
    @test parentindices(dfrv) == (3, 1:2)
    @test parent(dfrv) === df

    dfrv = @view dfrv[1:2]
    @test rownumber(dfrv) == 2
    @test parentindices(dfrv) == (3, 1:2)
    @test parent(dfrv) === df

    for (i, r) in enumerate(eachrow(dfv))
        @test rownumber(r) == i
        @test parentindices(r) == (i + 1, 1:3)
        @test parent(r) === df
    end
end

@testset "broadcasting" begin
    r = DataFrame(a=1)[1, :]
    @test_throws ArgumentError r .+ 1
end

@testset "comparison tests: DataFrameRow, NamedTuple and GroupKey" begin
    df = DataFrame(a=[1, 2], b=[missing, 3])
    dfr = [df[1, :], df[2, :], df[1, 1:1], df[2, 1:1]]
    nt = NamedTuple.(dfr)
    gk = [keys(groupby(df, [:a, :b])); keys(groupby(df, :a))]

    for l in (dfr[1], nt[1], gk[1]), r in (dfr[1], nt[1], gk[1])
        @test ismissing(l == r)
        @test ismissing(l == (a=1, b=2))
        @test l ≅ r
        @test l ≇ (a=1, b=2)
        # work around https://github.com/JuliaLang/julia/pull/40147
        if !(l isa NamedTuple && r isa NamedTuple)
            @test ismissing(l < r)
        end
        @test !isless(l, r)
    end

    for i in 2:4, l in (dfr, nt, gk), r in (dfr, nt, gk)
        @test l[i] == r[i]
        @test l[1] != l[i]
        @test l[1] != r[i]
        @test l[i] ≅ r[i]
        @test l[1] ≇ l[i]
        @test l[1] ≇ r[i]

        @test !(l[i] < r[i])
        @test !isless(l[i], r[i])

        if i > 2
            if l[1] isa NamedTuple && r[i] isa NamedTuple
                @test_throws MethodError l[1] < r[i]
                @test_throws MethodError isless(l[1], r[i])
            else
                @test_throws ArgumentError l[1] < r[i]
                @test_throws ArgumentError isless(l[1], r[i])
            end
        end
    end

    for l in (dfr, nt, gk), r in (dfr, nt, gk)
        @test l[1] < r[2]
        @test isless(l[1], r[2])
        @test !(l[2] < r[1])
        @test !isless(l[2], r[1])

        @test l[3] < r[4]
        @test isless(l[3], r[4])
        @test !(l[4] < r[3])
        @test !isless(l[4], r[3])
    end

    @test !(dfr[1] == (x=1, b=missing))
    @test !(gk[1] == (x=1, b=missing))
    @test !(dfr[1] ≅ (x=1, b=missing))
    @test !(gk[1] ≅ (x=1, b=missing))

    @test_throws ArgumentError dfr[1] < (x=1, b=missing)
    @test_throws ArgumentError gk[1] < (x=1, b=missing)
    @test_throws ArgumentError isless(dfr[1], (x=1, b=missing))
    @test_throws ArgumentError isless(gk[1], (x=1, b=missing))

    df2 = DataFrame(a=1, b=missing)
    df3 = DataFrame(a=2, b=1)
    dfr2 = df2[1, :]
    dfr3 = df3[1, :]
    nt2 = NamedTuple(dfr2)
    nt3 = NamedTuple(dfr3)
    gk2 = keys(groupby(df2, [:a, :b]))[1]
    gk3 = keys(groupby(df3, [:a, :b]))[1]

    for a in (dfr2, nt2, gk2), b in (dfr3, nt3, gk3)
        @test !(a == b)
        @test !(a ≅ b)
        @test a < b
        @test isless(a, b)
    end
end

end # module
