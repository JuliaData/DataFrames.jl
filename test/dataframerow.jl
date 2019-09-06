module TestDataFrameRow

using Test, DataFrames, Random
const ≅ = isequal
const ≇ = !isequal

ref_df = DataFrame(a=Union{Int, Missing}[1, 2, 3, 1, 2, 2],
                   b=[2.0, missing, 1.2, 2.0, missing, missing],
                   c=["A", "B", "C", "A", "B", missing],
                   d=CategoricalArray([:A, missing, :C, :A, missing, :C]))

@testset "constructors" begin
    df = deepcopy(ref_df)
    sdf = view(df, [5, 3], [3, 1, 2])

    @test names(DataFrameRow(df, 1, :)) == [:a, :b, :c, :d]
    @test DataFrameRow(df, 1) == DataFrameRow(df, 1, :)
    @test DataFrameRow(df, 1) == DataFrameRow(df, 1, r"")
    @test names(DataFrameRow(df, 3, [3, 2])) == [:c, :b]
    @test copy(DataFrameRow(df, 3, [3, 2])) == (c = "C", b = 1.2)
    @test copy(DataFrameRow(df, 3, r"[bc]")) == (b = 1.2, c = "C")
    @test copy(DataFrameRow(sdf, 2, [3, 2])) == (b = 1.2, a = 3)
    @test copy(DataFrameRow(sdf, 2, r"[bc]")) == (c = "C", b = 1.2)
    @test copy(DataFrameRow(sdf, 2, :)) == (c = "C", a = 3, b = 1.2)
    @test DataFrameRow(sdf, 2) == DataFrameRow(sdf, 2, :)
    @test DataFrameRow(df, 3, [3, 2]) == df[3, [3, 2]] == view(df, 3, [3, 2])
    @test DataFrameRow(df, 3, r"[bc]") == df[3, [2, 3]] == df[3, r"[bc]"]
    @test DataFrameRow(sdf, 2, [3, 2]) == sdf[2, [3, 2]] == view(sdf, 2, [3, 2])
    @test DataFrameRow(sdf, 2, r"[bc]") == sdf[2, [1, 3]] == sdf[2, r"[bc]"]
    @test DataFrameRow(sdf, 2, :) == sdf[2, :] == view(sdf, 2, :)
    @test DataFrameRow(df, 3, 2:3) === df[3, 2:3]
    @test view(df, 3, 2:3) === df[3, 2:3]
    @test_throws ArgumentError DataFrameRow(df, 1, :a)
    @test_throws ArgumentError DataFrameRow(df, 1, 1)
    @test_throws BoundsError DataFrameRow(df, 1, 1:10)
    @test_throws BoundsError DataFrameRow(df, 1, [1:10;])
    @test_throws BoundsError DataFrameRow(df, 100, 1:2)
    @test_throws BoundsError DataFrameRow(df, 100, [1:2;])
    @test_throws BoundsError DataFrameRow(df, 100, :)
    @test_throws BoundsError DataFrameRow(df, 100)
    @test_throws ArgumentError DataFrameRow(df, true, 1:2)
    @test_throws ArgumentError DataFrameRow(df, true)
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
    @test copy(r[[:c,:a]]) == (c = "BB", a = 2)
    @test copy(r[r"[ac]"]) == (a = 2, c = "BB")
    @test copy(r[r"x"]) == NamedTuple()
    @test copy(r[2:1]) == NamedTuple()
    @test copy(r[Symbol[]]) == NamedTuple()
    @test copy(view(r, [:c,:a])) == (c = "BB", a = 2)
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
    @test copy(r[[:c,:b]]) == (c = "CC", b = 1.2)
    @test copy(view(r, [:c,:b])) == (c = "CC", b = 1.2)
    @test copy(view(r, [:c,:b])) == (c = "CC", b = 1.2)
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
                    5 6 7 8])
    r = df[1, r"[1-3]"]
    @test names(r) == [:x1, :x2, :x3]
    r[:] .= 10
    @test df == DataFrame([10 10 10 4
                            5  6  7 8])
    r[r"[2-3]"] .= 20
    @test df == DataFrame([10 20 20 4
                            5  6  7 8])
end

@testset "equality" begin
    df = deepcopy(ref_df)
    df2 = DataFrame(a = [1, 2, 3])

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
    @test DataFrameRow(df, 1, [1,2]) != DataFrameRow(dc_df, 1, [2,1])
    dc_df[1,1] = 2
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
end

@testset "hashing" begin
    df = deepcopy(ref_df)

    @test hash(DataFrameRow(df, 1, :)) != hash(DataFrameRow(df, 2, :))
    @test hash(DataFrameRow(df, 1, :)) != hash(DataFrameRow(df, 3, :))
    @test hash(DataFrameRow(df, 1, :)) == hash(DataFrameRow(df, 4, :))
    @test hash(DataFrameRow(df, 2, :)) == hash(DataFrameRow(df, 5, :))
    @test hash(DataFrameRow(df, 2, :)) != hash(DataFrameRow(df, 6, :))

    # check that hashrows() function generates the same hashes as DataFrameRow
    df_rowhashes, _ = DataFrames.hashrows(Tuple(eachcol(df)), false)
    @test df_rowhashes == [hash(dr) for dr in eachrow(df)]
end

@testset "grouping" begin
    # test RowGroupDict
    Random.seed!(1234)
    df1 = DataFrame(d1=rand(1:2, 1000))
    df2 = DataFrame(d1=[2,3])

    # test_group("group_rows")
    gd = DataFrames.group_rows(df1)
    @test length(unique(gd.groups)) == 2

    # getting groups for the rows of the other frames
    @test length(gd[DataFrameRow(df2, 1, :)]) > 0
    @test_throws KeyError gd[DataFrameRow(df2, 2, :)]
    @test isempty(DataFrames.findrows(gd, df2, (gd.df[!, 1],), (df2[!, 1],), 2))

    # grouping empty frame
    gd = DataFrames.group_rows(DataFrame(x=Int[]))
    @test length(unique(gd.groups)) == 0

    # grouping single row
    gd = DataFrames.group_rows(df1[1:1,:])
    @test length(unique(gd.groups)) == 1
end

@testset "getproperty, setproperty! and propertynames" begin
    df = deepcopy(ref_df)

    r = DataFrameRow(df, 1, :)
    @test Base.propertynames(r) == names(df)
    @test r.a === 1
    @test r.b === 2.0
    @test copy(r[[:a,:b]]) === (a=1, b=2.0)

    r.a = 2
    @test r.a === 2
    r.b = 1
    @test r.b === 1.0
end

@testset "keys, values and iteration, size" begin
    df = deepcopy(ref_df)
    r = DataFrameRow(df, 1, :)

    @test keys(r) == Tuple(names(df))
    @test values(r) == (df[1, 1], df[1, 2], df[1, 3], df[1, 4])
    @test collect(pairs(r)) == [:a=>df[1, 1], :b=>df[1, 2], :c=>df[1, 3], :d=>df[1, 4]]

    @test haskey(r, :a)
    @test haskey(r, 1)
    @test !haskey(r, :zzz)
    @test !haskey(r, 0)
    @test !haskey(r, 1000)
    @test_throws ArgumentError haskey(r, true)

    x = DataFrame(ones(5,4))
    dfr = view(x, 2, 2:3)
    @test names(dfr) == names(x)[2:3]
    dfr = view(x, 2, [4,2])
    @test names(dfr) == names(x)[[4,2]]

    x = DataFrame(ones(10,10))
    r = x[3, [8, 5, 1, 3]]
    @test length(r) == 4
    @test lastindex(r) == 4
    @test ndims(r) == 1
    @test ndims(typeof(r)) == 1
    @test size(r) == (4,)
    @test size(r, 1) == 4
    @test_throws BoundsError size(r, 2)
    @test keys(r) == (:x8, :x5, :x1, :x3)
    r[:] .= 0.0
    r[1:2] .= 2.0
    @test values(r) == (2.0, 2.0, 0.0, 0.0)
    @test collect(pairs(r)) == [:x8 => 2.0, :x5 => 2.0, :x1 => 0.0, :x3 => 0.0]

    r = deepcopy(ref_df)[1, :]
    @test map(identity, r[1:3]) == (a = 1, b = 2.0, c = "A")
    @test map((a,b) -> (a,b), r[1:3], r[1:3]) == (a = (1, 1), b = (2.0, 2.0), c = ("A", "A"))
    @test get(r, 1, 100) == 1
    @test get(r, :a, 100) == 1
    @test get(r, 10, 100) == 100
    @test get(r, :z, 100) == 100
    @test get(() -> 100,r, 1) == 1
    @test get(() -> 100,r, :a) == 1
    @test get(() -> 100,r, 10) == 100
    @test get(() -> 100,r, :z) == 100
end

@testset "convert and copy" begin
    df = DataFrame(a=[1, missing], b=[2.0, 3.0])
    dfr = DataFrameRow(df, 1, :)

    @test convert(Vector, dfr)::Vector{Union{Float64, Missing}} == [1.0, 2.0]
    @test convert(Vector{Int}, dfr)::Vector{Int} == [1, 2]
    @test Vector(dfr)::Vector{Union{Float64, Missing}} == [1.0, 2.0]
    @test Vector{Int}(dfr)::Vector{Int} == [1, 2]

    df = ref_df[:, 1:3]
    @test copy(DataFrameRow(df, 1, :)) == (a = 1, b = 2.0, c = "A")
    @test copy(DataFrameRow(df, 2, :)) ≅ (a = 2, b = missing, c = "B")
end

@testset "parent and parentindices" begin
    df = ref_df[:, 1:3]

    @test parent(df[2, :]) === df
    @test parentindices(df[2, []]) == (2, Int[])
    @test parentindices(df[2, :]) == (2, Base.OneTo(3))
    @test parentindices(df[2, r""]) == (2, [1,2,3])
    @test parentindices(df[2, r"[ab]"]) == (2, [1,2])
    @test parentindices(df[2, r"x"]) == (2, Int[])
    @test parent(df[1, 1:3]) === df
    @test parentindices(df[1, [3,2]]) == (1, [3, 2])
    sdf = view(df, [4,3], [:c, :a])
    @test parent(sdf[2, :]) === df
    @test parentindices(sdf[2, :]) == (3, [3, 1])
    @test parentindices(sdf[2, r""]) == (3, [3, 1])
    @test parentindices(sdf[2, r"a"]) == (3, [1])
    @test parentindices(sdf[2, r"x"]) == (3, Int[])
    @test parent(sdf[1, 1:2]) === df
    @test parentindices(sdf[1, [2, 2]]) == (4, [1, 1])
    @test parent(df[2, r""]) === df
    @test parent(df[2, r"a"]) === df
    @test parent(df[2, r"x"]) === df
    @test parentindices(df[2, :]) == (2, Base.OneTo(3))
    @test parentindices(df[2, r""]) == (2, [1, 2, 3])
    @test parentindices(df[2, r"a"]) == (2, [1])
    @test parentindices(df[2, r"x"]) == (2, Int[])
    @test parent(df[1, 1:3]) === df
    @test parentindices(df[1, [3,2]]) == (1, [3, 2])
    sdf = view(df, [4,3], [:c, :a])
    @test parent(sdf[2, :]) === df
    @test parent(sdf[2, r""]) === df
    @test parent(sdf[2, r"a"]) === df
    @test parent(sdf[2, r"x"]) === df
    @test parentindices(sdf[2, :]) == (3, [3, 1])
    @test parentindices(sdf[2, r""]) == (3, [3, 1])
    @test parentindices(sdf[2, r"a"]) == (3, [1])
    @test parentindices(sdf[2, r"x"]) == (3, Int[])
    @test parent(sdf[1, 1:2]) === df
    @test parentindices(sdf[1, [2, 2]]) == (4, [1, 1])
end

@testset "iteration and collect" begin
    ref = ["a", "b", "c"]
    df = DataFrame(permutedims(ref))
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
    dfr = DataFrame(a=1, b=true, c=1.0)[1,:]
    @test eltype(collect(dfr)) === Real
end

@testset "duplicate column" begin
    df = DataFrame([11:16 21:26 31:36 41:46])
    sdf = view(df, [3,1,4], [3,1,4])
    dfr1 = df[2, [2,2,2]]
    dfr2 = sdf[2, [2,2,2]]
    @test names(dfr1) == fill(:x2, 3)
    @test names(dfr2) == fill(:x1, 3)
    @test values(dfr1) == (22, 22, 22)
    @test values(dfr2) == (11, 11, 11)
    @test dfr1.x2 == 22
    dfr1.x2 = 100
    @test values(dfr1) == (100, 100, 100)
    @test df[2, 2] == 100
    @test_throws ArgumentError dfr1.x1
    @test dfr2.x1 == 11
    dfr2.x1 = 200
    @test values(dfr2) == (200, 200, 200)
    @test df[1, 1] == 200
    @test_throws ArgumentError dfr2.x2
end

@testset "conversion and push!" begin
    df = DataFrame(x=1, y=2)

    @test df == DataFrame(df[1, :])
    @test df[1:1, [2,1]] == DataFrame(df[1, [2,1]])
    @test df[1:1, 1:1] == DataFrame(df[1, 1:1])
    @test_throws ArgumentError DataFrame(df[1, [1,1]])

    @test_throws ArgumentError push!(df, df[1, 1:1])
    @test df == DataFrame(x=1, y=2)

    @test_throws ArgumentError push!(df, df[1, [2,2]])
    @test df == DataFrame(x=1, y=2)

    @test_throws ArgumentError push!(df, df[1, [2,1,2]])
    @test df == DataFrame(x=1, y=2)

    @test push!(df, df[1, :]) == DataFrame(x=[1, 1], y=[2, 2])
    @test push!(df, df[1, [2,1]]) == DataFrame(x=[1, 1, 1], y=[2, 2, 2])

    push!(df, df[1, [2,1,2]], columns=:intersect)
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
        │ Row │ a       │ b     │
        │     │ Nothing │ $(Int) │
        ├─────┼─────────┼───────┤
        │ 1   │         │ 1     │"""


    df = DataFrame(a=1:3, b=["a", "b", "c"], c=Int64[1,0,1])
    dfr = df[2, 2:3]

    @test sprint(show, dfr) == """
        DataFrameRow
        │ Row │ b      │ c     │
        │     │ String │ Int64 │
        ├─────┼────────┼───────┤
        │ 2   │ b      │ 0     │"""

    # Test two-argument show
    str1, size = capture_stdout() do
        show(dfr)
    end
    io = IOContext(IOBuffer(), :limit=>true, :displaysize=>size)
    show(io, dfr)
    str2 = String(take!(io.io))
    @test str1 == str2

    @test sprint(show, "text/html", dfr) == "<p>DataFrameRow (2 columns)</p><table class=\"data-frame\">" *
                               "<thead><tr><th></th><th>b</th><th>c</th></tr>" *
                               "<tr><th></th><th>String</th><th>Int64</th></tr></thead>" *
                               "<tbody><tr><th>2</th>" *
                               "<td>b</td><td>0</td></tr></tbody></table>"

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
end

end # module
