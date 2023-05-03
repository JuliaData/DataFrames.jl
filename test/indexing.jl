module TestIndexing

using Test, DataFrames, Unicode, Random

@testset "getindex DataFrame" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)

    @test df[!, 1] == [1, 2, 3]
    @test df[!, 1] === eachcol(df)[1]
    @test df[!, :a] == [1, 2, 3]
    @test df[!, :a] === df[!, "a"] === eachcol(df)[1]
    @test df.a == [1, 2, 3]
    @test df.a === df."a" === eachcol(df)[1]

    for selector in [1:2, r"[ab]", Not(Not(r"[ab]")), Not(r"ab"), Not(3), Not(1:0), Not(1:2), :]
        dfx = df[!, selector]
        @test dfx == select(df, selector, copycols=false)
        @test dfx isa DataFrame
        @test dfx[!, 1] === df[!, names(dfx)[1]]
    end

    @test df[!, Not(1, 2)] == DataFrame(c=7:9)
    @test df[!, Not(1, 1, 2)] == DataFrame(c=7:9)
    @test df[!, Not([1, 1, 2])] == DataFrame(c=7:9)
    @test df[!, Not(:b, 1)] == DataFrame(c=7:9)
    @test df[!, Not(:b, :b, 1)] == DataFrame(c=7:9)
    @test df[!, Not("c", :a)] == DataFrame(b=4:6)
    @test df[!, Not("c", "c", :a)] == DataFrame(b=4:6)
    @test df[!, Not(:c, :c, :a)] == DataFrame(b=4:6)
    @test df[!, Not([:c, :c, :a])] == DataFrame(b=4:6)
    @test df[!, Not("c", "c", "a")] == DataFrame(b=4:6)
    @test df[!, Not(["c", "c", "a"])] == DataFrame(b=4:6)
    @test df[!, Not(:b, "c", :a)] == DataFrame()
    @test df[!, Not([1, 2], :b)] == DataFrame(c=7:9)
    @test df[!, Not([:c, :a], :b)] == DataFrame()
    @test df[!, Not([1, 2], 2)] == DataFrame(c=7:9)
    @test df[!, Not([1, 2], [1, 2])] == DataFrame(c=7:9)

    @test df[1, 1] == 1
    @test df[1, 1:2] isa DataFrameRow
    @test df[1, r"[ab]"] isa DataFrameRow
    @test df[1, Not(3)] isa DataFrameRow
    @test copy(df[1, 1:2]) == (a=1, b=4)
    @test copy(df[1, r"[ab]"]) == (a=1, b=4)
    @test copy(df[1, Not(Not(r"[ab]"))]) == (a=1, b=4)
    @test copy(df[1, Not(:c)]) == (a=1, b=4)
    @test df[1, :] isa DataFrameRow
    @test copy(df[1, :]) == (a=1, b=4, c=7)
    @test parent(df[1, :]) === df
    @test df[1, r""] isa DataFrameRow
    @test copy(df[1, r""]) == (a=1, b=4, c=7)
    @test parent(df[1, r""]) === df
    @test df[1, Not([])] isa DataFrameRow
    @test copy(df[1, Not([])]) == (a=1, b=4, c=7)
    @test parent(df[1, Not([])]) === df
    @test_throws ArgumentError df[true, 1]
    @test_throws ArgumentError df[true, 1:2]
    @test_throws BoundsError df[5, "a"]

    @test df[1:2, 1] == [1, 2]
    @test df[1:2, 1:2] == DataFrame(a=1:2, b=4:5)
    @test df[1:2, r"[ab]"] == DataFrame(a=1:2, b=4:5)
    @test df[1:2, Not([3])] == DataFrame(a=1:2, b=4:5)
    @test df[1:2, :] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test df[1:2, r""] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test df[1:2, Not(1:0)] == DataFrame(a=1:2, b=4:5, c=7:8)

    @test df[Not(Not(1:2)), 1] == [1, 2]
    @test df[Not(Not(1:2)), 1:2] == DataFrame(a=1:2, b=4:5)
    @test df[Not(Not(1:2)), r"[ab]"] == DataFrame(a=1:2, b=4:5)
    @test df[Not(Not(1:2)), Not([3])] == DataFrame(a=1:2, b=4:5)
    @test df[Not(Not(1:2)), :] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test df[Not(Not(1:2)), r""] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test df[Not(Not(1:2)), Not(1:0)] == DataFrame(a=1:2, b=4:5, c=7:8)

    @test df[:, 1] == [1, 2, 3]
    @test df[:, 1] !== df[!, 1]
    @test df[:, 1:2] == DataFrame(a=1:3, b=4:6)
    @test df[:, r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test df[:, Not(r"c")] == DataFrame(a=1:3, b=4:6)
    @test eachcol(df[:, 1:2])[1] !== df[!, 1]
    @test df[:, :] == df
    @test df[:, r""] == df
    @test df[:, Not(Not(r""))] == df
    @test eachcol(df[:, :])[1] !== df[!, 1]
    @test eachcol(df[:, r""])[1] !== df[!, 1]
    @test eachcol(df[:, Not([])])[1] !== df[!, 1]

    @test df[Not(Int[]), 1] == [1, 2, 3]
    @test df[Not(Int[]), 1] !== df[!, 1]
    @test df[Not(Int[]), 1:2] == DataFrame(a=1:3, b=4:6)
    @test df[Not(Int[]), r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test df[Not(Int[]), Not(r"c")] == DataFrame(a=1:3, b=4:6)
    @test eachcol(df[Not(Int[]), 1:2])[1] !== df[!, 1]
    @test df[Not(Int[]), :] == df
    @test df[Not(Int[]), r""] == df
    @test df[Not(Int[]), Not(Not(r""))] == df
    @test eachcol(df[Not(Int[]), :])[1] !== df[!, 1]
    @test eachcol(df[Not(Int[]), r""])[1] !== df[!, 1]
    @test eachcol(df[Not(Int[]), Not([])])[1] !== df[!, 1]
end

@testset "getindex df[!, col]" begin
    x = [1, 2, 3]
    df = DataFrame(x=x, copycols=false)
    @test df.x === df."x" === x
    @test df[!, :x] === df[!, "x"] === x
    @test df[!, 1] === x
    @test df[:, [:x]].x !== x
    @test df[:, :].x !== x
    @test df[:, r"x"].x !== x
    @test df[:, r""].x !== x
    @test df[:, Not(1:0)].x !== x
    @test df[!, [:x]].x === x
    @test df[!, :].x === x
    @test df[!, r"x"].x === x
    @test df[!, r""].x === x
    @test df[!, Not(1:0)].x === x
end

@testset "view DataFrame" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)

    @test view(df, !, 1) == [1, 2, 3]
    @test view(df, !, 1) isa SubArray
    @test view(df, !, :a) == [1, 2, 3]
    @test view(df, !, :a) isa SubArray
    @test view(df, !, "a") == [1, 2, 3]
    @test view(df, !, "a") isa SubArray

    for selector in [1:2, r"[ab]", Not(Not(r"[ab]")), Not(r"ab"), Not(3), Not(1:0), Not(1:2), :]
        dfx = @view df[!, selector]
        @test dfx == select(df, selector, copycols=false)
        @test dfx isa SubDataFrame
        @test parent(dfx[!, 1]) === df[!, names(dfx)[1]]
    end

    @test view(df, :, :) == df
    @test parent(view(df, :, :)) === df
    @test view(df, :, r"") isa SubDataFrame
    @test view(df, :, r"") == df
    @test parent(view(df, :, r"")) === df
    @test view(df, :, Not(1:0)) isa SubDataFrame
    @test view(df, :, Not(1:0)) == df
    @test parent(view(df, :, Not(1:0))) === df

    @test view(df, !, :) == df
    @test parent(view(df, !, :)) === df
    @test view(df, !, r"") isa SubDataFrame
    @test view(df, !, r"") == df
    @test parent(view(df, !, r"")) === df
    @test view(df, !, Not(1:0)) isa SubDataFrame
    @test view(df, !, Not(1:0)) == df
    @test parent(view(df, !, Not(1:0))) === df

    @test view(df, 1, 1) isa SubArray
    @test view(df, 1, 1)[] == 1
    @test view(df, 1, 1:2) isa DataFrameRow
    @test copy(view(df, 1, 1:2)) == (a=1, b=4)
    @test view(df, 1, r"[ab]") isa DataFrameRow
    @test copy(view(df, 1, r"[ab]")) == (a=1, b=4)
    @test view(df, 1, Not(Not(r"[ab]"))) isa DataFrameRow
    @test copy(view(df, 1, Not(Not(r"[ab]")))) == (a=1, b=4)
    @test view(df, 1, :) isa DataFrameRow
    @test copy(view(df, 1, :)) == (a=1, b=4, c=7)
    @test parent(view(df, 1, :)) === df
    @test view(df, 1, r"") isa DataFrameRow
    @test copy(view(df, 1, r"")) == (a=1, b=4, c=7)
    @test parent(view(df, 1, r"")) === df
    @test view(df, 1, Not(Symbol[])) isa DataFrameRow
    @test copy(view(df, 1, Not(Symbol[]))) == (a=1, b=4, c=7)
    @test parent(view(df, 1, Not(Symbol[]))) === df

    @test view(df, 1:2, 1) == [1, 2]
    @test view(df, 1:2, 1) isa SubArray
    @test view(df, 1:2, 1:2) isa SubDataFrame
    @test view(df, 1:2, 1:2) == DataFrame(a=1:2, b=4:5)
    @test view(df, 1:2, r"[ab]") isa SubDataFrame
    @test view(df, 1:2, r"[ab]") == DataFrame(a=1:2, b=4:5)
    @test view(df, 1:2, Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(df, 1:2, Not(Not(r"[ab]"))) == DataFrame(a=1:2, b=4:5)
    @test view(df, 1:2, :) isa SubDataFrame
    @test view(df, 1:2, :) == df[1:2, :]
    @test view(df, 1:2, r"") isa SubDataFrame
    @test view(df, 1:2, r"") == df[1:2, :]
    @test view(df, 1:2, Not(Int[])) isa SubDataFrame
    @test view(df, 1:2, Not(Int[])) == df[1:2, :]
    @test parent(view(df, 1:2, :)) === df
    @test parent(view(df, 1:2, r"")) === df
    @test parent(view(df, 1:2, Not(1:0))) === df

    @test view(df, Not(Not(1:2)), 1) == [1, 2]
    @test view(df, Not(Not(1:2)), 1) isa SubArray
    @test view(df, Not(Not(1:2)), 1:2) isa SubDataFrame
    @test view(df, Not(Not(1:2)), 1:2) == DataFrame(a=1:2, b=4:5)
    @test view(df, Not(Not(1:2)), r"[ab]") isa SubDataFrame
    @test view(df, Not(Not(1:2)), r"[ab]") == DataFrame(a=1:2, b=4:5)
    @test view(df, Not(Not(1:2)), Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(df, Not(Not(1:2)), Not(Not(r"[ab]"))) == DataFrame(a=1:2, b=4:5)
    @test view(df, Not(Not(1:2)), :) isa SubDataFrame
    @test view(df, Not(Not(1:2)), :) == df[1:2, :]
    @test view(df, Not(Not(1:2)), r"") isa SubDataFrame
    @test view(df, Not(Not(1:2)), r"") == df[1:2, :]
    @test view(df, Not(Not(1:2)), Not(Int[])) isa SubDataFrame
    @test view(df, Not(Not(1:2)), Not(Int[])) == df[1:2, :]
    @test parent(view(df, Not(Not(1:2)), :)) === df
    @test parent(view(df, Not(Not(1:2)), r"")) === df
    @test parent(view(df, Not(Not(1:2)), Not(1:0))) === df

    @test view(df, :, 1) == [1, 2, 3]
    @test view(df, :, 1) isa SubArray
    @test view(df, :, 1:2) isa SubDataFrame
    @test view(df, :, 1:2) == DataFrame(a=1:3, b=4:6)
    @test view(df, :, r"[ab]") isa SubDataFrame
    @test view(df, :, r"[ab]") == DataFrame(a=1:3, b=4:6)
    @test view(df, :, Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(df, :, Not(Not(r"[ab]"))) == DataFrame(a=1:3, b=4:6)
    @test view(df, :, :) isa SubDataFrame
    @test view(df, :, :) == df[:, :]
    @test view(df, :, r"") isa SubDataFrame
    @test view(df, :, r"") == df[:, :]
    @test view(df, :, Not(1:0)) isa SubDataFrame
    @test view(df, :, Not(1:0)) == df[:, :]
    @test parent(view(df, :, :)) === df
    @test parent(view(df, :, r"")) === df
    @test parent(view(df, :, Not(1:0))) === df

    @test view(df, Not(1:0), 1) == [1, 2, 3]
    @test view(df, Not(1:0), 1) isa SubArray
    @test view(df, Not(1:0), 1:2) isa SubDataFrame
    @test view(df, Not(1:0), 1:2) == DataFrame(a=1:3, b=4:6)
    @test view(df, Not(1:0), r"[ab]") isa SubDataFrame
    @test view(df, Not(1:0), r"[ab]") == DataFrame(a=1:3, b=4:6)
    @test view(df, Not(1:0), Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(df, Not(1:0), Not(Not(r"[ab]"))) == DataFrame(a=1:3, b=4:6)
    @test view(df, Not(1:0), :) isa SubDataFrame
    @test view(df, Not(1:0), :) == df[:, :]
    @test view(df, Not(1:0), r"") isa SubDataFrame
    @test view(df, Not(1:0), r"") == df[:, :]
    @test view(df, Not(1:0), Not(1:0)) isa SubDataFrame
    @test view(df, Not(1:0), Not(1:0)) == df[:, :]
    @test parent(view(df, Not(1:0), :)) === df
    @test parent(view(df, Not(1:0), r"")) === df
    @test parent(view(df, Not(1:0), Not(1:0))) === df
end

@testset "getindex SubDataFrame" begin
    df = DataFrame(x=-1:3, a=0:4, b=3:7, c=6:10, d=9:13)
    sdf = view(df, 2:4, 2:4)

    @test sdf[!, 1] == [1, 2, 3]
    @test sdf[!, 1] isa SubArray
    @test sdf[!, :a] == [1, 2, 3]
    @test sdf[!, :a] isa SubArray
    @test sdf[!, "a"] == [1, 2, 3]
    @test sdf[!, "a"] isa SubArray
    @test sdf.a == [1, 2, 3]
    @test sdf.a isa SubArray
    @test sdf.a === sdf."a"

    for selector in [1:2, r"[ab]", Not(Not(r"[ab]")), Not(r"ab"), Not(3), Not(1:0), Not(1:2), :]
        dfx = @view sdf[!, selector]
        @test dfx == select(sdf, selector, copycols=false)
        @test dfx isa SubDataFrame
        @test parent(dfx[!, 1]) === df[!, names(dfx)[1]]
    end

    @test sdf[:, 1:2] == DataFrame(a=1:3, b=4:6)
    @test sdf[:, 1:2] isa DataFrame
    @test sdf[:, r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test sdf[:, r"[ab]"] isa DataFrame
    @test sdf[:, Not(Not(r"[ab]"))] == DataFrame(a=1:3, b=4:6)
    @test sdf[:, Not(Not(r"[ab]"))] isa DataFrame
    @test sdf[:, :] == df[2:4, 2:4]
    @test sdf[:, :] isa DataFrame
    @test sdf[:, r""] == df[2:4, 2:4]
    @test sdf[:, r""] isa DataFrame
    @test sdf[:, Not(1:0)] == df[2:4, 2:4]
    @test sdf[:, Not(1:0)] isa DataFrame

    @test sdf[!, 1:2] == DataFrame(a=1:3, b=4:6)
    @test sdf[!, 1:2] isa SubDataFrame
    @test sdf[!, r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test sdf[!, r"[ab]"] isa SubDataFrame
    @test sdf[!, Not(Not(r"[ab]"))] == DataFrame(a=1:3, b=4:6)
    @test sdf[!, Not(Not(r"[ab]"))] isa SubDataFrame
    @test sdf[!, :] == df[2:4, 2:4]
    @test sdf[!, :] isa SubDataFrame
    @test sdf[!, r""] == df[2:4, 2:4]
    @test sdf[!, r""] isa SubDataFrame
    @test sdf[!, Not(1:0)] == df[2:4, 2:4]
    @test sdf[!, Not(1:0)] isa SubDataFrame
    @test parent(sdf[!, :]) === parent(sdf)
    @test parent(sdf[!, r""]) === parent(sdf)
    @test parent(sdf[!, Not([])]) === parent(sdf)

    @test sdf[1, 1] == 1
    @test sdf[1, 1:2] isa DataFrameRow
    @test copy(sdf[1, 1:2]) == (a=1, b=4)
    @test sdf[1, r"[ab]"] isa DataFrameRow
    @test copy(sdf[1, r"[ab]"]) == (a=1, b=4)
    @test sdf[1, Not(Not(r"[ab]"))] isa DataFrameRow
    @test copy(sdf[1, Not(Not(r"[ab]"))]) == (a=1, b=4)
    @test sdf[1, :] isa DataFrameRow
    @test copy(sdf[1, :]) == (a=1, b=4, c=7)
    @test sdf[1, r""] isa DataFrameRow
    @test copy(sdf[1, r""]) == (a=1, b=4, c=7)
    @test sdf[1, Not(1:0)] isa DataFrameRow
    @test copy(sdf[1, Not(1:0)]) == (a=1, b=4, c=7)
    @test parent(sdf[1, :]) === parent(sdf)
    @test parent(sdf[1, r""]) === parent(sdf)
    @test parent(sdf[1, Not(1:0)]) === parent(sdf)
    @test_throws ArgumentError sdf[true, 1]
    @test_throws ArgumentError sdf[true, 1:2]

    @test sdf[1:2, 1] == [1, 2]
    @test sdf[1:2, 1] isa Vector
    @test sdf[1:2, 1:2] == DataFrame(a=1:2, b=4:5)
    @test sdf[1:2, 1:2] isa DataFrame
    @test sdf[1:2, r"[ab]"] == DataFrame(a=1:2, b=4:5)
    @test sdf[1:2, r"[ab]"] isa DataFrame
    @test sdf[1:2, Not(Not(r"[ab]"))] == DataFrame(a=1:2, b=4:5)
    @test sdf[1:2, Not(Not(r"[ab]"))] isa DataFrame
    @test sdf[1:2, :] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test sdf[1:2, :] isa DataFrame
    @test sdf[1:2, r""] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test sdf[1:2, r""] isa DataFrame
    @test sdf[1:2, Not(Int[])] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test sdf[1:2, Not(Int[])] isa DataFrame

    @test sdf[Not(Not(1:2)), 1] == [1, 2]
    @test sdf[Not(Not(1:2)), 1] isa Vector
    @test sdf[Not(Not(1:2)), 1:2] == DataFrame(a=1:2, b=4:5)
    @test sdf[Not(Not(1:2)), 1:2] isa DataFrame
    @test sdf[Not(Not(1:2)), r"[ab]"] == DataFrame(a=1:2, b=4:5)
    @test sdf[Not(Not(1:2)), r"[ab]"] isa DataFrame
    @test sdf[Not(Not(1:2)), Not(Not(r"[ab]"))] == DataFrame(a=1:2, b=4:5)
    @test sdf[Not(Not(1:2)), Not(Not(r"[ab]"))] isa DataFrame
    @test sdf[Not(Not(1:2)), :] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test sdf[Not(Not(1:2)), :] isa DataFrame
    @test sdf[Not(Not(1:2)), r""] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test sdf[Not(Not(1:2)), r""] isa DataFrame
    @test sdf[Not(Not(1:2)), Not(Int[])] == DataFrame(a=1:2, b=4:5, c=7:8)
    @test sdf[Not(Not(1:2)), Not(Int[])] isa DataFrame

    @test sdf[:, 1] == [1, 2, 3]
    @test sdf[:, 1] isa Vector
    @test sdf[:, :a] == [1, 2, 3]
    @test sdf[:, :a] isa Vector
    @test sdf[:, "a"] == [1, 2, 3]
    @test sdf[:, "a"] isa Vector
    @test sdf[:, 1] !== df[!, 1]
    @test sdf[:, 1:2] == DataFrame(a=1:3, b=4:6)
    @test sdf[:, 1:2] isa DataFrame
    @test sdf[:, r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test sdf[:, r"[ab]"] isa DataFrame
    @test sdf[:, Not(Not(1:2))] == DataFrame(a=1:3, b=4:6)
    @test sdf[:, Not(Not(1:2))] isa DataFrame
    @test sdf[:, :] == df[2:4, 2:4]
    @test sdf[:, :] isa DataFrame
    @test sdf[:, r""] == df[2:4, 2:4]
    @test sdf[:, r""] isa DataFrame
    @test sdf[:, Not(1:0)] == df[2:4, 2:4]
    @test sdf[:, Not(1:0)] isa DataFrame

    @test sdf[Not(Not(:)), 1] == [1, 2, 3]
    @test sdf[Not(Not(:)), 1] isa Vector
    @test sdf[Not(Not(:)), 1] !== df[!, 1]
    @test sdf[Not(Not(:)), 1:2] == DataFrame(a=1:3, b=4:6)
    @test sdf[Not(Not(:)), 1:2] isa DataFrame
    @test sdf[Not(Not(:)), r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test sdf[Not(Not(:)), r"[ab]"] isa DataFrame
    @test sdf[Not(Not(:)), Not(Not(1:2))] == DataFrame(a=1:3, b=4:6)
    @test sdf[Not(Not(:)), Not(Not(1:2))] isa DataFrame
    @test sdf[Not(Not(:)), :] == df[2:4, 2:4]
    @test sdf[Not(Not(:)), :] isa DataFrame
    @test sdf[Not(Not(:)), r""] == df[2:4, 2:4]
    @test sdf[Not(Not(:)), r""] isa DataFrame
    @test sdf[Not(Not(:)), Not(1:0)] == df[2:4, 2:4]
    @test sdf[Not(Not(:)), Not(1:0)] isa DataFrame
end

@testset "view SubDataFrame" begin
    df = DataFrame(x=-1:3, a=0:4, b=3:7, c=6:10, d=9:13)
    sdf = view(df, 2:4, 2:4)

    @test view(sdf, !, 1) == [1, 2, 3]
    @test view(sdf, !, 1) isa SubArray
    @test view(sdf, !, :a) == [1, 2, 3]
    @test view(sdf, !, :a) isa SubArray
    @test view(sdf, !, "a") == [1, 2, 3]
    @test view(sdf, !, "a") isa SubArray

    for selector in [1:2, r"[ab]", Not(Not(r"[ab]")), Not(r"ab"), Not(3), Not(1:0), Not(1:2), :]
        dfx = @view sdf[!, selector]
        @test dfx == select(sdf, selector, copycols=false)
        @test dfx isa SubDataFrame
        @test parent(dfx[!, 1]) === df[!, names(dfx)[1]]
    end

    @test view(sdf, :, :) isa SubDataFrame
    @test view(sdf, :, :) == df[2:4, 2:4]
    @test view(sdf, :, r"") isa SubDataFrame
    @test view(sdf, :, r"") == df[2:4, 2:4]
    @test view(sdf, :, Not(1:0)) isa SubDataFrame
    @test view(sdf, :, Not(1:0)) == df[2:4, 2:4]

    @test view(sdf, !, :) isa SubDataFrame
    @test view(sdf, !, :) == df[2:4, 2:4]
    @test view(sdf, !, r"") isa SubDataFrame
    @test view(sdf, !, r"") == df[2:4, 2:4]
    @test view(sdf, !, Not(1:0)) isa SubDataFrame
    @test view(sdf, !, Not(1:0)) == df[2:4, 2:4]
    @test parent(view(sdf, !, :)) == parent(sdf)
    @test parent(view(sdf, !, r"")) == parent(sdf)
    @test parent(view(sdf, !, Not(1:0))) == parent(sdf)

    @test view(sdf, 1, 1) isa SubArray
    @test view(sdf, 1, 1)[] == 1
    @test view(sdf, 1, :a) isa SubArray
    @test view(sdf, 1, :a)[] == 1
    @test view(sdf, 1, "a") isa SubArray
    @test view(sdf, 1, "a")[] == 1
    @test view(sdf, 1, 1:2) isa DataFrameRow
    @test copy(view(sdf, 1, 1:2)) == (a=1, b=4)
    @test view(sdf, 1, r"[ab]") isa DataFrameRow
    @test copy(view(sdf, 1, r"[ab]")) == (a=1, b=4)
    @test view(sdf, 1, Not(Not(r"[ab]"))) isa DataFrameRow
    @test copy(view(sdf, 1, Not(Not(r"[ab]")))) == (a=1, b=4)
    @test view(sdf, 1, :) isa DataFrameRow
    @test copy(view(sdf, 1, :)) == (a=1, b=4, c=7)
    @test view(sdf, 1, r"") isa DataFrameRow
    @test copy(view(sdf, 1, r"")) == (a=1, b=4, c=7)
    @test view(sdf, 1, Not(1:0)) isa DataFrameRow
    @test copy(view(sdf, 1, Not(1:0))) == (a=1, b=4, c=7)
    @test parent(view(sdf, 1, :)) === parent(sdf)
    @test parent(view(sdf, 1, r"")) === parent(sdf)
    @test parent(view(sdf, 1, Not(1:0))) === parent(sdf)

    @test view(sdf, 1:2, 1) == [1, 2]
    @test view(sdf, 1:2, 1) isa SubArray
    @test view(sdf, 1:2, 1:2) isa SubDataFrame
    @test view(sdf, 1:2, 1:2) == DataFrame(a=1:2, b=4:5)
    @test view(sdf, 1:2, r"[ab]") isa SubDataFrame
    @test view(sdf, 1:2, r"[ab]") == DataFrame(a=1:2, b=4:5)
    @test view(sdf, 1:2, Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(sdf, 1:2, Not(Not(r"[ab]"))) == DataFrame(a=1:2, b=4:5)
    @test view(sdf, 1:2, :) isa SubDataFrame
    @test view(sdf, 1:2, :) == df[2:3, 2:4]
    @test view(sdf, 1:2, r"") isa SubDataFrame
    @test view(sdf, 1:2, r"") == df[2:3, 2:4]
    @test view(sdf, 1:2, Not(1:0)) isa SubDataFrame
    @test view(sdf, 1:2, Not(1:0)) == df[2:3, 2:4]
    @test parent(view(sdf, 1:2, :)) === parent(sdf)
    @test parent(view(sdf, 1:2, r"")) === parent(sdf)
    @test parent(view(sdf, 1:2, Not(1:0))) === parent(sdf)

    @test view(sdf, Not(Not(1:2)), 1) == [1, 2]
    @test view(sdf, Not(Not(1:2)), 1) isa SubArray
    @test view(sdf, Not(Not(1:2)), :a) == [1, 2]
    @test view(sdf, Not(Not(1:2)), :a) isa SubArray
    @test view(sdf, Not(Not(1:2)), "a") == [1, 2]
    @test view(sdf, Not(Not(1:2)), "a") isa SubArray
    @test view(sdf, Not(Not(1:2)), 1:2) isa SubDataFrame
    @test view(sdf, Not(Not(1:2)), 1:2) == DataFrame(a=1:2, b=4:5)
    @test view(sdf, Not(Not(1:2)), r"[ab]") isa SubDataFrame
    @test view(sdf, Not(Not(1:2)), r"[ab]") == DataFrame(a=1:2, b=4:5)
    @test view(sdf, Not(Not(1:2)), Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(sdf, Not(Not(1:2)), Not(Not(r"[ab]"))) == DataFrame(a=1:2, b=4:5)
    @test view(sdf, Not(Not(1:2)), :) isa SubDataFrame
    @test view(sdf, Not(Not(1:2)), :) == df[2:3, 2:4]
    @test view(sdf, Not(Not(1:2)), r"") isa SubDataFrame
    @test view(sdf, Not(Not(1:2)), r"") == df[2:3, 2:4]
    @test view(sdf, Not(Not(1:2)), Not(1:0)) isa SubDataFrame
    @test view(sdf, Not(Not(1:2)), Not(1:0)) == df[2:3, 2:4]
    @test parent(view(sdf, Not(Not(1:2)), :)) === parent(sdf)
    @test parent(view(sdf, Not(Not(1:2)), r"")) === parent(sdf)
    @test parent(view(sdf, Not(Not(1:2)), Not(1:0))) === parent(sdf)

    @test view(sdf, :, 1) == [1, 2, 3]
    @test view(sdf, :, 1) isa SubArray
    @test view(sdf, :, :a) == [1, 2, 3]
    @test view(sdf, :, :a) isa SubArray
    @test view(sdf, :, "a") == [1, 2, 3]
    @test view(sdf, :, "a") isa SubArray
    @test view(sdf, :, 1:2) isa SubDataFrame
    @test view(sdf, :, 1:2) == DataFrame(a=1:3, b=4:6)
    @test view(sdf, :, r"[ab]") isa SubDataFrame
    @test view(sdf, :, r"[ab]") == DataFrame(a=1:3, b=4:6)
    @test view(sdf, :, Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(sdf, :, Not(Not(r"[ab]"))) == DataFrame(a=1:3, b=4:6)
    @test view(sdf, :, :) isa SubDataFrame
    @test parent(view(sdf, :, :)) === parent(sdf)
    @test view(sdf, :, r"") isa SubDataFrame
    @test parent(view(sdf, :, r"")) === parent(sdf)
    @test view(sdf, :, Not(1:0)) isa SubDataFrame
    @test parent(view(sdf, :, Not(1:0))) === parent(sdf)
    @test view(sdf, :, :) == df[2:4, 2:4]
    @test view(sdf, :, r"") == df[2:4, 2:4]
    @test view(sdf, :, Not(1:0)) == df[2:4, 2:4]

    @test view(sdf, Not(Int[]), 1) == [1, 2, 3]
    @test view(sdf, Not(Int[]), 1) isa SubArray
    @test view(sdf, Not(Int[]), 1:2) isa SubDataFrame
    @test view(sdf, Not(Int[]), 1:2) == DataFrame(a=1:3, b=4:6)
    @test view(sdf, Not(Int[]), r"[ab]") isa SubDataFrame
    @test view(sdf, Not(Int[]), r"[ab]") == DataFrame(a=1:3, b=4:6)
    @test view(sdf, Not(Int[]), Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(sdf, Not(Int[]), Not(Not(r"[ab]"))) == DataFrame(a=1:3, b=4:6)
    @test view(sdf, Not(Int[]), :) isa SubDataFrame
    @test parent(view(sdf, Not(Int[]), :)) === parent(sdf)
    @test view(sdf, Not(Int[]), r"") isa SubDataFrame
    @test parent(view(sdf, Not(Int[]), r"")) === parent(sdf)
    @test view(sdf, Not(Int[]), Not(1:0)) isa SubDataFrame
    @test parent(view(sdf, Not(Int[]), Not(1:0))) === parent(sdf)
    @test view(sdf, Not(Int[]), :) == df[2:4, 2:4]
    @test view(sdf, Not(Int[]), r"") == df[2:4, 2:4]
    @test view(sdf, Not(Int[]), Not(1:0)) == df[2:4, 2:4]

    @test ncol(view(sdf, 1:2, [])) == 0
end

@testset "getindex DataFrameRow" begin
    df = DataFrame(a=1:4, b=4:7, c=7:10)
    dfr = df[1, :]

    @test dfr[1] == 1
    @test dfr[:a] == 1
    @test dfr["a"] == 1
    @test dfr[1:2] isa DataFrameRow
    @test copy(dfr[1:2]) == (a=1, b=4)
    @test dfr[[:a, :b]] isa DataFrameRow
    @test copy(dfr[[:a, :b]]) == (a=1, b=4)
    @test dfr[["a", "b"]] isa DataFrameRow
    @test copy(dfr[["a", "b"]]) == (a=1, b=4)
    @test dfr[r"[ab]"] isa DataFrameRow
    @test copy(dfr[r"[ab]"]) == (a=1, b=4)
    @test dfr[Not(Not(r"[ab]"))] isa DataFrameRow
    @test copy(dfr[Not(Not(r"[ab]"))]) == (a=1, b=4)
    @test dfr[:] isa DataFrameRow
    @test copy(dfr[:]) == (a=1, b=4, c=7)
    @test dfr[r""] isa DataFrameRow
    @test copy(dfr[r""]) == (a=1, b=4, c=7)
    @test dfr[Not(1:0)] isa DataFrameRow
    @test copy(dfr[Not(1:0)]) == (a=1, b=4, c=7)
    @test parent(dfr[:]) === df
    @test parent(dfr[r""]) === df
    @test parent(dfr[Not(Not(:))]) === df
end

@testset "view DataFrameRow" begin
    df = DataFrame(a=1:4, b=4:7, c=7:10)
    dfr = df[1, :]

    @test view(dfr, 1)[] == 1
    @test view(dfr, 1) isa SubArray
    @test view(dfr, :a)[] == 1
    @test view(dfr, :a) isa SubArray
    @test view(dfr, "a")[] == 1
    @test view(dfr, "a") isa SubArray
    @test view(dfr, 1:2) isa DataFrameRow
    @test copy(view(dfr, 1:2)) == (a=1, b=4)
    @test view(dfr, [:a, :b]) isa DataFrameRow
    @test copy(view(dfr, [:a, :b])) == (a=1, b=4)
    @test view(dfr, ["a", "b"]) isa DataFrameRow
    @test copy(view(dfr, ["a", "b"])) == (a=1, b=4)
    @test view(dfr, r"[ab]") isa DataFrameRow
    @test copy(view(dfr, r"[ab]")) == (a=1, b=4)
    @test view(dfr, Not(Not(r"[ab]"))) isa DataFrameRow
    @test copy(view(dfr, Not(Not(r"[ab]")))) == (a=1, b=4)
    @test dfr[:] isa DataFrameRow
    @test copy(view(dfr, :)) == (a=1, b=4, c=7)
    @test dfr[r""] isa DataFrameRow
    @test copy(view(dfr, r"")) == (a=1, b=4, c=7)
    @test dfr[Not(Not(:))] isa DataFrameRow
    @test copy(view(dfr, Not(Not(:)))) == (a=1, b=4, c=7)
    @test parent(dfr[:]) === df
    @test parent(dfr[r""]) === df
    @test parent(dfr[Not([])]) === df
end

@testset "additional tests of post-! getindex rules" begin
    df = DataFrame(reshape(1.5:16.5, (4, 4)), :auto)

    @test df[2, 2] == df[!, 2][2] == 6.5
    @test_throws BoundsError df[0, 2]
    @test_throws BoundsError df[5, 2]
    @test_throws BoundsError df[2, 0]
    @test_throws BoundsError df[2, 5]

    @test df[CartesianIndex(2, 2)] == df[!, 2][2] == 6.5
    @test_throws BoundsError df[CartesianIndex(0, 2)]
    @test_throws BoundsError df[CartesianIndex(5, 2)]
    @test_throws BoundsError df[CartesianIndex(2, 0)]
    @test_throws BoundsError df[CartesianIndex(2, 5)]

    df2 = copy(df)
    dfr = df2[2, :]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 100]
    df2[!, "y"] .= 1000
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 1000]

    df2 = copy(df)
    dfr = df2[2, 1:4]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    df2[!, "y"] .= 1000
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]

    @test df[2:3, :x2] == df[!, :x2][2:3] == [6.5, 7.5]
    @test df[2:3, "x2"] == df[!, "x2"][2:3] == [6.5, 7.5]
    @test_throws ArgumentError df[2:3, :x]
    @test_throws BoundsError df[0:3, :x2]
    @test_throws BoundsError df[1:5, :x2]
    @test_throws ArgumentError df[2:3, "x"]
    @test_throws BoundsError df[0:3, "x2"]
    @test_throws BoundsError df[1:5, "x2"]

    @test df[:, :x2] == df[!, :x2]
    @test df[:, :x2] !== df[!, :x2]
    @test df[:, "x2"] == df[!, "x2"]
    @test df[:, "x2"] !== df[!, "x2"]

    @test df[1:2, 1:2] == df[Not(3:4), Not(3:4)] == select(df, r"[12]")[1:2, :]
    @test df[1:2, 1:2] isa DataFrame
    @test df[:, 1:2] == df[Not(1:0), Not(3:4)] == select(df, r"[12]")
    @test df[:, 1:2][!, :x1] !== df.x1
    @test df[:, 1:2] isa DataFrame
    @test df[:, :] == df
    @test df[:, :] isa DataFrame
    @test df[:, :][!, 1] == df.x1
    @test df[:, :][!, 1] !== df.x1

    @test df[!, :x2] === df.x2 === DataFrames._columns(df)[2]
    @test_throws ArgumentError df[!, :x]
    @test df[!, "x2"] === df.x2 === DataFrames._columns(df)[2]
    @test_throws ArgumentError df[!, "x"]

    v = @view df[2, 2]
    @test v isa SubArray
    @test size(v) == ()
    @test  v[] == 6.5
    @test_throws BoundsError @view df[0, 2]
    @test_throws BoundsError @view df[5, 2]
    @test_throws BoundsError @view df[2, 0]
    @test_throws BoundsError @view df[2, 5]

    v = @view df[CartesianIndex(2, 2)]
    @test v isa SubArray
    @test size(v) == ()
    @test  v[] == 6.5
    @test_throws BoundsError @view df[CartesianIndex(0, 2)]
    @test_throws BoundsError @view df[CartesianIndex(5, 2)]
    @test_throws BoundsError @view df[CartesianIndex(2, 0)]
    @test_throws BoundsError @view df[CartesianIndex(2, 5)]

    df2 = copy(df)
    dfr = @view df2[2, :]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 100]
    df2[!, "y"] .= 1000
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 1000]

    df2 = copy(df)
    dfr = @view df2[2, 1:4]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    df2[!, "y"] .= 1000
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]

    v = @view df[2:3, :x2]
    @test v == [6.5, 7.5]
    @test v isa SubArray
    @test parent(v) === df.x2
    @test_throws ArgumentError @view df[2:3, :x]
    @test_throws BoundsError @view df[0:3, :x2]
    @test_throws BoundsError @view df[1:5, :x2]

    v = @view df[2:3, "x2"]
    @test v == [6.5, 7.5]
    @test v isa SubArray
    @test parent(v) === df.x2
    @test_throws ArgumentError @view df[2:3, "x"]
    @test_throws BoundsError @view df[0:3, "x2"]
    @test_throws BoundsError @view df[1:5, "x2"]

    @test @view(df[:, :x2]) == df[!, :x2]
    @test parent(@view(df[:, :x2])) === df[!, :x2]

    @test @view(df[:, "x2"]) == df[!, "x2"]
    @test parent(@view(df[:, "x2"])) === df[!, "x2"]

    sdf = @view df[1:2, 1:2]
    @test sdf == df[1:2, 1:2]
    @test sdf isa SubDataFrame
    @test parent(sdf) === df
    sdf = @view df[:, 1:2]
    @test sdf == df[:, 1:2]
    @test sdf isa SubDataFrame
    @test parent(sdf) === df
    sdf = @view df[:, :]
    @test sdf == df[:, :]
    @test sdf isa SubDataFrame
    @test parent(sdf) === df

    @test @view(df[!, :x2]) === @view(df[:, :x2])
    @test @view(df[!, :x2]) isa SubArray
    @test parent(@view(df[!, :x2])) === df.x2
    @test_throws ArgumentError @view df[!, :x]

    @test @view(df[!, "x2"]) === @view(df[:, "x2"])
    @test @view(df[!, "x2"]) isa SubArray
    @test parent(@view(df[!, "x2"])) === df.x2
    @test_throws ArgumentError @view df[!, "x"]

    sdf = @view df[Not(1:0), Not(r"zzz")]

    @test sdf[2, 2] == sdf[!, 2][2] == 6.5
    @test_throws BoundsError sdf[0, 2]
    @test_throws BoundsError sdf[5, 2]
    @test_throws BoundsError sdf[2, 0]
    @test_throws BoundsError sdf[2, 5]

    @test sdf[CartesianIndex(2, 2)] == sdf[!, 2][2] == 6.5
    @test_throws BoundsError sdf[CartesianIndex(0, 2)]
    @test_throws BoundsError sdf[CartesianIndex(5, 2)]
    @test_throws BoundsError sdf[CartesianIndex(2, 0)]
    @test_throws BoundsError sdf[CartesianIndex(2, 5)]

    df2 = copy(df)
    dfr = view(df2, 1:4, :)[2, :]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 100]
    df2[!, "y"] .= 1000
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 1000]

    df2 = copy(df)
    dfr = view(df2, 1:4, :)[2, 1:4]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    df2[!, "y"] .= 1000
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]

    @test sdf[2:3, :x2] == sdf[!, :x2][2:3] == [6.5, 7.5]
    @test sdf[2:3, :x2] isa Vector
    @test_throws ArgumentError sdf[2:3, :x]
    @test_throws BoundsError sdf[0:3, :x2]
    @test_throws BoundsError sdf[1:5, :x2]

    @test sdf[:, :x2] == sdf[!, :x2]
    @test sdf[:, :x2] !== sdf[!, :x2]
    @test sdf[:, :x2] isa Vector

    @test sdf[2:3, "x2"] == sdf[!, "x2"][2:3] == [6.5, 7.5]
    @test sdf[2:3, "x2"] isa Vector
    @test_throws ArgumentError sdf[2:3, "x"]
    @test_throws BoundsError sdf[0:3, "x2"]
    @test_throws BoundsError sdf[1:5, "x2"]

    @test sdf[:, "x2"] == sdf[!, "x2"]
    @test sdf[:, "x2"] !== sdf[!, "x2"]
    @test sdf[:, "x2"] isa Vector

    @test sdf[1:2, 1:2] == sdf[Not(3:4), Not(3:4)] == select(sdf, r"[12]")[1:2, :]
    @test sdf[1:2, 1:2] isa DataFrame
    @test sdf[:, 1:2] == sdf[Not(1:0), Not(3:4)] == select(sdf, r"[12]")
    @test sdf[:, 1:2][!, :x1] !== sdf.x1
    @test sdf[:, 1:2][!, "x1"] !== sdf.x1
    @test sdf[:, 1:2] isa DataFrame
    @test sdf[:, :] == sdf
    @test sdf[:, :] isa DataFrame
    @test sdf[:, :][!, 1] == sdf.x1
    @test sdf[:, :][!, 1] !== sdf.x1

    @test sdf[!, :x2] === sdf.x2
    @test sdf.x2 == DataFrames._columns(df)[2]
    @test sdf.x2 isa SubArray
    @test sdf[!, :x2] isa SubArray

    @test_throws ArgumentError sdf[!, :x]

    @test sdf[!, "x2"] === sdf."x2"
    @test sdf."x2" == DataFrames._columns(df)[2]
    @test sdf."x2" isa SubArray
    @test sdf[!, "x2"] isa SubArray

    @test_throws ArgumentError sdf[!, "x"]

    v = @view sdf[2, 2]
    @test v isa SubArray
    @test size(v) == ()
    @test  v[] == 6.5
    @test_throws BoundsError @view sdf[0, 2]
    @test_throws BoundsError @view sdf[5, 2]
    @test_throws BoundsError @view sdf[2, 0]
    @test_throws BoundsError @view sdf[2, 5]

    v = @view sdf[CartesianIndex(2, 2)]
    @test v isa SubArray
    @test size(v) == ()
    @test  v[] == 6.5
    @test_throws BoundsError @view sdf[CartesianIndex(0, 2)]
    @test_throws BoundsError @view sdf[CartesianIndex(5, 2)]
    @test_throws BoundsError @view sdf[CartesianIndex(2, 0)]
    @test_throws BoundsError @view sdf[CartesianIndex(2, 5)]

    df2 = copy(df)
    dfr = @view view(df2, 1:4, :)[2, :]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 100]
    df2[!, "y"] .= 1000
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 1000]

    df2 = copy(df)
    dfr = @view view(df2, 1:4, :)[2, 1:4]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    df2[!, "y"] .= 1000
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]

    v = @view sdf[2:3, :x2]
    @test v == [6.5, 7.5]
    @test v isa SubArray
    @test parent(v) === df.x2
    @test_throws ArgumentError @view sdf[2:3, :x]
    @test_throws BoundsError @view sdf[0:3, :x2]
    @test_throws BoundsError @view sdf[1:5, :x2]

    @test @view(sdf[:, :x2]) == sdf[!, :x2]
    @test parent(@view(sdf[:, :x2])) === df[!, :x2]

    @test @view(sdf[:, "x2"]) == sdf[!, "x2"]
    @test parent(@view(sdf[:, "x2"])) === df[!, "x2"]

    sdf2 = @view sdf[1:2, 1:2]
    @test sdf2 == sdf[1:2, 1:2]
    @test sdf2 isa SubDataFrame
    @test parent(sdf2) === df
    sdf2 = @view sdf[:, 1:2]
    @test sdf2 == sdf[:, 1:2]
    @test sdf2 isa SubDataFrame
    @test parent(sdf2) === df
    sdf2 = @view sdf[:, :]
    @test sdf2 == sdf[:, :]
    @test sdf2 isa SubDataFrame
    @test parent(sdf2) === df

    @test @view(sdf[!, :x2]) == df.x2
    @test sdf[!, :x2] isa SubArray
    @test parent(sdf[!, :x2]) === df.x2
    @test_throws ArgumentError @view sdf[!, :x]
    @test select(sdf, 1:2, copycols=false) == @view sdf[!, 1:2]

    @test @view(sdf[!, "x2"]) == df."x2"
    @test sdf[!, "x2"] isa SubArray
    @test parent(sdf[!, "x2"]) === df."x2"
    @test_throws ArgumentError @view sdf[!, "x"]

    dfr = df[2, :]
    @test dfr[2] == dfr.x2 == dfr."x2" == 6.5
    @test_throws BoundsError dfr[0]
    @test_throws BoundsError dfr[5]
    @test_throws ArgumentError dfr[:z]
    @test_throws ArgumentError dfr.z
    @test_throws ArgumentError dfr["z"]
    @test_throws ArgumentError dfr."z"

    @test Vector(dfr[2:3]) == [6.5, 10.5]
    @test dfr[2:3] isa DataFrameRow
    @test parent(dfr[2:3]) === df

    v = @view dfr[2]
    @test v[] == 6.5
    @test v isa SubArray
    @test size(v) == ()
    @test_throws BoundsError @view dfr[0]
    @test_throws BoundsError @view dfr[5]
    @test_throws ArgumentError @view dfr[:z]
    @test_throws ArgumentError @view dfr["z"]

    @test Vector(@view(dfr[2:3])) == [6.5, 10.5]
    @test @view(dfr[2:3]) isa DataFrameRow
    @test parent(@view(dfr[2:3])) === df
end

@testset "setindex! on DataFrame" begin
    # `df[row, col] = v` -> set value of `col` in row `row` to `v` in-place
    df = DataFrame(a=1:3, b=4:6, c=7:9)
    x = df.a
    df[1, 1] = 10
    @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    @test df.a === x
    @test_throws BoundsError df[0, 1] = 100
    @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    @test_throws ArgumentError df[1, 10] = 100
    @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    @test_throws ArgumentError df[true, 1] = 100
    @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    @test_throws MethodError df[1.0, 1] = 100
    @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    df[BigInt(1), 1] = 100
    @test df == DataFrame(a=[100, 2, 3], b=4:6, c=7:9)
    df[BigInt(1), :a] = 'a'
    @test df == DataFrame(a=[97, 2, 3], b=4:6, c=7:9)
    @test_throws ArgumentError df[BigInt(1), :z] = 'z'
    df[BigInt(1), "a"] = 'b'
    @test df == DataFrame(a=[98, 2, 3], b=4:6, c=7:9)
    @test_throws ArgumentError df[BigInt(1), "z"] = 'z'
    @test df == DataFrame(a=[98, 2, 3], b=4:6, c=7:9)
    @test_throws MethodError df[1, 1] = "a"
    @test df == DataFrame(a=[98, 2, 3], b=4:6, c=7:9)

    # `df[CartesianIndex(row, col)] = v` -> the same as `df[row, col] = v`
    df = DataFrame(a=1:3, b=4:6, c=7:9)
    x = df.a
    df[CartesianIndex(1, 1)] = 10
    @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    @test df.a === x
    @test_throws BoundsError df[CartesianIndex(0, 1)] = 100
    @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    @test_throws ArgumentError df[CartesianIndex(1, 10)] = 100
    @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    df[CartesianIndex(BigInt(1), 1)] = 100
    @test df == DataFrame(a=[100, 2, 3], b=4:6, c=7:9)
    @test_throws MethodError df[CartesianIndex(1, 1)] = "a"
    @test df == DataFrame(a=[100, 2, 3], b=4:6, c=7:9)

    # `df[row, cols] = v` -> set row `row` of columns `cols` in-place;
    # the same as `dfr = df[row, cols]; dfr[:] = v`

    df = DataFrame(a=[[1, 2]], b=[[1, 2]])
    dfr = df[1, :]
    @test_throws MethodError dfr[:] = [10, 11]
    @test df == DataFrame(a=[[1, 2]], b=[[1, 2]])
    @test_throws MethodError df[1, :] = [10, 11]
    @test df == DataFrame(a=[[1, 2]], b=[[1, 2]])

    df = DataFrame(a=1, b=2)
    df[1, :] = [10, 11]
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    dfr[:] = [10, 11]
    @test df == DataFrame(a=10, b=11)

    df = DataFrame(a=1, b=2)
    df[1, ["a", "b"]] = [10, 11]
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    dfr = df[1, ["a", "b"]]
    dfr[["a", "b"]] = [10, 11]
    @test df == DataFrame(a=10, b=11)

    df = DataFrame(a=1, b=2)
    df[1, :] = (10, 11)
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    dfr[:] = (10, 11)
    @test df == DataFrame(a=10, b=11)

    @test_throws DimensionMismatch df[1, :] = [1, 2, 3]
    @test_throws DimensionMismatch dfr[:] = [1, 2, 3]

    df = DataFrame(a=1, b=2)
    df[1, :] = Dict(:a=>10, :b=>11)
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    @test_throws ArgumentError df[1, :] = Dict(:a=>10, :c=>11)
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    @test_throws DimensionMismatch df[1, :] = Dict(:a=>10, :b=>11, :c=>12)
    @test df == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2)
    df[1, ["a", "b"]] = Dict("a"=>10, "b"=>11)
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    @test_throws ArgumentError df[1, ["a", "b"]] = Dict("a"=>10, "c"=>11)
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    @test_throws DimensionMismatch df[1, ["a", "b"]] = Dict("a"=>10, "b"=>11, "c"=>12)
    @test df == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2)
    df[1, :] = (a=10, b=11)
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    @test_throws ArgumentError df[1, :] = (a=10, c=11)
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    @test_throws ArgumentError df[1, :] = (b=10, a=11)
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    @test_throws DimensionMismatch df[1, :] = (a=10, b=11, c=12)
    @test df == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2)
    df[1, :] = DataFrame(a=10, b=11)[1, :]
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    @test_throws ArgumentError df[1, :] = DataFrame(a=10, c=11)[1, :]
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    @test_throws ArgumentError df[1, :] = DataFrame(b=10, a=11)[1, :]
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    @test_throws DimensionMismatch df[1, :] = DataFrame(a=10, b=11, c=12)[1, :]
    @test df == DataFrame(a=1, b=2)

    # `df[rows, col] = v` -> set rows `rows` of column `col` in-place; `v` must be an `AbstractVector`
    # the exception is `df[:, col] = v`, when col is not present in df, in which case `v` is copied
    # and column `col` is created

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    x = df.a
    df[1:3, 1] = 10:12
    @test df == DataFrame(a=10:12, b=4:6, c=7:9)
    @test df.a === x
    @test_throws MethodError df[1:3, 1] = ["a", "b", "c"]
    @test_throws DimensionMismatch df[1:3, 1] = [1]
    @test_throws MethodError df[1:3, 1] = 1
    @test_throws ArgumentError df[1:3, :z] = ["a", "b", "c"]
    @test_throws ArgumentError df[1:3, "z"] = ["a", "b", "c"]
    @test_throws BoundsError df[1:3, 4] = ["a", "b", "c"]

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    x = df.a
    df[1:3, "a"] = 10:12
    @test df == DataFrame(a=10:12, b=4:6, c=7:9)
    @test df.a === x

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    x = df.a
    df[:, 1] = 10:12
    @test df == DataFrame(a=10:12, b=4:6, c=7:9)
    @test df.a === x

    y = ["a", "b", "c"]
    df[:, :y] = y
    @test df.y == y
    @test df.y !== y

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    x = df.a
    df[:, "a"] = 10:12
    @test df == DataFrame(a=10:12, b=4:6, c=7:9)
    @test df."a" === x

    y = ["a", "b", "c"]
    df[:, "y"] = y
    @test df."y" == y
    @test df."y" !== y

    @test_throws MethodError df[:, 1] = ["a", "b", "c"]
    @test_throws DimensionMismatch df[:, 1] = [1]
    @test_throws MethodError df[:, 1] = 1
    @test_throws MethodError df[:, 2] = ["a", "b", "c"]
    @test_throws ArgumentError df[:, 10] = ["a", "b", "c"]

    # `df[rows, cols] = v` -> set rows `rows` of columns `cols` in-place;
    #                         `v` must be an `AbstractMatrix` or an `AbstractDataFrame`
    #                         (in this case column names must match)

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    df2 = DataFrame(a=11:13, b=14:16, c=17:19)
    x = df.a
    df[1:3, 1:3] = df2
    @test df == df2
    @test df.a == x
    @test_throws ArgumentError df[1:2, 1:2] = df2

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    df2 = DataFrame(a=11:13, b=14:16, c=17:19)
    m = Matrix(df2)
    x = df.a
    df[1:3, 1:3] = m
    @test df == df2
    @test df.a == x
    @test_throws DimensionMismatch df[1:2, 1:2] = m

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    df2 = df[!, :]
    @test_throws MethodError df[1:2, 1:2] = 1
    @test_throws ArgumentError df[1:2, 1:2] = DataFrame(ones(2, 2), :auto)
    @test df == DataFrame(a=1:3, b=4:6, c=7:9)
    df[:, :] = DataFrame(a=11:13, b=14:16, c=17:19)
    @test df2 == DataFrame(a=11:13, b=14:16, c=17:19)
    df[:, [1, 3]] = DataFrame(a=111:113, c=117:119)
    @test df2 == DataFrame(a=111:113, b=14:16, c=117:119)
    df[:, 2] = 1114:1116
    @test df2 == DataFrame(a=111:113, b=1114:1116, c=117:119)

    # `df[!, col] = v` -> replaces `col` with `v` without copying
    #                     (with the exception that if `v` is an `AbstractRange` it gets converted to a `Vector`);
    #                     also if `col` is a `Symbol` that is not present in `df` then a new column in `df` is created and holds `v`;
    #                     equivalent to `df.col = v` if `col` is a valid identifier

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    df[!, 1] = ["a", "b", "c"]
    @test df == DataFrame(a=["a", "b", "c"], b=4:6, c=7:9)
    @test_throws ArgumentError df[!, 1] = ["a", "b"]
    @test_throws ArgumentError df[!, 1] = ["a"]
    @test_throws ArgumentError df[!, 5] = ["a", "b", "c"]
    df[!, :a] = 'a':'c'
    @test df == DataFrame(a='a':'c', b=4:6, c=7:9)
    df.a = ["aaa", "bbb", 1]
    @test df == DataFrame(a=["aaa", "bbb", 1], b=4:6, c=7:9)
    df.z = 11:13
    @test df == DataFrame(a=["aaa", "bbb", 1], b=4:6, c=7:9, z=11:13)

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    @test_throws MethodError df[:, :a] = 1
    @test_throws ArgumentError df[:, 4] = 1:3
    @test df == DataFrame(a=1:3, b=4:6, c=7:9)
    df[:, :x] = 10:12
    @test df == DataFrame(a=1:3, b=4:6, c=7:9, x=10:12)

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    df[!, "a"] = ["a", "b", "c"]
    @test df == DataFrame(a=["a", "b", "c"], b=4:6, c=7:9)
    @test_throws ArgumentError df[!, "a"] = ["a", "b"]
    @test_throws ArgumentError df[!, "a"] = ["a"]
    df[!, "a"] = 'a':'c'
    @test df == DataFrame(a='a':'c', b=4:6, c=7:9)
    df."a" = ["aaa", "bbb", 1]
    @test df == DataFrame(a=["aaa", "bbb", 1], b=4:6, c=7:9)
    df."z" = 11:13
    @test df == DataFrame(a=["aaa", "bbb", 1], b=4:6, c=7:9, z=11:13)
end

@testset "setindex! on SubDataFrame" begin
    # `sdf[row, col] = v` -> set value of `col` in row `row` to `v` in-place;

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    for sdf in [view(df, :, :), view(df, :, 1:2), view(df, 1:2, :), view(df, 1:2, 1:2)]
        df.a = [1, 2, 3] # make sure we have a fresh first column in each iteration
        x = df.a
        sdf[1, 1] = 10
        @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
        @test x === df.a
        @test_throws BoundsError sdf[0, 1] = 100
        @test_throws BoundsError sdf[1, 0] = 100
        @test_throws ArgumentError sdf[1, true] = 100
        @test_throws ArgumentError sdf[true, 1] = 100
        @test_throws MethodError sdf[1, 1] = "a"
        @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    end

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    for sdf in [view(df, :, :), view(df, :, 1:2), view(df, 1:2, :), view(df, 1:2, 1:2)]
        df.a = [1, 2, 3] # make sure we have a fresh first column in each iteration
        x = df.a
        sdf[1, names(sdf)[1]] = 10
        @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
        @test x === df.a
        @test_throws BoundsError sdf[0, names(sdf)[1]] = 100
        @test_throws ArgumentError sdf[true, names(sdf)[1]] = 100
        @test_throws MethodError sdf[1, names(sdf)[1]] = "a"
        @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    end

    # `sdf[CartesianIndex(row, col)] = v` -> the same as `sdf[row, col] = v`;

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    for sdf in [view(df, :, :), view(df, :, 1:2), view(df, 1:2, :), view(df, 1:2, 1:2)]
        df.a = [1, 2, 3] # make sure we have a fresh first column in each iteration
        x = df.a
        sdf[CartesianIndex(1, 1)] = 10
        @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
        @test x === df.a
        @test_throws BoundsError sdf[CartesianIndex(0, 1)] = 100
        @test_throws BoundsError sdf[CartesianIndex(1, 0)] = 100
        @test_throws MethodError sdf[CartesianIndex(1, 1)] = "a"
        @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
    end

    # `sdf[row, cols] = v` -> the same as `dfr = df[row, cols]; dfr[:] = v` in-place;

    df = view(DataFrame(a=[[1, 2]], b=[[1, 2]]), :, :)
    dfr = df[1, :];
    @test_throws MethodError dfr[:] = [10, 11]
    @test df == DataFrame(a=[[1, 2]], b=[[1, 2]])
    @test_throws MethodError df[1, :] = [10, 11]
    @test df == DataFrame(a=[[1, 2]], b=[[1, 2]])

    df = view(DataFrame(a=1, b=2), :, :)
    df[1, :] = [10, 11]
    @test df == DataFrame(a=10, b=11)
    df = view(DataFrame(a=1, b=2), :, :)
    dfr = df[1, :]
    dfr[:] = [10, 11]
    @test df == DataFrame(a=10, b=11)

    df = view(DataFrame(a=1, b=2), :, :)
    df[1, :] = (10, 11)
    @test df == DataFrame(a=10, b=11)
    df = view(DataFrame(a=1, b=2), :, :)
    dfr = df[1, :]
    dfr[:] = (10, 11)
    @test df == DataFrame(a=10, b=11)

    @test_throws DimensionMismatch df[1, :] = [1, 2, 3]
    @test_throws DimensionMismatch dfr[:] = [1, 2, 3]
    @test_throws MethodError df[1, 1:2] = 3
    @test_throws MethodError dfr[:] = 3
    @test_throws MethodError dfr[1:1] = 100
    @test_throws MethodError df[1, 1:1] = 1000
    @test_throws MethodError dfr[1:1] = "d"
    @test_throws MethodError df[1, 1:1] = "e"

    df = view(DataFrame(a=1, b=2), :, :)
    df[1, :] = Dict(:a=>10, :b=>11)
    @test df == DataFrame(a=10, b=11)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws ArgumentError df[1, :] = Dict(:a=>10, :c=>11)
    @test df == DataFrame(a=1, b=2)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws DimensionMismatch df[1, :] = Dict(:a=>10, :b=>11, :c=>12)
    @test df == DataFrame(a=1, b=2)

    df = view(DataFrame(a=1, b=2), :, :)
    df[1, :] = Dict("a"=>101, "b"=>111)
    @test df == DataFrame(a=101, b=111)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws ArgumentError df[1, :] = Dict("a"=>10, "c"=>11)
    @test df == DataFrame(a=1, b=2)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws DimensionMismatch df[1, :] = Dict("a"=>10, "b"=>11, "c"=>12)
    @test df == DataFrame(a=1, b=2)

    df = view(DataFrame(a=1, b=2), :, :)
    df[1, :] = (a=10, b=11)
    @test df == DataFrame(a=10, b=11)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws ArgumentError df[1, :] = (a=10, c=11)
    @test df == DataFrame(a=1, b=2)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws ArgumentError df[1, :] = (b=10, a=11)
    @test df == DataFrame(a=1, b=2)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws DimensionMismatch df[1, :] = (a=10, b=11, c=12)
    @test df == DataFrame(a=1, b=2)

    df = view(DataFrame(a=1, b=2), :, :)
    df[1, :] = DataFrame(a=10, b=11)[1, :]
    @test df == DataFrame(a=10, b=11)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws ArgumentError df[1, :] = DataFrame(a=10, c=11)[1, :]
    @test df == DataFrame(a=1, b=2)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws ArgumentError df[1, :] = DataFrame(b=10, a=11)[1, :]
    @test df == DataFrame(a=1, b=2)
    df = view(DataFrame(a=1, b=2), :, :)
    @test_throws DimensionMismatch df[1, :] = DataFrame(a=10, b=11, c=12)[1, :]
    @test df == DataFrame(a=1, b=2)

    # `sdf[rows, col] = v` -> set rows `rows` of column `col`, in-place; `v` must be an abstract vector;

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    for sdf in [view(df, :, :), view(df, :, 1:3), view(df, 1:3, :), view(df, 1:3, 1:3)]
        df.a = [1, 2, 3]
        x = df.a
        sdf[1:3, 1] = 10:12
        @test sdf == DataFrame(a=10:12, b=4:6, c=7:9)
        @test df.a === x
        @test_throws MethodError sdf[1:3, 1] = ["a", "b", "c"]
        @test_throws DimensionMismatch sdf[1:3, 1] = [1]
        @test_throws MethodError sdf[1:3, 1] = 1
        @test_throws ArgumentError sdf[1:3, :z] = ["a", "b", "c"]
        @test_throws BoundsError sdf[1:3, 4] = ["a", "b", "c"]
    end

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    for sdf in [view(df, :, :), view(df, :, 1:3), view(df, 1:3, :),
                view(df, 1:3, 1:3), view(df, 1:3, ["a", "b", "c"])]
        df."a" = [1, 2, 3]
        x = df."a"
        sdf[1:3, names(sdf)[1]] = 10:12
        @test sdf == DataFrame(a=10:12, b=4:6, c=7:9)
        @test df.a === x
        @test_throws MethodError sdf[1:3, names(sdf)[1]] = ["a", "b", "c"]
        @test_throws ArgumentError sdf[1:3, "z"] = ["a", "b", "c"]
    end

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    for sdf in [view(df, :, :), view(df, :, 1:3), view(df, 1:3, :),
                view(df, 1:3, 1:3), view(df, 1:3, ["a", "b", "c"])]
        df.a = [1, 2, 3]
        x = df.a
        sdf[:, 1] = 10:12
        @test df == DataFrame(a=10:12, b=4:6, c=7:9)
        @test_throws MethodError sdf[:, 1] = ["a", "b", "c"]
        @test_throws BoundsError sdf[:, 4] = ["a", "b", "c"]
        @test_throws DimensionMismatch sdf[:, 1] = [1]
        @test_throws MethodError sdf[:, 1] = 1
        @test DataFrames.is_column_insertion_allowed(sdf) == (DataFrames.index(sdf) isa DataFrames.Index)
        if DataFrames.is_column_insertion_allowed(sdf)
            sdf[:, :z] = ["a", "b", "c"]
            @test df.z == ["a", "b", "c"]
            @test eltype(df.z) == Union{String, Missing}
            select!(df, 1:3)
        else
            @test_throws ArgumentError sdf[:, :z] = ["a", "b", "c"]
        end
    end

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    for sdf in [view(df, :, :), view(df, :, 1:3), view(df, 1:3, :),
                view(df, 1:3, 1:3), view(df, 1:3, ["a", "b", "c"])]
        df.a = [1, 2, 3]
        x = df.a
        sdf[:, names(sdf)[1]] = 10:12
        @test df == DataFrame(a=10:12, b=4:6, c=7:9)
        @test_throws MethodError sdf[:, names(sdf)[1]] = ["a", "b", "c"]
        @test DataFrames.is_column_insertion_allowed(sdf) == (DataFrames.index(sdf) isa DataFrames.Index)
        if DataFrames.is_column_insertion_allowed(sdf)
            sdf[:, "z"] = ["a", "b", "c"]
            @test df.z == ["a", "b", "c"]
            select!(df, 1:3)
        else
            @test_throws ArgumentError sdf[:, "z"] = ["a", "b", "c"]
        end
    end

    # `sdf[rows, cols] = v` -> set rows `rows` of columns `cols` in-place;
    #                          `v` can be an `AbstractMatrix` or `v` can be `AbstractDataFrame` when column names must match;

    for (row_sel, col_sel) in [(:, :), (:, 1:3), (1:3, :),
                               (1:3, 1:3), (1:3, ["a", "b", "c"])]
        df = DataFrame(a=1:3, b=4:6, c=7:9)
        sdf = view(df, row_sel, col_sel)
        df2 = DataFrame(a=11:13, b=14:16, c=17:19)
        x = df.a
        sdf[1:3, 1:3] = df2
        @test sdf == df2
        @test df.a == x
        @test_throws ArgumentError sdf[1:2, 1:2] = df2

        df = DataFrame(a=1:3, b=4:6, c=7:9)
        sdf = view(df, row_sel, col_sel)
        df2 = DataFrame(a=11:13, b=14:16, c=17:19)
        m = Matrix(df2)
        x = df.a
        sdf[1:3, 1:3] = m
        @test sdf == df2
        @test sdf.a == x
        @test_throws DimensionMismatch df[1:2, 1:2] = m

        @test_throws MethodError sdf[row_sel, col_sel] = 1
        @test_throws ArgumentError sdf[row_sel, col_sel] = DataFrame(ones(3, 3), :auto)
        @test (sdf[row_sel, col_sel] = df2) == df2
        @test df == df2
    end

    for (row_sel, col_sel) in [(:, :), (:, 1:3), (1:3, :), (1:3, 1:3), (1:3, ["a", "b", "c"])]
        df = DataFrame(a=1:3, b=4:6, c=7:9)
        sdf = view(df, row_sel, col_sel)
        sdf[!, 1] = [11, 12, 13]
        sdf[!, "b"] = [14, 15, 16]
        sdf[!, 3:3] = ones(Int, 3, 1)
        @test df == DataFrame(a=11:13, b=14:16, c=1)
    end
end

@testset "setindex! on DataFrameRow" begin
    # `dfr[col] = v` -> set value of `col` in row `row` to `v` in-place;
    #                   equivalent to `dfr.col = v` if `col` is a valid identifier;

    df = DataFrame(a=1:3, b=4:6, c=7:9)
    for dfr in [df[1, :], df[1, 1:2]]
        df.a = 1:3
        x = df.a
        dfr = df[1, :]
        dfr[1] = 10
        @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
        @test df.a === x
        @test_throws BoundsError dfr[10] = 10
        @test_throws ArgumentError dfr[true] = 10
        @test df == DataFrame(a=[10, 2, 3], b=4:6, c=7:9)
        dfr[BigInt(1)] = 100
        @test df == DataFrame(a=[100, 2, 3], b=4:6, c=7:9)
        dfr[:a] = 'a'
        @test df == DataFrame(a=[97, 2, 3], b=4:6, c=7:9)
        dfr["a"] = 'b'
        @test df == DataFrame(a=[98, 2, 3], b=4:6, c=7:9)
        @test_throws ArgumentError dfr[:z] = 'z'
        @test_throws ArgumentError dfr["z"] = 'z'
        @test df == DataFrame(a=[98, 2, 3], b=4:6, c=7:9)
        dfr.a = 'c'
        @test df == DataFrame(a=[99, 2, 3], b=4:6, c=7:9)
        @test_throws ArgumentError dfr.z = 'z'
        @test df == DataFrame(a=[99, 2, 3], b=4:6, c=7:9)
        @test_throws MethodError dfr.a = "a"
        @test df == DataFrame(a=[99, 2, 3], b=4:6, c=7:9)
        dfr."a" = 'd'
        @test df == DataFrame(a=[100, 2, 3], b=4:6, c=7:9)
        @test_throws ArgumentError dfr.z = 'z'
        @test df == DataFrame(a=[100, 2, 3], b=4:6, c=7:9)
        @test_throws MethodError dfr.a = "a"
        @test df == DataFrame(a=[100, 2, 3], b=4:6, c=7:9)
    end

    # * `dfr[cols] = v` -> set values of entries in columns `cols` in `dfr` by elements of `v` in place;
    #                      `v` can be:
    #                      1) a `Tuple` or an `AbstractArray`
    #                         in which cases it must have a number of elements equal to `length(dfr)`,
    #                      2) an `AbstractDict`, in which case column names must match,
    #                      3) a `NamedTuple` or `DataFrameRow`, in which case column names and order must match;

    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    dfr[:] = (10, 11)
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[:] = (10, 11, 12)
    @test df == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    dfr[:] = [10, 11]
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[:] = [10, 11, 12]
    @test df == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    dfr[:] = [10  11]
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[:] = [10 11 12]
    @test df == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws MethodError dfr[:] = (i for i in 10:11, _ in 1:1, _ in 1:1)

    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    dfr[:] = Dict(:a=>10, :b=>11)
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[:] = Dict(:a=>10, :c=>11)
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[:] = Dict(:a=>10, :b=>11, :c=>12)
    @test df == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    dfr[:] = (a=10, b=11)
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[:] = (a=10, c=11)
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[:] = (b=10, a=11)
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[:] = (a=10, b=11, c=12)
    @test df == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    dfr[:] = DataFrame(a=10, b=11)[1, :]
    @test df == DataFrame(a=10, b=11)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[:] = DataFrame(a=10, c=11)[1, :]
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[:] = DataFrame(b=10, a=11)[1, :]
    @test df == DataFrame(a=1, b=2)
    df = DataFrame(a=1, b=2)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[:] = DataFrame(a=10, b=11, c=12)[1, :]
    @test df == DataFrame(a=1, b=2)

    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    dfr[Not(3)] = (10, 11)
    @test df == DataFrame(a=10, b=11, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[Not(3)] = (10, 11, 12)
    @test df == DataFrame(a=1, b=2, c=3)

    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    dfr[Not(3)] = [10, 11]
    @test df == DataFrame(a=10, b=11, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[Not(3)] = [10, 11, 12]
    @test df == DataFrame(a=1, b=2, c=3)

    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    dfr[Not(3)] = [10 11]
    @test df == DataFrame(a=10, b=11, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[Not(3)] = [10 11 12]
    @test df == DataFrame(a=1, b=2, c=3)

    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws MethodError dfr[Not(3)] = (i for i in 10:11, _ in 1:1, _ in 1:1)

    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    dfr[Not(3)] = Dict(:a=>10, :b=>11)
    @test df == DataFrame(a=10, b=11, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[Not(3)] = Dict(:a=>10, :c=>11)
    @test df == DataFrame(a=1, b=2, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[Not(3)] = Dict(:a=>10, :b=>11, :c=>12)
    @test df == DataFrame(a=1, b=2, c=3)

    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    dfr[Not(3)] = (a=10, b=11)
    @test df == DataFrame(a=10, b=11, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[Not(3)] = (a=10, c=11)
    @test df == DataFrame(a=1, b=2, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[Not(3)] = (b=10, a=11)
    @test df == DataFrame(a=1, b=2, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[Not(3)] = (a=10, b=11, c=12)
    @test df == DataFrame(a=1, b=2, c=3)

    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    dfr[Not(3)] = DataFrame(a=10, b=11)[1, :]
    @test df == DataFrame(a=10, b=11, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[Not(3)] = DataFrame(a=10, c=11)[1, :]
    @test df == DataFrame(a=1, b=2, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws ArgumentError dfr[Not(3)] = DataFrame(b=10, a=11)[1, :]
    @test df == DataFrame(a=1, b=2, c=3)
    df = DataFrame(a=1, b=2, c=3)
    dfr = df[1, :]
    @test_throws DimensionMismatch dfr[Not(3)] = DataFrame(a=10, b=11, c=12)[1, :]
    @test df == DataFrame(a=1, b=2, c=3)

    @test_throws MethodError dfr[:] = "ab"
end

@testset "setindex! with ! or : and multiple cols" begin
    df = DataFrame(fill("x", 3, 4), :auto)
    df[!, :] = DataFrame(reshape(1:12, 3, :), :auto)
    @test df == DataFrame(reshape(1:12, 3, :), :auto)
    @test_throws ArgumentError df[!, :] = DataFrame(fill(1, 3, 4), :auto)[:, [3, 2, 1]]
    @test_throws ArgumentError df[!, :] = DataFrame(fill(1, 3, 4), :auto)[1:2, :]

    df = DataFrame(fill("x", 3, 4), :auto)
    df[!, Not(4)] = DataFrame(reshape(1:12, 3, :), :auto)[:, 1:3]
    @test df[:, 1:3] == DataFrame(reshape(1:12, 3, :), :auto)[:, 1:3]

    df = DataFrame(fill("x", 3, 4), :auto)
    df[!, :] = reshape(1:12, 3, :)
    @test df == DataFrame(reshape(1:12, 3, :), :auto)

    df = DataFrame(fill("x", 3, 4), :auto)
    df[!, Not(4)] = reshape(1:12, 3, :)[:, 1:3]
    @test df[:, 1:3] == DataFrame(reshape(1:12, 3, :), :auto)[:, 1:3]

    dfv = view(df, :, :)
    dfv[!, :] = DataFrame(reshape(1:12, 3, :), :auto)
    @test df == DataFrame(reshape(1:12, 3, :), :auto)
    dfv[!, :] = reshape(1:12, 3, :)
    @test df == DataFrame(reshape(1:12, 3, :), :auto)

    for rows in [:, 1:3], cols in [:, r"", Not(r"xx"), 1:4]
        df = DataFrame(ones(3, 4), :auto)
        df[rows, cols] = DataFrame(reshape(1:12, 3, :), :auto)
        @test df == DataFrame(reshape(1:12, 3, :), :auto)
    end

    for rows in [:, 1:3], cols in [:, r"", Not(r"xx"), 1:4]
        df = DataFrame(ones(3, 4), :auto)
        df[rows, cols] = reshape(1:12, 3, :)
        @test df == DataFrame(reshape(1:12, 3, :), :auto)
    end
end

@testset "additional setindex! tests" begin
    df = DataFrame(reshape(1:12, 4, :), :auto)
    df[1:2, :] = df[3:4, :]
    @test df == DataFrame([3  7  11
                          4  8  12
                          3  7  11
                          4  8  12], :auto)

    df[[true, false, true, false], :] = df[[2, 4], :]
    @test df == DataFrame([4  8  12
                          4  8  12
                          4  8  12
                          4  8  12], :auto)

    @test_throws MethodError df[1, :] = 1

    df[:, 2] = ones(4)
    @test df == DataFrame([4  1  12
                          4  1  12
                          4  1  12
                          4  1  12], :auto)

    @test_throws InexactError df[:, 2] = fill(1.5, 4)
end

@testset "invalid view tests" begin
    dfr = DataFrame(ones(2, 3), :auto)
    for df in (dfr, view(dfr, 1:2, 1:3))
        for r in (1, 1:1)
            @test_throws BoundsError view(df, r, 0:1)
            @test_throws BoundsError view(df, r, 1:4)
            @test_throws BoundsError view(df, r, [0, 1])
            @test_throws BoundsError view(df, r, [1, 4])
            @test_throws ArgumentError view(df, r, [1, 2, 1])
            @test_throws ArgumentError view(df, r, [:x1, :x2, :x1])
            @test_throws ArgumentError view(df, r, ["x1", "x2", "x1"])
        end
    end
end

# just to check that dispatch works correctly
@testset "string indexing" begin
    df_ref = DataFrame(a=1:3, b=4:6, c=7:9)
    for df in (df_ref[1:2, [2, 1]], df_ref[1:2, ["b", "a"]],
               view(df_ref, 1:2, [2, 1]), view(df_ref, 1:2, ["b", "a"]))
        @test df[1, "a"] == df[1, 2]
        @test df[1:2, "a"] == df[1:2, 2]
        @test df[1, ["a", "b"]] == df[1, [2, 1]]
        @test df[1:2, ["a", "b"]] == df[1:2, [2, 1]]
        @test df[:, ["a", "b"]] == df[:, [2, 1]]
        @test df[!, ["a", "b"]] == df[!, [2, 1]]

        @test view(df, 1, "a") == view(df, 1, 2)
        @test view(df, 1:2, "a") == view(df, 1:2, 2)
        @test view(df, 1, ["a", "b"]) == view(df, 1, [2, 1])
        @test view(df, 1:2, ["a", "b"]) == view(df, 1:2, [2, 1])
        @test view(df, :, ["a", "b"]) == view(df, :, [2, 1])
        @test view(df, !, ["a", "b"]) == view(df, !, [2, 1])

        df[1, "a"] = 100
        @test df[1, "a"] == 100
        df[1:2, "a"] = [20, 30]
        @test df[1:2, "a"] == [20, 30]
        df[:, "a"] = [30, 40]
        @test df[:, "a"] == [30, 40]
        df[!, "a"] = [1, 2]
        @test df[!, "a"] == [1, 2]

        df[1, ["a", "b"]] = (a=1000, b=2000)
        @test copy(df[1, ["a", "b"]]) == (a=1000, b=2000)
        df[1:1, ["a"]] = ones(1, 1)
        @test df[1, "a"] == 1
        df[1, ["a", "b"]] .= 50
        @test copy(df[1, ["a", "b"]]) == (a=50, b=50)
        df[1:1, ["a"]] .= 1
        @test df[1, "a"] == 1
    end

    df_ref."g1" = 11:13
    @test df_ref."g1" == 11:13
    df_ref[!, "g2"] = 11:13
    @test df_ref."g2" == 11:13
    df_ref[:, "g3"] = 11:13
    @test df_ref."g3" == 11:13

    for dfr in (df_ref[1, [2, 1]], df_ref[1, ["b", "a"]],
               view(df_ref, 1, [2, 1]), view(df_ref, 1, ["b", "a"]))
        @test dfr["a"] == dfr[2]
        @test dfr[["a", "b"]] == dfr[[2, 1]]
        @test view(dfr, "a") == view(dfr, 2)
        @test view(dfr, ["a", "b"]) == view(dfr, [2, 1])

        dfr["a"] = 100
        @test dfr."a" == 100
        dfr[["a", "b"]] = (a=1000, b=2000)
        @test copy(dfr) == (b=2000, a=1000)

        @test_throws MethodError dfr["a"] .= 100
        dfr[["a", "b"]] .= 50
        @test copy(dfr) == (b=50, a=50)
    end
end

@testset "setindex! for DataFrameRow" begin
    for selector in ([:y2, :y1], [4, 2])
        df = DataFrame(x=1:2, y1=3:4, z=5:6, y2=7:8)
        df[1, selector] = [100, 200]
        @test df == DataFrame(x=1:2, y1=[200, 4], z=5:6, y2=[100, 8])

        df = DataFrame(x=1:2, y1=3:4, z=5:6, y2=7:8)
        df[1, selector] .= [100, 200]
        @test df == DataFrame(x=1:2, y1=[200, 4], z=5:6, y2=[100, 8])
    end

    for selector in ([:y1, :y2], [2, 4], r"y", Not([:x, :z]))
        df = DataFrame(x=1:2, y1=3:4, z=5:6, y2=7:8)
        df[1, selector] = [200, 100]
        @test df == DataFrame(x=1:2, y1=[200, 4], z=5:6, y2=[100, 8])

        df = DataFrame(x=1:2, y1=3:4, z=5:6, y2=7:8)
        df[1, selector] .= [200, 100]
        @test df == DataFrame(x=1:2, y1=[200, 4], z=5:6, y2=[100, 8])
    end
end

@testset "setindex! for ncols(df)+1 and old tests" begin
    df = DataFrame()
    @test_throws ArgumentError df[:, 1] = 1:3

    df = DataFrame(a=1:3)
    @test_throws DimensionMismatch df[1:2, 1] = 1:3
    @test_throws ArgumentError df[1:2, 1:1] = DataFrame(b=1:2)
end

include("indexing_begin_tests.jl")

@testset "unsupported df[col] and df[col] for getindex, view, and setindex!" begin
    @testset "getindex DataFrame" begin
        df = DataFrame(a=1:3, b=4:6, c=7:9)
        @test_throws ArgumentError df[1]
        @test_throws MethodError df[end]
        @test_throws MethodError df[1:2]
        @test_throws MethodError df[r"[ab]"]
        @test_throws MethodError df[Not(3)]
        @test_throws MethodError df[:]
        @test_throws ArgumentError df[:a]
        @test_throws ArgumentError df["a"]
    end
    @testset "view DataFrame" begin
        df = DataFrame(a=1:3, b=4:6, c=7:9)
        @test_throws MethodError view(df, 1)
        @test_throws MethodError view(df, :a)
        @test_throws MethodError view(df, "a")
        @test_throws MethodError view(df, 1:2)
        @test_throws MethodError view(df, r"[ab]")
        @test_throws MethodError view(df, Not(Not(r"[ab]")))
        @test_throws MethodError view(df, :)
    end
    @testset "getindex SubDataFrame" begin
        df = DataFrame(x=-1:3, a=0:4, b=3:7, c=6:10, d=9:13)
        sdf = view(df, 2:4, 2:4)
        @test_throws ArgumentError sdf[1]
        @test_throws MethodError sdf[end]
        @test_throws ArgumentError sdf[:x]
        @test_throws ArgumentError sdf["x"]
        @test_throws MethodError sdf[1:2]
        @test_throws MethodError sdf[r"[ab]"]
        @test_throws MethodError sdf[Not(Not(r"[ab]"))]
        @test_throws MethodError sdf[:]
    end
    @testset "view SubDataFrame" begin
        df = DataFrame(x=-1:3, a=0:4, b=3:7, c=6:10, d=9:13)
        sdf = view(df, 2:4, 2:4)
        @test_throws MethodError view(sdf, 1)
        @test_throws MethodError view(sdf, :a)
        @test_throws MethodError view(sdf, "a")
        @test_throws MethodError view(sdf, 1:2)
        @test_throws MethodError view(sdf, r"[ab]")
        @test_throws MethodError view(sdf, Not(Not(r"[ab]")))
        @test_throws MethodError view(sdf, :)
    end

    @testset "old setindex! tests" begin
        df = DataFrame(reshape(1:12, 4, :), :auto)
        @test_throws MethodError df[1, :] = df[1:1, :]

        df = DataFrame(reshape(1:12, 4, :), :auto)

        # Scalar broadcasting assignment of rows
        @test_throws MethodError df[1:2, :] = 1
        @test_throws MethodError df[[true, false, false, true], :] = 3

        # Vector broadcasting assignment of rows
        @test_throws MethodError df[1:2, :] = [2, 3]
        @test_throws MethodError df[[true, false, false, true], :] = [2, 3]

        # Broadcasting assignment of columns
        @test_throws MethodError df[:, 1] = 1
        @test_throws ArgumentError df[:x3] = 2

        # assignment of subtables
        @test_throws MethodError df[1, 1:2] = df[2:2, 2:3]
        @test_throws ArgumentError df[[true, false, false, true], 2:3] = df[1:2, 1:2]

        # this is a different case - column names do not match
        @test_throws ArgumentError df[1:2, 1:2] = df[2:3, 2:3]

        # scalar broadcasting assignment of subtables
        @test_throws MethodError df[1:2, 1:2] = 3
        @test_throws MethodError df[[true, false, false, true], 2:3] = 3

        # vector broadcasting assignment of subtables
        @test_throws MethodError df[1:2, 1:2] = [3, 2]
        @test_throws MethodError df[[true, false, false, true], 2:3] = [2, 3]

        # test of 1-row DataFrame assignment
        df = DataFrame([1 2 3], :auto)
        @test_throws MethodError df[1, 2:3] = DataFrame([11 12], :auto)
        @test_throws MethodError df[1, [false, true, true]] = DataFrame([11 12], :auto)
    end

    @testset "cornercase of view indexing" begin
        df = DataFrame(reshape(1:12, 4, :), :auto)
        dfr = df[1, 3:2]
        for idx in [:x1, :x2, :x3, :x4]
            @test_throws ArgumentError dfr[idx]
        end

        sdf = @view df[1:1, 3:2]
        for idx in [:x1, :x2, :x3, :x4]
            @test_throws ArgumentError sdf[1, idx]
        end
    end
end

@testset "setproperty! corner cases" begin
    df = DataFrame(a=1)
    @test_throws ArgumentError df.a = 1
    @test_throws ArgumentError df."a" = 1
    dfv = @view df[:, :]
    dfv.a = [5]
    @test df == DataFrame(a=5)
    @test eltype(df.a) === Int
    dfv."a" = [6]
    @test df == DataFrame(a=6)
    @test eltype(df.a) === Int
    @test_throws ArgumentError dfv.a = 1
    @test_throws ArgumentError dfv."a" = 1
end

@testset "disallowed getindex and setindex! methods" begin
    df = DataFrame(a=1)
    @test_throws ArgumentError df[:a]
    @test_throws ArgumentError df[:a] = [2]
    @test_throws ArgumentError df["a"]
    @test_throws ArgumentError df["a"] = [2]
    @test_throws ArgumentError df[1]
    @test_throws ArgumentError df[1] = [2]
end

@testset "array interface tests for all types" begin
    df = DataFrame(reshape(1:12, 3, 4), :auto)
    @test_throws MethodError length(df)
    @test ndims(df) == ndims(typeof(df)) == 2
    @test size(df) == (3, 4)
    @test size(df, 1) == 3
    @test size(df, 2) == 4
    @test_throws ArgumentError size(df, 3)
    @test_throws ArgumentError size(df, 0)
    @test axes(df) == (1:3, 1:4)
    @test axes(df, 1) == 1:3
    @test axes(df, 2) == 1:4
    @test_throws ArgumentError axes(df, 3)
    @test_throws ArgumentError axes(df, 0)
    @test_throws MethodError firstindex(df)
    @test firstindex(df, 1) == 1
    @test firstindex(df, 2) == 1
    @test_throws ArgumentError firstindex(df, 3)
    @test_throws ArgumentError firstindex(df, 0)
    @test_throws MethodError lastindex(df)
    @test lastindex(df, 1) == 3
    @test lastindex(df, 2) == 4
    @test_throws ArgumentError lastindex(df, 3)
    @test_throws ArgumentError lastindex(df, 0)

    dfr = df[1, 1:3]
    @test length(dfr) == 3
    @test ndims(dfr) == ndims(typeof(dfr)) == 1
    @test size(dfr) == (3,)
    @test size(dfr, 1) == 3
    @test_throws BoundsError size(dfr, 2)
    @test_throws BoundsError size(dfr, 0)
    @test axes(dfr) == (1:3,)
    @test axes(dfr, 1) == 1:3
    @test_throws BoundsError axes(dfr, 2)
    @test_throws BoundsError axes(dfr, 0)
    @test firstindex(dfr) == 1
    @test firstindex(dfr, 1) == 1
    @test_throws BoundsError firstindex(dfr, 2)
    @test_throws BoundsError firstindex(dfr, 0)
    @test lastindex(dfr) == 3
    @test lastindex(dfr, 1) == 3
    @test_throws BoundsError lastindex(dfr, 2)
    @test_throws BoundsError lastindex(dfr, 0)

    er = eachrow(df)
    @test length(er) == 3
    @test ndims(er) == ndims(typeof(er)) == 1
    @test size(er) == (3,)
    @test size(er, 1) == 3
    @test size(er, 2) == 1
    @test_throws BoundsError size(er, 0)
    @test axes(er) == (1:3,)
    @test axes(er, 1) == 1:3
    @test axes(er, 2) == 1:1
    @test_throws BoundsError axes(er, 0)
    @test firstindex(er) == 1
    @test firstindex(er, 1) == 1
    @test firstindex(er, 2) == 1
    @test_throws BoundsError firstindex(er, 0)
    @test lastindex(er) == 3
    @test lastindex(er, 1) == 3
    @test lastindex(er, 2) == 1
    @test_throws BoundsError lastindex(er, 0)

    ec = eachcol(df)
    @test length(ec) == 4
    @test ndims(ec) == ndims(typeof(ec)) == 1
    @test size(ec) == (4,)
    @test size(ec, 1) == 4
    @test_throws ArgumentError size(ec, 2)
    @test_throws ArgumentError size(ec, 0)
    @test axes(ec) == (1:4,)
    @test axes(ec, 1) == 1:4
    @test_throws ArgumentError axes(ec, 2)
    @test_throws ArgumentError axes(ec, 0)
    @test firstindex(ec) == 1
    @test firstindex(ec, 1) == 1
    @test_throws ArgumentError firstindex(ec, 2)
    @test_throws ArgumentError firstindex(ec, 0)
    @test lastindex(ec) == 4
    @test lastindex(ec, 1) == 4
    @test_throws ArgumentError lastindex(ec, 2)
    @test_throws ArgumentError lastindex(ec, 0)

    gdf = groupby(df, [:x1, :x2, :x3])
    @test length(gdf) == 3
    @test ndims(gdf) == ndims(typeof(gdf)) == 1
    @test size(gdf) == (3,)
    @test size(gdf, 1) == 3
    @test_throws BoundsError size(gdf, 2)
    @test_throws BoundsError size(gdf, 0)
    @test axes(gdf) == (1:3,)
    @test axes(gdf, 1) == 1:3
    @test_throws BoundsError axes(gdf, 2)
    @test_throws BoundsError axes(gdf, 0)
    @test firstindex(gdf) == 1
    @test firstindex(gdf, 1) == 1
    @test_throws BoundsError firstindex(gdf, 2)
    @test_throws BoundsError firstindex(gdf, 0)
    @test lastindex(gdf) == 3
    @test lastindex(gdf, 1) == 3
    @test_throws BoundsError lastindex(gdf, 2)
    @test_throws BoundsError lastindex(gdf, 0)

    kgdf = keys(gdf)
    @test length(kgdf) == 3
    @test ndims(kgdf) == ndims(typeof(kgdf)) == 1
    @test size(kgdf) == (3,)
    @test size(kgdf, 1) == 3
    @test size(kgdf, 2) == 1
    @test_throws BoundsError size(kgdf, 0)
    @test axes(kgdf) == (1:3,)
    @test axes(kgdf, 1) == 1:3
    @test axes(kgdf, 2) == 1:1
    @test_throws BoundsError axes(kgdf, 0)
    @test firstindex(kgdf) == 1
    @test firstindex(kgdf, 1) == 1
    @test firstindex(kgdf, 2) == 1
    @test_throws BoundsError firstindex(kgdf, 0)
    @test lastindex(kgdf) == 3
    @test lastindex(kgdf, 1) == 3
    @test lastindex(kgdf, 2) == 1
    @test_throws BoundsError lastindex(kgdf, 0)

    gk = kgdf[1]
    @test length(gk) == 3
    @test ndims(gk) == ndims(typeof(gk)) == 1
    @test size(gk) == (3,)
    @test size(gk, 1) == 3
    @test_throws BoundsError size(gk, 2)
    @test_throws BoundsError size(gk, 0)
    @test axes(gk) == (1:3,)
    @test axes(gk, 1) == 1:3
    @test_throws BoundsError axes(gk, 2)
    @test_throws BoundsError axes(gk, 0)
    @test firstindex(gk) == 1
    @test firstindex(gk, 1) == 1
    @test_throws BoundsError firstindex(gk, 2)
    @test_throws BoundsError firstindex(gk, 0)
    @test lastindex(gk) == 3
    @test lastindex(gk, 1) == 3
    @test_throws BoundsError lastindex(gk, 2)
    @test_throws BoundsError lastindex(gk, 0)
end

include("indexing_offset.jl")

@testset "threading correctness tests" begin
    for x in (10, 110_000), y in 1:4
        vecvec = [rand(Int8, x) for _ in 1:y]
        df = DataFrame(vecvec, :auto, copycols=false)
        for rowrange in [:, 1:nrow(df)-5, collect(1:nrow(df)-5), axes(df, 1) .< nrow(df)-5],
            colrange in [:, axes(df, 2), collect(axes(df, 2)), 1:ncol(df) - 1]
            df2 = df[rowrange, colrange]
            for j in axes(df2, 2)
                @test df2[!, j] == view(vecvec[j], rowrange)
            end
        end
    end
end

@testset "name normalization" begin
    Random.seed!(1234)

    for (a1, a2) in [("", ""), ("", "1"), ("1", ""),
                     ("a", "a"), ("a", "a1"), ("a1", "a"),
                     ("ab", "ab"), ("ab", "ac"), ("ab", "ab1"), ("ab1", "ab"),]
        @test DataFrames._norm_eq(a1, a2, Symbol(a1)) == (a1 == a2)
    end

    for _ in 1:100
        x = randstring(5)
        y1 = String(chop(x))
        y2 = y1 * "α"
        y3 = x * "α"

        @test DataFrames._norm_eq(x, x, :x)
        @test !DataFrames._norm_eq(x, y1, :x)
        @test !DataFrames._norm_eq(x, y2, :x)
        @test !DataFrames._norm_eq(x, y3, :x)
    end

    d1 = DataFrame("no\u00EBl" => 1)
    d2 = DataFrame("noe\u0308l" => 1)
    d3 = DataFrame("noe\u0308\u00EBl" => 1)
    @test d1[:, "no\u00EBl"] == [1]
    @test_throws ArgumentError d1[:, "noe\u0308l"]
    @test d1[:, Unicode.normalize("noe\u0308l")] == [1]
    @test_throws ArgumentError d2[:, "no\u00EBl"]
    @test rename(Unicode.normalize, d2)[:, "no\u00EBl"] == [1]
    @test d2[:, "noe\u0308l"] == [1]
    @test_throws ArgumentError d3[:, "no\u00EBe\u0308l"]
    @test rename(Unicode.normalize, d3)[:, Unicode.normalize("no\u00EBe\u0308l")] == [1]
    @test d3[:, "noe\u0308\u00EBl"] == [1]

    rename!(DataFrames.Unicode.normalize, d1)
    rename!(DataFrames.Unicode.normalize, d2)
    rename!(DataFrames.Unicode.normalize, d3)
    @test d1[:, DataFrames.Unicode.normalize("no\u00EBl")] == [1]
    @test d1[:, DataFrames.Unicode.normalize("noe\u0308l")] == [1]
    @test d2[:, DataFrames.Unicode.normalize("no\u00EBl")] == [1]
    @test d2[:, DataFrames.Unicode.normalize("noe\u0308l")] == [1]
    @test d3[:, DataFrames.Unicode.normalize("no\u00EBe\u0308l")] == [1]
    @test d3[:, DataFrames.Unicode.normalize("noe\u0308\u00EBl")] == [1]

    d = DataFrame("power_µW" => 1:3)
    @test_throws ArgumentError d.power_µW
    @test d."power_µW" == 1:3

    for (a, b) in pairs(DataFrames._julia_charmap)
        # needed for cdot
        a = Unicode.normalize(string(a))[1]
        idx = Symbol(b)
        d = DataFrame(string(a) => 1)
        @test_throws ArgumentError d[:, string(b)]
        @test d[:, string(a)] == [1]

        idx = Symbol(a)
        d = DataFrame(string(b) => 1)
        @test_throws ArgumentError d[:, string(a)]
        @test d[:, string(b)] == [1]
    end

    for a in keys(DataFrames._julia_charmap)
        d = DataFrame(string(a) => 1)
        @test d[:, string(a)] == [1]
        @test d[:, Symbol(a)] == [1]
    end

    for a in values(DataFrames._julia_charmap)
        d = DataFrame(string(a) => 1)
        @test d[:, string(a)] == [1]
        @test d[:, Symbol(a)] == [1]
    end

    d = DataFrame("\u00B7" => 1)
    a = "\u0387"
    @test_throws ArgumentError d[:, string(a)]
    d = DataFrame("\u0387" => 1)
    a = "\u00B7"
    @test_throws ArgumentError d[:, string(a)]

end

@testset "haskey method error" begin
    df = DataFrame(a=1:2, b=2:3)
    sdf = view(df, 1:1, 1:1)
    for x in [df, sdf]
        @test_throws ArgumentError haskey(DataFrames.index(x), 1.0)
        @test_throws ArgumentError haskey(DataFrames.index(x), true)
        @test_throws ArgumentError haskey(x[1, :], 1.0)
        @test_throws ArgumentError haskey(x[1, :], true)
        gdf = groupby(df, :a)
        @test_throws ArgumentError haskey(gdf, 1.0)
        @test_throws ArgumentError haskey(gdf, true)
        key = first(keys(gdf))
        @test_throws ArgumentError haskey(key, 1.0)
        @test_throws ArgumentError haskey(key, true)
    end
end

@testset "fix #2969 (passing Pairs as selectors)" begin
    df = DataFrame(a=1:3, b=4:6)
    @test_throws ArgumentError  df[:, [:a => sum]]
    @test_throws ArgumentError  df[!, [:a => sum]]
    @test_throws ArgumentError  df[1:2, [:a => sum]]
    @test_throws ArgumentError  df[1, [:a => sum]]

    for rowsel in [:, 1:2, 1], colsel in [:, 1:2, 1:1]
        dfv = @view df[:, :]
        @test_throws ArgumentError  df[:, [:a => sum]]
        @test_throws ArgumentError  df[!, [:a => sum]]
        @test_throws ArgumentError  df[1:2, [:a => sum]]
        @test_throws ArgumentError  df[1, [:a => sum]]
    end
end

end # module
