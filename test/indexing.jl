module TestIndexing

using Test, DataFrames

@testset "getindex DataFrame" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)

    @test df[!, 1] == [1, 2, 3]
    @test df[!, 1] === eachcol(df)[1]
    @test df[!, :a] == [1, 2, 3]
    @test df[!, :a] === eachcol(df)[1]
    @test df.a == [1, 2, 3]
    @test df.a === eachcol(df)[1]

    @test_throws MethodError df[!, 1:2]
    @test_throws MethodError df[!, r"[ab]"]
    @test_throws MethodError df[!, Not(Not(r"[ab]"))]
    @test_throws MethodError df[!, Not(3)]
    @test_throws MethodError df[!, Not(1:0)]
    @test_throws MethodError df[!, :]

    @test eachcol(df)[1] === last(eachcol(df, true)[1])
    @test eachcol(df)[1] === last(eachcol(df, true)[1])

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
    @test df.x === x
    @test df[!, :x] === x
    @test df[!, 1] === x
end

@testset "view DataFrame" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)

    @test view(df, !, 1) == [1, 2, 3]
    @test view(df, !, 1) isa SubArray
    @test view(df, !, :a) == [1, 2, 3]
    @test view(df, !, :a) isa SubArray

    @test_throws MethodError view(df, !, 1:2)
    @test_throws MethodError view(df, !, :)
    @test_throws MethodError view(df, !, r"ab")
    @test_throws MethodError view(df, !, Not(1))
    @test_throws MethodError view(df, !, Not(1:2))
    @test_throws MethodError view(df, !, Not(r"ab"))
    @test_throws MethodError view(df, !, Not(Not(r"ab")))

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
    @test sdf.a == [1, 2, 3]
    @test sdf.a isa SubArray

    @test_throws MethodError sdf[!, 1:2]
    @test_throws MethodError sdf[!, r"[ab]"]
    @test_throws MethodError sdf[!, Not(Not(r"[ab]"))]
    @test_throws MethodError sdf[!, :]
    @test_throws MethodError sdf[!, r""]
    @test_throws MethodError sdf[!, Not(1:0)]

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

    @test_throws ArgumentError view(sdf, !, 1:2)
    @test_throws ArgumentError view(sdf, !, r"[ab]")
    @test_throws ArgumentError view(sdf, !, Not(Not(r"[ab]")))
    @test_throws ArgumentError view(sdf, !, :)
    @test_throws ArgumentError view(sdf, !, r"")
    @test_throws ArgumentError view(sdf, !, Not(1))
    @test_throws ArgumentError view(sdf, !, Not(1:0))

    @test view(sdf, 1, 1) isa SubArray
    @test view(sdf, 1, 1)[] == 1
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
end

@testset "getindex DataFrameRow" begin
    df = DataFrame(a=1:4, b=4:7, c=7:10)
    dfr = df[1, :]

    @test dfr[1] == 1
    @test dfr[1:2] isa DataFrameRow
    @test copy(dfr[1:2]) == (a=1, b=4)
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
    @test view(dfr, 1:2) isa DataFrameRow
    @test copy(dfr[1:2]) == (a=1, b=4)
    @test view(dfr, r"[ab]") isa DataFrameRow
    @test copy(dfr[r"[ab]"]) == (a=1, b=4)
    @test view(dfr, Not(Not(r"[ab]"))) isa DataFrameRow
    @test copy(dfr[Not(Not(r"[ab]"))]) == (a=1, b=4)
    @test dfr[:] isa DataFrameRow
    @test copy(dfr[:]) == (a=1, b=4, c=7)
    @test dfr[r""] isa DataFrameRow
    @test copy(dfr[r""]) == (a=1, b=4, c=7)
    @test dfr[Not(Not(:))] isa DataFrameRow
    @test copy(dfr[Not(Not(:))]) == (a=1, b=4, c=7)
    @test parent(dfr[:]) === df
    @test parent(dfr[r""]) === df
    @test parent(dfr[Not([])]) === df
end

@testset "additional tests of post-! getindex rules" begin
    df = DataFrame(reshape(1.5:16.5, (4,4)))

    @test df[2,2] == df[!, 2][2] == 6.5
    @test_throws BoundsError df[0,2]
    @test_throws BoundsError df[5,2]
    @test_throws BoundsError df[2,0]
    @test_throws BoundsError df[2,5]

    @test df[CartesianIndex(2,2)] == df[!, 2][2] == 6.5
    @test_throws BoundsError df[CartesianIndex(0,2)]
    @test_throws BoundsError df[CartesianIndex(5,2)]
    @test_throws BoundsError df[CartesianIndex(2,0)]
    @test_throws BoundsError df[CartesianIndex(2,5)]

    df2 = copy(df)
    dfr = df2[2, :]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 100]

    df2 = copy(df)
    dfr = df2[2, 1:4]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]

    @test df[2:3, :x2] == df[!, :x2][2:3] == [6.5, 7.5]
    @test_throws ArgumentError df[2:3, :x]
    @test_throws BoundsError df[0:3, :x2]
    @test_throws BoundsError df[1:5, :x2]

    @test df[:, :x2] == df[!, :x2]
    @test df[:, :x2] !== df[!, :x2]

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
    @test_throws MethodError df[!, 1:2]

    v = @view df[2,2]
    @test v isa SubArray
    @test size(v) == ()
    @test  v[] == 6.5
    @test_throws BoundsError @view df[0,2]
    @test_throws BoundsError @view df[5,2]
    @test_throws BoundsError @view df[2,0]
    @test_throws BoundsError @view df[2,5]

    v = @view df[CartesianIndex(2,2)]
    @test v isa SubArray
    @test size(v) == ()
    @test  v[] == 6.5
    @test_throws BoundsError @view df[CartesianIndex(0,2)]
    @test_throws BoundsError @view df[CartesianIndex(5,2)]
    @test_throws BoundsError @view df[CartesianIndex(2,0)]
    @test_throws BoundsError @view df[CartesianIndex(2,5)]

    df2 = copy(df)
    dfr = @view df2[2, :]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 100]

    df2 = copy(df)
    dfr = @view df2[2, 1:4]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]

    v = @view df[2:3, :x2]
    @test v == [6.5, 7.5]
    @test v isa SubArray
    @test parent(v) === df.x2
    @test_throws ArgumentError @view df[2:3, :x]
    @test_throws BoundsError @view df[0:3, :x2]
    @test_throws BoundsError @view df[1:5, :x2]

    @test @view(df[:, :x2]) == df[!, :x2]
    @test parent(@view(df[:, :x2])) === df[!, :x2]

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
    @test_throws MethodError @view df[!, 1:2]

    sdf = @view df[Not(1:0), Not(r"zzz")]

    @test sdf[2,2] == sdf[!, 2][2] == 6.5
    @test_throws BoundsError sdf[0,2]
    @test_throws BoundsError sdf[5,2]
    @test_throws BoundsError sdf[2,0]
    @test_throws BoundsError sdf[2,5]

    @test sdf[CartesianIndex(2,2)] == sdf[!, 2][2] == 6.5
    @test_throws BoundsError sdf[CartesianIndex(0,2)]
    @test_throws BoundsError sdf[CartesianIndex(5,2)]
    @test_throws BoundsError sdf[CartesianIndex(2,0)]
    @test_throws BoundsError sdf[CartesianIndex(2,5)]

    df2 = copy(df)
    dfr = view(df2, 1:4, :)[2, :]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 100]

    df2 = copy(df)
    dfr = view(df2, 1:4, :)[2, 1:4]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]

    @test sdf[2:3, :x2] == sdf[!, :x2][2:3] == [6.5, 7.5]
    @test sdf[2:3, :x2] isa Vector
    @test_throws ArgumentError sdf[2:3, :x]
    @test_throws BoundsError sdf[0:3, :x2]
    @test_throws BoundsError sdf[1:5, :x2]

    @test sdf[:, :x2] == sdf[!, :x2]
    @test sdf[:, :x2] !== sdf[!, :x2]
    @test sdf[:, :x2] isa Vector

    @test sdf[1:2, 1:2] == sdf[Not(3:4), Not(3:4)] == select(sdf, r"[12]")[1:2, :]
    @test sdf[1:2, 1:2] isa DataFrame
    @test sdf[:, 1:2] == sdf[Not(1:0), Not(3:4)] == select(sdf, r"[12]")
    @test sdf[:, 1:2][!, :x1] !== sdf.x1
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
    @test_throws MethodError sdf[!, 1:2]

    v = @view sdf[2,2]
    @test v isa SubArray
    @test size(v) == ()
    @test  v[] == 6.5
    @test_throws BoundsError @view sdf[0,2]
    @test_throws BoundsError @view sdf[5,2]
    @test_throws BoundsError @view sdf[2,0]
    @test_throws BoundsError @view sdf[2,5]

    v = @view sdf[CartesianIndex(2,2)]
    @test v isa SubArray
    @test size(v) == ()
    @test  v[] == 6.5
    @test_throws BoundsError @view sdf[CartesianIndex(0,2)]
    @test_throws BoundsError @view sdf[CartesianIndex(5,2)]
    @test_throws BoundsError @view sdf[CartesianIndex(2,0)]
    @test_throws BoundsError @view sdf[CartesianIndex(2,5)]

    df2 = copy(df)
    dfr = @view view(df2, 1:4, :)[2, :]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5, 100]

    df2 = copy(df)
    dfr = @view view(df2, 1:4, :)[2, 1:4]
    @test dfr isa DataFrameRow
    @test Vector(dfr) == [2.5, 6.5, 10.5, 14.5]
    @test parent(dfr) === df2
    df2[!, :y] .= 100
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
    @test_throws ArgumentError @view sdf[!, 1:2]

    dfr = df[2, :]
    @test dfr[2] == dfr.x2 == 6.5
    @test_throws BoundsError dfr[0]
    @test_throws BoundsError dfr[5]
    @test_throws ArgumentError dfr[:z]
    @test_throws ArgumentError dfr.z

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

    @test Vector(@view(dfr[2:3])) == [6.5, 10.5]
    @test @view(dfr[2:3]) isa DataFrameRow
    @test parent(@view(dfr[2:3])) === df
end

end # module
