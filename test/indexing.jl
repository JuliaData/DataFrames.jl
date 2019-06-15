module TestIndexing

using Test, DataFrames

@testset "getindex DataFrame" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)

    @test df[1] == [1, 2, 3]
    @test df[1] === eachcol(df)[1]
    @test df[1:2] == DataFrame(a=1:3, b=4:6)
    @test df[r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test df[Not(Not(r"[ab]"))] == DataFrame(a=1:3, b=4:6)
    @test df[Not(3)] == DataFrame(a=1:3, b=4:6)
    @test eachcol(df, false)[1] === df[1]
    @test eachcol(view(df,1:2), false)[1] == eachcol(df, false)[1]
    @test eachcol(df[1:2], false)[1] == eachcol(df, false)[1]
    @test eachcol(df[r"[ab]"], false)[1] == eachcol(df, false)[1]
    @test eachcol(df[Not(Not(r"[ab]"))], false)[1] == eachcol(df, false)[1]
    @test eachcol(df[Not(r"[c]")], false)[1] == eachcol(df, false)[1]
    @test eachcol(df[1:2], false)[1] !== eachcol(df, false)[1]
    @test df[:] == df
    @test df[r""] == df
    @test df[Not(Not(r""))] == df
    @test df[Not(1:0)] == df
    @test df[:] !== df
    @test df[r""] !== df
    @test df[Not(Not(r""))] !== df
    @test df[Not(1:0)] !== df
    @test eachcol(view(df, :), false)[1] == eachcol(df, false)[1]
    @test eachcol(df[:], false)[1] == eachcol(df, false)[1]
    @test eachcol(df[r""], false)[1] == eachcol(df, false)[1]
    @test eachcol(df[Not(1:0)], false)[1] == eachcol(df, false)[1]
    @test eachcol(df[:], false)[1] !== eachcol(df, false)[1]
    @test eachcol(df[r""], false)[1] !== eachcol(df, false)[1]
    @test eachcol(df[Not(1:0)], false)[1] !== eachcol(df, false)[1]
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
    @test df[:, 1] !== df[1]
    @test df[:, 1:2] == DataFrame(a=1:3, b=4:6)
    @test df[:, r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test df[:, Not(r"c")] == DataFrame(a=1:3, b=4:6)
    @test eachcol(df[:, 1:2])[1] !== df[1]
    @test df[:, :] == df
    @test df[:, r""] == df
    @test df[:, Not(Not(r""))] == df
    @test eachcol(df[:, :])[1] !== df[1]
    @test eachcol(df[:, r""])[1] !== df[1]
    @test eachcol(df[:, Not([])])[1] !== df[1]

    @test df[Not(Int[]), 1] == [1, 2, 3]
    @test df[Not(Int[]), 1] !== df[1]
    @test df[Not(Int[]), 1:2] == DataFrame(a=1:3, b=4:6)
    @test df[Not(Int[]), r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test df[Not(Int[]), Not(r"c")] == DataFrame(a=1:3, b=4:6)
    @test eachcol(df[Not(Int[]), 1:2])[1] !== df[1]
    @test df[Not(Int[]), :] == df
    @test df[Not(Int[]), r""] == df
    @test df[Not(Int[]), Not(Not(r""))] == df
    @test eachcol(df[Not(Int[]), :])[1] !== df[1]
    @test eachcol(df[Not(Int[]), r""])[1] !== df[1]
    @test eachcol(df[Not(Int[]), Not([])])[1] !== df[1]
end

@testset "getindex df[col] and df[cols]" begin
    x = [1, 2, 3]
    df = DataFrame(x=x, copycols=false)
    @test df.x === x
    @test df[:x] === x
    @test df[[:x]].x !== x
    @test df[:].x !== x
    @test df[r"x"].x !== x
    @test df[r""].x !== x
    @test df[Not(1:0)].x !== x
end

@testset "view DataFrame" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)

    @test view(df, 1) == [1, 2, 3]
    @test view(df, 1) isa SubArray
    @test view(df, 1:2) isa SubDataFrame
    @test view(df, 1:2) == df[1:2]
    @test view(df, r"[ab]") isa SubDataFrame
    @test view(df, r"[ab]") == df[1:2]
    @test view(df, Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(df, Not(Not(r"[ab]"))) == df[1:2]
    @test view(df, :) isa SubDataFrame
    @test view(df, :) == df
    @test parent(view(df, :)) === df
    @test view(df, r"") isa SubDataFrame
    @test view(df, r"") == df
    @test parent(view(df, r"")) === df
    @test view(df, Not(1:0)) isa SubDataFrame
    @test view(df, Not(1:0)) == df
    @test parent(view(df, Not(1:0))) === df

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

    @test sdf[1] == [1, 2, 3]
    @test sdf[1] isa SubArray
    @test sdf[1:2] == DataFrame(a=1:3, b=4:6)
    @test sdf[1:2] isa SubDataFrame
    @test sdf[r"[ab]"] == DataFrame(a=1:3, b=4:6)
    @test sdf[r"[ab]"] isa SubDataFrame
    @test sdf[Not(Not(r"[ab]"))] == DataFrame(a=1:3, b=4:6)
    @test sdf[Not(Not(r"[ab]"))] isa SubDataFrame
    @test sdf[:] == df[2:4, 2:4]
    @test sdf[:] isa SubDataFrame
    @test sdf[r""] == df[2:4, 2:4]
    @test sdf[r""] isa SubDataFrame
    @test sdf[Not(1:0)] == df[2:4, 2:4]
    @test sdf[Not(1:0)] isa SubDataFrame
    @test parent(sdf[:]) === parent(sdf)
    @test parent(sdf[r""]) === parent(sdf)
    @test parent(sdf[Not([])]) === parent(sdf)

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
    @test sdf[:, 1] !== df[1]
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
    @test sdf[Not(Not(:)), 1] !== df[1]
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

    @test view(sdf, 1) == [1, 2, 3]
    @test view(sdf, 1) isa SubArray
    @test view(sdf, 1:2) isa SubDataFrame
    @test view(sdf, 2:3) == df[2:4, 3:4]
    @test view(sdf, r"[ab]") isa SubDataFrame
    @test view(sdf, r"[ab]") == df[2:4, r"[ab]"]
    @test view(sdf, Not(Not(r"[ab]"))) isa SubDataFrame
    @test view(sdf, Not(Not(r"[ab]"))) == df[2:4, Not(Not(r"[ab]"))]
    @test view(sdf, :) isa SubDataFrame
    @test view(sdf, :) == df[2:4, 2:4]
    @test view(sdf, r"") isa SubDataFrame
    @test view(sdf, r"") == df[2:4, 2:4]
    @test view(sdf, Not(1:0)) isa SubDataFrame
    @test view(sdf, Not(1:0)) == df[2:4, 2:4]
    @test parent(view(sdf, :)) == parent(sdf)
    @test parent(view(sdf, r"")) == parent(sdf)
    @test parent(view(sdf, Not(1:0))) == parent(sdf)

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

end # module
