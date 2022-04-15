module TestJoin

using Test, DataFrames, Random, CategoricalArrays, PooledArrays
using DataFrames: similar_missing, OnCol
const ≅ = isequal

"""Check if passed data frames are `isequal` and have the same types of columns"""
isequal_coltyped(df1::AbstractDataFrame, df2::AbstractDataFrame) =
    isequal(df1, df2) && typeof.(eachcol(df1)) == typeof.(eachcol(df2))

name = DataFrame(ID=Union{Int, Missing}[1, 2, 3],
                Name=Union{String, Missing}["John Doe", "Jane Doe", "Joe Blogs"])
job = DataFrame(ID=Union{Int, Missing}[1, 2, 2, 4],
                Job=Union{String, Missing}["Lawyer", "Doctor", "Florist", "Farmer"])

# Test output of various join types
outer = DataFrame(ID=[1, 2, 2, 3, 4],
                  Name=["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
                  Job=["Lawyer", "Doctor", "Florist", missing, "Farmer"])

# (Tests use current column ordering but don't promote it)
right = outer[Bool[!ismissing(x) for x in outer.Job], [:ID, :Name, :Job]]
left = outer[Bool[!ismissing(x) for x in outer.Name], :]
inner = left[Bool[!ismissing(x) for x in left.Job], :]
semi = unique(inner[:, [:ID, :Name]])
anti = left[Bool[ismissing(x) for x in left.Job], [:ID, :Name]]

@testset "join types" begin
    # Join on symbols or vectors of symbols
    innerjoin(name, job, on=:ID)
    innerjoin(name, job, on=[:ID])

    @test_throws ArgumentError innerjoin(name, job)
    @test_throws ArgumentError innerjoin(name, job, on=:ID, matchmissing=:errors)
    @test_throws ArgumentError innerjoin(name, job, on=:ID, matchmissing=:weirdmatch)
    @test_throws ArgumentError outerjoin(name, job, on=:ID, matchmissing=:notequal)

    @test innerjoin(name, job, on=:ID) == inner
    @test outerjoin(name, job, on=:ID) ≅ outer
    @test leftjoin(name, job, on=:ID) ≅ left
    @test rightjoin(name, job, on=:ID) ≅ right
    @test semijoin(name, job, on=:ID) == semi
    @test antijoin(name, job, on=:ID) == anti

    # Join with no non-key columns
    on = [:ID]
    nameid = name[:, on]
    jobid = job[:, on]

    @test innerjoin(nameid, jobid, on=:ID) == inner[:, on]
    @test outerjoin(nameid, jobid, on=:ID) == outer[:, on]
    @test leftjoin(nameid, jobid, on=:ID) == left[:, on]
    @test rightjoin(nameid, jobid, on=:ID) == right[:, on]
    @test semijoin(nameid, jobid, on=:ID) == semi[:, on]
    @test antijoin(nameid, jobid, on=:ID) == anti[:, on]

    # Join on multiple keys
    df1 = DataFrame(A=1, B=2, C=3)
    df2 = DataFrame(A=1, B=2, D=4)

    @test innerjoin(df1, df2, on=[:A, :B]) == DataFrame(A=1, B=2, C=3, D=4)

    # Test output of cross joins
    df1 = DataFrame(A=1:2, B='a':'b')
    df2 = DataFrame(C=3:5)

    cross = DataFrame(A=[1, 1, 1, 2, 2, 2],
                      B=['a', 'a', 'a', 'b', 'b', 'b'],
                      C=[3, 4, 5, 3, 4, 5])

    @test crossjoin(df1, df2) == cross

    # Cross joins handle naming collisions
    @test size(crossjoin(df1, df1, makeunique=true)) == (4, 4)

    # Cross joins don't take keys
    @test_throws MethodError crossjoin(df1, df2, on=:A)
end

@testset "Test empty inputs 1" begin
    simple_df(len::Int, col=:A) = (df = DataFrame();
                                   df[!, col]=Vector{Union{Int, Missing}}(1:len);
                                   df)
    @test leftjoin(simple_df(0), simple_df(0), on=:A) == simple_df(0)
    @test leftjoin(simple_df(2), simple_df(0), on=:A) == simple_df(2)
    @test leftjoin(simple_df(0), simple_df(2), on=:A) == simple_df(0)
    @test rightjoin(simple_df(0), simple_df(0), on=:A) == simple_df(0)
    @test rightjoin(simple_df(0), simple_df(2), on=:A) == simple_df(2)
    @test rightjoin(simple_df(2), simple_df(0), on=:A) == simple_df(0)
    @test innerjoin(simple_df(0), simple_df(0), on=:A) == simple_df(0)
    @test innerjoin(simple_df(0), simple_df(2), on=:A) == simple_df(0)
    @test innerjoin(simple_df(2), simple_df(0), on=:A) == simple_df(0)
    @test outerjoin(simple_df(0), simple_df(0), on=:A) == simple_df(0)
    @test outerjoin(simple_df(0), simple_df(2), on=:A) == simple_df(2)
    @test outerjoin(simple_df(2), simple_df(0), on=:A) == simple_df(2)
    @test semijoin(simple_df(0), simple_df(0), on=:A) == simple_df(0)
    @test semijoin(simple_df(2), simple_df(0), on=:A) == simple_df(0)
    @test semijoin(simple_df(0), simple_df(2), on=:A) == simple_df(0)
    @test antijoin(simple_df(0), simple_df(0), on=:A) == simple_df(0)
    @test antijoin(simple_df(2), simple_df(0), on=:A) == simple_df(2)
    @test antijoin(simple_df(0), simple_df(2), on=:A) == simple_df(0)
    @test crossjoin(simple_df(0), simple_df(0, :B)) == DataFrame(A=Int[], B=Int[])
    @test crossjoin(simple_df(0), simple_df(2, :B)) == DataFrame(A=Int[], B=Int[])
    @test crossjoin(simple_df(2), simple_df(0, :B)) == DataFrame(A=Int[], B=Int[])
end

@testset "Test empty inputs 2" begin
    simple_df(len::Int, col=:A) = (df = DataFrame(); df[!, col]=collect(1:len); df)
    @test leftjoin(simple_df(0), simple_df(0), on=:A) ==  simple_df(0)
    @test leftjoin(simple_df(2), simple_df(0), on=:A) ==  simple_df(2)
    @test leftjoin(simple_df(0), simple_df(2), on=:A) ==  simple_df(0)
    @test rightjoin(simple_df(0), simple_df(0), on=:A) == simple_df(0)
    @test rightjoin(simple_df(0), simple_df(2), on=:A) == simple_df(2)
    @test rightjoin(simple_df(2), simple_df(0), on=:A) == simple_df(0)
    @test innerjoin(simple_df(0), simple_df(0), on=:A) == simple_df(0)
    @test innerjoin(simple_df(0), simple_df(2), on=:A) == simple_df(0)
    @test innerjoin(simple_df(2), simple_df(0), on=:A) == simple_df(0)
    @test outerjoin(simple_df(0), simple_df(0), on=:A) == simple_df(0)
    @test outerjoin(simple_df(0), simple_df(2), on=:A) == simple_df(2)
    @test outerjoin(simple_df(2), simple_df(0), on=:A) == simple_df(2)
    @test semijoin(simple_df(0), simple_df(0), on=:A) ==  simple_df(0)
    @test semijoin(simple_df(2), simple_df(0), on=:A) ==  simple_df(0)
    @test semijoin(simple_df(0), simple_df(2), on=:A) ==  simple_df(0)
    @test antijoin(simple_df(0), simple_df(0), on=:A) ==  simple_df(0)
    @test antijoin(simple_df(2), simple_df(0), on=:A) ==  simple_df(2)
    @test antijoin(simple_df(0), simple_df(2), on=:A) ==  simple_df(0)
    @test crossjoin(simple_df(0), simple_df(0, :B)) == DataFrame(A=Int[], B=Int[])
    @test crossjoin(simple_df(0), simple_df(2, :B)) == DataFrame(A=Int[], B=Int[])
    @test crossjoin(simple_df(2), simple_df(0, :B)) == DataFrame(A=Int[], B=Int[])
end

@testset "issue #960" begin
    df1 = DataFrame(A=categorical(1:50),
                    B=categorical(1:50),
                    C=1)
    @test innerjoin(df1, df1, on=[:A, :B], makeunique=true)[!, 1:3] == df1
    # Test that join works when mixing Array{Union{T, Missing}} with Array{T} (issue #1088)
    df = DataFrame(Name=Union{String, Missing}["A", "B", "C"],
                   Mass=[1.5, 2.2, 1.1])
    df2 = DataFrame(Name=["A", "B", "C", "A"],
                    Quantity=[3, 3, 2, 4])
    @test leftjoin(df2, df, on=:Name) == DataFrame(Name=["A", "B", "C", "A"],
                                                   Quantity=[3, 3, 2, 4],
                                                   Mass=[1.5, 2.2, 1.1, 1.5])

    # Test that join works when mixing Array{Union{T, Missing}} with Array{T} (issue #1151)
    df = DataFrame([collect(1:10), collect(2:11)], [:x, :y])
    dfmissing = DataFrame(x=Vector{Union{Int, Missing}}(1:10),
                          z=Vector{Union{Int, Missing}}(3:12))
    @test innerjoin(df, dfmissing, on=:x) ==
        DataFrame([collect(1:10), collect(2:11), collect(3:12)], [:x, :y, :z])
    @test innerjoin(dfmissing, df, on=:x) ==
        DataFrame([Vector{Union{Int, Missing}}(1:10), Vector{Union{Int, Missing}}(3:12),
                collect(2:11)], [:x, :z, :y])
end

@testset "all joins" begin
    df1 = DataFrame(Any[[1, 3, 5], [1.0, 3.0, 5.0]], [:id, :fid])
    df2 = DataFrame(Any[[0, 1, 2, 3, 4], [0.0, 1.0, 2.0, 3.0, 4.0]], [:id, :fid])

    @test crossjoin(df1, df2, makeunique=true) ==
        DataFrame(Any[repeat([1, 3, 5], inner=5),
                      repeat([1, 3, 5], inner=5),
                      repeat([0, 1, 2, 3, 4], outer=3),
                      repeat([0, 1, 2, 3, 4], outer=3)],
                  [:id, :fid, :id_1, :fid_1])
    @test typeof.(eachcol(crossjoin(df1, df2, makeunique=true))) ==
        [Vector{Int}, Vector{Float64}, Vector{Int}, Vector{Float64}]

    i(on) = innerjoin(df1, df2, on=on, makeunique=true)
    l(on) = leftjoin(df1, df2, on=on, makeunique=true)
    r(on) = rightjoin(df1, df2, on=on, makeunique=true)
    o(on) = outerjoin(df1, df2, on=on, makeunique=true)
    s(on) = semijoin(df1, df2, on=on, makeunique=true)
    a(on) = antijoin(df1, df2, on=on, makeunique=true)

    @test s(:id) ==
          s(:fid) ==
          s([:id, :fid]) == DataFrame([[1, 3], [1, 3]], [:id, :fid])
    @test typeof.(eachcol(s(:id))) ==
          typeof.(eachcol(s(:fid))) ==
          typeof.(eachcol(s([:id, :fid]))) == [Vector{Int}, Vector{Float64}]
    @test a(:id) ==
          a(:fid) ==
          a([:id, :fid]) == DataFrame([[5], [5]], [:id, :fid])
    @test typeof.(eachcol(a(:id))) ==
          typeof.(eachcol(a(:fid))) ==
          typeof.(eachcol(a([:id, :fid]))) == [Vector{Int}, Vector{Float64}]

    on = :id
    @test i(on) == DataFrame([[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
    @test typeof.(eachcol(i(on))) == [Vector{Int}, Vector{Float64}, Vector{Float64}]
    @test l(on) ≅ DataFrame(id=[1, 3, 5],
                            fid=[1, 3, 5],
                            fid_1=[1, 3, missing])
    @test typeof.(eachcol(l(on))) ==
        [Vector{Int}, Vector{Float64}, Vector{Union{Float64, Missing}}]
    @test r(on) ≅ DataFrame(id=[1, 3, 0, 2, 4],
                            fid=[1, 3, missing, missing, missing],
                            fid_1=[1, 3, 0, 2, 4])
    @test typeof.(eachcol(r(on))) ==
        [Vector{Int}, Vector{Union{Float64, Missing}}, Vector{Float64}]
    @test o(on) ≅ DataFrame(id=[1, 3, 5, 0, 2, 4],
                            fid=[1, 3, 5, missing, missing, missing],
                            fid_1=[1, 3, missing, 0, 2, 4])
    @test typeof.(eachcol(o(on))) ==
        [Vector{Int}, Vector{Union{Float64, Missing}}, Vector{Union{Float64, Missing}}]

    on = :fid
    @test i(on) == DataFrame([[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
    @test typeof.(eachcol(i(on))) == [Vector{Int}, Vector{Float64}, Vector{Int}]
    @test l(on) ≅ DataFrame(id=[1, 3, 5],
                            fid=[1, 3, 5],
                            id_1=[1, 3, missing])
    @test typeof.(eachcol(l(on))) == [Vector{Int}, Vector{Float64},
                                     Vector{Union{Int, Missing}}]
    @test r(on) ≅ DataFrame(id=[1, 3, missing, missing, missing],
                            fid=[1, 3, 0, 2, 4],
                            id_1=[1, 3, 0, 2, 4])
    @test typeof.(eachcol(r(on))) == [Vector{Union{Int, Missing}}, Vector{Float64},
                                     Vector{Int}]
    @test o(on) ≅ DataFrame(id=[1, 3, 5, missing, missing, missing],
                            fid=[1, 3, 5, 0, 2, 4],
                            id_1=[1, 3, missing, 0, 2, 4])
    @test typeof.(eachcol(o(on))) == [Vector{Union{Int, Missing}}, Vector{Float64},
                                     Vector{Union{Int, Missing}}]

    on = [:id, :fid]
    @test i(on) == DataFrame([[1, 3], [1, 3]], [:id, :fid])
    @test typeof.(eachcol(i(on))) == [Vector{Int}, Vector{Float64}]
    @test l(on) == DataFrame(id=[1, 3, 5], fid=[1, 3, 5])
    @test typeof.(eachcol(l(on))) == [Vector{Int}, Vector{Float64}]
    @test r(on) == DataFrame(id=[1, 3, 0, 2, 4], fid=[1, 3, 0, 2, 4])
    @test typeof.(eachcol(r(on))) == [Vector{Int}, Vector{Float64}]
    @test o(on) == DataFrame(id=[1, 3, 5, 0, 2, 4], fid=[1, 3, 5, 0, 2, 4])
    @test typeof.(eachcol(o(on))) == [Vector{Int}, Vector{Float64}]
end

@testset "all joins with CategoricalArrays" begin
    df1 = DataFrame(Any[CategoricalArray([1, 3, 5]),
                        CategoricalArray([1.0, 3.0, 5.0])], [:id, :fid])
    df2 = DataFrame(Any[CategoricalArray([0, 1, 2, 3, 4]),
                        CategoricalArray([0.0, 1.0, 2.0, 3.0, 4.0])], [:id, :fid])

    @test crossjoin(df1, df2, makeunique=true) ==
        DataFrame([repeat([1, 3, 5], inner=5),
                   repeat([1, 3, 5], inner=5),
                   repeat([0, 1, 2, 3, 4], outer=3),
                   repeat([0, 1, 2, 3, 4], outer=3)],
                  [:id, :fid, :id_1, :fid_1])
    @test all(isa.(eachcol(crossjoin(df1, df2, makeunique=true)),
                   [CategoricalVector{T} for T in (Int, Float64, Int, Float64)]))

    i(on) = innerjoin(df1, df2, on=on, makeunique=true)
    l(on) = leftjoin(df1, df2, on=on, makeunique=true)
    r(on) = rightjoin(df1, df2, on=on, makeunique=true)
    o(on) = outerjoin(df1, df2, on=on, makeunique=true)
    s(on) = semijoin(df1, df2, on=on, makeunique=true)
    a(on) = antijoin(df1, df2, on=on, makeunique=true)

    @test s(:id) ==
          s(:fid) ==
          s([:id, :fid]) == DataFrame([[1, 3], [1, 3]], [:id, :fid])
    @test typeof.(eachcol(s(:id))) ==
          typeof.(eachcol(s(:fid))) ==
          typeof.(eachcol(s([:id, :fid])))
    @test all(isa.(eachcol(s(:id)),
                   [CategoricalVector{T} for T in (Int, Float64)]))

    @test a(:id) ==
          a(:fid) ==
          a([:id, :fid]) == DataFrame([[5], [5]], [:id, :fid])
    @test typeof.(eachcol(a(:id))) ==
          typeof.(eachcol(a(:fid))) ==
          typeof.(eachcol(a([:id, :fid])))
    @test all(isa.(eachcol(a(:id)),
                   [CategoricalVector{T} for T in (Int, Float64)]))

    on = :id
    @test i(on) == DataFrame([[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
    @test all(isa.(eachcol(i(on)),
                   [CategoricalVector{T} for T in (Int, Float64, Float64)]))
    @test l(on) ≅ DataFrame(id=[1, 3, 5],
                            fid=[1, 3, 5],
                            fid_1=[1, 3, missing])
    @test all(isa.(eachcol(l(on)),
                   [CategoricalVector{T} for T in (Int, Float64, Union{Float64, Missing})]))
    @test r(on) ≅ DataFrame(id=[1, 3, 0, 2, 4],
                            fid=[1, 3, missing, missing, missing],
                            fid_1=[1, 3, 0, 2, 4])
    @test all(isa.(eachcol(r(on)),
                   [CategoricalVector{T} for T in (Int, Union{Float64, Missing}, Float64)]))
    @test o(on) ≅ DataFrame(id=[1, 3, 5, 0, 2, 4],
                            fid=[1, 3, 5, missing, missing, missing],
                            fid_1=[1, 3, missing, 0, 2, 4])
    @test all(isa.(eachcol(o(on)),
                   [CategoricalVector{T} for T in (Int, Union{Float64, Missing}, Union{Float64, Missing})]))

    on = :fid
    @test i(on) == DataFrame([[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
    @test all(isa.(eachcol(i(on)),
                   [CategoricalVector{T} for T in (Int, Float64, Int)]))
    @test l(on) ≅ DataFrame(id=[1, 3, 5],
                            fid=[1, 3, 5],
                            id_1=[1, 3, missing])
    @test all(isa.(eachcol(l(on)),
                   [CategoricalVector{T} for T in (Int, Float64, Union{Int, Missing})]))
    @test r(on) ≅ DataFrame(id=[1, 3, missing, missing, missing],
                            fid=[1, 3, 0, 2, 4],
                            id_1=[1, 3, 0, 2, 4])
    @test all(isa.(eachcol(r(on)),
                   [CategoricalVector{T} for T in (Union{Int, Missing}, Float64, Int)]))
    @test o(on) ≅ DataFrame(id=[1, 3, 5, missing, missing, missing],
                            fid=[1, 3, 5, 0, 2, 4],
                            id_1=[1, 3, missing, 0, 2, 4])
    @test all(isa.(eachcol(o(on)),
                   [CategoricalVector{T} for T in (Union{Int, Missing}, Float64, Union{Int, Missing})]))

    on = [:id, :fid]
    @test i(on) == DataFrame([[1, 3], [1, 3]], [:id, :fid])
    @test all(isa.(eachcol(i(on)),
                   [CategoricalVector{T} for T in (Int, Float64)]))
    @test l(on) == DataFrame(id=[1, 3, 5],
                             fid=[1, 3, 5])
    @test all(isa.(eachcol(l(on)),
                   [CategoricalVector{T} for T in (Int, Float64)]))
    @test r(on) == DataFrame(id=[1, 3, 0, 2, 4],
                             fid=[1, 3, 0, 2, 4])
    @test all(isa.(eachcol(r(on)),
                   [CategoricalVector{T} for T in (Int, Float64)]))
    @test o(on) == DataFrame(id=[1, 3, 5, 0, 2, 4],
                             fid=[1, 3, 5, 0, 2, 4])
    @test all(isa.(eachcol(o(on)),
                   [CategoricalVector{T} for T in (Int, Float64)]))
end

@testset "maintain CategoricalArray levels ordering on join - non-`on` cols" begin
    A = DataFrame(a=[1, 2, 3], b=["a", "b", "c"])
    B = DataFrame(b=["a", "b", "c"], c=CategoricalVector(["a", "b", "b"]))
    levels!(B.c, ["b", "a"])
    @test levels(innerjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(innerjoin(B, A, on=:b).c) == ["b", "a"]
    @test levels(leftjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(rightjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(outerjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(semijoin(B, A, on=:b).c) == ["b", "a"]
end

@testset "maintain CategoricalArray levels ordering on join - ordering conflicts" begin
    A = DataFrame(a=[1, 2, 3, 4], b=CategoricalVector(["a", "b", "c", "d"]))
    levels!(A.b, ["d", "c", "b", "a"])
    B = DataFrame(b=CategoricalVector(["a", "b", "c"]), c=[5, 6, 7])
    @test levels(innerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(innerjoin(B, A, on=:b).b) == ["a", "b", "c"]
    @test levels(leftjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(leftjoin(B, A, on=:b).b) == ["a", "b", "c"]
    @test levels(rightjoin(A, B, on=:b).b) == ["a", "b", "c"]
    @test levels(rightjoin(B, A, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(outerjoin(B, A, on=:b).b) == ["d", "a", "b", "c"]
    @test levels(outerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(semijoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(semijoin(B, A, on=:b).b) == ["a", "b", "c"]
end

@testset "maintain CategoricalArray levels ordering on join - left is categorical" begin
    A = DataFrame(a=[1, 2, 3, 4], b=CategoricalVector(["a", "b", "c", "d"]))
    levels!(A.b, ["d", "c", "b", "a"])
    B = DataFrame(b=["a", "b", "c"], c=[5, 6, 7])
    @test levels(innerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(innerjoin(B, A, on=:b).b) == ["a", "b", "c"]
    @test levels(leftjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(leftjoin(B, A, on=:b).b) == ["a", "b", "c"]
    @test levels(rightjoin(A, B, on=:b).b) == ["a", "b", "c"]
    @test levels(rightjoin(B, A, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(outerjoin(A, B, on=:b).b) == ["a", "b", "c", "d"]
    @test levels(outerjoin(B, A, on=:b).b) == ["a", "b", "c", "d"]
    @test levels(semijoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(semijoin(B, A, on=:b).b) == ["a", "b", "c"]
end

@testset "join on columns with different left/right names" begin
    left = DataFrame(id=1:7, sid=string.(1:7))
    right = DataFrame(ID=3:10, SID=string.(3:10))

    @test innerjoin(left, right, on=:id => :ID) ==
        DataFrame(id=3:7, sid=string.(3:7), SID=string.(3:7))
    @test innerjoin(left, right, on=[:id => :ID]) ==
        DataFrame(id=3:7, sid=string.(3:7), SID=string.(3:7))
    @test innerjoin(left, right, on=[:id => :ID, :sid => :SID]) ==
        DataFrame(id=3:7, sid=string.(3:7))

    @test leftjoin(left, right, on=:id => :ID) ≅
        DataFrame(id=[3:7; 1:2], sid=string.([3:7; 1:2]),
                  SID=[string.(3:7)..., missing, missing])
    @test leftjoin(left, right, on=[:id => :ID]) ≅
        DataFrame(id=[3:7; 1:2], sid=string.([3:7; 1:2]),
                  SID=[string.(3:7)..., missing, missing])
    @test leftjoin(left, right, on=[:id => :ID, :sid => :SID]) ==
        DataFrame(id=[3:7; 1:2], sid=string.([3:7; 1:2]))

    @test rightjoin(left, right, on=:id => :ID) ≅
        DataFrame(id=3:10, sid=[string.(3:7)..., missing, missing, missing],
                 SID=string.(3:10))
    @test rightjoin(left, right, on=[:id => :ID]) ≅
        DataFrame(id=3:10, sid=[string.(3:7)..., missing, missing, missing],
                 SID=string.(3:10))
    @test rightjoin(left, right, on=[:id => :ID, :sid => :SID]) ≅
        DataFrame(id=3:10, sid=string.(3:10))

    @test outerjoin(left, right, on=:id => :ID) ≅
        DataFrame(id=[3:7; 1:2; 8:10], sid=[string.([3:7; 1:2])..., missing, missing, missing],
                  SID=[string.(3:7)..., missing, missing, string.(8:10)...])
    @test outerjoin(left, right, on=[:id => :ID]) ≅
        DataFrame(id=[3:7; 1:2; 8:10], sid=[string.([3:7; 1:2])..., missing, missing, missing],
                  SID=[string.(3:7)..., missing, missing, string.(8:10)...])
    @test outerjoin(left, right, on=[:id => :ID, :sid => :SID]) ≅
        DataFrame(id=[3:7; 1:2; 8:10], sid=string.([3:7; 1:2; 8:10]))

    @test semijoin(left, right, on=:id => :ID) ==
        DataFrame(id=3:7, sid=string.(3:7))
    @test semijoin(left, right, on=[:id => :ID]) ==
        DataFrame(id=3:7, sid=string.(3:7))
    @test semijoin(left, right, on=[:id => :ID, :sid => :SID]) ==
        DataFrame(id=3:7, sid=string.(3:7))

    @test antijoin(left, right, on=:id => :ID) ==
        DataFrame(id=1:2, sid=string.(1:2))
    @test antijoin(left, right, on=[:id => :ID]) ==
        DataFrame(id=1:2, sid=string.(1:2))
    @test antijoin(left, right, on=[:id => :ID, :sid => :SID]) ==
        DataFrame(id=1:2, sid=string.(1:2))

    @test_throws ArgumentError innerjoin(left, right, on=(:id, :ID))
end

@testset "join with a column of type Any" begin
    l = DataFrame(a=Any[1:7;], b=[1:7;])
    r = DataFrame(a=Any[3:10;], b=[3:10;])

    # join by :a and :b (Any is the on-column)
    @test innerjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=Any[3:7;], b=3:7)
    @test eltype.(eachcol(innerjoin(l, r, on=[:a, :b]))) == [Any, Int]

    @test leftjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=Any[3:7;1:2], b=[3:7; 1:2])
    @test eltype.(eachcol(leftjoin(l, r, on=[:a, :b]))) == [Any, Int]

    @test rightjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=Any[3:10;], b=3:10)
    @test eltype.(eachcol(rightjoin(l, r, on=[:a, :b]))) == [Any, Int]

    @test outerjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=Any[3:7; 1:2; 8:10], b=[3:7; 1:2; 8:10])
    @test eltype.(eachcol(outerjoin(l, r, on=[:a, :b]))) == [Any, Int]

    # join by :b (Any is not on-column)
    @test innerjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=Any[3:7;], b=3:7, a_1=Any[3:7;])
    @test eltype.(eachcol(innerjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]

    @test leftjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=Any[3:7; 1:2], b=[3:7; 1:2], a_1=[3:7; missing; missing])
    @test eltype.(eachcol(leftjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]

    @test rightjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=[3:7; fill(missing, 3)], b=3:10, a_1=Any[3:10;])
    @test eltype.(eachcol(rightjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]

    @test outerjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=[3:7; 1:2; missing; missing; missing], b=[3:7; 1:2; 8:10],
                  a_1=[3:7; missing; missing; 8:10])
    @test eltype.(eachcol(outerjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]
end

@testset "joins with categorical columns and no matching rows" begin
    l = DataFrame(a=1:3, b=categorical(["a", "b", "c"]))
    r = DataFrame(a=4:5, b=categorical(["d", "e"]))
    nl = size(l, 1)
    nr = size(r, 1)

    CS = eltype(l.b)

    # joins by a and b
    @test innerjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=Int[], b=similar(l.a, 0))
    @test eltype.(eachcol(innerjoin(l, r, on=[:a, :b]))) == [Int, CS]

    @test leftjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=l.a, b=l.b)
    @test eltype.(eachcol(leftjoin(l, r, on=[:a, :b]))) == [Int, CS]

    @test rightjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=r.a, b=r.b)
    @test eltype.(eachcol(rightjoin(l, r, on=[:a, :b]))) == [Int, CS]

    @test outerjoin(l, r, on=[:a, :b]) ≅
        DataFrame(a=vcat(l.a, r.a), b=vcat(l.b, r.b))
    @test eltype.(eachcol(outerjoin(l, r, on=[:a, :b]))) == [Int, CS]

    # joins by a
    @test innerjoin(l, r, on=:a, makeunique=true) ≅
        DataFrame(a=Int[], b=similar(l.b, 0), b_1=similar(r.b, 0))
    @test eltype.(eachcol(innerjoin(l, r, on=:a, makeunique=true))) == [Int, CS, CS]

    @test leftjoin(l, r, on=:a, makeunique=true) ≅
        DataFrame(a=l.a, b=l.b, b_1=similar_missing(r.b, nl))
    @test eltype.(eachcol(leftjoin(l, r, on=:a, makeunique=true))) ==
        [Int, CS, Union{CS, Missing}]

    @test rightjoin(l, r, on=:a, makeunique=true) ≅
        DataFrame(a=r.a, b=similar_missing(l.b, nr), b_1=r.b)
    @test eltype.(eachcol(rightjoin(l, r, on=:a, makeunique=true))) ==
        [Int, Union{CS, Missing}, CS]

    @test outerjoin(l, r, on=:a, makeunique=true) ≅
        DataFrame(a=vcat(l.a, r.a),
                  b=vcat(l.b, fill(missing, nr)),
                  b_1=vcat(fill(missing, nl), r.b))
    @test eltype.(eachcol(outerjoin(l, r, on=:a, makeunique=true))) ==
        [Int, Union{CS, Missing}, Union{CS, Missing}]

    # joins by b
    @test innerjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=Int[], b=similar(l.b, 0), a_1=similar(r.b, 0))
    @test eltype.(eachcol(innerjoin(l, r, on=:b, makeunique=true))) == [Int, CS, Int]

    @test leftjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=l.a, b=l.b, a_1=fill(missing, nl))
    @test eltype.(eachcol(leftjoin(l, r, on=:b, makeunique=true))) ==
        [Int, CS, Union{Int, Missing}]

    @test rightjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=fill(missing, nr), b=r.b, a_1=r.a)
    @test eltype.(eachcol(rightjoin(l, r, on=:b, makeunique=true))) ==
        [Union{Int, Missing}, CS, Int]

    @test outerjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=vcat(l.a, fill(missing, nr)),
                  b=vcat(l.b, r.b),
                  a_1=vcat(fill(missing, nl), r.a))
    @test eltype.(eachcol(outerjoin(l, r, on=:b, makeunique=true))) ==
        [Union{Int, Missing}, CS, Union{Int, Missing}]
end

@testset "source columns" begin
    outer_indicator = DataFrame(ID=[1, 2, 2, 3, 4],
                                Name=["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
                                Job=["Lawyer", "Doctor", "Florist", missing, "Farmer"],
                                _merge=["both", "both", "both", "left_only", "right_only"])

    # Check that input data frame isn't modified (#1434)
    pre_join_name = copy(name)
    pre_join_job = copy(job)
    @test outerjoin(name, job, on=:ID, source=:_merge,
               makeunique=true) ≅
          outerjoin(name, job, on=:ID, source="_merge",
               makeunique=true) ≅ outer_indicator

    @test name ≅ pre_join_name
    @test job ≅ pre_join_job

    # Works with conflicting names
    name2 = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"],
                     _left=[1, 1, 1])
    job2 = DataFrame(ID=[1, 2, 2, 4], Job=["Lawyer", "Doctor", "Florist", "Farmer"],
                    _left=[1, 1, 1, 1])

    outer_indicator = DataFrame(ID=[1, 2, 2, 3, 4],
                                Name=["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
                                _left=[1, 1, 1, 1, missing],
                                Job=["Lawyer", "Doctor", "Florist", missing, "Farmer"],
                                _left_1=[1, 1, 1, missing, 1],
                                _left_2=["both", "both", "both", "left_only", "right_only"])

    @test outerjoin(name2, job2, on=:ID, source=:_left,
               makeunique=true) ≅ outer_indicator
end

@testset "test checks of merge key uniqueness" begin
    @test_throws ArgumentError innerjoin(name, job, on=:ID, validate=(false, true))
    @test_throws ArgumentError innerjoin(name, job, on=:ID, validate=(true, true))
    @test_throws ArgumentError innerjoin(job, name, on=:ID, validate=(true, false))
    @test_throws ArgumentError innerjoin(job, name, on=:ID, validate=(true, true))
    @test_throws ArgumentError innerjoin(job, job, on=:ID, validate=(true, true))

    @test innerjoin(name, job, on=:ID, validate=(true, false)) == inner
    @test innerjoin(name, job, on=:ID, validate=(false, false)) == inner

    # Make sure ok with various special values
    for special in [missing, NaN, -0.0]
        name_w_special = DataFrame(ID=[1, 2, 3, special],
                                   Name=["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
        @test_throws ArgumentError innerjoin(name_w_special, job, on=:ID)
        @test_throws ArgumentError leftjoin(name_w_special, job, on=:ID)
        @test_throws ArgumentError rightjoin(name_w_special, job, on=:ID)
        @test_throws ArgumentError outerjoin(name_w_special, job, on=:ID)
        @test_throws ArgumentError semijoin(name_w_special, job, on=:ID)
        @test_throws ArgumentError antijoin(name_w_special, job, on=:ID)
    end

    for special in [missing, 0.0]
        name_w_special = DataFrame(ID=[1, 2, 3, special],
                                   Name=["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
        @test innerjoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅ inner
        @test leftjoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅
              vcat(left, DataFrame(ID=special, Name="Maria Tester", Job=missing))
        @test rightjoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅ right
        @test outerjoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal)[[1:4;6;5], :] ≅
              vcat(outer, DataFrame(ID=special, Name="Maria Tester", Job=missing))
        @test semijoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅ semi
        @test antijoin(name_w_special, job, on=:ID, validate=(true, false), matchmissing=:equal) ≅
              vcat(anti, DataFrame(ID=special, Name="Maria Tester"))

        # Make sure duplicated special values still an exception
        name_w_special_dups = DataFrame(ID=[1, 2, 3, special, special],
                                        Name=["John Doe", "Jane Doe", "Joe Blogs",
                                              "Maria Tester", "Jill Jillerson"])
        @test_throws ArgumentError innerjoin(name_w_special_dups, name, on=:ID,
                                        validate=(true, false), matchmissing=:equal)
    end

    for special in [NaN, -0.0]
        name_w_special = DataFrame(ID=categorical([1, 2, 3, special]),
                                   Name=["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
        @test innerjoin(name_w_special, transform(job, :ID => categorical => :ID), on=:ID, validate=(true, false)) == inner

        # Make sure duplicated special values still an exception
        name_w_special_dups = DataFrame(ID=categorical([1, 2, 3, special, special]),
                                        Name=["John Doe", "Jane Doe", "Joe Blogs",
                                              "Maria Tester", "Jill Jillerson"])
        @test_throws ArgumentError innerjoin(name_w_special_dups, transform(name, :ID => categorical => :ID), on=:ID,
                                        validate=(true, false))
    end

    # Check 0.0 and -0.0 seen as different
    name_w_zeros = DataFrame(ID=categorical([1, 2, 3, 0.0, -0.0]),
                             Name=["John Doe", "Jane Doe",
                                   "Joe Blogs", "Maria Tester",
                                   "Jill Jillerson"])
    name_w_zeros2 = DataFrame(ID=categorical([1, 2, 3, 0.0, -0.0]),
                              Name=["John Doe", "Jane Doe",
                                    "Joe Blogs", "Maria Tester",
                                    "Jill Jillerson"],
                              Name_1=["John Doe", "Jane Doe",
                                      "Joe Blogs", "Maria Tester",
                                      "Jill Jillerson"])

    @test innerjoin(name_w_zeros, name_w_zeros, on=:ID, validate=(true, true),
               makeunique=true) ≅ name_w_zeros2

    # Check for multiple-column merge keys
    name_multi = DataFrame(ID1=[1, 1, 2],
                           ID2=["a", "b", "a"],
                           Name=["John Doe", "Jane Doe", "Joe Blogs"])
    job_multi = DataFrame(ID1=[1, 2, 2, 4],
                          ID2=["a", "b", "b", "c"],
                          Job=["Lawyer", "Doctor", "Florist", "Farmer"])
    outer_multi = DataFrame(ID1=[1, 1, 2, 2, 2, 4],
                            ID2=["a", "b", "a", "b", "b", "c"],
                            Name=["John Doe", "Jane Doe", "Joe Blogs",
                                  missing, missing, missing],
                            Job=["Lawyer", missing, missing,
                                 "Doctor", "Florist",  "Farmer"])

     @test outerjoin(name_multi, job_multi, on=[:ID1, :ID2],
                validate=(true, false)) ≅ outer_multi
     @test_throws ArgumentError outerjoin(name_multi, job_multi, on=[:ID1, :ID2],
                                     validate=(false, true))
end

@testset "consistency" begin
    # Join on symbols or vectors of symbols
    cname = copy(name)
    cjob = copy(job)
    push!(cname[!, 1], cname[1, 1])
    @test_throws AssertionError innerjoin(cname, cjob, on=:ID)

    cname = copy(name)
    cjob = copy(job)
    push!(cjob[!, 1], cjob[1, 1])
    @test_throws AssertionError innerjoin(cname, cjob, on=:ID)

    cname = copy(name)
    push!(DataFrames._columns(cname), cname[:, 1])
    @test_throws AssertionError innerjoin(cname, cjob, on=:ID)
end

@testset "multi data frame join" begin
    df1 = DataFrame(id=[1, 2, 3], x=[1, 2, 3])
    df2 = DataFrame(id=[1, 2, 4], y=[1, 2, 4])
    df3 = DataFrame(id=[1, 3, 4], z=[1, 3, 4])
    @test innerjoin(df1, df2, df3, on=:id) == DataFrame(id=1, x=1, y=1, z=1)
    @test outerjoin(df1, df2, df3, on=:id) ≅ DataFrame(id=[1, 3, 4, 2],
                                                       x=[1, 3, missing, 2],
                                                       y=[1, missing, 4, 2],
                                                       z=[1, 3, 4, missing])
    @test_throws MethodError leftjoin(df1, df2, df3, on=:id)
    @test_throws MethodError rightjoin(df1, df2, df3, on=:id)
    @test_throws MethodError semijoin(df1, df2, df3, on=:id)
    @test_throws MethodError antijoin(df1, df2, df3, on=:id)

    dfc = crossjoin(df1, df2, df3, makeunique=true)
    @test dfc.x == dfc.id == repeat(1:3, inner=9)
    @test dfc.y == dfc.id_1 == repeat([1, 2, 4], inner=3, outer=3)
    @test dfc.z == dfc.id_2 == repeat([1, 3, 4], outer=9)

    df3[1, 1] = 4
    @test_throws ArgumentError innerjoin(df1, df2, df3, on=:id, validate=(true, true))
end

@testset "flexible on in join" begin
    df1 = DataFrame(id=[1, 2, 3], id2=[11, 12, 13], x=[1, 2, 3])
    df2 = DataFrame(id=[1, 2, 4], ID2=[11, 12, 14], y=[1, 2, 4])
    @test innerjoin(df1, df2, on=[:id, :id2=>:ID2]) == DataFrame(id=[1, 2], id2=[11, 12],
                                                                 x=[1, 2], y=[1, 2])
    @test innerjoin(df1, df2, on=[:id2=>:ID2, :id]) == DataFrame(id=[1, 2], id2=[11, 12],
                                                                 x=[1, 2], y=[1, 2])
    @test innerjoin(df1, df2, on=[:id=>:id, :id2=>:ID2]) == DataFrame(id=[1, 2], id2=[11, 12],
                                                                      x=[1, 2], y=[1, 2])
    @test innerjoin(df1, df2, on=[:id2=>:ID2, :id=>:id]) == DataFrame(id=[1, 2], id2=[11, 12],
                                                                      x=[1, 2], y=[1, 2])
end

@testset "check naming of source" begin
    df = DataFrame(a=1)
    @test_throws ArgumentError outerjoin(df, df, on=:a, source=:a)
    @test outerjoin(df, df, on=:a, source=:a, makeunique=true) == DataFrame(a=1, a_1="both")
    @test outerjoin(df, df, on=:a, source="_left") == DataFrame(a=1, _left="both")
    @test outerjoin(df, df, on=:a, source="_right") == DataFrame(a=1, _right="both")

    df = DataFrame(_left=1)
    @test outerjoin(df, df, on=:_left, source="_leftX") == DataFrame(_left=1, _leftX="both")
    df = DataFrame(_right=1)
    @test outerjoin(df, df, on=:_right, source="_rightX") == DataFrame(_right=1, _rightX="both")
end

@testset "validate error message composition" begin
    for validate in ((true, false), (false, true), (true, true)),
        a in ([1; 1], [1:2; 1:2], [1:3; 1:3]),
        on in ([:a], [:a, :b])
        df = DataFrame(a=a, b=1, c=1)
        @test_throws ArgumentError outerjoin(df, df, on=on, validate=validate)
    end
    for validate in ((true, false), (false, true), (true, true)),
        a in ([1; 1], [1:2; 1:2], [1:3; 1:3]),
        on in ([:a=>:d], [:a => :d, :b])
        df1 = DataFrame(a=a, b=1, c=1)
        df2 = DataFrame(d=a, b=1, c=1)
        @test_throws ArgumentError outerjoin(df1, df2, on=on, validate=validate)
    end

    # make sure we do not error when we should not
    for validate in ((false, false), (true, false), (false, true), (true, true))
        df1 = DataFrame(a=1, b=1)
        df2 = DataFrame(d=1, b=1)
        @test outerjoin(df1, df1, on=[:a, :b], validate=validate) == df1
        @test outerjoin(df1, df2, on=[:a => :d, :b], validate=validate) == df1
    end
    df1 = DataFrame(a=[1, 1], b=1)
    df2 = DataFrame(d=1, b=1)
    @test outerjoin(df1, df2, on=[:a => :d, :b], validate=(false, true)) == df1
    df1 = DataFrame(a=1, b=1)
    df2 = DataFrame(d=[1, 1], b=1)
    @test outerjoin(df1, df2, on=[:a => :d, :b], validate=(true, false)) == [df1; df1]
    df1 = DataFrame(a=[1, 1], b=1)
    df2 = DataFrame(d=[1, 1], b=1)
    @test outerjoin(df1, df2, on=[:a => :d, :b], validate=(false, false)) == [df1; df1]
end

@testset "renamecols tests" begin
    df1 = DataFrame(id1=[1, 2, 3], id2=[1, 2, 3], x=1:3)
    df2 = DataFrame(id1=[1, 2, 4], ID2=[1, 2, 4], x=1:3)

    @test_throws ArgumentError innerjoin(df1, df2, on=:id1, renamecols=1=>1, makeunique=true)
    @test_throws ArgumentError leftjoin(df1, df2, on=:id1, renamecols=1=>1, makeunique=true)
    @test_throws ArgumentError rightjoin(df1, df2, on=:id1, renamecols=1=>1, makeunique=true)
    @test_throws ArgumentError outerjoin(df1, df2, on=:id1, renamecols=1=>1, makeunique=true)

    @test_throws ArgumentError innerjoin(df1, df2, on=:id1)
    @test innerjoin(df1, df2, on=:id1, makeunique=true) ==
        DataFrame(id1=[1, 2], id2=[1, 2], x=[1, 2], ID2=[1, 2], x_1=[1, 2])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test innerjoin(df1, df2, on=:id1,
                        makeunique = mu, validate = vl => vr, renamecols = l => r) ==
            DataFrame(id1=[1, 2], id2_left=[1, 2], x_left=[1, 2], ID2_right=[1, 2], x_right=[1, 2])
    end

    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2])
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], makeunique=true) ==
        DataFrame(id1=[1, 2], id2=[1, 2], x=[1, 2], x_1=[1, 2])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                        makeunique = mu, validate = vl => vr, renamecols = l => r) ==
            DataFrame(id1=[1, 2], id2=[1, 2], x_left=[1, 2], x_right=[1, 2])
    end

    @test_throws ArgumentError leftjoin(df1, df2, on=:id1)
    @test leftjoin(df1, df2, on=:id1, makeunique=true) ≅
        DataFrame(id1=[1, 2, 3], id2=[1, 2, 3], x=[1, 2, 3], ID2=[1, 2, missing], x_1=[1, 2, missing])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test leftjoin(df1, df2, on=:id1,
                       makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
            DataFrame(id1=[1, 2, 3], id2_left=[1, 2, 3], x_left=[1, 2, 3],
                      ID2_right=[1, 2, missing], x_right=[1, 2, missing])
    end

    @test_throws ArgumentError leftjoin(df1, df2, on=[:id1, :id2 => :ID2])
    @test leftjoin(df1, df2, on=[:id1, :id2 => :ID2], makeunique=true) ≅
        DataFrame(id1=[1, 2, 3], id2=[1, 2, 3], x=[1, 2, 3], x_1=[1, 2, missing])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test leftjoin(df1, df2, on=[:id1, :id2 => :ID2],
                       makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
            DataFrame(id1=[1, 2, 3], id2=[1, 2, 3], x_left=[1, 2, 3], x_right=[1, 2, missing])
    end

    @test_throws ArgumentError leftjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                        renamecols = "_left" => "_right", source=:id1)
    @test_throws ArgumentError leftjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                        renamecols = "_left" => "_right", source=:x_left)
    @test leftjoin(df1, df2, on=[:id1, :id2 => :ID2],
                   renamecols = "_left" => "_right", source=:ind) ≅
          DataFrame(id1=[1, 2, 3], id2=[1, 2, 3], x_left=[1, 2, 3],
                    x_right=[1, 2, missing], ind=["both", "both", "left_only"])

    @test_throws ArgumentError rightjoin(df1, df2, on=:id1)
    @test rightjoin(df1, df2, on=:id1, makeunique=true) ≅
        DataFrame(id1=[1, 2, 4], id2=[1, 2, missing], x=[1, 2, missing], ID2=[1, 2, 4], x_1=[1, 2, 3])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test rightjoin(df1, df2, on=:id1,
                       makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
            DataFrame(id1=[1, 2, 4], id2_left=[1, 2, missing], x_left=[1, 2, missing],
                      ID2_right=[1, 2, 4], x_right=[1, 2, 3])
    end

    @test_throws ArgumentError rightjoin(df1, df2, on=[:id1, :id2 => :ID2])
    @test rightjoin(df1, df2, on=[:id1, :id2 => :ID2], makeunique=true) ≅
        DataFrame(id1=[1, 2, 4], id2=[1, 2, 4], x=[1, 2, missing], x_1=[1, 2, 3])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test rightjoin(df1, df2, on=[:id1, :id2 => :ID2],
                       makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
            DataFrame(id1=[1, 2, 4], id2=[1, 2, 4], x_left=[1, 2, missing], x_right=[1, 2, 3])
    end

    @test_throws ArgumentError rightjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                         renamecols = "_left" => "_right", source=:id1)
    @test_throws ArgumentError rightjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                         renamecols = "_left" => "_right", source=:x_left)
    @test rightjoin(df1, df2, on=[:id1, :id2 => :ID2],
                    renamecols = "_left" => "_right", source=:ind) ≅
          DataFrame(id1=[1, 2, 4], id2=[1, 2, 4], x_left=[1, 2, missing],
                    x_right=[1, 2, 3], ind=["both", "both", "right_only"])

    @test_throws ArgumentError outerjoin(df1, df2, on=:id1)
    @test outerjoin(df1, df2, on=:id1, makeunique=true) ≅
        DataFrame(id1=[1, 2, 3, 4], id2=[1, 2, 3, missing], x=[1, 2, 3, missing],
                  ID2=[1, 2, missing, 4], x_1=[1, 2, missing, 3])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test outerjoin(df1, df2, on=:id1,
                       makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
            DataFrame(id1=[1, 2, 3, 4], id2_left=[1, 2, 3, missing], x_left=[1, 2, 3, missing],
                      ID2_right=[1, 2, missing, 4], x_right=[1, 2, missing, 3])
    end

    @test_throws ArgumentError outerjoin(df1, df2, on=[:id1, :id2 => :ID2])
    @test outerjoin(df1, df2, on=[:id1, :id2 => :ID2], makeunique=true) ≅
        DataFrame(id1=[1, 2, 3, 4], id2=[1, 2, 3, 4], x=[1, 2, 3, missing], x_1=[1, 2, missing, 3])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test outerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                       makeunique = mu, validate = vl => vr, renamecols = l => r) ≅
            DataFrame(id1=[1, 2, 3, 4], id2=[1, 2, 3, 4], x_left=[1, 2, 3, missing], x_right=[1, 2, missing, 3])
    end

    @test_throws ArgumentError outerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                         renamecols = "_left" => "_right", source=:id1)
    @test_throws ArgumentError outerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                         renamecols = "_left" => "_right", source=:x_left)
    @test outerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                    renamecols = "_left" => "_right", source=:ind) ≅
          DataFrame(id1=[1, 2, 3, 4], id2=[1, 2, 3, 4], x_left=[1, 2, 3, missing],
                    x_right=[1, 2, missing, 3], ind=["both", "both", "left_only", "right_only"])

    df1.x .+= 10
    df2.x .+= 100
    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2], renamecols = (x -> :id1) => "_right")
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], renamecols = (x -> :id1) => "_right", makeunique=true) ==
          DataFrame(id1=1:2, id2=1:2, id1_1=11:12, x_right=101:102)
    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2], renamecols = "_left" => (x -> :id2))
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], renamecols = "_left" => (x -> :id2), makeunique=true) ==
          DataFrame(id1=1:2, id2=1:2, x_left=11:12, id2_1=101:102)
    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2], renamecols = "_left" => "_left")
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], renamecols = "_left" => "_left", makeunique=true) ==
          DataFrame(id1=1:2, id2=1:2, x_left=11:12, x_left_1=101:102)
    df2.y = df2.x .+ 1
    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2], renamecols = "_left" => (x -> :newcol))
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], renamecols = "_left" => (x -> :newcol), makeunique=true) ==
          DataFrame(id1=1:2, id2=1:2, x_left=11:12, newcol=101:102, newcol_1=102:103)
end

@testset "careful source test" begin
    Random.seed!(1234)
    for i in 5:15, j in 5:15
        df1 = DataFrame(id=rand(1:10, i), x=1:i)
        df2 = DataFrame(id=rand(1:10, j), y=1:j)
        dfi = innerjoin(df1, df2, on=:id)
        dfl = leftjoin(df1, df2, on=:id, source=:ind)
        dfr = rightjoin(df1, df2, on=:id, source=:ind)
        dfo = outerjoin(df1, df2, on=:id, source=:ind)
        @test issorted(dfl, :ind)
        @test issorted(dfr, :ind)
        @test issorted(dfo, :ind)

        @test all(==("both"), dfl[1:nrow(dfi), :ind])
        @test dfl[1:nrow(dfi), 1:3] ≅ dfi
        @test all(==("left_only"), dfl[nrow(dfi)+1:end, :ind])

        @test all(==("both"), dfr[1:nrow(dfi), :ind])
        @test dfr[1:nrow(dfi), 1:3] ≅ dfi
        @test all(==("right_only"), dfr[nrow(dfi)+1:end, :ind])

        @test all(==("both"), dfo[1:nrow(dfi), :ind])
        @test dfl ≅ dfo[1:nrow(dfl), :]
        @test all(==("right_only"), dfo[nrow(dfl)+1:end, :ind])
    end
end

@testset "removed join function" begin
    df1 = DataFrame(id=[1, 2, 3], x=[1, 2, 3])
    df2 = DataFrame(id=[1, 2, 4], y=[1, 2, 4])
    df3 = DataFrame(id=[1, 3, 4], z=[1, 3, 4])
    @test_throws ArgumentError join(df1, df2, df3, on=:id, kind=:left)
    @test_throws ArgumentError join(df1, df2, on=:id, kind=:inner)
end

@testset "join mixing DataFrame and SubDataFrame" begin
    df1 = DataFrame(a=[1, 2, 3], b=[4, 5, 6])
    df1_copy = df1[df1.a .> 1, :]
    df1_view1 = @view df1[df1.a .> 1, :]
    df1_view2 = @view df1[df1.a .> 1, 1:2]
    df2 = DataFrame(a=[1, 2, 3], c=[7, 8, 9])
    @test innerjoin(df1_copy, df2, on=:a) ==
          innerjoin(df1_view1, df2, on=:a) ==
          innerjoin(df1_view2, df2, on=:a)
end

@testset "OnCol correctness tests" begin
    Random.seed!(1234)
    c1 = collect(1:10^2)
    c2 = collect(Float64, 1:10^2)
    c3 = collect(sort(string.(1:10^2)))
    c4 = repeat(1:10, inner=10)
    c5 = collect(Float64, repeat(1:50, inner=2))
    c6 = sort(string.(repeat(1:25,inner=4)))
    c7 = repeat(20:-1:1, inner=5)

    @test_throws AssertionError OnCol()
    @test_throws AssertionError OnCol(c1)
    @test_throws AssertionError OnCol(c1, [1])
    @test_throws MethodError OnCol(c1, 1)

    oncols = [OnCol(c1, c2), OnCol(c3, c4), OnCol(c5, c6), OnCol(c1, c2, c3),
              OnCol(c2, c3, c4), OnCol(c4, c5, c6), OnCol(c1, c2, c3, c4),
              OnCol(c2, c3, c4, c5), OnCol(c3, c4, c5, c6), OnCol(c1, c2, c3, c4, c5),
              OnCol(c2, c3, c4, c5, c6), OnCol(c1, c2, c3, c4, c5, c6),
              OnCol(c4, c7), OnCol(c4, c5, c7), OnCol(c4, c5, c6, c7)]
    tupcols = [tuple.(c1, c2), tuple.(c3, c4), tuple.(c5, c6), tuple.(c1, c2, c3),
               tuple.(c2, c3, c4), tuple.(c4, c5, c6), tuple.(c1, c2, c3, c4),
               tuple.(c2, c3, c4, c5), tuple.(c3, c4, c5, c6), tuple.(c1, c2, c3, c4, c5),
               tuple.(c2, c3, c4, c5, c6), tuple.(c1, c2, c3, c4, c5, c6),
               tuple.(c4, c7), tuple.(c4, c5, c7), tuple.(c4, c5, c6, c7)]

    for (oncol, tupcol) in zip(oncols, tupcols)
        @test issorted(oncol) == issorted(tupcol)
        @test IndexStyle(oncol) === IndexLinear()
        @test_throws MethodError oncol[1] == oncol[2]
    end

    for i in eachindex(c1), j in eachindex(oncols, tupcols)
        @test_throws MethodError hash(oncols[j][1], zero(UInt))
        DataFrames._prehash(oncols[j])
        @test hash(oncols[j][i]) == hash(tupcols[j][i])
        for k in eachindex(c1)
            @test isequal(oncols[j][i], oncols[j][k]) == isequal(tupcols[j][i], tupcols[j][k])
            @test isequal(oncols[j][k], oncols[j][i]) == isequal(tupcols[j][k], tupcols[j][i])
            @test isless(oncols[j][i], oncols[j][k]) == isless(tupcols[j][i], tupcols[j][k])
            @test isless(oncols[j][k], oncols[j][i]) == isless(tupcols[j][k], tupcols[j][i])
        end
    end

    foreach(shuffle!, [c1, c2, c3, c4, c5, c6])

    tupcols = [tuple.(c1, c2), tuple.(c3, c4), tuple.(c5, c6), tuple.(c1, c2, c3),
               tuple.(c2, c3, c4), tuple.(c4, c5, c6), tuple.(c1, c2, c3, c4),
               tuple.(c2, c3, c4, c5), tuple.(c3, c4, c5, c6), tuple.(c1, c2, c3, c4, c5),
               tuple.(c2, c3, c4, c5, c6), tuple.(c1, c2, c3, c4, c5, c6),
               tuple.(c4, c7), tuple.(c4, c5, c7), tuple.(c4, c5, c6, c7)]

    for i in eachindex(c1), j in eachindex(oncols, tupcols)
        DataFrames._prehash(oncols[j])
        @test hash(oncols[j][i]) == hash(tupcols[j][i])
        for k in eachindex(c1)
            @test isequal(oncols[j][i], oncols[j][k]) == isequal(tupcols[j][i], tupcols[j][k])
            @test isequal(oncols[j][k], oncols[j][i]) == isequal(tupcols[j][k], tupcols[j][i])
            @test isless(oncols[j][i], oncols[j][k]) == isless(tupcols[j][i], tupcols[j][k])
            @test isless(oncols[j][k], oncols[j][i]) == isless(tupcols[j][k], tupcols[j][i])
        end
    end
end

@testset "join correctness tests" begin

    @test_throws ArgumentError DataFrames.prepare_on_col()

    function test_join(df1, df2)
        @assert names(df1) == ["id", "x"]
        @assert names(df2) == ["id", "y"]

        df_inner = DataFrame(id=[], x=[], y=[])
        for i in axes(df1, 1), j in axes(df2, 1)
            if isequal(df1.id[i], df2.id[j])
                v = df1.id[i] isa CategoricalValue ? unwrap(df1.id[i]) : df1.id[i]
                push!(df_inner, (id=v, x=df1.x[i], y=df2.y[j]))
            end
        end

        df_left_part = DataFrame(id=[], x=[], y=[])
        for i in axes(df1, 1)
            if !(df1.id[i] in Set(df2.id))
                v = df1.id[i] isa CategoricalValue ? unwrap(df1.id[i]) : df1.id[i]
                push!(df_left_part, (id=v, x=df1.x[i], y=missing))
            end
        end

        df_right_part = DataFrame(id=[], x=[], y=[])
        for i in axes(df2, 1)
            if !(df2.id[i] in Set(df1.id))
                v = df2.id[i] isa CategoricalValue ? unwrap(df2.id[i]) : df2.id[i]
                push!(df_right_part, (id=v, x=missing, y=df2.y[i]))
            end
        end

        df_left = vcat(df_inner, df_left_part)
        df_right = vcat(df_inner, df_right_part)
        df_outer = vcat(df_inner, df_left_part, df_right_part)

        df_semi = df1[[x in Set(df2.id) for x in df1.id], :]
        df_anti = df1[[!(x in Set(df2.id)) for x in df1.id], :]

        df1x = copy(df1)
        df1x.id2 = copy(df1x.id)
        df2x = copy(df2)
        df2x.id2 = copy(df2x.id)

        df1x2 = copy(df1x)
        df1x2.id3 = copy(df1x2.id)
        df2x2 = copy(df2x)
        df2x2.id3 = copy(df2x2.id)

        sort!(df_inner, [:x, :y])
        sort!(df_left, [:x, :y])
        sort!(df_right, [:x, :y])
        sort!(df_outer, [:x, :y])

        df_inner2 = copy(df_inner)
        df_left2 = copy(df_left)
        df_right2 = copy(df_right)
        df_outer2 = copy(df_outer)
        df_semi2 = copy(df_semi)
        df_anti2 = copy(df_anti)
        insertcols!(df_inner2, 3, :id2 => df_inner2.id)
        insertcols!(df_left2, 3, :id2 => df_left2.id)
        insertcols!(df_right2, 3, :id2 => df_right2.id)
        insertcols!(df_outer2, 3, :id2 => df_outer2.id)
        insertcols!(df_semi2, 3, :id2 => df_semi2.id)
        insertcols!(df_anti2, 3, :id2 => df_anti2.id)
        df_inner3 = copy(df_inner2)
        df_left3 = copy(df_left2)
        df_right3 = copy(df_right2)
        df_outer3 = copy(df_outer2)
        df_semi3 = copy(df_semi2)
        df_anti3 = copy(df_anti2)
        insertcols!(df_inner3, 4, :id3 => df_inner3.id)
        insertcols!(df_left3, 4, :id3 => df_left3.id)
        insertcols!(df_right3, 4, :id3 => df_right3.id)
        insertcols!(df_outer3, 4, :id3 => df_outer3.id)
        insertcols!(df_semi3, 4, :id3 => df_semi3.id)
        insertcols!(df_anti3, 4, :id3 => df_anti3.id)

        test_leftjoin! =
            (any(nonunique(df2, :id)) ||
             df_left ≅ sort(leftjoin!(copy(df1), df2, on=:id, matchmissing=:equal), [:x, :y])) &&
            (any(nonunique(df2x, [:id, :id2])) ||
             df_left2 ≅ sort(leftjoin!(copy(df1x), df2x, on=[:id, :id2], matchmissing=:equal), [:x, :y])) &&
            (any(nonunique(df2x2, [:id, :id2, :id3])) ||
             df_left3 ≅ sort(leftjoin!(copy(df1x2), df2x2, on=[:id, :id2, :id3], matchmissing=:equal), [:x, :y]))

        return df_inner ≅ sort(innerjoin(df1, df2, on=:id, matchmissing=:equal), [:x, :y]) &&
               df_inner2 ≅ sort(innerjoin(df1x, df2x, on=[:id, :id2], matchmissing=:equal), [:x, :y]) &&
               df_inner3 ≅ sort(innerjoin(df1x2, df2x2, on=[:id, :id2, :id3], matchmissing=:equal), [:x, :y]) &&
               df_left ≅ sort(leftjoin(df1, df2, on=:id, matchmissing=:equal), [:x, :y]) &&
               df_left2 ≅ sort(leftjoin(df1x, df2x, on=[:id, :id2], matchmissing=:equal), [:x, :y]) &&
               df_left3 ≅ sort(leftjoin(df1x2, df2x2, on=[:id, :id2, :id3], matchmissing=:equal), [:x, :y]) &&
               test_leftjoin! &&
               df_right ≅ sort(rightjoin(df1, df2, on=:id, matchmissing=:equal), [:x, :y]) &&
               df_right2 ≅ sort(rightjoin(df1x, df2x, on=[:id, :id2], matchmissing=:equal), [:x, :y]) &&
               df_right3 ≅ sort(rightjoin(df1x2, df2x2, on=[:id, :id2, :id3], matchmissing=:equal), [:x, :y]) &&
               df_outer ≅ sort(outerjoin(df1, df2, on=:id, matchmissing=:equal), [:x, :y]) &&
               df_outer2 ≅ sort(outerjoin(df1x, df2x, on=[:id, :id2], matchmissing=:equal), [:x, :y]) &&
               df_outer3 ≅ sort(outerjoin(df1x2, df2x2, on=[:id, :id2, :id3], matchmissing=:equal), [:x, :y]) &&
               df_semi ≅ semijoin(df1, df2, on=:id, matchmissing=:equal) &&
               df_semi2 ≅ semijoin(df1x, df2x, on=[:id, :id2], matchmissing=:equal) &&
               df_semi3 ≅ semijoin(df1x2, df2x2, on=[:id, :id2, :id3], matchmissing=:equal) &&
               df_anti ≅ antijoin(df1, df2, on=:id, matchmissing=:equal) &&
               df_anti2 ≅ antijoin(df1x, df2x, on=[:id, :id2], matchmissing=:equal) &&
               df_anti3 ≅ antijoin(df1x2, df2x2, on=[:id, :id2, :id3], matchmissing=:equal)
    end

    Random.seed!(1234)
    for i in 1:5, j in 0:2
        for df1 in [DataFrame(id=rand(1:i+j, i+j), x=1:i+j), DataFrame(id=rand(1:i, i), x=1:i),
                    DataFrame(id=[rand(1:i+j, i+j); missing], x=1:i+j+1),
                    DataFrame(id=[rand(1:i, i); missing], x=1:i+1)],
            df2 in [DataFrame(id=rand(1:i+j, i+j), y=1:i+j), DataFrame(id=rand(1:i, i), y=1:i),
                    DataFrame(id=[rand(1:i+j, i+j); missing], y=1:i+j+1),
                    DataFrame(id=[rand(1:i, i); missing], y=1:i+1)]
            for opleft = [identity, sort, x -> unique(x, :id), x -> sort(unique(x, :id))],
                opright = [identity, sort, x -> unique(x, :id), x -> sort(unique(x, :id))]

                # integers
                @test test_join(opleft(df1), opright(df2))
                @test test_join(opleft(df1), opright(rename(df1, :x => :y)))

                # strings
                df1s = copy(df1)
                df1s[!, 1] = passmissing(string).(df1s[!, 1])
                df2s = copy(df2)
                df2s[!, 1] = passmissing(string).(df2s[!, 1])
                @test test_join(opleft(df1s), opright(df2s))
                @test test_join(opleft(df1s), opright(rename(df1s, :x => :y)))

                # PooledArrays
                df1p = copy(df1)
                df1p[!, 1] = PooledArray(df1p[!, 1])
                df2p = copy(df2)
                df2p[!, 1] = PooledArray(df2p[!, 1])
                @test test_join(opleft(df1), opright(df2p))
                @test test_join(opleft(df1p), opright(df2))
                @test test_join(opleft(df1p), opright(df2p))
                @test test_join(opleft(df1p), opright(rename(df1p, :x => :y)))

                # add unused level
                df1p[1, 1] = 0
                df2p[1, 1] = 0
                df1p[1, 1] = 1
                df2p[1, 1] = 1
                @test test_join(opleft(df1), opright(df2p))
                @test test_join(opleft(df1p), opright(df2))
                @test test_join(opleft(df1p), opright(df2p))
                @test test_join(opleft(df1p), opright(rename(df1p, :x => :y)))

                # CategoricalArrays
                df1c = copy(df1)
                df1c[!, 1] = categorical(df1c[!, 1])
                df2c = copy(df2)
                df2c[!, 1] = categorical(df2c[!, 1])
                @test test_join(opleft(df1), opright(df2c))
                @test test_join(opleft(df1c), opright(df2c))
                @test test_join(opleft(df1c), opright(df2))
                @test test_join(opleft(df1c), opright(rename(df1c, :x => :y)))
                @test test_join(opleft(df1p), opright(df2c))
                @test test_join(opleft(df1c), opright(df2p))

                # add unused level
                df1c[1, 1] = 0
                df2c[1, 1] = 0
                df1c[1, 1] = 1
                df2c[1, 1] = 1
                @test test_join(opleft(df1), opright(df2c))
                @test test_join(opleft(df1c), opright(df2c))
                @test test_join(opleft(df1c), opright(df2))
                @test test_join(opleft(df1c), opright(rename(df1c, :x => :y)))
                @test test_join(opleft(df1p), opright(df2c))
                @test test_join(opleft(df1c), opright(df2p))
            end
        end
    end

    # some special cases
    @test isequal_coltyped(innerjoin(DataFrame(id=[]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(semijoin(DataFrame(id=[]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(antijoin(DataFrame(id=[]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[]))

    @test isequal_coltyped(innerjoin(DataFrame(id=[]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=[1, 2, 3]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=Any[1, 2, 3]))
    @test isequal_coltyped(semijoin(DataFrame(id=[]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(antijoin(DataFrame(id=[]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=[]))

    @test isequal_coltyped(innerjoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[]), on=:id),
                           DataFrame(id=Int[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[1, 2, 3]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[]), on=:id),
                           DataFrame(id=Any[]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[]), on=:id),
                           DataFrame(id=Any[1, 2, 3]))
    @test isequal_coltyped(semijoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[]), on=:id),
                           DataFrame(id=Int[]))
    @test isequal_coltyped(antijoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[1, 2, 3]))

    @test isequal_coltyped(innerjoin(DataFrame(id=[4, 5, 6]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=Int[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[4, 5, 6]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=Int[4, 5, 6]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[4, 5, 6]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=Int[1, 2, 3]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[4, 5, 6]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=Int[4, 5, 6, 1, 2, 3]))
    @test isequal_coltyped(semijoin(DataFrame(id=[4, 5, 6]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=Int[]))
    @test isequal_coltyped(antijoin(DataFrame(id=[4, 5, 6]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=[4, 5, 6]))

    @test isequal_coltyped(innerjoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[4, 5, 6]), on=:id),
                           DataFrame(id=Int[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[4, 5, 6]), on=:id),
                           DataFrame(id=Int[1, 2, 3]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[4, 5, 6]), on=:id),
                           DataFrame(id=Int[4, 5, 6]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[4, 5, 6]), on=:id),
                           DataFrame(id=Int[1, 2, 3, 4, 5, 6]))
    @test isequal_coltyped(semijoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[4, 5, 6]), on=:id),
                           DataFrame(id=Int[]))
    @test isequal_coltyped(antijoin(DataFrame(id=[1, 2, 3]), DataFrame(id=[4, 5, 6]), on=:id),
                           DataFrame(id=[1, 2, 3]))

    @test isequal_coltyped(innerjoin(DataFrame(id=[missing]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Missing[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[missing]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=[missing]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[missing]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=[1]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[missing]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=[missing, 1]))
    @test isequal_coltyped(semijoin(DataFrame(id=[missing]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Missing[]))
    @test isequal_coltyped(antijoin(DataFrame(id=[missing]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=[missing]))

    @test isequal_coltyped(innerjoin(DataFrame(id=Missing[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Missing[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=Missing[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Missing[]))
    @test isequal_coltyped(rightjoin(DataFrame(id=Missing[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=[1]))
    @test isequal_coltyped(outerjoin(DataFrame(id=Missing[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[1]))
    @test isequal_coltyped(semijoin(DataFrame(id=Missing[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Missing[]))
    @test isequal_coltyped(antijoin(DataFrame(id=Missing[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Missing[]))

    @test isequal_coltyped(innerjoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(rightjoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=[1]))
    @test isequal_coltyped(outerjoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[1]))
    @test isequal_coltyped(semijoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(antijoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))

    @test isequal_coltyped(innerjoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[2, 1, 2]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[2, 1, 2]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(rightjoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[2, 1, 2]), on=:id, matchmissing=:equal),
                           DataFrame(id=[2, 1, 2]))
    @test isequal_coltyped(outerjoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[2, 1, 2]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[2, 1, 2]))
    @test isequal_coltyped(semijoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[2, 1, 2]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(antijoin(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[2, 1, 2]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))

    @test isequal_coltyped(innerjoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(leftjoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1]),
                                    on=:id, matchmissing=:equal) ,
                           DataFrame(id=Union{Int, Missing}[missing]))
    @test isequal_coltyped(rightjoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=[1]))
    @test isequal_coltyped(outerjoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=[missing, 1]))
    @test isequal_coltyped(semijoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(antijoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[missing]))

    @test isequal_coltyped(innerjoin(DataFrame(id=[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=[missing]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[missing]), DataFrame(id=[1, missing]),
                                    on=:id, matchmissing=:equal),
                           DataFrame(id=[missing]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=[missing, 1]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=[missing, 1]))
    @test isequal_coltyped(semijoin(DataFrame(id=[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=[missing]))
    @test isequal_coltyped(antijoin(DataFrame(id=[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=Missing[]))

    @test isequal_coltyped(innerjoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[missing]))
    @test isequal_coltyped(leftjoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1, missing]),
                                    on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[missing]))
    @test isequal_coltyped(rightjoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=[missing, 1]))
    @test isequal_coltyped(outerjoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=[missing, 1]))
    @test isequal_coltyped(semijoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[missing]))
    @test isequal_coltyped(antijoin(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1, missing]),
                                     on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))

    @test isequal_coltyped(innerjoin(DataFrame(id=[typemin(Int) + 1, typemin(Int)]), DataFrame(id=[typemin(Int)]), on=:id),
                           DataFrame(id=[typemin(Int)]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[typemin(Int) + 1, typemin(Int)]), DataFrame(id=[typemin(Int)]), on=:id),
                           DataFrame(id=[typemin(Int), typemin(Int) + 1]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[typemin(Int) + 1, typemin(Int)]), DataFrame(id=[typemin(Int)]), on=:id),
                           DataFrame(id=[typemin(Int)]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[typemin(Int) + 1, typemin(Int)]), DataFrame(id=[typemin(Int)]), on=:id),
                           DataFrame(id=[typemin(Int), typemin(Int) + 1]))
    @test isequal_coltyped(semijoin(DataFrame(id=[typemin(Int) + 1, typemin(Int)]), DataFrame(id=[typemin(Int)]), on=:id),
                           DataFrame(id=[typemin(Int)]))
    @test isequal_coltyped(antijoin(DataFrame(id=[typemin(Int) + 1, typemin(Int)]), DataFrame(id=[typemin(Int)]), on=:id),
                           DataFrame(id=[typemin(Int) + 1]))

    @test isequal_coltyped(innerjoin(DataFrame(id=[typemax(Int), typemax(Int) - 1]), DataFrame(id=[typemax(Int)]), on=:id),
                           DataFrame(id=[typemax(Int)]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[typemax(Int), typemax(Int) - 1]), DataFrame(id=[typemax(Int)]), on=:id),
                           DataFrame(id=[typemax(Int), typemax(Int) - 1]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[typemax(Int), typemax(Int) - 1]), DataFrame(id=[typemax(Int)]), on=:id),
                           DataFrame(id=[typemax(Int)]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[typemax(Int), typemax(Int) - 1]), DataFrame(id=[typemax(Int)]), on=:id),
                           DataFrame(id=[typemax(Int), typemax(Int) - 1]))
    @test isequal_coltyped(semijoin(DataFrame(id=[typemax(Int), typemax(Int) - 1]), DataFrame(id=[typemax(Int)]), on=:id),
                           DataFrame(id=[typemax(Int)]))
    @test isequal_coltyped(antijoin(DataFrame(id=[typemax(Int), typemax(Int) - 1]), DataFrame(id=[typemax(Int)]), on=:id),
                           DataFrame(id=[typemax(Int) - 1]))

    @test isequal_coltyped(innerjoin(DataFrame(id=[2000, 2, 100]), DataFrame(id=[2000, 1, 100]), on=:id),
                           DataFrame(id=[2000, 100]))
    @test isequal_coltyped(leftjoin(DataFrame(id=[2000, 2, 100]), DataFrame(id=[2000, 1, 100]), on=:id),
                           DataFrame(id=[2000, 100, 2]))
    @test isequal_coltyped(rightjoin(DataFrame(id=[2000, 2, 100]), DataFrame(id=[2000, 1, 100]), on=:id),
                           DataFrame(id=[2000, 100, 1]))
    @test isequal_coltyped(outerjoin(DataFrame(id=[2000, 2, 100]), DataFrame(id=[2000, 1, 100]), on=:id),
                           DataFrame(id=[2000, 100, 2, 1]))
    @test isequal_coltyped(semijoin(DataFrame(id=[2000, 2, 100]), DataFrame(id=[2000, 1, 100]), on=:id),
                           DataFrame(id=[2000, 100]))
    @test isequal_coltyped(antijoin(DataFrame(id=[2000, 2, 100]), DataFrame(id=[2000, 1, 100]), on=:id),
                           DataFrame(id=[2]))

    @test isequal_coltyped(outerjoin(DataFrame(id=[1]), DataFrame(id=[4.5]), on=:id),
                           DataFrame(id=[1, 4.5]))
    @test isequal_coltyped(outerjoin(DataFrame(id=categorical([1])), DataFrame(id=[(1, 2)]), on=:id),
                           DataFrame(id=[1, (1, 2)]))
end

@testset "legacy merge tests" begin
    Random.seed!(1)
    df1 = DataFrame(a=shuffle!(Vector{Union{Int, Missing}}(1:10)),
                    b=rand(Union{Symbol, Missing}[:A, :B], 10),
                    v1=Vector{Union{Float64, Missing}}(randn(10)))

    df2 = DataFrame(a=shuffle!(Vector{Union{Int, Missing}}(1:5)),
                    b2=rand(Union{Symbol, Missing}[:A, :B, :C], 5),
                    v2=Vector{Union{Float64, Missing}}(randn(5)))

    m1 = innerjoin(df1, df2, on=:a)
    @test m1[!, :a] == df1[!, :a][df1[!, :a] .<= 5] # preserves df1 order
    m2 = outerjoin(df1, df2, on=:a)
    @test m2[!, :a] != df1[!, :a] # does not preserve df1 order
    @test m2[!, :b] != df1[!, :b] # does not preserve df1 order
    @test sort(m2[!, [:a, :b]]) == sort(df1[!, [:a, :b]]) # but keeps values
    @test m1 == m2[1:nrow(m1), :] # and is consistent with innerjoin in the first rows
    @test m2[indexin(df1[!, :a], m2[!, :a]), :b] == df1[!, :b]
    @test m2[indexin(df2[!, :a], m2[!, :a]), :b2] == df2[!, :b2]
    @test m2[indexin(df1[!, :a], m2[!, :a]), :v1] == df1[!, :v1]
    @test m2[indexin(df2[!, :a], m2[!, :a]), :v2] == df2[!, :v2]
    @test all(ismissing, m2[map(x -> !in(x, df2[!, :a]), m2[!, :a]), :b2])
    @test all(ismissing, m2[map(x -> !in(x, df2[!, :a]), m2[!, :a]), :v2])

    df1 = DataFrame(a=Union{Int, Missing}[1, 2, 3],
                    b=Union{String, Missing}["America", "Europe", "Africa"])
    df2 = DataFrame(a=Union{Int, Missing}[1, 2, 4],
                    c=Union{String, Missing}["New World", "Old World", "New World"])

    m1 = innerjoin(df1, df2, on=:a)
    @test m1[!, :a] == [1, 2]

    m2 = leftjoin(df1, df2, on=:a)
    @test m2[!, :a] == [1, 2, 3]

    m3 = rightjoin(df1, df2, on=:a)
    @test m3[!, :a] == [1, 2, 4]

    m4 = outerjoin(df1, df2, on=:a)
    @test m4[!, :a] == [1, 2, 3, 4]

    # test with missings (issue #185)
    df1 = DataFrame()
    df1[!, :A] = ["a", "b", "a", missing]
    df1[!, :B] = Union{Int, Missing}[1, 2, 1, 3]

    df2 = DataFrame()
    df2[!, :A] = ["a", missing, "c"]
    df2[!, :C] = Union{Int, Missing}[1, 2, 4]

    @test_throws ArgumentError innerjoin(df1, df2, on=:A)
    m1 = innerjoin(df1, df2, on=:A, matchmissing=:equal)
    @test size(m1) == (3, 3)
    @test m1[!, :A] ≅ ["a", "a", missing]

    @test_throws ArgumentError outerjoin(df1, df2, on=:A)
    m2 = outerjoin(df1, df2, on=:A, matchmissing=:equal)
    @test size(m2) == (5, 3)
    @test m2[!, :A] ≅ ["a", "a", missing, "b", "c"]
end

@testset "legacy join tests" begin
    df1 = DataFrame(a=Union{Symbol, Missing}[:x, :y][[1, 1, 1, 2, 1, 1]],
                    b=Union{Symbol, Missing}[:A, :B, :D][[1, 1, 2, 2, 1, 3]],
                    v1=1:6)

    df2 = DataFrame(a=Union{Symbol, Missing}[:x, :y][[2, 2, 1, 1, 1, 1]],
                    b=Union{Symbol, Missing}[:A, :B, :C][[1, 2, 1, 2, 3, 1]],
                    v2=1:6)
    df2[1, :a] = missing

    m1 = innerjoin(df1, df2, on=[:a, :b], matchmissing=:equal)
    @test sort(m1) == sort(DataFrame(a=[:x, :x, :x, :x, :x, :y, :x, :x],
                                     b=[:A, :A, :A, :A, :B, :B, :A, :A],
                                     v1=[1, 1, 2, 2, 3, 4, 5, 5],
                                     v2=[3, 6, 3, 6, 4, 2, 3, 6]))
    m2 = outerjoin(df1, df2, on=[:a, :b], matchmissing=:equal)
    @test sort(m2) ≅ sort(DataFrame(a=[:x, :x, :x, :x, :x, :y, :x, :x, :x, missing, :x],
                                    b=[:A, :A, :A, :A, :B, :B, :A, :A, :D, :A, :C],
                                    v1=[1, 1, 2, 2, 3, 4, 5, 5, 6, missing, missing],
                                    v2=[3, 6, 3, 6, 4, 2, 3, 6, missing, 1, 5]))

    Random.seed!(1)
    df1 = DataFrame(a=["abc", "abx", "axz", "def", "dfr"], v1=randn(5))
    df2 = DataFrame(a=["def", "abc", "abx", "axz", "xyz"], v2=randn(5))
    transform!(df1, :a => ByRow(collect) => AsTable)
    transform!(df2, :a => ByRow(collect) => AsTable)

    m1 = innerjoin(df1, df2, on=:a, makeunique=true)
    m2 = innerjoin(df1, df2, on=[:x1, :x2, :x3], makeunique=true)
    @test m1[!, :a] == m2[!, :a]
end

@testset "threaded correctness" begin
    df1 = DataFrame(id=[1:10^6; 10^7+1:10^7+2])
    df1.left_row = axes(df1, 1)
    df2 = DataFrame(id=[1:10^6; 10^8+1:10^8+4])
    df2.right_row = axes(df2, 1)

    @test try
        innerjoin(df1, df2, on=:id) ≅
        DataFrame(id=1:10^6, left_row=1:10^6, right_row=1:10^6)
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping innerjoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        leftjoin(df1, df2, on=:id) ≅
        DataFrame(id=[1:10^6; 10^7+1:10^7+2], left_row=1:10^6+2,
                  right_row=[1:10^6; missing; missing])
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping leftjoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        rightjoin(df1, df2, on=:id) ≅
        DataFrame(id=[1:10^6; 10^8+1:10^8+4],
                  left_row=[1:10^6; fill(missing, 4)],
                  right_row=1:10^6+4)
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping rightjoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        outerjoin(df1, df2, on=:id) ≅
        DataFrame(id=[1:10^6; 10^7+1:10^7+2; 10^8+1:10^8+4],
                  left_row=[1:10^6+2; fill(missing, 4)],
                  right_row=[1:10^6; missing; missing; 10^6+1:10^6+4])
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping outerjoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        semijoin(df1, df2, on=:id) ≅
        DataFrame(id=1:10^6, left_row=1:10^6)
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping semijoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        antijoin(df1, df2, on=:id) ≅
        DataFrame(id=10^7+1:10^7+2, left_row=10^6+1:10^6+2)
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping antijoin test."
            true
        else
            rethrow(e)
        end
    end

    Random.seed!(1234)
    for i in 1:4
        df1 = df1[shuffle(axes(df1, 1)), :]
        df2 = df2[shuffle(axes(df2, 1)), :]

        @test try
            sort!(innerjoin(df1, df2, on=:id)) ≅
            DataFrame(id=1:10^6, left_row=1:10^6, right_row=1:10^6)
        catch e
            if Int === Int32 && e isa OutOfMemoryError
                @warn "OutOfMemoryError. Skipping innerjoin test."
                true
            else
                rethrow(e)
            end
        end

        @test try
            sort!(leftjoin(df1, df2, on=:id)) ≅
            DataFrame(id=[1:10^6; 10^7+1:10^7+2], left_row=1:10^6+2,
                      right_row=[1:10^6; missing; missing])
        catch e
            if Int === Int32 && e isa OutOfMemoryError
                @warn "OutOfMemoryError. Skipping leftjoin test."
                true
            else
                rethrow(e)
            end
        end

        @test try
            sort!(rightjoin(df1, df2, on=:id)) ≅
              DataFrame(id=[1:10^6; 10^8+1:10^8+4],
                        left_row=[1:10^6; fill(missing, 4)],
                        right_row=1:10^6+4)
        catch e
            if Int === Int32 && e isa OutOfMemoryError
                @warn "OutOfMemoryError. Skipping rightjoin test."
                true
            else
                rethrow(e)
            end
        end

        @test try
            sort!(outerjoin(df1, df2, on=:id)) ≅
            DataFrame(id=[1:10^6; 10^7+1:10^7+2; 10^8+1:10^8+4],
                      left_row=[1:10^6+2; fill(missing, 4)],
                      right_row=[1:10^6; missing; missing; 10^6+1:10^6+4])
        catch e
            if Int === Int32 && e isa OutOfMemoryError
                @warn "OutOfMemoryError. Skipping outerjoin test."
                true
            else
                rethrow(e)
            end
        end

        @test try
            sort!(semijoin(df1, df2, on=:id)) ≅
            DataFrame(id=1:10^6, left_row=1:10^6)
        catch e
            if Int === Int32 && e isa OutOfMemoryError
                @warn "OutOfMemoryError. Skipping semijoin test."
                true
            else
                rethrow(e)
            end
        end

        @test try
            sort!(antijoin(df1, df2, on=:id)) ≅
            DataFrame(id=10^7+1:10^7+2, left_row=10^6+1:10^6+2)
        catch e
            if Int === Int32 && e isa OutOfMemoryError
                @warn "OutOfMemoryError. Skipping antijoin test."
                true
            else
                rethrow(e)
            end
        end
    end

    # test correctness of column order
    df1 = DataFrame(a=Int8(1), id2=-[1:10^6; 10^7+1:10^7+2], b=Int8(2),
                    id1=[1:10^6; 10^7+1:10^7+2], c=Int8(3), d=Int8(4))
    df2 = DataFrame(e=Int8(5), id1=[1:10^6; 10^8+1:10^8+4], f=Int8(6), g=Int8(7),
                    id2=-[1:10^6; 10^8+1:10^8+4], h=Int8(8))

    @test try
        innerjoin(df1, df2, on=[:id1, :id2]) ≅
        DataFrame(a=Int8(1), id2=-(1:10^6), b=Int8(2), id1=1:10^6,
                  c=Int8(3), d=Int8(4), e=Int8(5), f=Int8(6), g=Int8(7), h=Int8(8))
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping innerjoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        leftjoin(df1, df2, on=[:id1, :id2])[1:10^6, :] ≅
        DataFrame(a=Int8(1), id2=-(1:10^6), b=Int8(2), id1=1:10^6,
                  c=Int8(3), d=Int8(4), e=Int8(5), f=Int8(6), g=Int8(7), h=Int8(8))
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping leftjoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        rightjoin(df1, df2, on=[:id1, :id2])[1:10^6, :] ≅
        DataFrame(a=Int8(1), id2=-(1:10^6), b=Int8(2), id1=1:10^6,
                  c=Int8(3), d=Int8(4), e=Int8(5), f=Int8(6), g=Int8(7), h=Int8(8))
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping rightjoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        outerjoin(df1, df2, on=[:id1, :id2])[1:10^6, :] ≅
        DataFrame(a=Int8(1), id2=-(1:10^6), b=Int8(2), id1=1:10^6,
                  c=Int8(3), d=Int8(4), e=Int8(5), f=Int8(6), g=Int8(7), h=Int8(8))
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping outerjoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        semijoin(df1, df2, on=[:id1, :id2]) ≅
        DataFrame(a=Int8(1), id2=-(1:10^6), b=Int8(2), id1=1:10^6, c=Int8(3), d=Int8(4))
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping semijoin test."
            true
        else
            rethrow(e)
        end
    end

    @test try
        antijoin(df1, df2, on=[:id1, :id2]) ≅
        DataFrame(a=Int8(1), id2=-(10^7+1:10^7+2), b=Int8(2), id1=(10^7+1:10^7+2),
                  c=Int8(3), d=Int8(4))
    catch e
        if Int === Int32 && e isa OutOfMemoryError
            @warn "OutOfMemoryError. Skipping antijoin test."
            true
        else
            rethrow(e)
        end
    end
end

@testset "matchmissing :notequal correctness" begin
    Random.seed!(1337)
    names = [
        DataFrame(ID=[1, 2, missing],
                  Name=["John Doe", "Jane Doe", "Joe Blogs"]),
        DataFrame(ID=[],
                  Name=[]),
        DataFrame(ID=missings(3),
                  Name=["John Doe", "Jane Doe", "Joe Blogs"]),
        DataFrame(ID=[1, 2, 3],
                  Name=[missing, "Jane Doe", missing]),
        DataFrame(ID=[1:100; missings(100)],
                  Name=repeat(["Jane Doe"], 200)),
        DataFrame(ID=[missings(100); 1:100],
                  Name=repeat(["Jane Doe"], 200)),
        DataFrame(ID=[1:50; missings(100); 51:100],
                  Name=repeat(["Jane Doe"], 200)),
        DataFrame(ID=[1:64; missings(64); 129:200],
                  Name=repeat(["Jane Doe"], 200)),
        DataFrame(ID=[1:63; missings(65); 129:200],
                  Name=repeat(["Jane Doe"], 200)),
        DataFrame(ID=rand([1:1000; missing], 10000),
                  Name=rand(["John Doe", "Jane Doe", "Joe Blogs", missing], 10000)),
    ]
    jobs = [
        DataFrame(ID=[1, 2, 2, 4],
                  Job=["Lawyer", "Doctor", "Florist", "Farmer"]),
        DataFrame(ID=[missing, 2, 2, 4],
                  Job=["Lawyer", "Doctor", "Florist", "Farmer"]),
        DataFrame(ID=[missing, 2, 2, 4],
                  Job=["Lawyer", "Doctor", missing, "Farmer"]),
        DataFrame(ID=[],
                  Job=[]),
        DataFrame(ID=[1:100; missings(100)],
                  Job=repeat(["Lawyer"], 200)),
        DataFrame(ID=[missings(100); 1:100],
                  Job=repeat(["Lawyer"], 200)),
        DataFrame(ID=[1:50; missings(100); 51:100],
                  Job=repeat(["Lawyer"], 200)),
        DataFrame(ID=[1:64; missings(64); 129:200],
                  Job=repeat(["Lawyer"], 200)),
        DataFrame(ID=[1:63; missings(65); 129:200],
                  Job=repeat(["Lawyer"], 200)),
        DataFrame(ID=rand([1:1000; missing], 10000),
                  Job=rand(["Lawyer", "Doctor", "Florist", missing], 10000)),
    ]
    for name in names, job in jobs
        @test leftjoin(name, dropmissing(job, :ID), on=:ID, matchmissing=:equal) ≅
            leftjoin(name, job, on=:ID, matchmissing=:notequal)
        @test leftjoin!(copy(name), dropmissing(unique(job, :ID), :ID), on=:ID, matchmissing=:equal) ≅
            leftjoin!(copy(name), unique(job, :ID), on=:ID, matchmissing=:notequal)
        @test semijoin(name, dropmissing(job, :ID), on=:ID, matchmissing=:equal) ≅
            semijoin(name, job, on=:ID, matchmissing=:notequal)
        @test antijoin(name, dropmissing(job, :ID), on=:ID, matchmissing=:equal) ≅
            antijoin(name, job, on=:ID, matchmissing=:notequal)
        @test rightjoin(dropmissing(name, :ID), job, on=:ID, matchmissing=:equal) ≅
            rightjoin(name, job, on=:ID, matchmissing=:notequal)
        @test innerjoin(dropmissing(name, :ID), dropmissing(job, :ID), on=:ID, matchmissing=:equal) ≅
            innerjoin(name, job, on=:ID, matchmissing=:notequal)
    end

    rl(n) = rand(["a", "b", "c"], n)
    names2 = [
        DataFrame(ID1=[1, 1, 2],
                  ID2=["a", "b", "a"],
                  Name=["John Doe", "Jane Doe", "Joe Blogs"]),
        DataFrame(ID1=[1, 1, 2, missing],
                  ID2=["a", "b", "a", missing],
                  Name=["John Doe", "Jane Doe", "Joe Blogs", missing]),
        DataFrame(ID1=[missing, 1, 2, missing],
                  ID2=["a", "b", missing, missing],
                  Name=[missing, "Jane Doe", "Joe Blogs", missing]),
        DataFrame(ID1=[missing, 1, 2, missing],
                  ID2=["a", "b", missing, missing],
                  Name=missings(4)),
        DataFrame(ID1=[missing, 1, 2, missing],
                  ID2=missings(4),
                  Name=["John Doe", "Jane Doe", "Joe Blogs", missing]),
        DataFrame(ID1=[1:100; missings(100)],
                  ID2=[rl(100); missings(100)],
                  Name=rand(["Jane Doe", "Jane Doe"], 200)),
        DataFrame(ID1=[missings(100); 1:100],
                  ID2=[missings(100); rl(100)],
                  Name=rand(["Jane Doe", "Jane Doe"], 200)),
        DataFrame(ID1=[1:50; missings(100); 51:100],
                  ID2=[rl(50); missings(100); rl(50)],
                  Name=rand(["Jane Doe", "Jane Doe"], 200)),
        DataFrame(ID1=[1:64; missings(64); 129:200],
                  ID2=[rl(64); missings(64); rl(200 - 128)],
                  Name=rand(["Jane Doe", "Jane Doe"], 200)),
        DataFrame(ID1=[1:63; missings(65); 129:200],
                  ID2=[rl(64); missings(65); rl(200 - 129)],
                  Name=rand(["Jane Doe", "Jane Doe"], 200)),
        DataFrame(ID1=rand([1:100; missing], 10000),
                  ID2=rand(["a", "b", "c", missing], 10000),
                  Name=rand(["John Doe", "Jane Doe", "Joe Blogs", missing], 10000)),
    ]
    jobs2 = [
        DataFrame(ID1=[1, 2, 2, 4],
                  ID2=["a", "b", "b", "c"],
                  Job=["Lawyer", "Doctor", "Florist", "Farmer"]),
        DataFrame(ID1=[1, 2, 2, 4, missing],
                  ID2=["a", "b", "b", "c", missing],
                  Job=["Lawyer", "Doctor", "Florist", "Farmer", missing]),
        DataFrame(ID1=[1, 2, missing, 4, missing],
                  ID2=["a", "b", missing, "c", missing],
                  Job=[missing, "Doctor", "Florist", "Farmer", missing]),
        DataFrame(ID1=[1:100; missings(100)],
                  ID2=[rl(100); missings(100)],
                  Job=rand(["Doctor", "Florist"], 200)),
        DataFrame(ID1=[missings(100); 1:100],
                  ID2=[missings(100); rl(100)],
                  Job=rand(["Doctor", "Florist"], 200)),
        DataFrame(ID1=[1:50; missings(100); 51:100],
                  ID2=[rl(50); missings(100); rl(50)],
                  Job=rand(["Doctor", "Florist"], 200)),
        DataFrame(ID1=[1:64; missings(64); 129:200],
                  ID2=[rl(64); missings(64); rl(200 - 128)],
                  Job=rand(["Doctor", "Florist"], 200)),
        DataFrame(ID1=[1:63; missings(65); 129:200],
                  ID2=[rl(64); missings(65); rl(200 - 129)],
                  Job=rand(["Doctor", "Florist"], 200)),
        DataFrame(ID1=rand([1:100; missing], 10000),
                  ID2=rand(["a", "b", "c", missing], 10000),
                  Job=rand(["Doctor", "Florist", "Farmer", missing], 10000)),
    ]
    k = [:ID1, :ID2]
    for name in names2, job in jobs2
        @test leftjoin(name, dropmissing(job, k), on=k, matchmissing=:equal) ≅
            leftjoin(name, job, on=k, matchmissing=:notequal)
        @test leftjoin!(copy(name), dropmissing(unique(job, k), k), on=k, matchmissing=:equal) ≅
            leftjoin!(copy(name), unique(job, k), on=k, matchmissing=:notequal)
        @test semijoin(name, dropmissing(job, k), on=k, matchmissing=:equal) ≅
            semijoin(name, job, on=k, matchmissing=:notequal)
        @test antijoin(name, dropmissing(job, k), on=k, matchmissing=:equal) ≅
            antijoin(name, job, on=k, matchmissing=:notequal)
        @test rightjoin(dropmissing(name, k), job, on=k, matchmissing=:equal) ≅
            rightjoin(name, job, on=k, matchmissing=:notequal)
        @test innerjoin(dropmissing(name, k), dropmissing(job, k), on=k, matchmissing=:equal) ≅
            innerjoin(name, job, on=k, matchmissing=:notequal)
    end
end

@testset "leftjoin!" begin
    dfl = copy(name)
    @test_throws ArgumentError leftjoin!(dfl, job, on=:ID)
    @test isequal_coltyped(name, dfl)

    df1 = DataFrame(A=1, B=2, C=3)
    df2 = DataFrame(A=1, B=2, D=4)
    @test leftjoin!(df1, df2, on=[:A, :B]) === df1
    @test df1 == DataFrame(A=1, B=2, C=3, D=4)

    simple_df1(len::Int) = DataFrame(A=allowmissing(1:len))
    @test leftjoin!(simple_df1(0), simple_df1(0), on=:A) == simple_df1(0)
    @test leftjoin!(simple_df1(2), simple_df1(0), on=:A) == simple_df1(2)

    simple_df2(len::Int) = DataFrame(A=1:len)
    @test leftjoin!(simple_df2(0), simple_df2(0), on=:A) ==  simple_df2(0)
    @test leftjoin!(simple_df2(2), simple_df2(0), on=:A) ==  simple_df2(2)
    @test leftjoin!(simple_df2(0), simple_df2(2), on=:A) ==  simple_df2(0)

    df = DataFrame(Name=Union{String, Missing}["A", "B", "C"],
                   Mass=[1.5, 2.2, 1.1])
    df2 = DataFrame(Name=["A", "B", "C", "A"],
                    Quantity=[3, 3, 2, 4])
    @test leftjoin!(df2, df, on=:Name) == DataFrame(Name=["A", "B", "C", "A"],
                                                    Quantity=[3, 3, 2, 4],
                                                    Mass=[1.5, 2.2, 1.1, 1.5])

    df1 = DataFrame(Any[[1, 3, 5], [1.0, 3.0, 5.0]], [:id, :fid])
    df2 = DataFrame(Any[[0, 1, 2, 3, 4], [0.0, 1.0, 2.0, 3.0, 4.0]], [:id, :fid])
    l(on) = leftjoin!(copy(df1), df2, on=on, makeunique=true)
    on = :id
    @test l(on) ≅ DataFrame(id=[1, 3, 5],
                            fid=[1, 3, 5],
                            fid_1=[1, 3, missing])
    @test typeof.(eachcol(l(on))) ==
        [Vector{Int}, Vector{Float64}, Vector{Union{Float64, Missing}}]
    on = :fid
    @test l(on) ≅ DataFrame(id=[1, 3, 5],
                            fid=[1, 3, 5],
                            id_1=[1, 3, missing])
    @test typeof.(eachcol(l(on))) == [Vector{Int}, Vector{Float64},
                                     Vector{Union{Int, Missing}}]
    on = [:id, :fid]
    @test l(on) == DataFrame(id=[1, 3, 5], fid=[1, 3, 5])
    @test typeof.(eachcol(l(on))) == [Vector{Int}, Vector{Float64}]

    df1 = DataFrame(Any[CategoricalArray([1, 3, 5]),
                        CategoricalArray([1.0, 3.0, 5.0])], [:id, :fid])
    df2 = DataFrame(Any[CategoricalArray([0, 1, 2, 3, 4]),
                        CategoricalArray([0.0, 1.0, 2.0, 3.0, 4.0])], [:id, :fid])
    on = :id
    @test l(on) ≅ DataFrame(id=[1, 3, 5],
                            fid=[1, 3, 5],
                            fid_1=[1, 3, missing])
    @test all(isa.(eachcol(l(on)),
                   [CategoricalVector{T} for T in (Int, Float64, Union{Float64, Missing})]))
    on = :fid
    @test l(on) ≅ DataFrame(id=[1, 3, 5],
                            fid=[1, 3, 5],
                            id_1=[1, 3, missing])
    @test all(isa.(eachcol(l(on)),
                   [CategoricalVector{T} for T in (Int, Float64, Union{Int, Missing})]))
    on = [:id, :fid]
    @test l(on) == DataFrame(id=[1, 3, 5],
                             fid=[1, 3, 5])
    @test all(isa.(eachcol(l(on)),
                   [CategoricalVector{T} for T in (Int, Float64)]))

    A = DataFrame(a=[1, 2, 3], b=["a", "b", "c"])
    B = DataFrame(b=["a", "b", "c"], c=CategoricalVector(["a", "b", "b"]))
    levels!(B.c, ["b", "a"])
    @test levels(leftjoin!(copy(A), B, on=:b).c) == ["b", "a"]
    A = DataFrame(a=[1, 2, 3, 4], b=CategoricalVector(["a", "b", "c", "d"]))
    levels!(A.b, ["d", "c", "b", "a"])
    B = DataFrame(b=CategoricalVector(["a", "b", "c"]), c=[5, 6, 7])
    @test levels(leftjoin!(copy(A), B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(leftjoin!(copy(B), A, on=:b).b) == ["a", "b", "c"]
    A = DataFrame(a=[1, 2, 3, 4], b=CategoricalVector(["a", "b", "c", "d"]))
    levels!(A.b, ["d", "c", "b", "a"])
    B = DataFrame(b=["a", "b", "c"], c=[5, 6, 7])
    @test levels(leftjoin!(copy(A), B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(leftjoin!(copy(B), A, on=:b).b) == ["a", "b", "c"]

    left = DataFrame(id=1:7, sid=string.(1:7))
    right = DataFrame(ID=3:10, SID=string.(3:10))
    @test leftjoin!(copy(left), right, on=:id => :ID) ≅
        DataFrame(id=1:7, sid=string.(1:7),
                  SID=[missing, missing, string.(3:7)...])
    @test leftjoin!(copy(left), right, on=[:id => :ID]) ≅
        DataFrame(id=1:7, sid=string.(1:7),
                  SID=[missing, missing, string.(3:7)...])
    @test leftjoin!(copy(left), right, on=[:id => :ID, :sid => :SID]) ==
        DataFrame(id=1:7, sid=string.(1:7))
    @test_throws ArgumentError leftjoin!(left, right, on=(:id, :ID))

    ldf = DataFrame(a=Any[1:7;], b=[1:7;])
    rdf = DataFrame(a=Any[3:10;], b=[3:10;])
    @test leftjoin!(copy(ldf), rdf, on=[:a, :b]) ≅ DataFrame(a=1:7, b=1:7)
    @test eltype.(eachcol(leftjoin!(copy(ldf), rdf, on=[:a, :b]))) == [Any, Int]
    @test leftjoin!(copy(ldf), rdf, on=:b, makeunique=true) ≅
        DataFrame(a=1:7, b=1:7, a_1=[missing; missing; 3:7])
    @test eltype.(eachcol(leftjoin!(copy(ldf), rdf, on=:b, makeunique=true))) == [Any, Int, Any]

    ldf = DataFrame(a=1:3, b=categorical(["a", "b", "c"]))
    rdf = DataFrame(a=4:5, b=categorical(["d", "e"]))
    nl = size(ldf, 1)
    nr = size(rdf, 1)
    CS = eltype(ldf.b)
    @test leftjoin!(copy(ldf), rdf, on=[:a, :b]) ≅ DataFrame(a=ldf.a, b=ldf.b)
    @test eltype.(eachcol(leftjoin!(copy(ldf), rdf, on=[:a, :b]))) == [Int, CS]
    @test leftjoin!(copy(ldf), rdf, on=:a, makeunique=true) ≅
        DataFrame(a=ldf.a, b=ldf.b, b_1=similar_missing(rdf.b, nl))
    @test eltype.(eachcol(leftjoin!(copy(ldf), rdf, on=:a, makeunique=true))) ==
        [Int, CS, Union{CS, Missing}]
    @test leftjoin!(copy(ldf), rdf, on=:b, makeunique=true) ≅
        DataFrame(a=ldf.a, b=ldf.b, a_1=fill(missing, nl))
    @test eltype.(eachcol(leftjoin!(copy(ldf), rdf, on=:b, makeunique=true))) ==
        [Int, CS, Union{Int, Missing}]

    namedf = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
    jobdf = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
    @test leftjoin!(namedf, jobdf, on=:ID) ≅
        DataFrame(ID=[1, 2, 3],
                  Name=["John Doe", "Jane Doe", "Joe Blogs"],
                  Job=["Lawyer", "Doctor", missing])
    jobdf2 = DataFrame(identifier=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
    @test leftjoin!(namedf, jobdf2, on=:ID => :identifier, makeunique=true, source=:source) ≅
        DataFrame(ID=[1, 2, 3],
                  Name=["John Doe", "Jane Doe", "Joe Blogs"],
                  Job=["Lawyer", "Doctor", missing],
                  Job_1=["Lawyer", "Doctor", missing],
                  source=["both", "both", "left_only"])

    jobdf = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
    for special in [missing, NaN, -0.0]
        name_w_special = DataFrame(ID=[1, 2, 3, special],
                                   Name=["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
        @test_throws ArgumentError leftjoin!(name_w_special, jobdf, on=:ID)
    end
    for special in [missing, 0.0]
        name_w_special = DataFrame(ID=[1, 2, 3, special],
                                   Name=["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
        @test leftjoin!(copy(name_w_special), jobdf, on=:ID, matchmissing=:equal) ≅
              hcat(name_w_special, DataFrame(Job=["Lawyer", "Doctor", missing, missing]))
        jobdf2 = DataFrame(ID=[1, 2, special], Job=["Lawyer", "Doctor", "Farmer"])
        @test leftjoin!(copy(name_w_special), jobdf2, on=:ID, matchmissing=:equal) ≅
              hcat(name_w_special, DataFrame(Job=["Lawyer", "Doctor", missing, "Farmer"]))
    end
    for special in [NaN, -0.0]
        name_w_special = DataFrame(ID=categorical([1, 2, 3, special]),
                                   Name=["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
        @test leftjoin!(copy(name_w_special),
                        transform(jobdf, :ID => categorical => :ID), on=:ID) ≅
              hcat(name_w_special, DataFrame(Job=["Lawyer", "Doctor", missing, missing]))
    end

    name_w_zeros = DataFrame(ID=categorical([1, 2, 3, 0.0, -0.0]),
                             Name=["John Doe", "Jane Doe",
                                   "Joe Blogs", "Maria Tester",
                                   "Jill Jillerson"])
    name_w_zeros2 = DataFrame(ID=categorical([1, 2, 3, 0.0, -0.0]),
                              Name=["John Doe", "Jane Doe",
                                    "Joe Blogs", "Maria Tester",
                                    "Jill Jillerson"],
                              Name_1=["John Doe", "Jane Doe",
                                      "Joe Blogs", "Maria Tester",
                                      "Jill Jillerson"])
    @test leftjoin!(copy(name_w_zeros), name_w_zeros, on=:ID,
                    makeunique=true) ≅ name_w_zeros2

    name_multi = DataFrame(ID1=[1, 1, 2],
                           ID2=["a", "b", "a"],
                           Name=["John Doe", "Jane Doe", "Joe Blogs"])
    job_multi = DataFrame(ID1=[1, 2, 2, 4],
                          ID2=["a", "b", "b", "c"],
                          Job=["Lawyer", "Doctor", "Florist", "Farmer"])
    # job_multi has non-offending duplicates
    @test leftjoin!(copy(name_multi), job_multi, on=[:ID1, :ID2]) ≅
          hcat(name_multi, DataFrame(Job=["Lawyer", missing, missing]))
    @test leftjoin!(copy(job_multi), name_multi, on=[:ID1, :ID2]) ≅
          hcat(job_multi, DataFrame(Name=["John Doe", missing, missing, missing]))

    namedf = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
    jobdf = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
    cname = copy(namedf)
    cjob = copy(jobdf)
    push!(cname[!, 1], cname[1, 1])
    @test_throws AssertionError innerjoin(cname, cjob, on=:ID)
    cname = copy(namedf)
    push!(cjob[!, 1], cjob[1, 1])
    @test_throws AssertionError innerjoin(cname, cjob, on=:ID)
    cjob = copy(jobdf)
    push!(DataFrames._columns(cname), cname[:, 1])
    @test_throws AssertionError innerjoin(cname, cjob, on=:ID)

    df1 = DataFrame(id=[1, 2, 3], id2=[11, 12, 13], x=[1, 2, 3])
    df2 = DataFrame(id=[1, 2, 4], ID2=[11, 12, 14], y=[1, 2, 4])
    @test leftjoin!(copy(df1), df2, on=[:id, :id2=>:ID2]) ≅
          hcat(df1, DataFrame(y=[1, 2, missing]))
    @test leftjoin!(copy(df1), df2, on=[:id2=>:ID2, :id]) ≅
          hcat(df1, DataFrame(y=[1, 2, missing]))
    @test leftjoin!(copy(df1), df2, on=[:id=>:id, :id2=>:ID2]) ≅
          hcat(df1, DataFrame(y=[1, 2, missing]))
    @test leftjoin!(copy(df1), df2, on=[:id2=>:ID2, :id=>:id]) ≅
          hcat(df1, DataFrame(y=[1, 2, missing]))

    df = DataFrame(a=1)
    @test_throws ArgumentError leftjoin!(copy(df), df, on=:a, source=:a)
    @test leftjoin!(copy(df), df, on=:a, source=:a, makeunique=true) == DataFrame(a=1, a_1="both")
    @test leftjoin!(copy(df), df, on=:a, source="_left") == DataFrame(a=1, _left="both")
    @test leftjoin!(copy(df), df, on=:a, source="_right") == DataFrame(a=1, _right="both")
    df = DataFrame(_left=1)
    @test leftjoin!(copy(df), df, on=:_left, source="_leftX") == DataFrame(_left=1, _leftX="both")
    df = DataFrame(_right=1)
    @test leftjoin!(copy(df), df, on=:_right, source="_rightX") == DataFrame(_right=1, _rightX="both")

    Random.seed!(1234)
    for i in 5:15, j in 5:15
        df1 = DataFrame(id=rand(1:10, i), x=1:i)
        df2 = unique(DataFrame(id=rand(1:10, j), y=1:j), :id)
        @test leftjoin!(copy(df1), df2, on=:id, source=:ind) ≅
              sort!(leftjoin(df1, df2, on=:id, source=:ind), :x)
    end

    @test isequal_coltyped(leftjoin!(DataFrame(id=[]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=[]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=[]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=[1, 2, 3]), DataFrame(id=[]), on=:id),
                           DataFrame(id=[1, 2, 3]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=[4, 5, 6]), DataFrame(id=[1, 2, 3]), on=:id),
                           DataFrame(id=Int[4, 5, 6]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=[1, 2, 3]), DataFrame(id=[4, 5, 6]), on=:id),
                           DataFrame(id=Int[1, 2, 3]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=[missing]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=[missing]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=Missing[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Missing[]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[1]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=Union{Int, Missing}[]), DataFrame(id=[2, 1, 2]), on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1]),
                                    on=:id, matchmissing=:equal) ,
                           DataFrame(id=Union{Int, Missing}[missing]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=[missing]), DataFrame(id=[1, missing]),
                                    on=:id, matchmissing=:equal),
                           DataFrame(id=[missing]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=Union{Int, Missing}[missing]), DataFrame(id=[1, missing]),
                                    on=:id, matchmissing=:equal),
                           DataFrame(id=Union{Int, Missing}[missing]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=[typemin(Int) + 1, typemin(Int)]), DataFrame(id=[typemin(Int)]), on=:id),
                           DataFrame(id=[typemin(Int) + 1, typemin(Int)]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=[typemax(Int), typemax(Int) - 1]), DataFrame(id=[typemax(Int)]), on=:id),
                           DataFrame(id=[typemax(Int), typemax(Int) - 1]))
    @test isequal_coltyped(leftjoin!(DataFrame(id=[2000, 2, 100]), DataFrame(id=[2000, 1, 100]), on=:id),
                           DataFrame(id=[2000, 2, 100]))

    @test_throws ArgumentError leftjoin!(DataFrame(), DataFrame())
    @test_throws ArgumentError leftjoin!(DataFrame(), DataFrame(), on=Symbol[])
    @test_throws ArgumentError leftjoin!(DataFrame(a=1, b=2), DataFrame(a=1, b=2), on=:a)

    @test_throws ArgumentError leftjoin!(view(DataFrame(a=1, b=2), :, 1:2), DataFrame(a=1, c=2), on=:a)

    df1 = DataFrame(id=1:5, x=1:5)
    df2 = DataFrame(id=1:5, y=1:5)
    df1v = view(df1, [3, 2], :)
    @test leftjoin!(df1v, df2, on=:id) == DataFrame(id=[3, 2], x=[3, 2], y=[3, 2])
    @test df1 ≅ DataFrame(id=1:5, x=1:5, y=[missing, 2, 3, missing, missing])

    df1 = DataFrame(id=1, x=1:5)
    df2 = DataFrame(id=1:5, y=1:5)
    df1v = view(df1, [3, 2], :)
    @test leftjoin!(df1v, df2, on=:id) == DataFrame(id=[1, 1], x=[3, 2], y=[1, 1])
    @test df1 ≅ DataFrame(id=1, x=1:5, y=[missing, 1, 1, missing, missing])

    df1 = DataFrame(id=1:5, x=1:5)
    df2 = DataFrame(id=1:5, y=1:5)
    df1v = view(df1, 1:0, :)
    @test leftjoin!(df1v, df2, on=:id) == DataFrame(id=[], x=[], y=[])
    @test df1 ≅ DataFrame(id=1:5, x=1:5, y=[missing, missing, missing, missing, missing])
    @test df1.y isa Vector{Union{Int, Missing}}
end

@testset "passing matchmissing in multi-data frame innerjoin and outerjoin" begin
    @test innerjoin(DataFrame(a=missing, b=1),
                    DataFrame(a=missing, c=2),
                    DataFrame(a=missing, d=3),
                    on=:a, matchmissing=:equal) ≅ DataFrame(a=missing, b=1, c=2, d=3)
    @test isempty(innerjoin(DataFrame(a=missing, b=1),
                  DataFrame(a=missing, c=2),
                  DataFrame(a=missing, d=3),
                  on=:a, matchmissing=:notequal))
    @test outerjoin(DataFrame(a=missing, b=1),
                    DataFrame(a=missing, c=2),
                    DataFrame(a=missing, d=3),
                    on=:a, matchmissing=:equal) ≅ DataFrame(a=missing, b=1, c=2, d=3)
end

end # module
