module TestJoin

using Test, DataFrames, Random
using DataFrames: similar_missing
const ≅ = isequal

name = DataFrame(ID = Union{Int, Missing}[1, 2, 3],
                Name = Union{String, Missing}["John Doe", "Jane Doe", "Joe Blogs"])
job = DataFrame(ID = Union{Int, Missing}[1, 2, 2, 4],
                Job = Union{String, Missing}["Lawyer", "Doctor", "Florist", "Farmer"])

# Test output of various join types
outer = DataFrame(ID = [1, 2, 2, 3, 4],
                  Name = ["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
                  Job = ["Lawyer", "Doctor", "Florist", missing, "Farmer"])

# (Tests use current column ordering but don't promote it)
right = outer[Bool[!ismissing(x) for x in outer.Job], [:ID, :Name, :Job]]
left = outer[Bool[!ismissing(x) for x in outer.Name], :]
inner = left[Bool[!ismissing(x) for x in left.Job], :]
semi = unique(inner[:, [:ID, :Name]])
anti = left[Bool[ismissing(x) for x in left.Job], [:ID, :Name]]

@testset "join types" begin
    # Join on symbols or vectors of symbols
    innerjoin(name, job, on = :ID)
    innerjoin(name, job, on = [:ID])

    @test_throws ArgumentError innerjoin(name, job)

    @test innerjoin(name, job, on = :ID) == inner
    @test outerjoin(name, job, on = :ID) ≅ outer
    @test leftjoin(name, job, on = :ID) ≅ left
    @test rightjoin(name, job, on = :ID) ≅ right
    @test semijoin(name, job, on = :ID) == semi
    @test antijoin(name, job, on = :ID) == anti

    # Join with no non-key columns
    on = [:ID]
    nameid = name[:, on]
    jobid = job[:, on]

    @test innerjoin(nameid, jobid, on = :ID) == inner[:, on]
    @test outerjoin(nameid, jobid, on = :ID) == outer[:, on]
    @test leftjoin(nameid, jobid, on = :ID) == left[:, on]
    @test rightjoin(nameid, jobid, on = :ID) == right[:, on]
    @test semijoin(nameid, jobid, on = :ID) == semi[:, on]
    @test antijoin(nameid, jobid, on = :ID) == anti[:, on]

    # Join on multiple keys
    df1 = DataFrame(A = 1, B = 2, C = 3)
    df2 = DataFrame(A = 1, B = 2, D = 4)

    @test innerjoin(df1, df2, on = [:A, :B]) == DataFrame(A = 1, B = 2, C = 3, D = 4)

    # Test output of cross joins
    df1 = DataFrame(A = 1:2, B = 'a':'b')
    df2 = DataFrame(C = 3:5)

    cross = DataFrame(A = [1, 1, 1, 2, 2, 2],
                    B = ['a', 'a', 'a', 'b', 'b', 'b'],
                    C = [3, 4, 5, 3, 4, 5])

    @test crossjoin(df1, df2) == cross

    # Cross joins handle naming collisions
    @test size(crossjoin(df1, df1, makeunique=true)) == (4, 4)

    # Cross joins don't take keys
    @test_throws MethodError crossjoin(df1, df2, on = :A)
end

@testset "Test empty inputs 1" begin
    simple_df(len::Int, col=:A) = (df = DataFrame();
                                   df[!, col]=Vector{Union{Int, Missing}}(1:len);
                                   df)
    @test leftjoin(simple_df(0), simple_df(0), on = :A) == simple_df(0)
    @test leftjoin(simple_df(2), simple_df(0), on = :A) == simple_df(2)
    @test leftjoin(simple_df(0), simple_df(2), on = :A) == simple_df(0)
    @test rightjoin(simple_df(0), simple_df(0), on = :A) == simple_df(0)
    @test rightjoin(simple_df(0), simple_df(2), on = :A) == simple_df(2)
    @test rightjoin(simple_df(2), simple_df(0), on = :A) == simple_df(0)
    @test innerjoin(simple_df(0), simple_df(0), on = :A) == simple_df(0)
    @test innerjoin(simple_df(0), simple_df(2), on = :A) == simple_df(0)
    @test innerjoin(simple_df(2), simple_df(0), on = :A) == simple_df(0)
    @test outerjoin(simple_df(0), simple_df(0), on = :A) == simple_df(0)
    @test outerjoin(simple_df(0), simple_df(2), on = :A) == simple_df(2)
    @test outerjoin(simple_df(2), simple_df(0), on = :A) == simple_df(2)
    @test semijoin(simple_df(0), simple_df(0), on = :A) == simple_df(0)
    @test semijoin(simple_df(2), simple_df(0), on = :A) == simple_df(0)
    @test semijoin(simple_df(0), simple_df(2), on = :A) == simple_df(0)
    @test antijoin(simple_df(0), simple_df(0), on = :A) == simple_df(0)
    @test antijoin(simple_df(2), simple_df(0), on = :A) == simple_df(2)
    @test antijoin(simple_df(0), simple_df(2), on = :A) == simple_df(0)
    @test crossjoin(simple_df(0), simple_df(0, :B)) == DataFrame(A=Int[], B=Int[])
    @test crossjoin(simple_df(0), simple_df(2, :B)) == DataFrame(A=Int[], B=Int[])
    @test crossjoin(simple_df(2), simple_df(0, :B)) == DataFrame(A=Int[], B=Int[])
end

@testset "Test empty inputs 2" begin
    simple_df(len::Int, col=:A) = (df = DataFrame(); df[!, col]=collect(1:len); df)
    @test leftjoin(simple_df(0), simple_df(0), on = :A) ==  simple_df(0)
    @test leftjoin(simple_df(2), simple_df(0), on = :A) ==  simple_df(2)
    @test leftjoin(simple_df(0), simple_df(2), on = :A) ==  simple_df(0)
    @test rightjoin(simple_df(0), simple_df(0), on = :A) == simple_df(0)
    @test rightjoin(simple_df(0), simple_df(2), on = :A) == simple_df(2)
    @test rightjoin(simple_df(2), simple_df(0), on = :A) == simple_df(0)
    @test innerjoin(simple_df(0), simple_df(0), on = :A) == simple_df(0)
    @test innerjoin(simple_df(0), simple_df(2), on = :A) == simple_df(0)
    @test innerjoin(simple_df(2), simple_df(0), on = :A) == simple_df(0)
    @test outerjoin(simple_df(0), simple_df(0), on = :A) == simple_df(0)
    @test outerjoin(simple_df(0), simple_df(2), on = :A) == simple_df(2)
    @test outerjoin(simple_df(2), simple_df(0), on = :A) == simple_df(2)
    @test semijoin(simple_df(0), simple_df(0), on = :A) ==  simple_df(0)
    @test semijoin(simple_df(2), simple_df(0), on = :A) ==  simple_df(0)
    @test semijoin(simple_df(0), simple_df(2), on = :A) ==  simple_df(0)
    @test antijoin(simple_df(0), simple_df(0), on = :A) ==  simple_df(0)
    @test antijoin(simple_df(2), simple_df(0), on = :A) ==  simple_df(2)
    @test antijoin(simple_df(0), simple_df(2), on = :A) ==  simple_df(0)
    @test crossjoin(simple_df(0), simple_df(0, :B)) == DataFrame(A=Int[], B=Int[])
    @test crossjoin(simple_df(0), simple_df(2, :B)) == DataFrame(A=Int[], B=Int[])
    @test crossjoin(simple_df(2), simple_df(0, :B)) == DataFrame(A=Int[], B=Int[])
end

@testset "issue #960" begin
    df1 = DataFrame(A = 1:50,
                    B = 1:50,
                    C = 1)
    categorical!(df1, :A)
    categorical!(df1, :B)
    @test innerjoin(df1, df1, on = [:A, :B], makeunique=true)[!, 1:3] == df1
    # Test that join works when mixing Array{Union{T, Missing}} with Array{T} (issue #1088)
    df = DataFrame(Name = Union{String, Missing}["A", "B", "C"],
                Mass = [1.5, 2.2, 1.1])
    df2 = DataFrame(Name = ["A", "B", "C", "A"],
                    Quantity = [3, 3, 2, 4])
    @test leftjoin(df2, df, on=:Name) == DataFrame(Name = ["A", "B", "C", "A"],
                                                   Quantity = [3, 3, 2, 4],
                                                   Mass = [1.5, 2.2, 1.1, 1.5])

    # Test that join works when mixing Array{Union{T, Missing}} with Array{T} (issue #1151)
    df = DataFrame([collect(1:10), collect(2:11)], [:x, :y])
    dfmissing = DataFrame(x = Vector{Union{Int, Missing}}(1:10),
                        z = Vector{Union{Int, Missing}}(3:12))
    @test innerjoin(df, dfmissing, on = :x) ==
        DataFrame([collect(1:10), collect(2:11), collect(3:12)], [:x, :y, :z])
    @test innerjoin(dfmissing, df, on = :x) ==
        DataFrame([Vector{Union{Int, Missing}}(1:10), Vector{Union{Int, Missing}}(3:12),
                collect(2:11)], [:x, :z, :y])
end

@testset "all joins" begin
    df1 = DataFrame(Any[[1, 3, 5], [1.0, 3.0, 5.0]], [:id, :fid])
    df2 = DataFrame(Any[[0, 1, 2, 3, 4], [0.0, 1.0, 2.0, 3.0, 4.0]], [:id, :fid])

    @test crossjoin(df1, df2, makeunique=true) ==
        DataFrame(Any[repeat([1, 3, 5], inner = 5),
                      repeat([1, 3, 5], inner = 5),
                      repeat([0, 1, 2, 3, 4], outer = 3),
                      repeat([0, 1, 2, 3, 4], outer = 3)],
                  [:id, :fid, :id_1, :fid_1])
    @test typeof.(eachcol(crossjoin(df1, df2, makeunique=true))) ==
        [Vector{Int}, Vector{Float64}, Vector{Int}, Vector{Float64}]

    i(on) = innerjoin(df1, df2, on = on, makeunique=true)
    l(on) = leftjoin(df1, df2, on = on, makeunique=true)
    r(on) = rightjoin(df1, df2, on = on, makeunique=true)
    o(on) = outerjoin(df1, df2, on = on, makeunique=true)
    s(on) = semijoin(df1, df2, on = on, makeunique=true)
    a(on) = antijoin(df1, df2, on = on, makeunique=true)

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
    @test l(on) ≅ DataFrame(id = [1, 3, 5],
                            fid = [1, 3, 5],
                            fid_1 = [1, 3, missing])
    @test typeof.(eachcol(l(on))) ==
        [Vector{Int}, Vector{Float64}, Vector{Union{Float64, Missing}}]
    @test r(on) ≅ DataFrame(id = [1, 3, 0, 2, 4],
                            fid = [1, 3, missing, missing, missing],
                            fid_1 = [1, 3, 0, 2, 4])
    @test typeof.(eachcol(r(on))) ==
        [Vector{Int}, Vector{Union{Float64, Missing}}, Vector{Float64}]
    @test o(on) ≅ DataFrame(id = [1, 3, 5, 0, 2, 4],
                            fid = [1, 3, 5, missing, missing, missing],
                            fid_1 = [1, 3, missing, 0, 2, 4])
    @test typeof.(eachcol(o(on))) ==
        [Vector{Int}, Vector{Union{Float64, Missing}}, Vector{Union{Float64, Missing}}]

    on = :fid
    @test i(on) == DataFrame([[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
    @test typeof.(eachcol(i(on))) == [Vector{Int}, Vector{Float64}, Vector{Int}]
    @test l(on) ≅ DataFrame(id = [1, 3, 5],
                            fid = [1, 3, 5],
                            id_1 = [1, 3, missing])
    @test typeof.(eachcol(l(on))) == [Vector{Int}, Vector{Float64},
                                     Vector{Union{Int, Missing}}]
    @test r(on) ≅ DataFrame(id = [1, 3, missing, missing, missing],
                            fid = [1, 3, 0, 2, 4],
                            id_1 = [1, 3, 0, 2, 4])
    @test typeof.(eachcol(r(on))) == [Vector{Union{Int, Missing}}, Vector{Float64},
                                     Vector{Int}]
    @test o(on) ≅ DataFrame(id = [1, 3, 5, missing, missing, missing],
                            fid = [1, 3, 5, 0, 2, 4],
                            id_1 = [1, 3, missing, 0, 2, 4])
    @test typeof.(eachcol(o(on))) == [Vector{Union{Int, Missing}}, Vector{Float64},
                                     Vector{Union{Int, Missing}}]

    on = [:id, :fid]
    @test i(on) == DataFrame([[1, 3], [1, 3]], [:id, :fid])
    @test typeof.(eachcol(i(on))) == [Vector{Int}, Vector{Float64}]
    @test l(on) == DataFrame(id = [1, 3, 5], fid = [1, 3, 5])
    @test typeof.(eachcol(l(on))) == [Vector{Int}, Vector{Float64}]
    @test r(on) == DataFrame(id = [1, 3, 0, 2, 4], fid = [1, 3, 0, 2, 4])
    @test typeof.(eachcol(r(on))) == [Vector{Int}, Vector{Float64}]
    @test o(on) == DataFrame(id = [1, 3, 5, 0, 2, 4], fid = [1, 3, 5, 0, 2, 4])
    @test typeof.(eachcol(o(on))) == [Vector{Int}, Vector{Float64}]
end

@testset "all joins with CategoricalArrays" begin
    df1 = DataFrame(Any[CategoricalArray([1, 3, 5]),
                        CategoricalArray([1.0, 3.0, 5.0])], [:id, :fid])
    df2 = DataFrame(Any[CategoricalArray([0, 1, 2, 3, 4]),
                        CategoricalArray([0.0, 1.0, 2.0, 3.0, 4.0])], [:id, :fid])

    @test crossjoin(df1, df2, makeunique=true) ==
        DataFrame([repeat([1, 3, 5], inner = 5),
                   repeat([1, 3, 5], inner = 5),
                   repeat([0, 1, 2, 3, 4], outer = 3),
                   repeat([0, 1, 2, 3, 4], outer = 3)],
                  [:id, :fid, :id_1, :fid_1])
    @test all(isa.(eachcol(crossjoin(df1, df2, makeunique=true)),
                   [CategoricalVector{T} for T in (Int, Float64, Int, Float64)]))

    i(on) = innerjoin(df1, df2, on = on, makeunique=true)
    l(on) = leftjoin(df1, df2, on = on, makeunique=true)
    r(on) = rightjoin(df1, df2, on = on, makeunique=true)
    o(on) = outerjoin(df1, df2, on = on, makeunique=true)
    s(on) = semijoin(df1, df2, on = on, makeunique=true)
    a(on) = antijoin(df1, df2, on = on, makeunique=true)

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
    @test l(on) ≅ DataFrame(id = [1, 3, 5],
                            fid = [1, 3, 5],
                            fid_1 = [1, 3, missing])
    @test all(isa.(eachcol(l(on)),
                   [CategoricalVector{T} for T in (Int,Float64,Union{Float64, Missing})]))
    @test r(on) ≅ DataFrame(id = [1, 3, 0, 2, 4],
                            fid = [1, 3, missing, missing, missing],
                            fid_1 = [1, 3, 0, 2, 4])
    @test all(isa.(eachcol(r(on)),
                   [CategoricalVector{T} for T in (Int,Union{Float64, Missing},Float64)]))
    @test o(on) ≅ DataFrame(id = [1, 3, 5, 0, 2, 4],
                            fid = [1, 3, 5, missing, missing, missing],
                            fid_1 = [1, 3, missing, 0, 2, 4])
    @test all(isa.(eachcol(o(on)),
                   [CategoricalVector{T} for T in (Int,Union{Float64,Missing},Union{Float64, Missing})]))

    on = :fid
    @test i(on) == DataFrame([[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
    @test all(isa.(eachcol(i(on)),
                   [CategoricalVector{T} for T in (Int, Float64, Int)]))
    @test l(on) ≅ DataFrame(id = [1, 3, 5],
                            fid = [1, 3, 5],
                            id_1 = [1, 3, missing])
    @test all(isa.(eachcol(l(on)),
                   [CategoricalVector{T} for T in (Int, Float64, Union{Int, Missing})]))
    @test r(on) ≅ DataFrame(id = [1, 3, missing, missing, missing],
                            fid = [1, 3, 0, 2, 4],
                            id_1 = [1, 3, 0, 2, 4])
    @test all(isa.(eachcol(r(on)),
                   [CategoricalVector{T} for T in (Union{Int, Missing}, Float64, Int)]))
    @test o(on) ≅ DataFrame(id = [1, 3, 5, missing, missing, missing],
                            fid = [1, 3, 5, 0, 2, 4],
                            id_1 = [1, 3, missing, 0, 2, 4])
    @test all(isa.(eachcol(o(on)),
                   [CategoricalVector{T} for T in (Union{Int, Missing}, Float64, Union{Int, Missing})]))

    on = [:id, :fid]
    @test i(on) == DataFrame([[1, 3], [1, 3]], [:id, :fid])
    @test all(isa.(eachcol(i(on)),
                   [CategoricalVector{T} for T in (Int, Float64)]))
    @test l(on) == DataFrame(id = [1, 3, 5],
                             fid = [1, 3, 5])
    @test all(isa.(eachcol(l(on)),
                   [CategoricalVector{T} for T in (Int, Float64)]))
    @test r(on) == DataFrame(id = [1, 3, 0, 2, 4],
                             fid = [1, 3, 0, 2, 4])
    @test all(isa.(eachcol(r(on)),
                   [CategoricalVector{T} for T in (Int, Float64)]))
    @test o(on) == DataFrame(id = [1, 3, 5, 0, 2, 4],
                             fid = [1, 3, 5, 0, 2, 4])
    @test all(isa.(eachcol(o(on)),
                   [CategoricalVector{T} for T in (Int, Float64)]))
end

@testset "maintain CategoricalArray levels ordering on join - non-`on` cols" begin
    A = DataFrame(a = [1, 2, 3], b = ["a", "b", "c"])
    B = DataFrame(b = ["a", "b", "c"], c = CategoricalVector(["a", "b", "b"]))
    levels!(B.c, ["b", "a"])
    @test levels(innerjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(innerjoin(B, A, on=:b).c) == ["b", "a"]
    @test levels(leftjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(rightjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(outerjoin(A, B, on=:b).c) == ["b", "a"]
    @test levels(semijoin(B, A, on=:b).c) == ["b", "a"]
end

@testset "maintain CategoricalArray levels ordering on join - ordering conflicts" begin
    A = DataFrame(a = [1, 2, 3, 4], b = CategoricalVector(["a", "b", "c", "d"]))
    levels!(A.b, ["d", "c", "b", "a"])
    B = DataFrame(b = CategoricalVector(["a", "b", "c"]), c = [5, 6, 7])
    @test levels(innerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(innerjoin(B, A, on=:b).b) == ["a", "b", "c"]
    @test levels(leftjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(leftjoin(B, A, on=:b).b) == ["a", "b", "c"]
    @test levels(rightjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(rightjoin(B, A, on=:b).b) == ["a", "b", "d", "c"]
    @test levels(outerjoin(B, A, on=:b).b) == ["a", "b", "d", "c"]
    @test levels(outerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(semijoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(semijoin(B, A, on=:b).b) == ["a", "b", "c"]
end

@testset "maintain CategoricalArray levels ordering on join - left is categorical" begin
    A = DataFrame(a = [1, 2, 3, 4], b = CategoricalVector(["a", "b", "c", "d"]))
    levels!(A.b, ["d", "c", "b", "a"])
    B = DataFrame(b = ["a", "b", "c"], c = [5, 6, 7])
    @test levels(innerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(innerjoin(B, A, on=:b).b) == ["a", "b", "c"]
    @test levels(leftjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(leftjoin(B, A, on=:b).b) == ["a", "b", "c"]
    @test levels(rightjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(rightjoin(B, A, on=:b).b) == ["a", "b", "c", "d"]
    @test levels(outerjoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(outerjoin(B, A, on=:b).b) == ["a", "b", "c", "d"]
    @test levels(semijoin(A, B, on=:b).b) == ["d", "c", "b", "a"]
    @test levels(semijoin(B, A, on=:b).b) == ["a", "b", "c"]
end

@testset "join on columns with different left/right names" begin
    left = DataFrame(id = 1:7, sid = string.(1:7))
    right = DataFrame(ID = 3:10, SID = string.(3:10))

    @test innerjoin(left, right, on = :id => :ID) ==
        DataFrame(id = 3:7, sid = string.(3:7), SID = string.(3:7))
    @test innerjoin(left, right, on = [:id => :ID]) ==
        DataFrame(id = 3:7, sid = string.(3:7), SID = string.(3:7))
    @test innerjoin(left, right, on = [:id => :ID, :sid => :SID]) ==
        DataFrame(id = 3:7, sid = string.(3:7))

    @test leftjoin(left, right, on = :id => :ID) ≅
        DataFrame(id = 1:7, sid = string.(1:7),
                  SID = [missing, missing, string.(3:7)...])
    @test leftjoin(left, right, on = [:id => :ID]) ≅
        DataFrame(id = 1:7, sid = string.(1:7),
                  SID = [missing, missing, string.(3:7)...])
    @test leftjoin(left, right, on = [:id => :ID, :sid => :SID]) ==
        DataFrame(id = 1:7, sid = string.(1:7))

    @test rightjoin(left, right, on = :id => :ID) ≅
        DataFrame(id = 3:10, sid = [string.(3:7)..., missing, missing, missing],
                 SID = string.(3:10))
    @test rightjoin(left, right, on = [:id => :ID]) ≅
        DataFrame(id = 3:10, sid = [string.(3:7)..., missing, missing, missing],
                 SID = string.(3:10))
    @test rightjoin(left, right, on = [:id => :ID, :sid => :SID]) ≅
        DataFrame(id = 3:10, sid = string.(3:10))

    @test outerjoin(left, right, on = :id => :ID) ≅
        DataFrame(id = 1:10, sid = [string.(1:7)..., missing, missing, missing],
                  SID = [missing, missing, string.(3:10)...])
    @test outerjoin(left, right, on = [:id => :ID]) ≅
        DataFrame(id = 1:10, sid = [string.(1:7)..., missing, missing, missing],
                  SID = [missing, missing, string.(3:10)...])
    @test outerjoin(left, right, on = [:id => :ID, :sid => :SID]) ≅
        DataFrame(id = 1:10, sid = string.(1:10))

    @test semijoin(left, right, on = :id => :ID) ==
        DataFrame(id = 3:7, sid = string.(3:7))
    @test semijoin(left, right, on = [:id => :ID]) ==
        DataFrame(id = 3:7, sid = string.(3:7))
    @test semijoin(left, right, on = [:id => :ID, :sid => :SID]) ==
        DataFrame(id = 3:7, sid = string.(3:7))

    @test antijoin(left, right, on = :id => :ID) ==
        DataFrame(id = 1:2, sid = string.(1:2))
    @test antijoin(left, right, on = [:id => :ID]) ==
        DataFrame(id = 1:2, sid = string.(1:2))
    @test antijoin(left, right, on = [:id => :ID, :sid => :SID]) ==
        DataFrame(id = 1:2, sid = string.(1:2))

    @test_throws ArgumentError innerjoin(left, right, on = (:id, :ID))
end

@testset "join with a column of type Any" begin
    l = DataFrame(a=Any[1:7;], b=[1:7;])
    r = DataFrame(a=Any[3:10;], b=[3:10;])

    # join by :a and :b (Any is the on-column)
    @test innerjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=Any[3:7;], b=3:7)
    @test eltype.(eachcol(innerjoin(l, r, on=[:a, :b]))) == [Any, Int]

    @test leftjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=Any[1:7;], b=1:7)
    @test eltype.(eachcol(leftjoin(l, r, on=[:a, :b]))) == [Any, Int]

    @test rightjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=Any[3:10;], b=3:10)
    @test eltype.(eachcol(rightjoin(l, r, on=[:a, :b]))) == [Any, Int]

    @test outerjoin(l, r, on=[:a, :b]) ≅ DataFrame(a=Any[1:10;], b=1:10)
    @test eltype.(eachcol(outerjoin(l, r, on=[:a, :b]))) == [Any, Int]

    # join by :b (Any is not on-column)
    @test innerjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=Any[3:7;], b=3:7, a_1=Any[3:7;])
    @test eltype.(eachcol(innerjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]

    @test leftjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=Any[1:7;], b=1:7, a_1=[fill(missing, 2); 3:7;])
    @test eltype.(eachcol(leftjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]

    @test rightjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=[3:7; fill(missing, 3)], b=3:10, a_1=Any[3:10;])
    @test eltype.(eachcol(rightjoin(l, r, on=:b, makeunique=true))) == [Any, Int, Any]

    @test outerjoin(l, r, on=:b, makeunique=true) ≅
        DataFrame(a=[1:7; fill(missing, 3)], b=1:10, a_1=[fill(missing, 2); 3:10;])
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

@testset "indicator columns" begin
    outer_indicator = DataFrame(ID = [1, 2, 2, 3, 4],
                                Name = ["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
                                Job = ["Lawyer", "Doctor", "Florist", missing, "Farmer"],
                                _merge = ["both", "both", "both", "left_only", "right_only"])

    # Check that input data frame isn't modified (#1434)
    pre_join_name = copy(name)
    pre_join_job = copy(job)
    @test outerjoin(name, job, on = :ID, indicator=:_merge,
               makeunique=true) ≅
          outerjoin(name, job, on = :ID, indicator="_merge",
               makeunique=true) ≅ outer_indicator

    @test name ≅ pre_join_name
    @test job ≅ pre_join_job

    # Works with conflicting names
    name2 = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"],
                     _left = [1, 1, 1])
    job2 = DataFrame(ID = [1, 2, 2, 4], Job = ["Lawyer", "Doctor", "Florist", "Farmer"],
                    _left = [1, 1, 1, 1])

    outer_indicator = DataFrame(ID = [1, 2, 2, 3, 4],
                                Name = ["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
                                _left = [1, 1, 1, 1, missing],
                                Job = ["Lawyer", "Doctor", "Florist", missing, "Farmer"],
                                _left_1 = [1, 1, 1, missing, 1],
                                _left_2 = ["both", "both", "both", "left_only", "right_only"])

    @test outerjoin(name2, job2, on = :ID, indicator=:_left,
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
    for special in [missing, NaN, 0.0, -0.0]
        name_w_special = DataFrame(ID = [1, 2, 3, special],
                                   Name = ["John Doe", "Jane Doe", "Joe Blogs", "Maria Tester"])
        @test innerjoin(name_w_special, job, on=:ID, validate=(true, false)) == inner

        # Make sure duplicated special values still an exception
        name_w_special_dups = DataFrame(ID = [1, 2, 3, special, special],
                                        Name = ["John Doe", "Jane Doe", "Joe Blogs",
                                                "Maria Tester", "Jill Jillerson"])
        @test_throws ArgumentError innerjoin(name_w_special_dups, name, on=:ID,
                                        validate=(true, false))
    end

    # Check 0.0 and -0.0 seen as different
    name_w_zeros = DataFrame(ID = [1, 2, 3, 0.0, -0.0],
                             Name = ["John Doe", "Jane Doe",
                                     "Joe Blogs", "Maria Tester",
                                     "Jill Jillerson"])
    name_w_zeros2 = DataFrame(ID = [1, 2, 3, 0.0, -0.0],
                              Name = ["John Doe", "Jane Doe",
                                      "Joe Blogs", "Maria Tester",
                                      "Jill Jillerson"],
                              Name_1 = ["John Doe", "Jane Doe",
                                        "Joe Blogs", "Maria Tester",
                                        "Jill Jillerson"])

    @test innerjoin(name_w_zeros, name_w_zeros, on=:ID, validate=(true, true),
               makeunique=true) ≅ name_w_zeros2

    # Check for multiple-column merge keys
    name_multi = DataFrame(ID1 = [1, 1, 2],
                           ID2 = ["a", "b", "a"],
                           Name = ["John Doe", "Jane Doe", "Joe Blogs"])
    job_multi = DataFrame(ID1 = [1, 2, 2, 4],
                          ID2 = ["a", "b", "b", "c"],
                          Job = ["Lawyer", "Doctor", "Florist", "Farmer"])
    outer_multi = DataFrame(ID1 = [1, 1, 2, 2, 2, 4],
                            ID2 = ["a", "b", "a", "b", "b", "c"],
                            Name = ["John Doe", "Jane Doe", "Joe Blogs",
                                    missing, missing, missing],
                            Job = ["Lawyer", missing, missing,
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
    @test_throws AssertionError innerjoin(cname, cjob, on = :ID)

    cname = copy(name)
    cjob = copy(job)
    push!(cjob[!, 1], cjob[1, 1])
    @test_throws AssertionError innerjoin(cname, cjob, on = :ID)

    cname = copy(name)
    push!(DataFrames._columns(cname), cname[:, 1])
    @test_throws AssertionError innerjoin(cname, cjob, on = :ID)
end

@testset "multi data frame join" begin
    df1 = DataFrame(id=[1,2,3], x=[1,2,3])
    df2 = DataFrame(id=[1,2,4], y=[1,2,4])
    df3 = DataFrame(id=[1,3,4], z=[1,3,4])
    @test innerjoin(df1, df2, df3, on=:id) == DataFrame(id=1, x=1, y=1, z=1)
    @test outerjoin(df1, df2, df3, on=:id) ≅ DataFrame(id=[1,2,3,4],
                                                       x=[1,2,3,missing],
                                                       y=[1,2,missing,4],
                                                       z=[1,missing,3,4])
    @test_throws MethodError leftjoin(df1, df2, df3, on=:id)
    @test_throws MethodError rightjoin(df1, df2, df3, on=:id)
    @test_throws MethodError semijoin(df1, df2, df3, on=:id)
    @test_throws MethodError antijoin(df1, df2, df3, on=:id)

    dfc = crossjoin(df1, df2, df3, makeunique=true)
    @test dfc.x == dfc.id == repeat(1:3, inner=9)
    @test dfc.y == dfc.id_1 == repeat([1,2,4], inner=3, outer=3)
    @test dfc.z == dfc.id_2 == repeat([1,3,4], outer=9)

    df3[1,1] = 4
    @test_throws ArgumentError innerjoin(df1, df2, df3, on=:id, validate=(true,true))
end

@testset "flexible on in join" begin
    df1 = DataFrame(id=[1,2,3], id2=[11,12,13], x=[1,2,3])
    df2 = DataFrame(id=[1,2,4], ID2=[11,12,14], y=[1,2,4])
    @test innerjoin(df1, df2, on=[:id, :id2=>:ID2]) == DataFrame(id=[1,2], id2=[11, 12],
                                                                 x=[1,2], y=[1,2])
    @test innerjoin(df1, df2, on=[:id2=>:ID2, :id]) == DataFrame(id=[1,2], id2=[11, 12],
                                                                 x=[1,2], y=[1,2])
    @test innerjoin(df1, df2, on=[:id=>:id, :id2=>:ID2]) == DataFrame(id=[1,2], id2=[11, 12],
                                                                      x=[1,2], y=[1,2])
    @test innerjoin(df1, df2, on=[:id2=>:ID2, :id=>:id]) == DataFrame(id=[1,2], id2=[11, 12],
                                                                      x=[1,2], y=[1,2])
end

@testset "check naming of indicator" begin
    df = DataFrame(a=1)
    @test_throws ArgumentError outerjoin(df, df, on=:a, indicator=:a)
    @test outerjoin(df, df, on=:a, indicator=:a, makeunique=true) == DataFrame(a=1, a_1="both")
    @test outerjoin(df, df, on=:a, indicator="_left") == DataFrame(a=1, _left="both")
    @test outerjoin(df, df, on=:a, indicator="_right") == DataFrame(a=1, _right="both")

    df = DataFrame(_left=1)
    @test outerjoin(df, df, on=:_left, indicator="_leftX") == DataFrame(_left=1, _leftX="both")
    df = DataFrame(_right=1)
    @test outerjoin(df, df, on=:_right, indicator="_rightX") == DataFrame(_right=1, _rightX="both")
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
    df2 = DataFrame(d=[1,1], b=1)
    @test outerjoin(df1, df2, on=[:a => :d, :b], validate=(true, false)) == [df1; df1]
    df1 = DataFrame(a=[1, 1], b=1)
    df2 = DataFrame(d=[1, 1], b=1)
    @test outerjoin(df1, df2, on=[:a => :d, :b], validate=(false, false)) == [df1; df1]
end

@testset "rename tests" begin
    df1 = DataFrame(id1=[1,2,3], id2=[1,2,3], x=1:3)
    df2 = DataFrame(id1=[1,2,4], ID2=[1,2,4], x=1:3)

    @test_throws ArgumentError innerjoin(df1, df2, on=:id1)
    @test innerjoin(df1, df2, on=:id1, makeunique=true) ==
        DataFrame(id1=[1,2], id2=[1,2], x=[1,2], ID2=[1,2], x_1=[1,2])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test innerjoin(df1, df2, on=:id1,
                        makeunique = mu, validate = vl => vr, rename = l => r) ==
            DataFrame(id1=[1,2], id2_left=[1,2], x_left=[1,2], ID2_right=[1,2], x_right=[1,2])
    end

    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2])
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], makeunique=true) ==
        DataFrame(id1=[1,2], id2=[1,2], x=[1,2], x_1=[1,2])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                        makeunique = mu, validate = vl => vr, rename = l => r) ==
            DataFrame(id1=[1,2], id2=[1,2], x_left=[1,2], x_right=[1,2])
    end

    @test_throws ArgumentError leftjoin(df1, df2, on=:id1)
    @test leftjoin(df1, df2, on=:id1, makeunique=true) ≅
        DataFrame(id1=[1,2,3], id2=[1,2,3], x=[1,2,3], ID2=[1,2,missing], x_1=[1,2,missing])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test leftjoin(df1, df2, on=:id1,
                       makeunique = mu, validate = vl => vr, rename = l => r) ≅
            DataFrame(id1=[1,2,3], id2_left=[1,2,3], x_left=[1,2,3],
                      ID2_right=[1,2,missing], x_right=[1,2,missing])
    end

    @test_throws ArgumentError leftjoin(df1, df2, on=[:id1, :id2 => :ID2])
    @test leftjoin(df1, df2, on=[:id1, :id2 => :ID2], makeunique=true) ≅
        DataFrame(id1=[1,2,3], id2=[1,2,3], x=[1,2,3], x_1=[1,2,missing])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test leftjoin(df1, df2, on=[:id1, :id2 => :ID2],
                       makeunique = mu, validate = vl => vr, rename = l => r) ≅
            DataFrame(id1=[1,2,3], id2=[1,2,3], x_left=[1,2,3], x_right=[1,2,missing])
    end

    @test_throws ArgumentError leftjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                        rename = "_left" => "_right", indicator=:id1)
    @test_throws ArgumentError leftjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                        rename = "_left" => "_right", indicator=:x_left)
    @test leftjoin(df1, df2, on=[:id1, :id2 => :ID2],
                   rename = "_left" => "_right", indicator=:ind) ≅
          DataFrame(id1=[1,2,3], id2=[1,2,3], x_left=[1,2,3],
                    x_right=[1,2,missing], ind=["both", "both", "left_only"])

    @test_throws ArgumentError rightjoin(df1, df2, on=:id1)
    @test rightjoin(df1, df2, on=:id1, makeunique=true) ≅
        DataFrame(id1=[1,2,4], id2=[1,2,missing], x=[1,2,missing], ID2=[1,2,4], x_1=[1,2,3])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test rightjoin(df1, df2, on=:id1,
                       makeunique = mu, validate = vl => vr, rename = l => r) ≅
            DataFrame(id1=[1,2,4], id2_left=[1,2,missing], x_left=[1,2,missing],
                      ID2_right=[1,2,4], x_right=[1,2,3])
    end

    @test_throws ArgumentError rightjoin(df1, df2, on=[:id1, :id2 => :ID2])
    @test rightjoin(df1, df2, on=[:id1, :id2 => :ID2], makeunique=true) ≅
        DataFrame(id1=[1,2,4], id2=[1,2,4], x=[1,2,missing], x_1=[1,2,3])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test rightjoin(df1, df2, on=[:id1, :id2 => :ID2],
                       makeunique = mu, validate = vl => vr, rename = l => r) ≅
            DataFrame(id1=[1,2,4], id2=[1,2,4], x_left=[1,2,missing], x_right=[1,2,3])
    end

    @test_throws ArgumentError rightjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                         rename = "_left" => "_right", indicator=:id1)
    @test_throws ArgumentError rightjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                         rename = "_left" => "_right", indicator=:x_left)
    @test rightjoin(df1, df2, on=[:id1, :id2 => :ID2],
                    rename = "_left" => "_right", indicator=:ind) ≅
          DataFrame(id1=[1,2,4], id2=[1,2,4], x_left=[1,2,missing],
                    x_right=[1,2,3], ind=["both", "both", "right_only"])

    @test_throws ArgumentError outerjoin(df1, df2, on=:id1)
    @test outerjoin(df1, df2, on=:id1, makeunique=true) ≅
        DataFrame(id1=[1,2,3,4], id2=[1,2,3,missing], x=[1,2,3,missing],
                  ID2=[1,2,missing,4], x_1=[1,2,missing,3])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test outerjoin(df1, df2, on=:id1,
                       makeunique = mu, validate = vl => vr, rename = l => r) ≅
            DataFrame(id1=[1,2,3,4], id2_left=[1,2,3,missing], x_left=[1,2,3,missing],
                      ID2_right=[1,2,missing,4], x_right=[1,2,missing,3])
    end

    @test_throws ArgumentError outerjoin(df1, df2, on=[:id1, :id2 => :ID2])
    @test outerjoin(df1, df2, on=[:id1, :id2 => :ID2], makeunique=true) ≅
        DataFrame(id1=[1,2,3,4], id2=[1,2,3,4], x=[1,2,3,missing], x_1=[1,2,missing,3])
    for l in ["_left", :_left, x -> x * "_left"],
        r in ["_right", :_right, x -> x * "_right"],
        mu in [true, false], vl in [true, false], vr in [true, false]
        @test outerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                       makeunique = mu, validate = vl => vr, rename = l => r) ≅
            DataFrame(id1=[1,2,3,4], id2=[1,2,3,4], x_left=[1,2,3,missing], x_right=[1,2,missing,3])
    end

    @test_throws ArgumentError outerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                         rename = "_left" => "_right", indicator=:id1)
    @test_throws ArgumentError outerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                                         rename = "_left" => "_right", indicator=:x_left)
    @test outerjoin(df1, df2, on=[:id1, :id2 => :ID2],
                    rename = "_left" => "_right", indicator=:ind) ≅
          DataFrame(id1=[1,2,3,4], id2=[1,2,3,4], x_left=[1,2,3,missing],
                    x_right=[1,2,missing,3], ind=["both", "both", "left_only", "right_only"])

    df1.x .+= 10
    df2.x .+= 100
    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2], rename = (x -> :id1) => "_right")
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], rename = (x -> :id1) => "_right", makeunique=true) ==
          DataFrame(id1=1:2, id2=1:2, id1_1=11:12, x_right=101:102)
    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2], rename = "_left" => (x -> :id2))
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], rename = "_left" => (x -> :id2), makeunique=true) ==
          DataFrame(id1=1:2, id2=1:2, x_left=11:12, id2_1=101:102)
    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2], rename = "_left" => "_left")
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], rename = "_left" => "_left", makeunique=true) ==
          DataFrame(id1=1:2, id2=1:2, x_left=11:12, x_left_1=101:102)
    df2.y = df2.x .+ 1
    @test_throws ArgumentError innerjoin(df1, df2, on=[:id1, :id2 => :ID2], rename = "_left" => (x -> :newcol))
    @test innerjoin(df1, df2, on=[:id1, :id2 => :ID2], rename = "_left" => (x -> :newcol), makeunique=true) ==
          DataFrame(id1=1:2, id2=1:2, x_left=11:12, newcol=101:102, newcol_1=102:103)
end

@testset "careful indicator test" begin
    Random.seed!(1234)
    for i in 5:15, j in 5:15
        df1 = DataFrame(id=rand(1:10, i), x=1:i)
        df2 = DataFrame(id=rand(1:10, j), y=1:j)
        dfl = leftjoin(df1, df2, on=:id, indicator=:ind)
        dfr = rightjoin(df1, df2, on=:id, indicator=:ind)
        dfo = outerjoin(df1, df2, on=:id, indicator=:ind)
        @test issorted(dfl.x)
        @test issorted(string.(dfr.ind)) # use the fact that "both" < "right_only"
        @test issorted(dfr.y[dfr.ind .== "both"])
        @test issorted(dfr.y[dfr.ind .== "right_only"])
        @test dfl ≅ dfo[1:nrow(dfl), :]
        @test issorted(dfo[nrow(dfl)+1:end, :y])
        @test all(==("right_only"), dfo[nrow(dfl)+1:end, :ind])
    end
end

@testset "removed join function" begin
    df1 = DataFrame(id=[1,2,3], x=[1,2,3])
    df2 = DataFrame(id=[1,2,4], y=[1,2,4])
    df3 = DataFrame(id=[1,3,4], z=[1,3,4])
    @test_throws ArgumentError join(df1, df2, df3, on=:id, kind=:left)
    @test_throws ArgumentError join(df1, df2, on=:id, kind=:inner)
end

end # module
