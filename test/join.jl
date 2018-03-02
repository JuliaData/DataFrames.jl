module TestJoin
    using Compat, Compat.Test, DataFrames
    using DataFrames: similar_missing
    const ≅ = isequal

    name = DataFrame(ID = Union{Int, Missing}[1, 2, 3],
                     Name = Union{String, Missing}["John Doe", "Jane Doe", "Joe Blogs"])
    job = DataFrame(ID = Union{Int, Missing}[1, 2, 2, 4],
                    Job = Union{String, Missing}["Lawyer", "Doctor", "Florist", "Farmer"])

    # Join on symbols or vectors of symbols
    join(name, job, on = :ID)
    join(name, job, on = [:ID])

    # Soon we won't allow natural joins
    @test_throws ArgumentError join(name, job)

    # Test output of various join types
    outer = DataFrame(ID = [1, 2, 2, 3, 4],
                      Name = ["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", missing],
                      Job = ["Lawyer", "Doctor", "Florist", missing, "Farmer"])

    # (Tests use current column ordering but don't promote it)
    right = outer[Bool[!ismissing(x) for x in outer[:Job]], [:ID, :Name, :Job]]
    left = outer[Bool[!ismissing(x) for x in outer[:Name]], :]
    inner = left[Bool[!ismissing(x) for x in left[:Job]], :]
    semi = unique(inner[:, [:ID, :Name]])
    anti = left[Bool[ismissing(x) for x in left[:Job]], [:ID, :Name]]

    @test join(name, job, on = :ID) == inner
    @test join(name, job, on = :ID, kind = :inner) == inner
    @test join(name, job, on = :ID, kind = :outer) ≅ outer
    @test join(name, job, on = :ID, kind = :left) ≅ left
    @test join(name, job, on = :ID, kind = :right) ≅ right
    @test join(name, job, on = :ID, kind = :semi) == semi
    @test join(name, job, on = :ID, kind = :anti) == anti
    @test_throws ArgumentError join(name, job)
    @test_throws ArgumentError join(name, job, on=:ID, kind=:other)

    # Join with no non-key columns
    on = [:ID]
    nameid = name[on]
    jobid = job[on]

    @test join(nameid, jobid, on = :ID) == inner[on]
    @test join(nameid, jobid, on = :ID, kind = :inner) == inner[on]
    @test join(nameid, jobid, on = :ID, kind = :outer) == outer[on]
    @test join(nameid, jobid, on = :ID, kind = :left) == left[on]
    @test join(nameid, jobid, on = :ID, kind = :right) == right[on]
    @test join(nameid, jobid, on = :ID, kind = :semi) == semi[on]
    @test join(nameid, jobid, on = :ID, kind = :anti) == anti[on]

    # Join on multiple keys
    df1 = DataFrame(A = 1, B = 2, C = 3)
    df2 = DataFrame(A = 1, B = 2, D = 4)

    join(df1, df2, on = [:A, :B])

    # Test output of cross joins
    df1 = DataFrame(A = 1:2, B = 'a':'b')
    df2 = DataFrame(A = 1:3, C = 3:5)

    cross = DataFrame(A = [1, 1, 1, 2, 2, 2],
                      B = ['a', 'a', 'a', 'b', 'b', 'b'],
                      C = [3, 4, 5, 3, 4, 5])

    @test join(df1, df2[[:C]], kind = :cross) == cross

    # Cross joins handle naming collisions
    @test size(join(df1, df1, kind = :cross, makeunique=true)) == (4, 4)

    # Cross joins don't take keys
    @test_throws ArgumentError join(df1, df2, on = :A, kind = :cross)

    @testset "Test empty inputs 1" begin
        simple_df(len::Int, col=:A) = (df = DataFrame();
                                       df[col]=Vector{Union{Int, Missing}}(1:len);
                                       df)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :left) == simple_df(0)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :left) == simple_df(2)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :left) == simple_df(0)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :right) == simple_df(0)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :right) == simple_df(2)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :right) == simple_df(0)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :inner) == simple_df(0)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :inner) == simple_df(0)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :inner) == simple_df(0)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :outer) == simple_df(0)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :outer) == simple_df(2)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :outer) == simple_df(2)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :semi) == simple_df(0)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :semi) == simple_df(0)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :semi) == simple_df(0)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :anti) == simple_df(0)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :anti) == simple_df(2)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :anti) == simple_df(0)
        @test join(simple_df(0), simple_df(0, :B), kind = :cross) == DataFrame(A=Int[],
                                                                               B=Int[])
        @test join(simple_df(0), simple_df(2, :B), kind = :cross) == DataFrame(A=Int[],
                                                                               B=Int[])
        @test join(simple_df(2), simple_df(0, :B), kind = :cross) == DataFrame(A=Int[],
                                                                               B=Int[])
    end

    @testset "Test empty inputs 2" begin
        simple_df(len::Int, col=:A) = (df = DataFrame(); df[col]=collect(1:len); df)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :left) ==  simple_df(0)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :left) ==  simple_df(2)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :left) ==  simple_df(0)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :right) == simple_df(0)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :right) == simple_df(2)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :right) == simple_df(0)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :inner) == simple_df(0)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :inner) == simple_df(0)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :inner) == simple_df(0)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :outer) == simple_df(0)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :outer) == simple_df(2)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :outer) == simple_df(2)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :semi) ==  simple_df(0)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :semi) ==  simple_df(0)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :semi) ==  simple_df(0)
        @test join(simple_df(0), simple_df(0), on = :A, kind = :anti) ==  simple_df(0)
        @test join(simple_df(2), simple_df(0), on = :A, kind = :anti) ==  simple_df(2)
        @test join(simple_df(0), simple_df(2), on = :A, kind = :anti) ==  simple_df(0)
        @test join(simple_df(0), simple_df(0, :B), kind = :cross) == DataFrame(A=Int[],
                                                                               B=Int[])
        @test join(simple_df(0), simple_df(2, :B), kind = :cross) == DataFrame(A=Int[],
                                                                               B=Int[])
        @test join(simple_df(2), simple_df(0, :B), kind = :cross) == DataFrame(A=Int[],
                                                                               B=Int[])
    end

    # issue #960
    df1 = DataFrame(A = 1:50,
                    B = 1:50,
                    C = 1)
    categorical!(df1, :A)
    categorical!(df1, :B)
    join(df1, df1, on = [:A, :B], kind = :inner, makeunique=true)

    # Test that join works when mixing Array{Union{T, Missing}} with Array{T} (issue #1088)
    df = DataFrame(Name = Union{String, Missing}["A", "B", "C"],
                   Mass = [1.5, 2.2, 1.1])
    df2 = DataFrame(Name = ["A", "B", "C", "A"],
                    Quantity = [3, 3, 2, 4])
    @test join(df2, df, on=:Name, kind=:left) == DataFrame(Name = ["A", "B", "C", "A"],
                                                           Quantity = [3, 3, 2, 4],
                                                           Mass = [1.5, 2.2, 1.1, 1.5])

    # Test that join works when mixing Array{Union{T, Missing}} with Array{T} (issue #1151)
    df = DataFrame([collect(1:10), collect(2:11)], [:x, :y])
    dfmissing = DataFrame(x = Vector{Union{Int, Missing}}(1:10),
                          z = Vector{Union{Int, Missing}}(3:12))
    @test join(df, dfmissing, on = :x) ==
        DataFrame([collect(1:10), collect(2:11), collect(3:12)], [:x, :y, :z])
    @test join(dfmissing, df, on = :x) ==
        DataFrame([Vector{Union{Int, Missing}}(1:10), Vector{Union{Int, Missing}}(3:12),
                   collect(2:11)], [:x, :z, :y])

    @testset "all joins" begin
        df1 = DataFrame(Any[[1, 3, 5], [1.0, 3.0, 5.0]], [:id, :fid])
        df2 = DataFrame(Any[[0, 1, 2, 3, 4], [0.0, 1.0, 2.0, 3.0, 4.0]], [:id, :fid])

        @test join(df1, df2, kind=:cross, makeunique=true) ==
            DataFrame(Any[repeat([1, 3, 5], inner = 5),
                          repeat([1, 3, 5], inner = 5),
                          repeat([0, 1, 2, 3, 4], outer = 3),
                          repeat([0, 1, 2, 3, 4], outer = 3)],
                      [:id, :fid, :id_1, :fid_1])
        @test typeof.(join(df1, df2, kind=:cross, makeunique=true).columns) ==
            [Vector{Int}, Vector{Float64}, Vector{Int}, Vector{Float64}]

        i(on) = join(df1, df2, on = on, kind = :inner, makeunique=true)
        l(on) = join(df1, df2, on = on, kind = :left, makeunique=true)
        r(on) = join(df1, df2, on = on, kind = :right, makeunique=true)
        o(on) = join(df1, df2, on = on, kind = :outer, makeunique=true)
        s(on) = join(df1, df2, on = on, kind = :semi, makeunique=true)
        a(on) = join(df1, df2, on = on, kind = :anti, makeunique=true)

        @test s(:id) ==
              s(:fid) ==
              s([:id, :fid]) == DataFrame(Any[[1, 3], [1, 3]], [:id, :fid])
        @test typeof.(s(:id).columns) ==
              typeof.(s(:fid).columns) ==
              typeof.(s([:id, :fid]).columns) == [Vector{Int}, Vector{Float64}]
        @test a(:id) ==
              a(:fid) ==
              a([:id, :fid]) == DataFrame(Any[[5], [5]], [:id, :fid])
        @test typeof.(a(:id).columns) ==
              typeof.(a(:fid).columns) ==
              typeof.(a([:id, :fid]).columns) == [Vector{Int}, Vector{Float64}]

        on = :id
        @test i(on) == DataFrame(Any[[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}, Vector{Float64}]
        @test l(on) ≅ DataFrame(id = [1, 3, 5],
                                fid = [1, 3, 5],
                                fid_1 = [1, 3, missing])
        @test typeof.(l(on).columns) ==
            [Vector{Int}, Vector{Float64}, Vector{Union{Float64, Missing}}]
        @test r(on) ≅ DataFrame(id = [1, 3, 0, 2, 4],
                                fid = [1, 3, missing, missing, missing],
                                fid_1 = [1, 3, 0, 2, 4])
        @test typeof.(r(on).columns) ==
            [Vector{Int}, Vector{Union{Float64, Missing}}, Vector{Float64}]
        @test o(on) ≅ DataFrame(id = [1, 3, 5, 0, 2, 4],
                                fid = [1, 3, 5, missing, missing, missing],
                                fid_1 = [1, 3, missing, 0, 2, 4])
        @test typeof.(o(on).columns) ==
            [Vector{Int}, Vector{Union{Float64, Missing}}, Vector{Union{Float64, Missing}}]

        on = :fid
        @test i(on) == DataFrame(Any[[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}, Vector{Int}]
        @test l(on) ≅ DataFrame(id = [1, 3, 5],
                                fid = [1, 3, 5],
                                id_1 = [1, 3, missing])
        @test typeof.(l(on).columns) == [Vector{Int}, Vector{Float64},
                                         Vector{Union{Int, Missing}}]
        @test r(on) ≅ DataFrame(id = [1, 3, missing, missing, missing],
                                fid = [1, 3, 0, 2, 4],
                                id_1 = [1, 3, 0, 2, 4])
        @test typeof.(r(on).columns) == [Vector{Union{Int, Missing}}, Vector{Float64},
                                         Vector{Int}]
        @test o(on) ≅ DataFrame(id = [1, 3, 5, missing, missing, missing],
                                fid = [1, 3, 5, 0, 2, 4],
                                id_1 = [1, 3, missing, 0, 2, 4])
        @test typeof.(o(on).columns) == [Vector{Union{Int, Missing}}, Vector{Float64},
                                         Vector{Union{Int, Missing}}]

        on = [:id, :fid]
        @test i(on) == DataFrame(Any[[1, 3], [1, 3]], [:id, :fid])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}]
        @test l(on) == DataFrame(id = [1, 3, 5], fid = [1, 3, 5])
        @test typeof.(l(on).columns) == [Vector{Int}, Vector{Float64}]
        @test r(on) == DataFrame(id = [1, 3, 0, 2, 4], fid = [1, 3, 0, 2, 4])
        @test typeof.(r(on).columns) == [Vector{Int}, Vector{Float64}]
        @test o(on) == DataFrame(id = [1, 3, 5, 0, 2, 4], fid = [1, 3, 5, 0, 2, 4])
        @test typeof.(o(on).columns) == [Vector{Int}, Vector{Float64}]
    end

    @testset "all joins with CategoricalArrays" begin
        df1 = DataFrame(Any[CategoricalArray([1, 3, 5]),
                            CategoricalArray([1.0, 3.0, 5.0])], [:id, :fid])
        df2 = DataFrame(Any[CategoricalArray([0, 1, 2, 3, 4]),
                            CategoricalArray([0.0, 1.0, 2.0, 3.0, 4.0])], [:id, :fid])

        @test join(df1, df2, kind=:cross, makeunique=true) ==
            DataFrame(Any[repeat([1, 3, 5], inner = 5),
                          repeat([1, 3, 5], inner = 5),
                          repeat([0, 1, 2, 3, 4], outer = 3),
                          repeat([0, 1, 2, 3, 4], outer = 3)],
                      [:id, :fid, :id_1, :fid_1])
        @test all(isa.(join(df1, df2, kind=:cross, makeunique=true).columns,
                       [CategoricalVector{T} for T in (Int, Float64, Int, Float64)]))

        i(on) = join(df1, df2, on = on, kind = :inner, makeunique=true)
        l(on) = join(df1, df2, on = on, kind = :left, makeunique=true)
        r(on) = join(df1, df2, on = on, kind = :right, makeunique=true)
        o(on) = join(df1, df2, on = on, kind = :outer, makeunique=true)
        s(on) = join(df1, df2, on = on, kind = :semi, makeunique=true)
        a(on) = join(df1, df2, on = on, kind = :anti, makeunique=true)

        @test s(:id) ==
              s(:fid) ==
              s([:id, :fid]) == DataFrame(Any[[1, 3], [1, 3]], [:id, :fid])
        @test typeof.(s(:id).columns) ==
              typeof.(s(:fid).columns) ==
              typeof.(s([:id, :fid]).columns)
        @test all(isa.(s(:id).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))

        @test a(:id) ==
              a(:fid) ==
              a([:id, :fid]) == DataFrame(Any[[5], [5]], [:id, :fid])
        @test typeof.(a(:id).columns) ==
              typeof.(a(:fid).columns) ==
              typeof.(a([:id, :fid]).columns)
        @test all(isa.(a(:id).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))

        on = :id
        @test i(on) == DataFrame(Any[[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
        @test all(isa.(i(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64, Float64)]))
        @test l(on) ≅ DataFrame(id = [1, 3, 5],
                                fid = [1, 3, 5],
                                fid_1 = [1, 3, missing])
        @test all(isa.(l(on).columns,
                       [CategoricalVector{T} for T in (Int,Float64,Union{Float64, Missing})]))
        @test r(on) ≅ DataFrame(id = [1, 3, 0, 2, 4],
                                fid = [1, 3, missing, missing, missing],
                                fid_1 = [1, 3, 0, 2, 4])
        @test all(isa.(r(on).columns,
                       [CategoricalVector{T} for T in (Int,Union{Float64, Missing},Float64)]))
        @test o(on) ≅ DataFrame(id = [1, 3, 5, 0, 2, 4],
                                fid = [1, 3, 5, missing, missing, missing],
                                fid_1 = [1, 3, missing, 0, 2, 4])
        @test all(isa.(o(on).columns,
                       [CategoricalVector{T} for T in (Int,Union{Float64,Missing},Union{Float64, Missing})]))

        on = :fid
        @test i(on) == DataFrame(Any[[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
        @test all(isa.(i(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64, Int)]))
        @test l(on) ≅ DataFrame(id = [1, 3, 5],
                                fid = [1, 3, 5],
                                id_1 = [1, 3, missing])
        @test all(isa.(l(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64, Union{Int, Missing})]))
        @test r(on) ≅ DataFrame(id = [1, 3, missing, missing, missing],
                                fid = [1, 3, 0, 2, 4],
                                id_1 = [1, 3, 0, 2, 4])
        @test all(isa.(r(on).columns,
                       [CategoricalVector{T} for T in (Union{Int, Missing}, Float64, Int)]))
        @test o(on) ≅ DataFrame(id = [1, 3, 5, missing, missing, missing],
                                fid = [1, 3, 5, 0, 2, 4],
                                id_1 = [1, 3, missing, 0, 2, 4])
        @test all(isa.(o(on).columns,
                       [CategoricalVector{T} for T in (Union{Int, Missing}, Float64, Union{Int, Missing})]))

        on = [:id, :fid]
        @test i(on) == DataFrame(Any[[1, 3], [1, 3]], [:id, :fid])
        @test all(isa.(i(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))
        @test l(on) == DataFrame(id = [1, 3, 5],
                                 fid = [1, 3, 5])
        @test all(isa.(l(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))
        @test r(on) == DataFrame(id = [1, 3, 0, 2, 4],
                                 fid = [1, 3, 0, 2, 4])
        @test all(isa.(r(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))
        @test o(on) == DataFrame(id = [1, 3, 5, 0, 2, 4],
                                 fid = [1, 3, 5, 0, 2, 4])
        @test all(isa.(o(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))
    end

    @testset "maintain CategoricalArray levels ordering on join - non-`on` cols" begin
        A = DataFrame(a = [1, 2, 3], b = ["a", "b", "c"])
        B = DataFrame(b = ["a", "b", "c"], c = CategoricalVector(["a", "b", "b"]))
        levels!(B[:c], ["b", "a"])
        @test levels(join(A, B, on=:b, kind=:inner)[:c]) == ["b", "a"]
        @test levels(join(B, A, on=:b, kind=:inner)[:c]) == ["b", "a"]
        @test levels(join(A, B, on=:b, kind=:left)[:c]) == ["b", "a"]
        @test levels(join(A, B, on=:b, kind=:right)[:c]) == ["b", "a"]
        @test levels(join(A, B, on=:b, kind=:outer)[:c]) == ["b", "a"]
        @test levels(join(B, A, on=:b, kind = :semi)[:c]) == ["b", "a"]
    end

    @testset "maintain CategoricalArray levels ordering on join - ordering conflicts" begin
        A = DataFrame(a = [1, 2, 3, 4], b = CategoricalVector(["a", "b", "c", "d"]))
        levels!(A[:b], ["d", "c", "b", "a"])
        B = DataFrame(b = CategoricalVector(["a", "b", "c"]), c = [5, 6, 7])
        @test levels(join(A, B, on=:b, kind=:inner)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b, kind=:inner)[:b]) == ["a", "b", "c"]
        @test levels(join(A, B, on=:b, kind=:left)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b, kind=:left)[:b]) == ["a", "b", "c"]
        @test levels(join(A, B, on=:b, kind=:right)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b, kind=:right)[:b]) == ["a", "b", "d", "c"]
        @test levels(join(B, A, on=:b, kind=:outer)[:b]) == ["a", "b", "d", "c"]
        @test levels(join(A, B, on=:b, kind=:outer)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(A, B, on=:b, kind = :semi)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b, kind = :semi)[:b]) == ["a", "b", "c"]
    end

    @testset "maintain CategoricalArray levels ordering on join - left is categorical" begin
        A = DataFrame(a = [1, 2, 3, 4], b = CategoricalVector(["a", "b", "c", "d"]))
        levels!(A[:b], ["d", "c", "b", "a"])
        B = DataFrame(b = ["a", "b", "c"], c = [5, 6, 7])
        @test levels(join(A, B, on=:b)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b)[:b]) == ["a", "b", "c"]
        @test levels(join(A, B, on=:b, kind=:inner)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b, kind=:inner)[:b]) == ["a", "b", "c"]
        @test levels(join(A, B, on=:b, kind=:left)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b, kind=:left)[:b]) == ["a", "b", "c"]
        @test levels(join(A, B, on=:b, kind=:right)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b, kind=:right)[:b]) == ["a", "b", "c", "d"]
        @test levels(join(A, B, on=:b, kind=:outer)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b, kind=:outer)[:b]) == ["a", "b", "c", "d"]
        @test levels(join(A, B, on=:b, kind = :semi)[:b]) == ["d", "c", "b", "a"]
        @test levels(join(B, A, on=:b, kind = :semi)[:b]) == ["a", "b", "c"]
    end

    @testset "join on columns with different left/right names" begin
        left = DataFrame(id = 1:7, sid = string.(1:7))
        right = DataFrame(ID = 3:10, SID = string.(3:10))
        @test join(left, right, on = (:id, :ID), kind=:inner) ==
            DataFrame(id = 3:7, sid = string.(3:7), SID = string.(3:7))
        @test join(left, right, on = :id => :ID, kind=:inner) ==
            DataFrame(id = 3:7, sid = string.(3:7), SID = string.(3:7))
        @test join(left, right, on = [(:id, :ID)], kind=:inner) ==
            DataFrame(id = 3:7, sid = string.(3:7), SID = string.(3:7))
        @test join(left, right, on = [:id => :ID], kind=:inner) ==
            DataFrame(id = 3:7, sid = string.(3:7), SID = string.(3:7))
        @test join(left, right, on = [(:id, :ID), (:sid, :SID)], kind=:inner) ==
            DataFrame(id = 3:7, sid = string.(3:7))
        @test join(left, right, on = [:id => :ID, :sid => :SID], kind=:inner) ==
            DataFrame(id = 3:7, sid = string.(3:7))

        @test join(left, right, on = (:id, :ID), kind=:left) ≅
            DataFrame(id = 1:7, sid = string.(1:7),
                      SID = [missing, missing, string.(3:7)...])
        @test join(left, right, on = :id => :ID, kind=:left) ≅
            DataFrame(id = 1:7, sid = string.(1:7),
                      SID = [missing, missing, string.(3:7)...])
        @test join(left, right, on = [(:id, :ID)], kind=:left) ≅
            DataFrame(id = 1:7, sid = string.(1:7),
                      SID = [missing, missing, string.(3:7)...])
        @test join(left, right, on = [:id => :ID], kind=:left) ≅
            DataFrame(id = 1:7, sid = string.(1:7),
                      SID = [missing, missing, string.(3:7)...])
        @test join(left, right, on = [(:id, :ID), (:sid, :SID)], kind=:left) ==
            DataFrame(id = 1:7, sid = string.(1:7))
        @test join(left, right, on = [:id => :ID, :sid => :SID], kind=:left) ==
            DataFrame(id = 1:7, sid = string.(1:7))

        @test join(left, right, on = (:id, :ID), kind=:right) ≅
            DataFrame(id = 3:10, sid = [string.(3:7)..., missing, missing, missing],
                     SID = string.(3:10))
        @test join(left, right, on = :id => :ID, kind=:right) ≅
            DataFrame(id = 3:10, sid = [string.(3:7)..., missing, missing, missing],
                     SID = string.(3:10))
        @test join(left, right, on = [(:id, :ID)], kind=:right) ≅
            DataFrame(id = 3:10, sid = [string.(3:7)..., missing, missing, missing],
                     SID = string.(3:10))
        @test join(left, right, on = [:id => :ID], kind=:right) ≅
            DataFrame(id = 3:10, sid = [string.(3:7)..., missing, missing, missing],
                     SID = string.(3:10))
        @test join(left, right, on = [(:id, :ID), (:sid, :SID)], kind=:right) ≅
            DataFrame(id = 3:10, sid = string.(3:10))
        @test join(left, right, on = [:id => :ID, :sid => :SID], kind=:right) ≅
            DataFrame(id = 3:10, sid = string.(3:10))

        @test join(left, right, on = (:id, :ID), kind=:outer) ≅
            DataFrame(id = 1:10, sid = [string.(1:7)..., missing, missing, missing],
                      SID = [missing, missing, string.(3:10)...])
        @test join(left, right, on = :id => :ID, kind=:outer) ≅
            DataFrame(id = 1:10, sid = [string.(1:7)..., missing, missing, missing],
                      SID = [missing, missing, string.(3:10)...])
        @test join(left, right, on = [(:id, :ID)], kind=:outer) ≅
            DataFrame(id = 1:10, sid = [string.(1:7)..., missing, missing, missing],
                      SID = [missing, missing, string.(3:10)...])
        @test join(left, right, on = [:id => :ID], kind=:outer) ≅
            DataFrame(id = 1:10, sid = [string.(1:7)..., missing, missing, missing],
                      SID = [missing, missing, string.(3:10)...])
        @test join(left, right, on = [(:id, :ID), (:sid, :SID)], kind=:outer) ≅
            DataFrame(id = 1:10, sid = string.(1:10))
        @test join(left, right, on = [:id => :ID, :sid => :SID], kind=:outer) ≅
            DataFrame(id = 1:10, sid = string.(1:10))

        @test join(left, right, on = (:id, :ID), kind=:semi) ==
            DataFrame(id = 3:7, sid = string.(3:7))
        @test join(left, right, on = :id => :ID, kind=:semi) ==
            DataFrame(id = 3:7, sid = string.(3:7))
        @test join(left, right, on = [(:id, :ID)], kind=:semi) ==
            DataFrame(id = 3:7, sid = string.(3:7))
        @test join(left, right, on = [:id => :ID], kind=:semi) ==
            DataFrame(id = 3:7, sid = string.(3:7))
        @test join(left, right, on = [(:id, :ID), (:sid, :SID)], kind=:semi) ==
            DataFrame(id = 3:7, sid = string.(3:7))
        @test join(left, right, on = [:id => :ID, :sid => :SID], kind=:semi) ==
            DataFrame(id = 3:7, sid = string.(3:7))

        @test join(left, right, on = (:id, :ID), kind=:anti) ==
            DataFrame(id = 1:2, sid = string.(1:2))
        @test join(left, right, on = :id => :ID, kind=:anti) ==
            DataFrame(id = 1:2, sid = string.(1:2))
        @test join(left, right, on = [(:id, :ID)], kind=:anti) ==
            DataFrame(id = 1:2, sid = string.(1:2))
        @test join(left, right, on = [:id => :ID], kind=:anti) ==
            DataFrame(id = 1:2, sid = string.(1:2))
        @test join(left, right, on = [(:id, :ID), (:sid, :SID)], kind=:anti) ==
            DataFrame(id = 1:2, sid = string.(1:2))
        @test join(left, right, on = [:id => :ID, :sid => :SID], kind=:anti) ==
            DataFrame(id = 1:2, sid = string.(1:2))
    end

    @testset "join with a column of type Any" begin
        l = DataFrame(a=Any[1:7;], b=[1:7;])
        r = DataFrame(a=Any[3:10;], b=[3:10;])

        # join by :a and :b (Any is the on-column)
        @test join(l, r, on=[:a, :b], kind=:inner) ≅ DataFrame(a=Any[3:7;], b=3:7)
        @test eltypes(join(l, r, on=[:a, :b], kind=:inner)) == [Any, Int]

        @test join(l, r, on=[:a, :b], kind=:left) ≅ DataFrame(a=Any[1:7;], b=1:7)
        @test eltypes(join(l, r, on=[:a, :b], kind=:left)) == [Any, Int]

        @test join(l, r, on=[:a, :b], kind=:right) ≅ DataFrame(a=Any[3:10;], b=3:10)
        @test eltypes(join(l, r, on=[:a, :b], kind=:right)) == [Any, Int]

        @test join(l, r, on=[:a, :b], kind=:outer) ≅ DataFrame(a=Any[1:10;], b=1:10)
        @test eltypes(join(l, r, on=[:a, :b], kind=:outer)) == [Any, Int]

        # join by :b (Any is not on-column)
        @test join(l, r, on=:b, kind=:inner, makeunique=true) ≅
            DataFrame(a=Any[3:7;], b=3:7, a_1=Any[3:7;])
        @test eltypes(join(l, r, on=:b, kind=:inner, makeunique=true)) == [Any, Int, Any]

        @test join(l, r, on=:b, kind=:left, makeunique=true) ≅
            DataFrame(a=Any[1:7;], b=1:7, a_1=[fill(missing, 2); 3:7;])
        @test eltypes(join(l, r, on=:b, kind=:left, makeunique=true)) == [Any, Int, Any]

        @test join(l, r, on=:b, kind=:right, makeunique=true) ≅
            DataFrame(a=[3:7; fill(missing, 3)], b=3:10, a_1=Any[3:10;])
        @test eltypes(join(l, r, on=:b, kind=:right, makeunique=true)) == [Any, Int, Any]

        @test join(l, r, on=:b, kind=:outer, makeunique=true) ≅
            DataFrame(a=[1:7; fill(missing, 3)], b=1:10, a_1=[fill(missing, 2); 3:10;])
        @test eltypes(join(l, r, on=:b, kind=:outer, makeunique=true)) == [Any, Int, Any]
    end

    @testset "joins with categorical columns and no matching rows" begin
        l = DataFrame(a=1:3, b=categorical(["a", "b", "c"]))
        r = DataFrame(a=4:5, b=categorical(["d", "e"]))
        nl = size(l, 1)
        nr = size(r, 1)

        CS = eltype(l[:b])

        # joins by a and b
        @test join(l, r, on=[:a, :b], kind=:inner) ≅ DataFrame(a=Int[], b=similar(l[:a], 0))
        @test eltypes(join(l, r, on=[:a, :b], kind=:inner)) == [Int, CS]

        @test join(l, r, on=[:a, :b], kind=:left) ≅ DataFrame(a=l[:a], b=l[:b])
        @test eltypes(join(l, r, on=[:a, :b], kind=:left)) == [Int, CS]

        @test join(l, r, on=[:a, :b], kind=:right) ≅ DataFrame(a=r[:a], b=r[:b])
        @test eltypes(join(l, r, on=[:a, :b], kind=:right)) == [Int, CS]

        @test join(l, r, on=[:a, :b], kind=:outer) ≅
            DataFrame(a=vcat(l[:a], r[:a]), b=vcat(l[:b], r[:b]))
        @test eltypes(join(l, r, on=[:a, :b], kind=:outer)) == [Int, CS]

        # joins by a
        @test join(l, r, on=:a, kind=:inner, makeunique=true) ≅
            DataFrame(a=Int[], b=similar(l[:b], 0), b_1=similar(r[:b], 0))
        @test eltypes(join(l, r, on=:a, kind=:inner, makeunique=true)) == [Int, CS, CS]

        @test join(l, r, on=:a, kind=:left, makeunique=true) ≅
            DataFrame(a=l[:a], b=l[:b], b_1=similar_missing(r[:b], nl))
        @test eltypes(join(l, r, on=:a, kind=:left, makeunique=true)) ==
            [Int, CS, Union{CS, Missing}]

        @test join(l, r, on=:a, kind=:right, makeunique=true) ≅
            DataFrame(a=r[:a], b=similar_missing(l[:b], nr), b_1=r[:b])
        @test eltypes(join(l, r, on=:a, kind=:right, makeunique=true)) ==
            [Int, Union{CS, Missing}, CS]

        @test join(l, r, on=:a, kind=:outer, makeunique=true) ≅
            DataFrame(a=vcat(l[:a], r[:a]),
                      b=vcat(l[:b], fill(missing, nr)),
                      b_1=vcat(fill(missing, nl), r[:b]))
        @test eltypes(join(l, r, on=:a, kind=:outer, makeunique=true)) ==
            [Int, Union{CS, Missing}, Union{CS, Missing}]

        # joins by b
        @test join(l, r, on=:b, kind=:inner, makeunique=true) ≅
            DataFrame(a=Int[], b=similar(l[:b], 0), a_1=similar(r[:b], 0))
        @test eltypes(join(l, r, on=:b, kind=:inner, makeunique=true)) == [Int, CS, Int]

        @test join(l, r, on=:b, kind=:left, makeunique=true) ≅
            DataFrame(a=l[:a], b=l[:b], a_1=fill(missing, nl))
        @test eltypes(join(l, r, on=:b, kind=:left, makeunique=true)) ==
            [Int, CS, Union{Int, Missing}]

        @test join(l, r, on=:b, kind=:right, makeunique=true) ≅
            DataFrame(a=fill(missing, nr), b=r[:b], a_1=r[:a])
        @test eltypes(join(l, r, on=:b, kind=:right, makeunique=true)) ==
            [Union{Int, Missing}, CS, Int]

        @test join(l, r, on=:b, kind=:outer, makeunique=true) ≅
            DataFrame(a=vcat(l[:a], fill(missing, nr)),
                      b=vcat(l[:b], r[:b]),
                      a_1=vcat(fill(missing, nl), r[:a]))
        @test eltypes(join(l, r, on=:b, kind=:outer, makeunique=true)) ==
            [Union{Int, Missing}, CS, Union{Int, Missing}]
    end
end
