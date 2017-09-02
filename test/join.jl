module TestJoin
    using Base.Test, DataFrames

    name = DataFrame(ID = Union{Int, Null}[1, 2, 3],
                     Name = Union{String, Null}["John Doe", "Jane Doe", "Joe Blogs"])
    job = DataFrame(ID = Union{Int, Null}[1, 2, 2, 4],
                    Job = Union{String, Null}["Lawyer", "Doctor", "Florist", "Farmer"])

    # Join on symbols or vectors of symbols
    join(name, job, on = :ID)
    join(name, job, on = [:ID])

    # Soon we won't allow natural joins
    @test_throws ArgumentError join(name, job)

    # Test output of various join types
    outer = DataFrame(ID = [1, 2, 2, 3, 4],
                      Name = ["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", null],
                      Job = ["Lawyer", "Doctor", "Florist", null, "Farmer"])

    # (Tests use current column ordering but don't promote it)
    right = outer[Bool[!isnull(x) for x in outer[:Job]], [:ID, :Name, :Job]]
    left = outer[Bool[!isnull(x) for x in outer[:Name]], :]
    inner = left[Bool[!isnull(x) for x in left[:Job]], :]
    semi = unique(inner[:, [:ID, :Name]])
    anti = left[Bool[isnull(x) for x in left[:Job]], [:ID, :Name]]

    @test join(name, job, on = :ID) == inner
    @test join(name, job, on = :ID, kind = :inner) == inner
    @test join(name, job, on = :ID, kind = :outer) == outer
    @test join(name, job, on = :ID, kind = :left) == left
    @test join(name, job, on = :ID, kind = :right) == right
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
    @test size(join(df1, df1, kind = :cross)) == (4, 4)

    # Cross joins don't take keys
    @test_throws ArgumentError join(df1, df2, on = :A, kind = :cross)

    # test empty inputs
    simple_df(len::Int, col=:A) = (df = DataFrame();
                                   df[col]=Vector{Union{Int, Null}}(1:len);
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
    @test join(simple_df(0), simple_df(0, :B), kind = :cross) == DataFrame(A=Int[], B=Int[])
    @test join(simple_df(0), simple_df(2, :B), kind = :cross) == DataFrame(A=Int[], B=Int[])
    @test join(simple_df(2), simple_df(0, :B), kind = :cross) == DataFrame(A=Int[], B=Int[])

    # test empty inputs
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
    @test join(simple_df(0), simple_df(0, :B), kind = :cross) == DataFrame(A=Int[], B=Int[])
    @test join(simple_df(0), simple_df(2, :B), kind = :cross) == DataFrame(A=Int[], B=Int[])
    @test join(simple_df(2), simple_df(0, :B), kind = :cross) == DataFrame(A=Int[], B=Int[])

    # issue #960
    df1 = DataFrame(A = 1:50,
                    B = 1:50,
                    C = 1)
    categorical!(df1, :A)
    categorical!(df1, :B)
    join(df1, df1, on = [:A, :B], kind = :inner)

    # Test that join works when mixing Array{Union{T, Null}} with Array{T} (issue #1088)
    df = DataFrame(Name = Union{String, Null}["A", "B", "C"],
                   Mass = [1.5, 2.2, 1.1])
    df2 = DataFrame(Name = ["A", "B", "C", "A"],
                    Quantity = [3, 3, 2, 4])
    @test join(df2, df, on=:Name, kind=:left) == DataFrame(Name = ["A", "B", "C", "A"],
                                                           Quantity = [3, 3, 2, 4],
                                                           Mass = [1.5, 2.2, 1.1, 1.5])

    # Test that join works when mixing Array{Union{T, Null}} with Array{T} (issue #1151)
    df = DataFrame([collect(1:10), collect(2:11)], [:x, :y])
    dfnull = DataFrame(x = Vector{Union{Int, Null}}(1:10), z = Vector{Union{Int, Null}}(3:12))
    @test join(df, dfnull, on = :x) ==
        DataFrame([collect(1:10), collect(2:11), collect(3:12)], [:x, :y, :z])
    @test join(dfnull, df, on = :x) ==
        DataFrame([Vector{Union{Int, Null}}(1:10), Vector{Union{Int, Null}}(3:12), collect(2:11)], [:x, :z, :y])

    @testset "all joins" begin
        df1 = DataFrame(Any[[1, 3, 5], [1.0, 3.0, 5.0]], [:id, :fid])
        df2 = DataFrame(Any[[0, 1, 2, 3, 4], [0.0, 1.0, 2.0, 3.0, 4.0]], [:id, :fid])
        N = null

        @test join(df1, df2, kind=:cross) ==
            DataFrame(Any[repeat([1, 3, 5], inner = 5),
                          repeat([1, 3, 5], inner = 5),
                          repeat([0, 1, 2, 3, 4], outer = 3),
                          repeat([0, 1, 2, 3, 4], outer = 3)],
                      [:id, :fid, :id_1, :fid_1])
        @test typeof.(join(df1, df2, kind=:cross).columns) ==
            [Vector{Int}, Vector{Float64}, Vector{Int}, Vector{Float64}]

        i(on) = join(df1, df2, on = on, kind = :inner)
        l(on) = join(df1, df2, on = on, kind = :left)
        r(on) = join(df1, df2, on = on, kind = :right)
        o(on) = join(df1, df2, on = on, kind = :outer)
        s(on) = join(df1, df2, on = on, kind = :semi)
        a(on) = join(df1, df2, on = on, kind = :anti)

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
        @test l(on) == DataFrame(id = [1, 3, 5],
                                 fid = [1, 3, 5],
                                 fid_1 = [1, 3, N])
        @test typeof.(l(on).columns) ==
            [Vector{Union{T, Null}} for T in (Int, Float64, Float64)]
        @test r(on) == DataFrame(id = [1, 3, 0, 2, 4],
                                 fid = [1, 3, N, N, N],
                                 fid_1 = [1, 3, 0, 2, 4])
        @test typeof.(r(on).columns) ==
            [Vector{Union{T, Null}} for T in (Int, Float64, Float64)]
        @test o(on) == DataFrame(id = [1, 3, 5, 0, 2, 4],
                                 fid = [1, 3, 5, N, N, N],
                                 fid_1 = [1, 3, N, 0, 2, 4])
        @test typeof.(o(on).columns) ==
            [Vector{Union{T, Null}} for T in (Int, Float64, Float64)]

        on = :fid
        @test i(on) == DataFrame(Any[[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}, Vector{Int}]
        @test l(on) == DataFrame(id = [1, 3, 5],
                                 fid = [1, 3, 5],
                                 id_1 = [1, 3, N])
        @test typeof.(l(on).columns) == [Vector{Union{T, Null}} for T in (Int,Float64,Int)]
        @test r(on) == DataFrame(id = [1, 3, N, N, N],
                                 fid = [1, 3, 0, 2, 4],
                                 id_1 = [1, 3, 0, 2, 4])
        @test typeof.(r(on).columns) == [Vector{Union{T, Null}} for T in (Int,Float64,Int)]
        @test o(on) == DataFrame(id = [1, 3, 5, N, N, N],
                                 fid = [1, 3, 5, 0, 2, 4],
                                 id_1 = [1, 3, N, 0, 2, 4])
        @test typeof.(o(on).columns) == [Vector{Union{T, Null}} for T in (Int,Float64,Int)]

        on = [:id, :fid]
        @test i(on) == DataFrame(Any[[1, 3], [1, 3]], [:id, :fid])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}]
        @test l(on) == DataFrame(id = [1, 3, 5], fid = [1, 3, 5])
        @test typeof.(l(on).columns) == [Vector{Union{Int, Null}},
                                         Vector{Union{Float64, Null}}]
        @test r(on) == DataFrame(id = [1, 3, 0, 2, 4], fid = [1, 3, 0, 2, 4])
        @test typeof.(r(on).columns) == [Vector{Union{Int, Null}},
                                         Vector{Union{Float64, Null}}]
        @test o(on) == DataFrame(id = [1, 3, 5, 0, 2, 4], fid = [1, 3, 5, 0, 2, 4])
        @test typeof.(o(on).columns) == [Vector{Union{Int, Null}},
                                         Vector{Union{Float64, Null}}]
    end

    @testset "all joins with CategoricalArrays" begin
        df1 = DataFrame(Any[CategoricalArray([1, 3, 5]),
                            CategoricalArray([1.0, 3.0, 5.0])], [:id, :fid])
        df2 = DataFrame(Any[CategoricalArray([0, 1, 2, 3, 4]),
                            CategoricalArray([0.0, 1.0, 2.0, 3.0, 4.0])], [:id, :fid])
        N = null

        @test join(df1, df2, kind=:cross) ==
            DataFrame(Any[repeat([1, 3, 5], inner = 5),
                          repeat([1, 3, 5], inner = 5),
                          repeat([0, 1, 2, 3, 4], outer = 3),
                          repeat([0, 1, 2, 3, 4], outer = 3)],
                      [:id, :fid, :id_1, :fid_1])
        @test all(isa.(join(df1, df2, kind=:cross).columns,
                       [CategoricalVector{T} for T in (Int, Float64, Int, Float64)]))

        i(on) = join(df1, df2, on = on, kind = :inner)
        l(on) = join(df1, df2, on = on, kind = :left)
        r(on) = join(df1, df2, on = on, kind = :right)
        o(on) = join(df1, df2, on = on, kind = :outer)
        s(on) = join(df1, df2, on = on, kind = :semi)
        a(on) = join(df1, df2, on = on, kind = :anti)

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
        @test l(on) == DataFrame(id = [1, 3, 5],
                                 fid = [1, 3, 5],
                                 fid_1 = [1, 3, N])
        @test all(isa.(l(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int,Float64,Float64)]))
        @test r(on) == DataFrame(id = [1, 3, 0, 2, 4],
                                 fid = [1, 3, N, N, N],
                                 fid_1 = [1, 3, 0, 2, 4])
        @test all(isa.(r(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int,Float64,Float64)]))
        @test o(on) == DataFrame(id = [1, 3, 5, 0, 2, 4],
                                 fid = [1, 3, 5, N, N, N],
                                 fid_1 = [1, 3, N, 0, 2, 4])
        @test all(isa.(o(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int,Float64,Float64)]))

        on = :fid
        @test i(on) == DataFrame(Any[[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
        @test all(isa.(i(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64, Int)]))
        @test l(on) == DataFrame(id = [1, 3, 5],
                                 fid = [1, 3, 5],
                                 id_1 = [1, 3, N])
        @test all(isa.(l(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64, Int)]))
        @test r(on) == DataFrame(id = [1, 3, N, N, N],
                                 fid = [1, 3, 0, 2, 4],
                                 id_1 = [1, 3, 0, 2, 4])
        @test all(isa.(r(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64, Int)]))
        @test o(on) == DataFrame(id = [1, 3, 5, N, N, N],
                                 fid = [1, 3, 5, 0, 2, 4],
                                 id_1 = [1, 3, N, 0, 2, 4])
        @test all(isa.(o(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64, Int)]))

        on = [:id, :fid]
        @test i(on) == DataFrame(Any[[1, 3], [1, 3]], [:id, :fid])
        @test all(isa.(i(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))
        @test l(on) == DataFrame(id = [1, 3, 5],
                                 fid = [1, 3, 5])
        @test all(isa.(l(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64)]))
        @test r(on) == DataFrame(id = [1, 3, 0, 2, 4],
                                 fid = [1, 3, 0, 2, 4])
        @test all(isa.(r(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64)]))
        @test o(on) == DataFrame(id = [1, 3, 5, 0, 2, 4],
                                 fid = [1, 3, 5, 0, 2, 4])
        @test all(isa.(o(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64)]))
    end
end
