module TestJoin
    using Base.Test, DataTables

    name = DataTable(ID = Union{Int, Null}[1, 2, 3],
                     Name = Union{String, Null}["John Doe", "Jane Doe", "Joe Blogs"])
    job = DataTable(ID = Union{Int, Null}[1, 2, 2, 4],
                    Job = Union{String, Null}["Lawyer", "Doctor", "Florist", "Farmer"])

    # Join on symbols or vectors of symbols
    join(name, job, on = :ID)
    join(name, job, on = [:ID])

    # Soon we won't allow natural joins
    @test_throws ArgumentError join(name, job)

    # Test output of various join types
    outer = DataTable(ID = [1, 2, 2, 3, 4],
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
    dt1 = DataTable(A = 1, B = 2, C = 3)
    dt2 = DataTable(A = 1, B = 2, D = 4)

    join(dt1, dt2, on = [:A, :B])

    # Test output of cross joins
    dt1 = DataTable(A = 1:2, B = 'a':'b')
    dt2 = DataTable(A = 1:3, C = 3:5)

    cross = DataTable(A = [1, 1, 1, 2, 2, 2],
                      B = ['a', 'a', 'a', 'b', 'b', 'b'],
                      C = [3, 4, 5, 3, 4, 5])

    @test join(dt1, dt2[[:C]], kind = :cross) == cross

    # Cross joins handle naming collisions
    @test size(join(dt1, dt1, kind = :cross)) == (4, 4)

    # Cross joins don't take keys
    @test_throws ArgumentError join(dt1, dt2, on = :A, kind = :cross)

    # test empty inputs
    simple_dt(len::Int, col=:A) = (dt = DataTable();
                                   dt[col]=Vector{Union{Int, Null}}(1:len);
                                   dt)
    @test join(simple_dt(0), simple_dt(0), on = :A, kind = :left) == simple_dt(0)
    @test join(simple_dt(2), simple_dt(0), on = :A, kind = :left) == simple_dt(2)
    @test join(simple_dt(0), simple_dt(2), on = :A, kind = :left) == simple_dt(0)
    @test join(simple_dt(0), simple_dt(0), on = :A, kind = :right) == simple_dt(0)
    @test join(simple_dt(0), simple_dt(2), on = :A, kind = :right) == simple_dt(2)
    @test join(simple_dt(2), simple_dt(0), on = :A, kind = :right) == simple_dt(0)
    @test join(simple_dt(0), simple_dt(0), on = :A, kind = :inner) == simple_dt(0)
    @test join(simple_dt(0), simple_dt(2), on = :A, kind = :inner) == simple_dt(0)
    @test join(simple_dt(2), simple_dt(0), on = :A, kind = :inner) == simple_dt(0)
    @test join(simple_dt(0), simple_dt(0), on = :A, kind = :outer) == simple_dt(0)
    @test join(simple_dt(0), simple_dt(2), on = :A, kind = :outer) == simple_dt(2)
    @test join(simple_dt(2), simple_dt(0), on = :A, kind = :outer) == simple_dt(2)
    @test join(simple_dt(0), simple_dt(0), on = :A, kind = :semi) == simple_dt(0)
    @test join(simple_dt(2), simple_dt(0), on = :A, kind = :semi) == simple_dt(0)
    @test join(simple_dt(0), simple_dt(2), on = :A, kind = :semi) == simple_dt(0)
    @test join(simple_dt(0), simple_dt(0), on = :A, kind = :anti) == simple_dt(0)
    @test join(simple_dt(2), simple_dt(0), on = :A, kind = :anti) == simple_dt(2)
    @test join(simple_dt(0), simple_dt(2), on = :A, kind = :anti) == simple_dt(0)
    @test join(simple_dt(0), simple_dt(0, :B), kind = :cross) == DataTable(A=Int[], B=Int[])
    @test join(simple_dt(0), simple_dt(2, :B), kind = :cross) == DataTable(A=Int[], B=Int[])
    @test join(simple_dt(2), simple_dt(0, :B), kind = :cross) == DataTable(A=Int[], B=Int[])

    # issue #960
    dt1 = DataTable(A = 1:50,
                    B = 1:50,
                    C = 1)
    categorical!(dt1, :A)
    categorical!(dt1, :B)
    join(dt1, dt1, on = [:A, :B], kind = :inner)

    # Test that join works when mixing Array{Union{T, Null}} with Array{T} (issue #1088)
    dt = DataTable(Name = Union{String, Null}["A", "B", "C"],
                   Mass = [1.5, 2.2, 1.1])
    dt2 = DataTable(Name = ["A", "B", "C", "A"],
                    Quantity = [3, 3, 2, 4])
    @test join(dt2, dt, on=:Name, kind=:left) == DataTable(Name = ["A", "B", "C", "A"],
                                                           Quantity = [3, 3, 2, 4],
                                                           Mass = [1.5, 2.2, 1.1, 1.5])

    # Test that join works when mixing Array{Union{T, Null}} with Array{T} (issue #1151)
    dt = DataTable([collect(1:10), collect(2:11)], [:x, :y])
    dtnull = DataTable(x = Vector{Union{Int, Null}}(1:10), z = Vector{Union{Int, Null}}(3:12))
    @test join(dt, dtnull, on = :x) ==
        DataTable([collect(1:10), collect(2:11), collect(3:12)], [:x, :y, :z])
    @test join(dtnull, dt, on = :x) ==
        DataTable([Vector{Union{Int, Null}}(1:10), Vector{Union{Int, Null}}(3:12), collect(2:11)], [:x, :z, :y])

    @testset "all joins" begin
        dt1 = DataTable(Any[[1, 3, 5], [1.0, 3.0, 5.0]], [:id, :fid])
        dt2 = DataTable(Any[[0, 1, 2, 3, 4], [0.0, 1.0, 2.0, 3.0, 4.0]], [:id, :fid])
        N = null

        @test join(dt1, dt2, kind=:cross) ==
            DataTable(Any[repeat([1, 3, 5], inner = 5),
                          repeat([1, 3, 5], inner = 5),
                          repeat([0, 1, 2, 3, 4], outer = 3),
                          repeat([0, 1, 2, 3, 4], outer = 3)],
                      [:id, :fid, :id_1, :fid_1])
        @test typeof.(join(dt1, dt2, kind=:cross).columns) ==
            [Vector{Int}, Vector{Float64}, Vector{Int}, Vector{Float64}]

        i(on) = join(dt1, dt2, on = on, kind = :inner)
        l(on) = join(dt1, dt2, on = on, kind = :left)
        r(on) = join(dt1, dt2, on = on, kind = :right)
        o(on) = join(dt1, dt2, on = on, kind = :outer)
        s(on) = join(dt1, dt2, on = on, kind = :semi)
        a(on) = join(dt1, dt2, on = on, kind = :anti)

        @test s(:id) ==
              s(:fid) ==
              s([:id, :fid]) == DataTable(Any[[1, 3], [1, 3]], [:id, :fid])
        @test typeof.(s(:id).columns) ==
              typeof.(s(:fid).columns) ==
              typeof.(s([:id, :fid]).columns) == [Vector{Int}, Vector{Float64}]
        @test a(:id) ==
              a(:fid) ==
              a([:id, :fid]) == DataTable(Any[[5], [5]], [:id, :fid])
        @test typeof.(a(:id).columns) ==
              typeof.(a(:fid).columns) ==
              typeof.(a([:id, :fid]).columns) == [Vector{Int}, Vector{Float64}]

        on = :id
        @test i(on) == DataTable(Any[[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}, Vector{Float64}]
        @test l(on) == DataTable(id = [1, 3, 5],
                                 fid = [1, 3, 5],
                                 fid_1 = [1, 3, N])
        @test typeof.(l(on).columns) ==
            [Vector{Union{T, Null}} for T in (Int, Float64, Float64)]
        @test r(on) == DataTable(id = [1, 3, 0, 2, 4],
                                 fid = [1, 3, N, N, N],
                                 fid_1 = [1, 3, 0, 2, 4])
        @test typeof.(r(on).columns) ==
            [Vector{Union{T, Null}} for T in (Int, Float64, Float64)]
        @test o(on) == DataTable(id = [1, 3, 5, 0, 2, 4],
                                 fid = [1, 3, 5, N, N, N],
                                 fid_1 = [1, 3, N, 0, 2, 4])
        @test typeof.(o(on).columns) ==
            [Vector{Union{T, Null}} for T in (Int, Float64, Float64)]

        on = :fid
        @test i(on) == DataTable(Any[[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}, Vector{Int}]
        @test l(on) == DataTable(id = [1, 3, 5],
                                 fid = [1, 3, 5],
                                 id_1 = [1, 3, N])
        @test typeof.(l(on).columns) == [Vector{Union{T, Null}} for T in (Int,Float64,Int)]
        @test r(on) == DataTable(id = [1, 3, N, N, N],
                                 fid = [1, 3, 0, 2, 4],
                                 id_1 = [1, 3, 0, 2, 4])
        @test typeof.(r(on).columns) == [Vector{Union{T, Null}} for T in (Int,Float64,Int)]
        @test o(on) == DataTable(id = [1, 3, 5, N, N, N],
                                 fid = [1, 3, 5, 0, 2, 4],
                                 id_1 = [1, 3, N, 0, 2, 4])
        @test typeof.(o(on).columns) == [Vector{Union{T, Null}} for T in (Int,Float64,Int)]

        on = [:id, :fid]
        @test i(on) == DataTable(Any[[1, 3], [1, 3]], [:id, :fid])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}]
        @test l(on) == DataTable(id = [1, 3, 5], fid = [1, 3, 5])
        @test typeof.(l(on).columns) == [Vector{Union{Int, Null}},
                                         Vector{Union{Float64, Null}}]
        @test r(on) == DataTable(id = [1, 3, 0, 2, 4], fid = [1, 3, 0, 2, 4])
        @test typeof.(r(on).columns) == [Vector{Union{Int, Null}},
                                         Vector{Union{Float64, Null}}]
        @test o(on) == DataTable(id = [1, 3, 5, 0, 2, 4], fid = [1, 3, 5, 0, 2, 4])
        @test typeof.(o(on).columns) == [Vector{Union{Int, Null}},
                                         Vector{Union{Float64, Null}}]
    end

    @testset "all joins with CategoricalArrays" begin
        dt1 = DataTable(Any[CategoricalArray([1, 3, 5]),
                            CategoricalArray([1.0, 3.0, 5.0])], [:id, :fid])
        dt2 = DataTable(Any[CategoricalArray([0, 1, 2, 3, 4]),
                            CategoricalArray([0.0, 1.0, 2.0, 3.0, 4.0])], [:id, :fid])
        N = null

        @test join(dt1, dt2, kind=:cross) ==
            DataTable(Any[repeat([1, 3, 5], inner = 5),
                          repeat([1, 3, 5], inner = 5),
                          repeat([0, 1, 2, 3, 4], outer = 3),
                          repeat([0, 1, 2, 3, 4], outer = 3)],
                      [:id, :fid, :id_1, :fid_1])
        @test all(isa.(join(dt1, dt2, kind=:cross).columns,
                       [CategoricalVector{T} for T in (Int, Float64, Int, Float64)]))

        i(on) = join(dt1, dt2, on = on, kind = :inner)
        l(on) = join(dt1, dt2, on = on, kind = :left)
        r(on) = join(dt1, dt2, on = on, kind = :right)
        o(on) = join(dt1, dt2, on = on, kind = :outer)
        s(on) = join(dt1, dt2, on = on, kind = :semi)
        a(on) = join(dt1, dt2, on = on, kind = :anti)

        @test s(:id) ==
              s(:fid) ==
              s([:id, :fid]) == DataTable(Any[[1, 3], [1, 3]], [:id, :fid])
        @test typeof.(s(:id).columns) ==
              typeof.(s(:fid).columns) ==
              typeof.(s([:id, :fid]).columns)
        @test all(isa.(s(:id).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))

        @test a(:id) ==
              a(:fid) ==
              a([:id, :fid]) == DataTable(Any[[5], [5]], [:id, :fid])
        @test typeof.(a(:id).columns) ==
              typeof.(a(:fid).columns) ==
              typeof.(a([:id, :fid]).columns)
        @test all(isa.(a(:id).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))

        on = :id
        @test i(on) == DataTable(Any[[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
        @test all(isa.(i(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64, Float64)]))
        @test l(on) == DataTable(id = [1, 3, 5],
                                 fid = [1, 3, 5],
                                 fid_1 = [1, 3, N])
        @test all(isa.(l(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int,Float64,Float64)]))
        @test r(on) == DataTable(id = [1, 3, 0, 2, 4],
                                 fid = [1, 3, N, N, N],
                                 fid_1 = [1, 3, 0, 2, 4])
        @test all(isa.(r(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int,Float64,Float64)]))
        @test o(on) == DataTable(id = [1, 3, 5, 0, 2, 4],
                                 fid = [1, 3, 5, N, N, N],
                                 fid_1 = [1, 3, N, 0, 2, 4])
        @test all(isa.(o(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int,Float64,Float64)]))

        on = :fid
        @test i(on) == DataTable(Any[[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
        @test all(isa.(i(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64, Int)]))
        @test l(on) == DataTable(id = [1, 3, 5],
                                 fid = [1, 3, 5],
                                 id_1 = [1, 3, N])
        @test all(isa.(l(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64, Int)]))
        @test r(on) == DataTable(id = [1, 3, N, N, N],
                                 fid = [1, 3, 0, 2, 4],
                                 id_1 = [1, 3, 0, 2, 4])
        @test all(isa.(r(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64, Int)]))
        @test o(on) == DataTable(id = [1, 3, 5, N, N, N],
                                 fid = [1, 3, 5, 0, 2, 4],
                                 id_1 = [1, 3, N, 0, 2, 4])
        @test all(isa.(o(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64, Int)]))

        on = [:id, :fid]
        @test i(on) == DataTable(Any[[1, 3], [1, 3]], [:id, :fid])
        @test all(isa.(i(on).columns,
                       [CategoricalVector{T} for T in (Int, Float64)]))
        @test l(on) == DataTable(id = [1, 3, 5],
                                 fid = [1, 3, 5])
        @test all(isa.(l(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64)]))
        @test r(on) == DataTable(id = [1, 3, 0, 2, 4],
                                 fid = [1, 3, 0, 2, 4])
        @test all(isa.(r(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64)]))
        @test o(on) == DataTable(id = [1, 3, 5, 0, 2, 4],
                                 fid = [1, 3, 5, 0, 2, 4])
        @test all(isa.(o(on).columns,
                       [CategoricalVector{Union{T, Null}} for T in (Int, Float64)]))
    end
end
