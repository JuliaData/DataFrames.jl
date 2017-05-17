module TestJoin
    using Base.Test
    using DataTables

    name = DataTable(ID = NullableArray([1, 2, 3]),
                     Name = NullableArray(["John Doe", "Jane Doe", "Joe Blogs"]))
    job = DataTable(ID = NullableArray([1, 2, 2, 4]),
                    Job = NullableArray(["Lawyer", "Doctor", "Florist", "Farmer"]))

    # Join on symbols or vectors of symbols
    join(name, job, on = :ID)
    join(name, job, on = [:ID])

    # Soon we won't allow natural joins
    #@test_throws join(name, job)

    # Test output of various join types
    outer = DataTable(ID = NullableArray([1, 2, 2, 3, 4]),
                      Name = NullableArray(["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", Nullable()]),
                      Job = NullableArray(["Lawyer", "Doctor", "Florist", Nullable(), "Farmer"]))

    # (Tests use current column ordering but don't promote it)
    right = outer[Bool[!isnull(x) for x in outer[:Job]], [:ID, :Name, :Job]]
    left = outer[Bool[!isnull(x) for x in outer[:Name]], :]
    inner = left[Bool[!isnull(x) for x in left[:Job]], :]
    semi = unique(inner[:, [:ID, :Name]])
    anti = left[Bool[isnull(x) for x in left[:Job]], [:ID, :Name]]

    @test isequal(join(name, job, on = :ID), inner)
    @test isequal(join(name, job, on = :ID, kind = :inner), inner)
    @test isequal(join(name, job, on = :ID, kind = :outer), outer)
    @test isequal(join(name, job, on = :ID, kind = :left), left)
    @test isequal(join(name, job, on = :ID, kind = :right), right)
    @test isequal(join(name, job, on = :ID, kind = :semi), semi)
    @test isequal(join(name, job, on = :ID, kind = :anti), anti)
    @test_throws ArgumentError join(name, job)
    @test_throws ArgumentError join(name, job, on=:ID, kind=:other)

    # Join with no non-key columns
    on = [:ID]
    nameid = name[on]
    jobid = job[on]

    @test isequal(join(nameid, jobid, on = :ID), inner[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :inner), inner[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :outer), outer[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :left), left[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :right), right[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :semi), semi[on])
    @test isequal(join(nameid, jobid, on = :ID, kind = :anti), anti[on])

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

    @test isequal(join(dt1, dt2[[:C]], kind = :cross), cross)

    # Cross joins handle naming collisions
    @test size(join(dt1, dt1, kind = :cross)) == (4, 4)

    # Cross joins don't take keys
    @test_throws ArgumentError join(dt1, dt2, on = :A, kind = :cross)

    # test empty inputs
    simple_dt(len::Int, col=:A) = (dt = DataTable(); dt[col]=NullableArray(collect(1:len)); dt)
    @test isequal(join(simple_dt(0), simple_dt(0), on = :A, kind = :left),  simple_dt(0))
    @test isequal(join(simple_dt(2), simple_dt(0), on = :A, kind = :left),  simple_dt(2))
    @test isequal(join(simple_dt(0), simple_dt(2), on = :A, kind = :left),  simple_dt(0))
    @test isequal(join(simple_dt(0), simple_dt(0), on = :A, kind = :right), simple_dt(0))
    @test isequal(join(simple_dt(0), simple_dt(2), on = :A, kind = :right), simple_dt(2))
    @test isequal(join(simple_dt(2), simple_dt(0), on = :A, kind = :right), simple_dt(0))
    @test isequal(join(simple_dt(0), simple_dt(0), on = :A, kind = :inner), simple_dt(0))
    @test isequal(join(simple_dt(0), simple_dt(2), on = :A, kind = :inner), simple_dt(0))
    @test isequal(join(simple_dt(2), simple_dt(0), on = :A, kind = :inner), simple_dt(0))
    @test isequal(join(simple_dt(0), simple_dt(0), on = :A, kind = :outer), simple_dt(0))
    @test isequal(join(simple_dt(0), simple_dt(2), on = :A, kind = :outer), simple_dt(2))
    @test isequal(join(simple_dt(2), simple_dt(0), on = :A, kind = :outer), simple_dt(2))
    @test isequal(join(simple_dt(0), simple_dt(0), on = :A, kind = :semi),  simple_dt(0))
    @test isequal(join(simple_dt(2), simple_dt(0), on = :A, kind = :semi),  simple_dt(0))
    @test isequal(join(simple_dt(0), simple_dt(2), on = :A, kind = :semi),  simple_dt(0))
    @test isequal(join(simple_dt(0), simple_dt(0), on = :A, kind = :anti),  simple_dt(0))
    @test isequal(join(simple_dt(2), simple_dt(0), on = :A, kind = :anti),  simple_dt(2))
    @test isequal(join(simple_dt(0), simple_dt(2), on = :A, kind = :anti),  simple_dt(0))
    @test isequal(join(simple_dt(0), simple_dt(0, :B), kind = :cross), DataTable(A=Int[], B=Int[]))
    @test isequal(join(simple_dt(0), simple_dt(2, :B), kind = :cross), DataTable(A=Int[], B=Int[]))
    @test isequal(join(simple_dt(2), simple_dt(0, :B), kind = :cross), DataTable(A=Int[], B=Int[]))

    # issue #960
    dt1 = DataTable(A = 1:50,
                    B = 1:50,
                    C = 1)
    categorical!(dt1, :A)
    categorical!(dt1, :B)
    join(dt1, dt1, on = [:A, :B], kind = :inner)

    # Test that Array{Nullable} works when combined with NullableArray (#1088)
    dt = DataTable(Name = Nullable{String}["A", "B", "C"],
                   Mass = [1.5, 2.2, 1.1])
    dt2 = DataTable(Name = ["A", "B", "C", "A"],
                    Quantity = [3, 3, 2, 4])
    @test join(dt2, dt, on=:Name, kind=:left) == DataTable(Name = NullableArray(["A", "B", "C", "A"]),
                                                           Quantity = NullableArray([3, 3, 2, 4]),
                                                           Mass = NullableArray([1.5, 2.2, 1.1, 1.5]))

    # Test that join works when mixing Array and NullableArray (#1151)
    dt = DataTable([collect(1:10), collect(2:11)], [:x, :y])
    dtnull = DataTable(x = NullableArray(1:10), z = NullableArray(3:12))
    @test join(dt, dtnull, on = :x) ==
        DataTable([collect(1:10), collect(2:11), NullableArray(3:12)], [:x, :y, :z])
    @test join(dtnull, dt, on = :x) ==
        DataTable([NullableArray(1:10), NullableArray(3:12), collect(2:11)], [:x, :z, :y])

    @testset "all joins" begin
        dt1 = DataTable(Any[[1, 3, 5], [1.0, 3.0, 5.0]], [:id, :fid])
        dt2 = DataTable(Any[[0, 1, 2, 3, 4], [0.0, 1.0, 2.0, 3.0, 4.0]], [:id, :fid])
        N = Nullable()

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
        @test l(on) == DataTable(id = NullableArray([1, 3, 5]),
                                 fid = NullableArray([1, 3, 5]),
                                 fid_1 = NullableArray([1, 3, N]))
        @test typeof.(l(on).columns) == [NullableVector{Int},
                                         NullableVector{Float64},
                                         NullableVector{Float64}]
        @test r(on) == DataTable(id = NullableArray([1, 3, 0, 2, 4]),
                                 fid = NullableArray([1, 3, N, N, N]),
                                 fid_1 = NullableArray([1, 3, 0, 2, 4]))
        @test typeof.(r(on).columns) == [NullableVector{Int},
                                         NullableVector{Float64},
                                         NullableVector{Float64}]
        @test o(on) == DataTable(id = NullableArray([1, 3, 5, 0, 2, 4]),
                                 fid = NullableArray([1, 3, 5, N, N, N]),
                                 fid_1 = NullableArray([1, 3, N, 0, 2, 4]))
        @test typeof.(o(on).columns) == [NullableVector{Int},
                                         NullableVector{Float64},
                                         NullableVector{Float64}]

        on = :fid
        @test i(on) == DataTable(Any[[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}, Vector{Int}]
        @test l(on) == DataTable(id = NullableArray([1, 3, 5]),
                                 fid = NullableArray([1, 3, 5]),
                                 id_1 = NullableArray([1, 3, N]))
        @test typeof.(l(on).columns) == [NullableVector{Int},
                                         NullableVector{Float64},
                                         NullableVector{Int}]
        @test r(on) == DataTable(id = NullableArray([1, 3, N, N, N]),
                                 fid = NullableArray([1, 3, 0, 2, 4]),
                                 id_1 = NullableArray([1, 3, 0, 2, 4]))
        @test typeof.(r(on).columns) == [NullableVector{Int},
                                         NullableVector{Float64},
                                         NullableVector{Int}]
        @test o(on) == DataTable(id = NullableArray([1, 3, 5, N, N, N]),
                                 fid = NullableArray([1, 3, 5, 0, 2, 4]),
                                 id_1 = NullableArray([1, 3, N, 0, 2, 4]))
        @test typeof.(o(on).columns) == [NullableVector{Int},
                                         NullableVector{Float64},
                                         NullableVector{Int}]

        on = [:id, :fid]
        @test i(on) == DataTable(Any[[1, 3], [1, 3]], [:id, :fid])
        @test typeof.(i(on).columns) == [Vector{Int}, Vector{Float64}]
        @test l(on) == DataTable(id = NullableArray([1, 3, 5]),
                                 fid = NullableArray([1, 3, 5]))
        @test typeof.(l(on).columns) == [NullableVector{Int},
                                         NullableVector{Float64}]
        @test r(on) == DataTable(id = NullableArray([1, 3, 0, 2, 4]),
                                 fid = NullableArray([1, 3, 0, 2, 4]))
        @test typeof.(r(on).columns) == [NullableVector{Int},
                                         NullableVector{Float64}]
        @test o(on) == DataTable(id = NullableArray([1, 3, 5, 0, 2, 4]),
                                 fid = NullableArray([1, 3, 5, 0, 2, 4]))
        @test typeof.(o(on).columns) == [NullableVector{Int},
                                         NullableVector{Float64}]
    end

    @testset "all joins with CategoricalArrays" begin
        dt1 = DataTable(Any[CategoricalArray([1, 3, 5]),
                              CategoricalArray([1.0, 3.0, 5.0])], [:id, :fid])
        dt2 = DataTable(Any[CategoricalArray([0, 1, 2, 3, 4]),
                              CategoricalArray([0.0, 1.0, 2.0, 3.0, 4.0])], [:id, :fid])
        N = Nullable()
        DRT = CategoricalArrays.DefaultRefType

        @test join(dt1, dt2, kind=:cross) ==
            DataTable(Any[repeat([1, 3, 5], inner = 5),
                          repeat([1, 3, 5], inner = 5),
                          repeat([0, 1, 2, 3, 4], outer = 3),
                          repeat([0, 1, 2, 3, 4], outer = 3)],
                      [:id, :fid, :id_1, :fid_1])
        @test typeof.(join(dt1, dt2, kind=:cross).columns) ==
            [CategoricalVector{i, DRT} for i in [Int, Float64, Int, Float64]]

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
              typeof.(s([:id, :fid]).columns) == [CategoricalVector{Int, DRT},
                                                  CategoricalVector{Float64, DRT}]
        @test a(:id) ==
              a(:fid) ==
              a([:id, :fid]) == DataTable(Any[[5], [5]], [:id, :fid])
        @test typeof.(a(:id).columns) ==
              typeof.(a(:fid).columns) ==
              typeof.(a([:id, :fid]).columns) == [CategoricalVector{Int, DRT},
                                                  CategoricalVector{Float64, DRT}]

        on = :id
        @test i(on) == DataTable(Any[[1, 3], [1, 3], [1, 3]], [:id, :fid, :fid_1])
        @test typeof.(i(on).columns) == [CategoricalVector{Int, DRT},
                                         CategoricalVector{Float64, DRT},
                                         CategoricalVector{Float64, DRT}]
        @test l(on) == DataTable(id = NullableArray([1, 3, 5]),
                                 fid = NullableArray([1, 3, 5]),
                                 fid_1 = NullableArray([1, 3, N]))
        @test typeof.(l(on).columns) == [NullableCategoricalVector{Int, DRT},
                                         NullableCategoricalVector{Float64, DRT},
                                         NullableCategoricalVector{Float64, DRT}]
        @test r(on) == DataTable(id = NullableArray([1, 3, 0, 2, 4]),
                                 fid = NullableArray([1, 3, N, N, N]),
                                 fid_1 = NullableArray([1, 3, 0, 2, 4]))
        @test typeof.(r(on).columns) == [NullableCategoricalVector{Int, DRT},
                                         NullableCategoricalVector{Float64, DRT},
                                         NullableCategoricalVector{Float64, DRT}]
        @test o(on) == DataTable(id = NullableArray([1, 3, 5, 0, 2, 4]),
                                 fid = NullableArray([1, 3, 5, N, N, N]),
                                 fid_1 = NullableArray([1, 3, N, 0, 2, 4]))
        @test typeof.(o(on).columns) == [NullableCategoricalVector{Int, DRT},
                                         NullableCategoricalVector{Float64, DRT},
                                         NullableCategoricalVector{Float64, DRT}]

        on = :fid
        @test i(on) == DataTable(Any[[1, 3], [1.0, 3.0], [1, 3]], [:id, :fid, :id_1])
        @test typeof.(i(on).columns) == [CategoricalVector{Int, DRT},
                                         CategoricalVector{Float64, DRT},
                                         CategoricalVector{Int, DRT}]
        @test l(on) == DataTable(id = NullableArray([1, 3, 5]),
                                 fid = NullableArray([1, 3, 5]),
                                 id_1 = NullableArray([1, 3, N]))
        @test typeof.(l(on).columns) == [NullableCategoricalVector{Int, DRT},
                                         NullableCategoricalVector{Float64, DRT},
                                         NullableCategoricalVector{Int, DRT}]
        @test r(on) == DataTable(id = NullableArray([1, 3, N, N, N]),
                                 fid = NullableArray([1, 3, 0, 2, 4]),
                                 id_1 = NullableArray([1, 3, 0, 2, 4]))
        @test typeof.(r(on).columns) == [NullableCategoricalVector{Int, DRT},
                                         NullableCategoricalVector{Float64, DRT},
                                         NullableCategoricalVector{Int, DRT}]
        @test o(on) == DataTable(id = NullableArray([1, 3, 5, N, N, N]),
                                 fid = NullableArray([1, 3, 5, 0, 2, 4]),
                                 id_1 = NullableArray([1, 3, N, 0, 2, 4]))
        @test typeof.(o(on).columns) == [NullableCategoricalVector{Int, DRT},
                                         NullableCategoricalVector{Float64, DRT},
                                         NullableCategoricalVector{Int, DRT}]

        on = [:id, :fid]
        @test i(on) == DataTable(Any[[1, 3], [1, 3]], [:id, :fid])
        @test typeof.(i(on).columns) == [CategoricalVector{Int, DRT},
                                         CategoricalVector{Float64, DRT}]
        @test l(on) == DataTable(id = NullableArray([1, 3, 5]),
                                 fid = NullableArray([1, 3, 5]))
        @test typeof.(l(on).columns) == [NullableCategoricalVector{Int, DRT},
                                         NullableCategoricalVector{Float64, DRT}]
        @test r(on) == DataTable(id = NullableArray([1, 3, 0, 2, 4]),
                                 fid = NullableArray([1, 3, 0, 2, 4]))
        @test typeof.(r(on).columns) == [NullableCategoricalVector{Int, DRT},
                                         NullableCategoricalVector{Float64, DRT}]
        @test o(on) == DataTable(id = NullableArray([1, 3, 5, 0, 2, 4]),
                                 fid = NullableArray([1, 3, 5, 0, 2, 4]))
        @test typeof.(o(on).columns) == [NullableCategoricalVector{Int, DRT},
                                         NullableCategoricalVector{Float64, DRT}]
    end
end
