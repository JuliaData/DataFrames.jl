module TestJoin
    using Base.Test
    using DataFrames

    name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
    job = DataFrame(ID = [1, 2, 2, 4], Job = ["Lawyer", "Doctor", "Florist", "Farmer"])

    # Join on symbols or vectors of symbols
    join(name, job, on = :ID)
    join(name, job, on = [:ID])

    # Soon we won't allow natural joins
    #@test_throws join(name, job)

    # Test output of various join types
    outer = DataFrame(ID = [1, 2, 2, 3, 4],
                      Name = NullableArray(Nullable{String}["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", Nullable()]),
                      Job = NullableArray(Nullable{String}["Lawyer", "Doctor", "Florist", Nullable(), "Farmer"]))

    # (Tests use current column ordering but don't promote it)
    right = outer[Bool[!isnull(x) for x in outer[:Job]], [:Name, :ID, :Job]]
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
    df1 = DataFrame(A = 1, B = 2, C = 3)
    df2 = DataFrame(A = 1, B = 2, D = 4)

    join(df1, df2, on = [:A, :B])

    # Test output of cross joins
    df1 = DataFrame(A = 1:2, B = 'a':'b')
    df2 = DataFrame(A = 1:3, C = 3:5)

    cross = DataFrame(A = [1, 1, 1, 2, 2, 2],
                      B = ['a', 'a', 'a', 'b', 'b', 'b'],
                      C = [3, 4, 5, 3, 4, 5])

    @test isequal(join(df1, df2[[:C]], kind = :cross), cross)

    # Cross joins handle naming collisions
    @test size(join(df1, df1, kind = :cross)) == (4, 4)

    # Cross joins don't take keys
    @test_throws ArgumentError join(df1, df2, on = :A, kind = :cross)

    # test empty inputs
    simple_df(len::Int, col=:A) = (df = DataFrame(); df[col]=collect(1:len); df)
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :left),  simple_df(0))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :left),  simple_df(2))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :left),  simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :right), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :right), simple_df(2))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :right), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :inner), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :inner), simple_df(0))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :inner), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :outer), simple_df(0))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :outer), simple_df(2))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :outer), simple_df(2))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :semi),  simple_df(0))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :semi),  simple_df(0))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :semi),  simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0), on = :A, kind = :anti),  simple_df(0))
    @test isequal(join(simple_df(2), simple_df(0), on = :A, kind = :anti),  simple_df(2))
    @test isequal(join(simple_df(0), simple_df(2), on = :A, kind = :anti),  simple_df(0))
    @test isequal(join(simple_df(0), simple_df(0, :B), kind = :cross), DataFrame(A=Int[], B=Int[]))
    @test isequal(join(simple_df(0), simple_df(2, :B), kind = :cross), DataFrame(A=Int[], B=Int[]))
    @test isequal(join(simple_df(2), simple_df(0, :B), kind = :cross), DataFrame(A=Int[], B=Int[]))

    # issue #960
    df1 = DataFrame(A = 1:50,
                    B = 1:50,
                    C = 1)
    categorical!(df1, :A)
    categorical!(df1, :B)
    join(df1, df1, on = [:A, :B], kind = :inner)

    # Test that Array{Nullable} works when combined with NullableArray (#1088)
    df = DataFrame(Name = Nullable{String}["A", "B", "C"],
                   Mass = [1.5, 2.2, 1.1])
    df2 = DataFrame(Name = ["A", "B", "C", "A"],
                    Quantity = [3, 3, 2, 4])
    @test join(df2, df, on=:Name, kind=:left) == DataFrame(Name = ["A", "A", "B", "C"],
                                                           Quantity = [3, 4, 3, 2],
                                                           Mass = [1.5, 1.5, 2.2, 1.1])

    # Test that join works when mixing Array and NullableArray (#1151)
    df = DataFrame([collect(1:10), collect(2:11)], [:x, :y])
    dfnull = DataFrame(x = 1:10, z = 3:12)
    @test join(df, dfnull, on = :x) ==
        DataFrame([collect(1:10), collect(2:11), NullableArray(3:12)], [:x, :y, :z])
    @test join(dfnull, df, on = :x) ==
        DataFrame([NullableArray(1:10), NullableArray(3:12), NullableArray(2:11)], [:x, :z, :y])
end
