@testset "DataFrame joins" begin

    name = DataFrame(Name = ["John Doe", "Jane Doe", "Joe Blogs"], ID = [1, 2, 3])
    job = DataFrame(ID = [1, 2, 2, 4], Job = ["Lawyer", "Doctor", "Florist", "Farmer"])

    # Join on symbols or vectors of symbols
    @test isa(join(name, job, on = :ID), AbstractDataFrame)
    @test isa(join(name, job, on = [:ID]), AbstractDataFrame)
    # on is requied for any join except :cross
    @test_throws ArgumentError join(name, job)

    # Test output of various join types
    outer = DataFrame(Name = NullableArray(Nullable{String}["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", Nullable()]),
                      ID = [1, 2, 2, 3, 4],
                      Job = NullableArray(Nullable{String}["Lawyer", "Doctor", "Florist", Nullable(), "Farmer"]))

    # (Tests use current column ordering but don't promote it)
    right = outer[!isnull(outer[:Job]), [:Name, :ID, :Job]]
    left = outer[!isnull(outer[:Name]), :]
    inner = left[!isnull(left[:Job]), :]
    semi = unique(inner[:, [:Name, :ID]])
    anti = left[isnull(left[:Job]), [:Name, :ID]]

    @test isequal(join(name, job, on = :ID), inner)
    @test isequal(join(name, job, on = :ID, kind = :inner), inner)
    @test isequal(join(name, job, on = :ID, kind = :outer), outer)
    @test isequal(join(name, job, on = :ID, kind = :left), left)
    @test isequal(join(name, job, on = :ID, kind = :right), right)
    @test isequal(join(name, job, on = :ID, kind = :semi), semi)
    @test isequal(join(name, job, on = :ID, kind = :anti), anti)

@testset "Join with no non-key columns" begin
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
end

@testset "Join using categorical vectors" begin
    cname = DataFrame(Name = ["John Doe", "Jane Doe", "Joe Blogs"], ID = categorical(NullableArray([1, 2, 3])))
    cjob = DataFrame(ID = categorical(NullableArray([1, 2, 2, 4])), Job = ["Lawyer", "Doctor", "Florist", "Farmer"])
    couter = DataFrame(Name = NullableArray(Nullable{String}["John Doe", "Jane Doe", "Jane Doe", "Joe Blogs", Nullable()]),
                       ID = categorical(NullableArray([1, 2, 2, 3, 4])),
                       Job = categorical(NullableArray(Nullable{String}["Lawyer", "Doctor", "Florist", Nullable(), "Farmer"])))
    cright = couter[!isnull(couter[:Job]), [:Name, :ID, :Job]]
    cleft = couter[!isnull(couter[:Name]), :]
    cinner = cleft[!isnull(cleft[:Job]), :]
    @test isequal(join(cname, cjob, on = :ID), cinner)
    @test isequal(join(cname, cjob, on = :ID, kind = :inner), cinner)
    @test isequal(join(cname, cjob, on = :ID, kind = :outer), couter)
    @test isequal(join(cname, cjob, on = :ID, kind = :left), cleft)
    @test isequal(join(cname, cjob, on = :ID, kind = :right), cright)
end

@testset "Join on multiple keys" begin
    df1 = DataFrame(A = 1, B = 2, C = 3)
    df2 = DataFrame(A = 1, B = 2, D = 4)

    @test isequal(join(df1, df2, on = [:A, :B]),
                  DataFrame(A = 1, B = 2, C = 3, D = 4))

    # Join on multiple keys with different order of "on" columns
    df1 = DataFrame(A = 1, B = :A, C = 3)
    df2 = DataFrame(B = :A, A = 1, D = 4)

    @test isequal(join(df1, df2, on = [:A, :B]),
                  DataFrame(A = 1, B = :A, C = 3, D = 4))
end

@testset "Crossjoin" begin
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
end

@testset "Join empty inputs" begin
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
end

@testset "issue #960" begin
    df1 = DataFrame(A = 1:50, B = 1:50, C = 1)
    categorical!(df1, :A)
    categorical!(df1, :B)
    @test isequal(join(df1, df1, on = [:A, :B], kind = :inner),
                  DataFrame(A=1:50, B=1:50, C=1, C_1=1))
end

@testset "Array{Nullable} works with NullableArray (#1088)" begin
    df = DataFrame(Name = Nullable{String}["A", "B", "C"],
                   Mass = [1.5, 2.2, 1.1])
    df2 = DataFrame(Name = ["A", "B", "C", "A"],
                    Quantity = [3, 3, 2, 4])
    @test join(df2, df, on=:Name, kind=:left) == DataFrame(Name = ["A", "B", "C", "A"],
                                                           Quantity = [3, 3, 2, 4],
                                                           Mass = [1.5, 2.2, 1.1, 1.5])
end

end
