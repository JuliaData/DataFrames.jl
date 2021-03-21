module TestDeprecated

using Test, DataFrames

const ≅ = isequal

@testset "by and aggregate" begin
    @test_throws ArgumentError by()
    @test_throws ArgumentError aggregate()
end

@testset "All indexing" begin
    df = DataFrame(a=1, b=2, c=3)

    @test select(df, All(1, 2)) == df[:, 1:2]
    @test select(df, All(1, :b)) == df[:, 1:2]
    @test select(df, All(:a, 2)) == df[:, 1:2]
    @test select(df, All(:a, :b)) == df[:, 1:2]
    @test select(df, All(2, 1)) == df[:, [2, 1]]
    @test select(df, All(:b, 1)) == df[:, [2, 1]]
    @test select(df, All(2, :a)) == df[:, [2, 1]]
    @test select(df, All(:b, :a)) == df[:, [2, 1]]

    @test df[:, All(1, 2)] == df[:, 1:2]
    @test df[:, All(1, :b)] == df[:, 1:2]
    @test df[:, All(:a, 2)] == df[:, 1:2]
    @test df[:, All(:a, :b)] == df[:, 1:2]
    @test df[:, All(2, 1)] == df[:, [2, 1]]
    @test df[:, All(:b, 1)] == df[:, [2, 1]]
    @test df[:, All(2, :a)] == df[:, [2, 1]]
    @test df[:, All(:b, :a)] == df[:, [2, 1]]

    @test df[:, All(1, 1, 2)] == df[:, 1:2]
    @test df[:, All(:a, 1, :b)] == df[:, 1:2]
    @test df[:, All(:a, 2, :b)] == df[:, 1:2]
    @test df[:, All(:a, :b, 2)] == df[:, 1:2]
    @test df[:, All(2, 1, :a)] == df[:, [2, 1]]

    @test select(df, All(1, "b")) == df[:, 1:2]
    @test select(df, All("a", 2)) == df[:, 1:2]
    @test select(df, All("a", "b")) == df[:, 1:2]
    @test select(df, All("b", 1)) == df[:, [2, 1]]
    @test select(df, All(2, "a")) == df[:, [2, 1]]
    @test select(df, All("b", "a")) == df[:, [2, 1]]

    @test df[:, All(1, "b")] == df[:, 1:2]
    @test df[:, All("a", 2)] == df[:, 1:2]
    @test df[:, All("a", "b")] == df[:, 1:2]
    @test df[:, All("b", 1)] == df[:, [2, 1]]
    @test df[:, All(2, "a")] == df[:, [2, 1]]
    @test df[:, All("b", "a")] == df[:, [2, 1]]

    @test df[:, All("a", 1, "b")] == df[:, 1:2]
    @test df[:, All("a", 2, "b")] == df[:, 1:2]
    @test df[:, All("a", "b", 2)] == df[:, 1:2]
    @test df[:, All(2, 1, "a")] == df[:, [2, 1]]

    df = DataFrame(a1=1, a2=2, b1=3, b2=4)
    @test df[:, All(r"a", Not(r"1"))] == df[:, [1, 2, 4]]
    @test df[:, All(Not(r"1"), r"a")] == df[:, [2, 4, 1]]
end

@testset "indicator in joins" begin
    name = DataFrame(ID = [1, 2, 3], Name = ["John Doe", "Jane Doe", "Joe Blogs"])
    job = DataFrame(ID = [1, 2, 4], Job = ["Lawyer", "Doctor", "Farmer"])

    @test outerjoin(name, job, on = :ID, indicator=:source) ≅
          outerjoin(name, job, on = :ID, source=:source)
    @test leftjoin(name, job, on = :ID, indicator=:source) ≅
          leftjoin(name, job, on = :ID, source=:source)
    @test rightjoin(name, job, on = :ID, indicator=:source) ≅
          rightjoin(name, job, on = :ID, source=:source)

    @test_throws ArgumentError outerjoin(name, job, on = :ID,
                                         indicator=:source, source=:source)
    @test_throws ArgumentError leftjoin(name, job, on = :ID,
                                       indicator=:source, source=:source)
    @test_throws ArgumentError rightjoin(name, job, on = :ID,
                                         indicator=:source, source=:source)
end

@testset "map on GroupedDataFrame" begin
    df = DataFrame(a=1:3, b=4:6, c=7:9)
    dfv = @view df[1:3, 1:3]
    gdf = groupby(df, :a)
    gdfv = groupby(dfv, :a)

    for x in (gdf, gdfv)
        @test collect(x) == map(identity, x)
    end
end

@testset "new map behavior" begin
    df = DataFrame(g=[1, 2, 3])
    gdf = groupby(df, :g)
    @test map(nrow, gdf) == [1, 1, 1]
end

end # module
