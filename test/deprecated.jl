module TestDeprecated

using Test, DataFrames

@testset "by and aggregate" begin
    @test_throws by()
    @test_throws aggregate()
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

end # module
