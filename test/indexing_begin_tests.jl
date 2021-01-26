@testset "begin and end tests" begin
    df = DataFrame([(i, j) for i in 1:3, j in 1:4], :auto)
    @test df[begin, begin] == df[1, 1]
    @test df[begin, end] == df[1, 4]
    @test df[end, begin] == df[3, 1]
    @test df[end, end] == df[3, 4]

    @test df[Not(begin), Not(begin)] == df[Not(1), Not(1)]
    @test df[Not(begin), Not(end)] == df[Not(1), Not(4)]
    @test df[Not(end), Not(begin)] == df[Not(3), Not(1)]
    @test df[Not(end), Not(end)] == df[Not(3), Not(4)]

    df[begin, begin] = (101, 101)
    @test df[begin, begin] == (101, 101)
    df[begin, end] = (101, 104)
    @test df[begin, end] == (101, 104)
    df[end, begin] = (103, 101)
    @test df[end, begin] == (103, 101)
    df[end, end] = (103, 104)
    @test df[end, end] == (103, 104)

    df[!, begin] .= [1, 2, 3]
    @test df[:, 1] == [1, 2, 3]
    df[!, end] .= [11, 12, 13]
    @test df[:, 4] == [11, 12, 13]

    @test df[begin:end, [begin, end]] == df[:, [1, 4]]
    df[begin:end, [begin, end]] .= [111, 222, 333]
    @test df.x1 == df.x4 == [111, 222, 333]
    @test df[[begin, end], [begin, end]] == df[[1, 3], [1, 4]]
    df[[begin, end], [begin, end]] .= 1000
    @test df.x1 == df.x4 == [1000, 222, 1000]

    @test eachcol(df)[begin] == df[!, begin]
    @test eachcol(df)[end] == df[!, end]
end
