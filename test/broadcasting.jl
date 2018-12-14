module TestDataFrame
    using DataFrames, Test

    @testset "broadcast DataFrame & DataFrameRow" begin
        df = DataFrame(x=1:4, y=5:8, z=9:12)
        @test sum.(df) == [15, 18, 21, 24]
        @test ((row -> row .+ 1)).(df) == [i .+ [0, 4, 8] for i in 2:5]
    end
end
