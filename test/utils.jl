module TestUtils
    using Test, DataFrames, Statistics, StatsBase, Random
    @testset "make_unique" begin
        @test DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=true) == [:x, :x_2, :x_1, :x2]
        # TODO uncomment this line after deprecation period when makeunique=false throws error
        #@test_throws ArgumentError DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=false)
        @test DataFrames.make_unique([:x, :x_1, :x2], makeunique=false) == [:x, :x_1, :x2]
    end
end
