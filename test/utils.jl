module TestUtils
    using Test, DataFrames, Statistics, StatsBase, Random
    @testset "make_unique" begin
        @test DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=true) == [:x, :x_2, :x_1, :x2]
        # TODO uncomment this line after deprecation period when makeunique=false throws error
        #@test_throws ArgumentError DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=false)
        @test DataFrames.make_unique([:x, :x_1, :x2], makeunique=false) == [:x, :x_1, :x2]
    end

    @testset "countmissing" begin
        @test DataFrames.countmissing([1:3;]) == 0

        data = Vector{Union{Float64, Missing}}(rand(20))
        @test DataFrames.countmissing(data) == 0
        data[sample(1:20, 11, replace=false)] .= missing
        @test DataFrames.countmissing(data) == 11
        data[1:end] .= missing
        @test DataFrames.countmissing(data) == 20

        pdata = Vector{Union{Int, Missing}}(sample(1:5, 20))
        @test DataFrames.countmissing(pdata) == 0
        pdata[sample(1:20, 11, replace=false)] .= missing
        @test DataFrames.countmissing(pdata) == 11
        pdata[1:end] .= missing
        @test DataFrames.countmissing(pdata) == 20
    end
end
