module TestUtils

using Test, DataFrames
#
# make_unique(df::AbstractDataFrame, makeunique::Bool)
#
@testset "make_unique" begin
    @test DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=true) == [:x, :x_2, :x_1, :x2]
    @test_throws ArgumentError DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=false)
    @test DataFrames.make_unique([:x, :x_1, :x2], makeunique=false) == [:x, :x_1, :x2]
end
#
# repeat(df::AbstractDataFrame, count::Integer)
#
@testset "count" begin
    df = DataFrame(a = 1:2, b = 3:4)
    ref = DataFrame(a = repeat(1:2, 2),
                    b = repeat(3:4, 2))
    @test repeat(df, 2) == ref
    @test repeat(view(df, 1:2, :), 2) == ref
end
#
# repeat(df::AbstractDataFrame; inner::Integer = 1, outer::Integer = 1)
#
@testset "inner_outer" begin
    df = DataFrame(a = 1:2, b = 3:4)
    ref = DataFrame(a = repeat(1:2, inner = 2, outer = 3),
                    b = repeat(3:4, inner = 2, outer = 3))
    @test repeat(df, inner = 2, outer = 3) == ref
    @test repeat(view(df, 1:2, :), inner = 2, outer = 3) == ref
end
#
# flatten(df::AbstractDataFrame, veccol::Union{Integer, Symbol})
#
@testset "flatten" begin
    df = DataFrame(a = [1, 2], b = [[1, 2], [3, 4]])
    ref = DataFrame(a = [1, 1, 2, 2], b = [1, 2, 3, 4])
    @test flatten(df, :b) == ref
    df = DataFrame(a = [1, 2], b = [[1, 2], ["x", "y"]])
    ref = DataFrame(a = [1, 1, 2, 2], b = [1, 2, "x", "y"])
    @test flatten(df, :b) == ref
end

end # module
