module TestPrecompile

using Test, DataFrames

@testset "precompile" begin
    @test DataFrames.precompile() === nothing
    @test DataFrames.precompile(true) === nothing
end

end