module TestPrecompile

using Test, DataFrames

@testset "precompile"
    @test DataFrames.precompile() === nothing
    @test DataFrames.precompile(true) === nothing
end

end