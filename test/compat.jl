module TestCompat

import Compat, DataFrames
using Test

@testset "overload Compat functions" begin
    @testset "DataFrames.$f === Compat.$f" for f in intersect(names(DataFrames), names(Compat))
        @test getproperty(DataFrames, f) === getproperty(Compat, f)
    end
end

end  # module
