module TestDuplicates

using Test, DataFrames
const ≅ = isequal

@testset "nonunique" begin
    df = DataFrame(a = [1, 2, 3, 3, 4])
    udf = DataFrame(a = [1, 2, 3, 4])
    @test nonunique(df) == [false, false, false, true, false]
    @test udf == unique(df)
    unique!(df)
    @test df == udf

    pdf = DataFrame(a = CategoricalArray(["a", "a", missing, missing, "b", missing, "a", missing]),
                    b = CategoricalArray(["a", "b", missing, missing, "b", "a", "a", "a"]))
    updf = DataFrame(a = CategoricalArray(["a", "a", missing, "b", missing]),
                    b = CategoricalArray(["a", "b", missing, "b", "a"]))
    @test nonunique(pdf) == [false, false, false, true, false, false, true, true]
    @test nonunique(updf) == falses(5)
    @test updf ≅ unique(pdf)
    unique!(pdf)
    @test pdf ≅ updf
end

end # module
