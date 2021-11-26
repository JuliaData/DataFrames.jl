module TestDuplicates

using Test, DataFrames, CategoricalArrays
const ≅ = isequal

@testset "nonunique" begin
    df = DataFrame(a=[1, 2, 3, 3, 4])
    udf = DataFrame(a=[1, 2, 3, 4])
    @test nonunique(df) == [false, false, false, true, false]
    @test udf == unique(df)
    unique!(df)
    @test df == udf
    @test_throws ArgumentError unique(df, true)

    pdf = DataFrame(a=CategoricalArray(["a", "a", missing, missing, "b", missing, "a", missing]),
                    b=CategoricalArray(["a", "b", missing, missing, "b", "a", "a", "a"]))
    updf = DataFrame(a=CategoricalArray(["a", "a", missing, "b", missing]),
                    b=CategoricalArray(["a", "b", missing, "b", "a"]))
    @test nonunique(pdf) == [false, false, false, true, false, false, true, true]
    @test nonunique(updf) == falses(5)
    @test updf ≅ unique(pdf)
    unique!(pdf)
    @test pdf ≅ updf
    @test_throws ArgumentError unique(pdf, true)

    df = view(DataFrame(a=[1, 2, 3, 3, 4]), :, :)
    udf = DataFrame(a=[1, 2, 3, 4])
    @test nonunique(df) == [false, false, false, true, false]
    @test udf == unique(df)
    @test_throws ArgumentError unique!(df)
    @test_throws ArgumentError unique(df, true)

     pdf = view(DataFrame(a=CategoricalArray(["a", "a", missing, missing, "b", missing, "a", missing]),
                          b=CategoricalArray(["a", "b", missing, missing, "b", "a", "a", "a"])), :,  :)
    updf = DataFrame(a=CategoricalArray(["a", "a", missing, "b", missing]),
                     b=CategoricalArray(["a", "b", missing, "b", "a"]))
    @test nonunique(pdf) == [false, false, false, true, false, false, true, true]
    @test nonunique(updf) == falses(5)
    @test updf ≅ unique(pdf)
    @test_throws ArgumentError unique!(pdf)
    @test_throws ArgumentError unique(pdf, true)
end

end # module
