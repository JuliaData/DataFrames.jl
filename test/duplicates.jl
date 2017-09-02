module TestDuplicates
    using Base.Test, DataFrames

    df = DataFrame(a = [1, 2, 3, 3, 4])
    udf = DataFrame(a = [1, 2, 3, 4])
    @test nonunique(df) == [false, false, false, true, false]
    @test udf == unique(df)
    unique!(df)
    @test df == udf

    pdf = DataFrame(a = CategoricalArray(["a", "a", null, null, "b", null, "a", null]),
                    b = CategoricalArray(["a", "b", null, null, "b", "a", "a", "a"]))
    updf = DataFrame(a = CategoricalArray(["a", "a", null, "b", null]),
                     b = CategoricalArray(["a", "b", null, "b", "a"]))
    @test nonunique(pdf) == [false, false, false, true, false, false, true, true]
    @test nonunique(updf) == falses(5)
    @test updf == unique(pdf)
    unique!(pdf)
    @test pdf == updf

    @testset "missing" begin
        df = DataFrame(A = 1:12, B = repeat('A':'C', inner=4))
        @test DataFrames.colmissing(df) == [0, 0]
    end
end
