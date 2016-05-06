module TestDuplicates
    using Base.Test
    using DataFrames

    df = DataFrame(a = [1, 2, 3, 3, 4])
    udf = DataFrame(a = [1, 2, 3, 4])
    @test isequal(nonunique(df), [false, false, false, true, false])
    @test isequal(udf, unique(df))
    unique!(df)
    @test isequal(df, udf)

    pdf = DataFrame(a = NullableNominalArray(Nullable{String}["a", "a", Nullable(),
                                             Nullable(), "b", Nullable(), "a", Nullable()]),
                    b = NullableNominalArray(Nullable{String}["a", "b", Nullable(),
                                                              Nullable(), "b", "a", "a", "a"]))
    updf = DataFrame(a = NullableNominalArray(Nullable{String}["a", "a", Nullable(), "b", Nullable()]),
                     b = NullableNominalArray(Nullable{String}["a", "b", Nullable(), "b", "a"]))
    @test isequal(nonunique(pdf), [false, false, false, true, false, false, true, true])
    @test isequal(nonunique(updf), falses(5) )
    @test isequal(updf, unique(pdf))
    unique!(pdf)
    @test isequal(pdf, updf)
end
