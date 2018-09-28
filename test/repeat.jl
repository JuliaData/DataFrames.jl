module TestRepeat
    using DataFrames, Test

    #
    # repeat(df::AbstractDataFrame, count::Integer)
    #
    @testset "count" begin
        df = DataFrame(a = 1:2, b = 3:4)
        @test repeat(df, 2) == DataFrame(a = repeat(1:2, 2),
                                         b = repeat(3:4, 2))
        df = DataFrame(a = repeat(1:2, 2), b = 1:4)
        subdfs = by(df, :a) do subdf
            repeat(subdf, 2)
        end
        @test subdfs[[:a,:b]] ==
            DataFrame(a = repeat(1:2, inner = 4),
                      b = append!(repeat([1,3], outer = 2),
                                  repeat([2,4], outer = 2),
                                  ))
    end
    #
    # repeat(df::AbstractDataFrame; inner::Integer = 1, outer::Integer = 1)
    #
    @testset "inner_outer" begin
        df = DataFrame(a = 1:2, b = 3:4)
        @test repeat(df, inner = 2, outer = 3) ==
            DataFrame(a = repeat(1:2, inner = 2, outer = 3),
                      b = repeat(3:4, inner = 2, outer = 3))
        df = DataFrame(a = repeat(1:2, 2), b = 1:4)
        subdfs = by(df, :a) do subdf
            repeat(subdf, inner = 2, outer = 3)
        end
        @test subdfs[[:a,:b]] ==
            DataFrame(a = repeat(1:2, inner = 12),
                      b = append!(repeat([1,3], inner = 2, outer = 3),
                                  repeat([2,4], inner = 2, outer = 3),
                                  ))
    end
end
