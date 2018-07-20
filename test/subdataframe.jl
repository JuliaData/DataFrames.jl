module TestSubDataFrame
    using Test, DataFrames

    @testset "view -- DataFrame" begin
        df = DataFrame(x = 1:10, y = 1.0:10.0)
        @test view(df, 1) == head(df, 1)
        @test view(df, UInt(1)) == head(df, 1)
        @test view(df, BigInt(1)) == head(df, 1)
        @test view(df, 1:2) == head(df, 2)
        @test view(df, vcat(trues(2), falses(8))) == head(df, 2)
        @test view(df, [1, 2]) == head(df, 2)
        @test view(df, 1, :x) == head(df[[:x]], 1)
        @test view(df, 1:2, :x) == head(df[[:x]], 2)
        @test view(df, vcat(trues(2), falses(8)), :x) == head(df[[:x]], 2)
        @test view(df, [1, 2], :x) == head(df[[:x]], 2)
        @test view(df, 1, 1) == head(df[[:x]], 1)
        @test view(df, 1:2, 1) == head(df[[:x]], 2)
        @test view(df, vcat(trues(2), falses(8)), 1) == head(df[[:x]], 2)
        @test view(df, [1, 2], 1) == head(df[[:x]], 2)
        @test view(df, 1, [:x, :y]) == head(df, 1)
        @test view(df, 1:2, [:x, :y]) == head(df, 2)
        @test view(df, vcat(trues(2), falses(8)), [:x, :y]) == head(df, 2)
        @test view(df, [1, 2], [:x, :y]) == head(df, 2)
        @test view(df, 1, [1, 2]) == head(df, 1)
        @test view(df, 1:2, [1, 2]) == head(df, 2)
        @test view(df, vcat(trues(2), falses(8)), [1, 2]) == head(df, 2)
        @test view(df, [1, 2], [1, 2]) == head(df, 2)
        @test view(df, 1, trues(2)) == head(df, 1)
        @test view(df, 1:2, trues(2)) == head(df, 2)
        @test view(df, vcat(trues(2), falses(8)), trues(2)) == head(df, 2)
        @test view(df, [1, 2], trues(2)) == head(df, 2)
        @test view(df, Integer[1, 2]) == head(df, 2)
        @test view(df, UInt[1, 2]) == head(df, 2)
        @test view(df, BigInt[1, 2]) == head(df, 2)
        @test view(df, Union{Int, Missing}[1, 2]) == head(df, 2)
        @test view(df, Union{Integer, Missing}[1, 2]) == head(df, 2)
        @test view(df, Union{UInt, Missing}[1, 2]) == head(df, 2)
        @test view(df, Union{BigInt, Missing}[1, 2]) == head(df, 2)
        @test view(df, :) == df
        @test view(df, :, :) == df
        @test view(df, 1, :) == head(df, 1)
        @test view(df, :, 1) == df[:, [1]]
        @test_throws MissingException view(df, [missing, 1])
    end

    @testset "view -- SubDataFrame" begin
        df = view(DataFrame(x = 1:10, y = 1.0:10.0), 1:10)
        @test view(df, 1) == head(df, 1)
        @test view(df, UInt(1)) == head(df, 1)
        @test view(df, BigInt(1)) == head(df, 1)
        @test view(df, 1:2) == head(df, 2)
        @test view(df, vcat(trues(2), falses(8))) == head(df, 2)
        @test view(df, [1, 2]) == head(df, 2)
        @test view(df, 1, :x) == head(df[[:x]], 1)
        @test view(df, 1:2, :x) == head(df[[:x]], 2)
        @test view(df, vcat(trues(2), falses(8)), :x) == head(df[[:x]], 2)
        @test view(df, [1, 2], :x) == head(df[[:x]], 2)
        @test view(df, 1, 1) == head(df[[:x]], 1)
        @test view(df, 1:2, 1) == head(df[[:x]], 2)
        @test view(df, vcat(trues(2), falses(8)), 1) == head(df[[:x]], 2)
        @test view(df, [1, 2], 1) == head(df[[:x]], 2)
        @test view(df, 1, [:x, :y]) == head(df, 1)
        @test view(df, 1:2, [:x, :y]) == head(df, 2)
        @test view(df, vcat(trues(2), falses(8)), [:x, :y]) == head(df, 2)
        @test view(df, [1, 2], [:x, :y]) == head(df, 2)
        @test view(df, 1, [1, 2]) == head(df, 1)
        @test view(df, 1:2, [1, 2]) == head(df, 2)
        @test view(df, vcat(trues(2), falses(8)), [1, 2]) == head(df, 2)
        @test view(df, [1, 2], [1, 2]) == head(df, 2)
        @test view(df, 1, trues(2)) == head(df, 1)
        @test view(df, 1:2, trues(2)) == head(df, 2)
        @test view(df, vcat(trues(2), falses(8)), trues(2)) == head(df, 2)
        @test view(df, [1, 2], trues(2)) == head(df, 2)
        @test view(df, Integer[1, 2]) == head(df, 2)
        @test view(df, UInt[1, 2]) == head(df, 2)
        @test view(df, BigInt[1, 2]) == head(df, 2)
        @test view(df, Union{Int, Missing}[1, 2]) == head(df, 2)
        @test view(df, Union{Integer, Missing}[1, 2]) == head(df, 2)
        @test view(df, Union{UInt, Missing}[1, 2]) == head(df, 2)
        @test view(df, Union{BigInt, Missing}[1, 2]) == head(df, 2)
        @test view(df, :) == df
        @test view(df, :, :) == df
        @test view(df, 1, :) == head(df, 1)
        @test view(df, :, 1) == df[:, [1]]
        @test_throws MissingException view(df, [missing, 1])
    end

    @testset "getproperty, setproperty! and propertynames" begin
        x = collect(1:10)
        y = collect(1.0:10.0)
        df = view(DataFrame(x = x, y = y), 2:6)

        @test Base.propertynames(df) == names(df)

        @test df.x == 2:6
        @test df.y == 2:6
        @test_throws KeyError df.z

        df.x = 1:5
        @test df.x == 1:5
        @test x == [1; 1:5; 7:10]
        df.y = 1
        @test df.y == [1, 1, 1, 1, 1]
        @test y == [1; 1; 1; 1; 1; 1; 7:10]
        @test_throws ErrorException df.z = 1:5
        @test_throws ErrorException df.z = 1
    end
end
