module TestUtils

using Test, DataFrames

@testset "make_unique" begin
    @test DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=true) == [:x, :x_2, :x_1, :x2]
    @test_throws ArgumentError DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=false)
    @test DataFrames.make_unique([:x, :x_1, :x2], makeunique=false) == [:x, :x_1, :x2]
end

@testset "repeat count" begin
    df = DataFrame(a = 1:2, b = 3:4)
    ref = DataFrame(a = repeat(1:2, 2),
                    b = repeat(3:4, 2))
    @test repeat(df, 2) == ref
    @test repeat(view(df, 1:2, :), 2) == ref

    @test size(repeat(df, 0)) == (0, 2)
    @test size(repeat(df, false)) == (0, 2)
    @test_throws ArgumentError repeat(df, -1)
end

@testset "repeat inner_outer" begin
    df = DataFrame(a = 1:2, b = 3:4)
    ref = DataFrame(a = repeat(1:2, inner = 2, outer = 3),
                    b = repeat(3:4, inner = 2, outer = 3))
    @test repeat(df, inner = 2, outer = 3) == ref
    @test repeat(view(df, 1:2, :), inner = 2, outer = 3) == ref

    @test size(repeat(df, inner = 2, outer = 0)) == (0, 2)
    @test size(repeat(df, inner = 0, outer = 3)) == (0, 2)
    @test size(repeat(df, inner = 2, outer = false)) == (0, 2)
    @test size(repeat(df, inner = false, outer = 3)) == (0, 2)
    @test_throws ArgumentError repeat(df, inner = 2, outer = -1)
    @test_throws ArgumentError repeat(df, inner = -1, outer = 3)
end

@testset "repeat! count" begin
    df = DataFrame(a = 1:2, b = 3:4)
    ref = DataFrame(a = repeat(1:2, 2),
                    b = repeat(3:4, 2))
    a = df.a
    b = df.b
    repeat!(df, 2)
    @test df == ref
    @test a == 1:2
    @test b == 3:4

    for v in (0, false)
        df = DataFrame(a = 1:2, b = 3:4)
        repeat!(df, v)
        @test size(df) == (0, 2)
    end

    df = DataFrame(a = 1:2, b = 3:4)
    @test_throws ArgumentError repeat(df, -1)
    @test df == DataFrame(a = 1:2, b = 3:4)

    @test_throws MethodError repeat!(view(df, 1:2, :), 2)
end

@testset "repeat! inner_outer" begin
    df = DataFrame(a = 1:2, b = 3:4)
    ref = DataFrame(a = repeat(1:2, inner = 2, outer = 3),
                    b = repeat(3:4, inner = 2, outer = 3))
    a = df.a
    b = df.b
    repeat!(df, inner = 2, outer = 3)
    @test df == ref
    @test a == 1:2
    @test b == 3:4

    for v in (0, false)
        df = DataFrame(a = 1:2, b = 3:4)
        repeat!(df, inner = 2, outer = v)
        @test size(df) == (0, 2)

        df = DataFrame(a = 1:2, b = 3:4)
        repeat!(df, inner = v, outer = 3)
        @test size(df) == (0, 2)
    end

    df = DataFrame(a = 1:2, b = 3:4)
    @test_throws ArgumentError repeat(df, inner = 2, outer = -1)
    @test_throws ArgumentError repeat(df, inner = -1, outer = 3)
    @test df == DataFrame(a = 1:2, b = 3:4)

    @test_throws MethodError repeat!(view(df, 1:2, :), inner = 2, outer = 3)
end

@testset "funname" begin
    @test DataFrames.funname(sum ∘ skipmissing ∘ Base.div12) ==
          :sum_skipmissing_div12
end

@testset "pre-Julia 1.3 @spawn replacement" begin
    t = @sync DataFrames.@spawn begin
        sleep(1)
        true
    end
    @test fetch(t) === true
end

@testset "split_indices" begin
    for len in 0:12
        basesize = 10
        x = DataFrames.split_indices(len, basesize)

        @test length(x) == max(1, div(len, basesize))
        @test reduce(vcat, x) === 1:len
        vmin, vmax = extrema(length(v) for v in x)
        @test vmin + 1 == vmax || vmin == vmax
        @test len < basesize || vmin >= basesize
    end

    # Check overflow on 32-bit
    len = typemax(Int32)
    basesize = 100_000_000
    x = collect(DataFrames.split_indices(len, basesize))
    @test length(x) == div(len, basesize)
    @test x[1][1] === 1
    @test x[end][end] === Int(len)
    vmin, vmax = extrema(length(v) for v in x)
    @test vmin + 1 == vmax || vmin == vmax
    @test len < basesize || vmin >= basesize
end

@testset "_findall(B::BitVector)" begin
    BD = Dict(
        "T Big" => trues(100000),
        "F Big" => falses(100000),
        "T64 F64" => [trues(64); falses(64)],
        "F64 T64" => [falses(64); trues(64)],
        "F80 T100" => [falses(85); trues(100)],
        "F256 T32" => [falses(256); trues(32)],
        "F260 T32" => [falses(260); trues(32)],
        "TF Big" => [trues(100000); falses(100000)],
        "FT Big" => [falses(100000);trues(100000)] ,

        # some edge cases
        "TFT small" => [trues(85); falses(100); trues(85)],
        "FTFFT small" => [falses(64 + 32); trues(32); falses(128); trues(32)],
        "TFTF small" => [falses(64); trues(64); falses(64); trues(64)],
        "TFT small" => [trues(64); falses(10); trues(100)],

        "FTF Big" => [falses(8500); trues(100000); falses(65000)],
        "TFT Big" => [trues(8500); falses(100000); trues(65000)],
        "FTFTFTF Big" => [falses(65000); trues(65000); falses(65000); trues(65000); falses(65000); trues(65000); falses(65000)],

        "FTFR small" => [falses(85); trues(100); falses(65); rand([true, false], 20)],
        "R Big" => BitVector(rand([true, false], 200000)),
        "RF Big" => [BitVector(rand([true, false], 100000)) ; falses(100000)],
        "RT Big" => [BitVector(rand([true, false], 100000)) ; trues(100000)],
        "FTFR Big" => [falses(65000);  trues(65000);  falses(65000); rand([true, false], 20000)],
        "T256 R100" => [trues(256);  rand([true, false], 100)],
        "F256 R100" => [falses(256); rand([true, false], 100)],
    )
    for (_, B) in BD
        @test Base.findall(B) == DataFrames._findall(B)
    end
end

end # module
