module TestUtils

using Test, Random, DataFrames

@testset "make_unique" begin
    @test DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=true) == [:x, :x_2, :x_1, :x2]
    @test_throws ArgumentError DataFrames.make_unique([:x, :x, :x_1, :x2], makeunique=false)
    @test DataFrames.make_unique([:x, :x_1, :x2], makeunique=false) == [:x, :x_1, :x2]
end

@testset "repeat count" begin
    df = DataFrame(a=1:2, b=3:4)
    ref = DataFrame(a=repeat(1:2, 2),
                    b=repeat(3:4, 2))
    @test repeat(df, 2) == ref
    @test repeat(view(df, 1:2, :), 2) == ref

    @test size(repeat(df, 0)) == (0, 2)
    @test size(repeat(df, false)) == (0, 2)
    @test_throws ArgumentError repeat(df, -1)
end

@testset "repeat inner_outer" begin
    df = DataFrame(a=1:2, b=3:4)
    ref = DataFrame(a=repeat(1:2, inner=2, outer=3),
                    b=repeat(3:4, inner=2, outer=3))
    @test repeat(df, inner=2, outer=3) == ref
    @test repeat(view(df, 1:2, :), inner=2, outer=3) == ref

    @test size(repeat(df, inner=2, outer=0)) == (0, 2)
    @test size(repeat(df, inner=0, outer=3)) == (0, 2)
    @test size(repeat(df, inner=2, outer=false)) == (0, 2)
    @test size(repeat(df, inner=false, outer=3)) == (0, 2)
    @test_throws ArgumentError repeat(df, inner=2, outer=-1)
    @test_throws ArgumentError repeat(df, inner=-1, outer=3)
end

@testset "repeat! count" begin
    df = DataFrame(a=1:2, b=3:4)
    ref = DataFrame(a=repeat(1:2, 2),
                    b=repeat(3:4, 2))
    a = df.a
    b = df.b
    repeat!(df, 2)
    @test df == ref
    @test a == 1:2
    @test b == 3:4

    for v in (0, false)
        df = DataFrame(a=1:2, b=3:4)
        repeat!(df, v)
        @test size(df) == (0, 2)
    end

    df = DataFrame(a=1:2, b=3:4)
    @test_throws ArgumentError repeat(df, -1)
    @test df == DataFrame(a=1:2, b=3:4)

    @test_throws MethodError repeat!(view(df, 1:2, :), 2)
end

@testset "repeat! inner_outer" begin
    df = DataFrame(a=1:2, b=3:4)
    ref = DataFrame(a=repeat(1:2, inner=2, outer=3),
                    b=repeat(3:4, inner=2, outer=3))
    a = df.a
    b = df.b
    repeat!(df, inner = 2, outer = 3)
    @test df == ref
    @test a == 1:2
    @test b == 3:4

    for v in (0, false)
        df = DataFrame(a=1:2, b=3:4)
        repeat!(df, inner=2, outer=v)
        @test size(df) == (0, 2)

        df = DataFrame(a=1:2, b=3:4)
        repeat!(df, inner=v, outer=3)
        @test size(df) == (0, 2)
    end

    df = DataFrame(a=1:2, b=3:4)
    @test_throws ArgumentError repeat(df, inner = 2, outer = -1)
    @test_throws ArgumentError repeat(df, inner = -1, outer = 3)
    @test df == DataFrame(a=1:2, b=3:4)

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
    for len in 1:100, basesize in 1:10
        x = DataFrames.split_indices(len, basesize)

        @test length(x) == max(1, div(len, basesize))
        @test reduce(vcat, x) == 1:len
        vmin, vmax = extrema(length(v) for v in x)
        @test vmin + 1 == vmax || vmin == vmax
        @test len < basesize || vmin >= basesize
    end

    @test_throws AssertionError DataFrames.split_indices(0, 10)
    @test_throws AssertionError DataFrames.split_indices(10, 0)

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

@testset "split_to_chunks" begin
    for lg in 1:100, nt in 1:11
        if lg < nt
            @test_throws AssertionError DataFrames.split_to_chunks(lg, nt)
            continue
        end
        x = collect(DataFrames.split_to_chunks(lg, nt))
        @test reduce(vcat, x) == 1:lg
        @test sum(length, x) == lg
        @test first(x[1]) == 1
        @test last(x[end]) == lg
        @test length(x) == nt
        for i in 1:nt-1
            @test first(x[i+1])-last(x[i]) == 1
        end
    end

    @test_throws AssertionError DataFrames.split_to_chunks(0, 10)
    @test_throws AssertionError DataFrames.split_to_chunks(10, 0)
    @test_throws AssertionError DataFrames.split_to_chunks(10, 11)
end

@testset "_findall(B::BitVector)" begin
    Random.seed!(1234)
    BD = Dict(
        "Empty" => (BitVector([]), UnitRange{Int}),
        "T Big" => (trues(100000), UnitRange{Int}),
        "F Big" => (falses(100000), UnitRange{Int}),
        "T64 F64" => ([trues(64); falses(64)], UnitRange{Int}),
        "F64 T64" => ([falses(64); trues(64)], UnitRange{Int}),
        "F80 T100" => ([falses(85); trues(100)], UnitRange{Int}),
        "F256 T32" => ([falses(256); trues(32)], UnitRange{Int}),
        "F260 T32" => ([falses(260); trues(32)], UnitRange{Int}),
        "TF Big" => ([trues(100000); falses(100000)], UnitRange{Int}),
        "FT Big" => ([falses(100000); trues(100000)], UnitRange{Int}),

        # some edge cases
        "TFT small" => ([trues(85); falses(100); trues(85)], Vector{Int}),
        "FTFFT small" => ([falses(64 + 32); trues(32); falses(128); trues(32)], Vector{Int}),
        "TFTF small" => ([falses(64); trues(64); falses(64); trues(64)], Vector{Int}),
        "TFT small2" => ([trues(64); falses(10); trues(100)], Vector{Int}),

        "FTF Big" => ([falses(8500); trues(100000); falses(65000)], UnitRange{Int}),
        "TFT Big" => ([trues(8500); falses(100000); trues(65000)], Vector{Int}),
        "FTFTFTF Big" => ([falses(65000); trues(65000); falses(65000); trues(65000); falses(65000); trues(65000); falses(65000)], Vector{Int}),

        "FTFR small" => ([falses(85); trues(100); falses(65); rand([true, false], 2000)], Vector{Int}),
        "R Big" => (BitVector(rand([true, false], 2000000)), Vector{Int}),
        "RF Big" => ([BitVector(rand([true, false], 1000000)); falses(1000000)], Vector{Int}),
        "RT Big" => ([BitVector(rand([true, false], 1000000));  trues(1000000)], Vector{Int}),
        "FR Big" => ([falses(1000000); BitVector(rand([true, false], 1000000))], Vector{Int}),
        "TR Big" => ([trues(1000000);  BitVector(rand([true, false], 1000000))], Vector{Int}),
        "FRT Big" => ([falses(1000000); BitVector(rand([true, false], 1000000)); trues(1000000)], Vector{Int}),
        "TRF Big" => ([trues(1000000); BitVector(rand([true, false], 1000000)); falses(1000000)], Vector{Int}),
        "FRF Big" => ([falses(1000000); BitVector(rand([true, false], 1000000)); falses(1000000)], Vector{Int}),
        "TRT Big" => ([trues(1000000); BitVector(rand([true, false], 1000000)); trues(1000000)], Vector{Int}),
        "RFR Big" => ([BitVector(rand([true, false], 1000000)); falses(1000000); BitVector(rand([true, false], 1000000))], Vector{Int}),
        "FTFR Big" => ([falses(65000);  trues(65000);  falses(65000); rand([true, false], 20000)], Vector{Int}),
        "T256 R100" => ([trues(256);  rand([true, false], 2000)], Vector{Int}),
        "F256 R100" => ([falses(256); rand([true, false], 2000)], Vector{Int}),
    )
    for (_, (B, T)) in BD
        res = DataFrames._findall(B)
        @test Base.findall(B) == res
        @test res isa T
        res = DataFrames._findall(Vector{Bool}(B))
        @test Base.findall(B) == res
        @test res isa T
    end

    # 1:200 is to test all small cases
    # 1000 is to test skipping multiple 64-bit blocks of 0 and 1
    for n in [1:200; 1000], i in 1:n, j in i:n
        x = falses(n)
        x[i:j] .= true
        res = DataFrames._findall(x)
        @test res == i:j
        @test res isa UnitRange{Int}
        res = DataFrames._findall(Vector{Bool}(x))
        @test res == i:j
        @test res isa UnitRange{Int}
        if j + 1 < n
            x[j + 2] = true
            # add one false and then true
            @test  DataFrames._findall(x) == [i:j; j+2]
            @test  DataFrames._findall(Vector{Bool}(x)) == [i:j; j+2]
            # sprinkle trues randomly after one false
            rand!(view(x, j + 2:n), Bool)
            @test  DataFrames._findall(x) == findall(x)
            @test  DataFrames._findall(Vector{Bool}(x)) == findall(x)
        end
    end
end

end # module
