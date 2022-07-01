module TestMetadata

using Test, DataFrames

@testset "hasmetadata & metadata" begin
    for x in (DataFrame(), DataFrame(a=1))
        @test !hasmetadata(x)
        m = metadata(x)
        @test !hasmetadata(x)
        @test isempty(m)
        m["name"] = "empty"
        @test hasmetadata(x)
        @test metadata(x) === m
        empty!(m)
        @test !hasmetadata(x)
        @test metadata(x) === m
    end

    for fun in (eachcol, eachrow, x -> groupby(x, :a),
                x -> x[1, :], x -> @view x[:, :])
        x = fun(DataFrame(a=1))
        @test !hasmetadata(x)
        m = metadata(x)
        @test !hasmetadata(x)
        @test isempty(m)
        m["name"] = "empty"
        @test hasmetadata(x)
        @test metadata(x) === m
        empty!(m)
        @test !hasmetadata(x)
        @test metadata(x) === m
    end
end

@testset "hascolmetadata & colmetadata" begin
    for fun in (identity, eachcol, eachrow, x -> groupby(x, :a),
                x -> x[1, :], x -> @view x[:, :])
        x = fun(DataFrame(b=2, a=1))
        @test !hascolmetadata(x)
        @test_throws ArgumentError hascolmetadata(x, :x)
        @test !hascolmetadata(x, :a)
        @test !hascolmetadata(x, :b)
        @test_throws ArgumentError colmetadata(x, :x)
        m = colmetadata(x, :a)
        @test !hascolmetadata(x)
        @test_throws ArgumentError hascolmetadata(x, :x)
        @test !hascolmetadata(x, :a)
        @test !hascolmetadata(x, :b)
        @test isempty(m)
        m["name"] = "empty"
        @test hascolmetadata(x)
        @test_throws ArgumentError hascolmetadata(x, :x)
        @test hascolmetadata(x, :a)
        @test !hascolmetadata(x, :b)
        @test colmetadata(x, :a) === m
        empty!(m)
        @test !hascolmetadata(x)
        @test_throws ArgumentError hascolmetadata(x, :x)
        @test !hascolmetadata(x, :a)
        @test !hascolmetadata(x, :b)
        @test colmetadata(x, :a) === m
    end
end

end