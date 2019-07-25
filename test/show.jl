module TestShow

using DataFrames, Random, Test

function capture_stdout(f::Function)
    oldstdout = stdout
    rd, wr = redirect_stdout()
    f()
    redirect_stdout(oldstdout)
    size = displaysize(rd)
    close(wr)
    str = read(rd, String)
    close(rd)
    str, size
end

@testset "Basic show test with allrows and allcols" begin
    df = DataFrame(A = Int64[1:4;], B = ["x\"", "∀ε>0: x+ε>x", "z\$", "A\nC"],
                   C = Float32[1.0, 2.0, 3.0, 4.0])

    refstr = """
    4×3 DataFrame
    │ Row │ A     │ B           │ C       │
    │     │ Int64 │ String      │ Float32 │
    ├─────┼───────┼─────────────┼─────────┤
    │ 1   │ 1     │ x"          │ 1.0     │
    │ 2   │ 2     │ ∀ε>0: x+ε>x │ 2.0     │
    │ 3   │ 3     │ z\$          │ 3.0     │
    │ 4   │ 4     │ A\\nC        │ 4.0     │"""

    for allrows in [true, false], allcols in [true, false]
        io = IOBuffer()
        show(io, df, allcols=allcols, allrows=allrows)
        str = String(take!(io))
        @test str == refstr
        io = IOBuffer()
        show(io, MIME("text/plain"), df, allcols=allcols, allrows=allrows)
        str = String(take!(io))
        @test str == refstr
    end

    df = DataFrame(A = Vector{String}(undef, 3))
    @test sprint(show, df) == """
    3×1 DataFrame
    │ Row │ A      │
    │     │ String │
    ├─────┼────────┤
    │ 1   │ #undef │
    │ 2   │ #undef │
    │ 3   │ #undef │"""
end

@testset "displaysize test" begin
    Random.seed!(1)
    df_big = DataFrame(rand(25,5))

    io = IOContext(IOBuffer(), :displaysize=>(11,40), :limit=>true)
    show(io, df_big)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame. Omitted printing of 2 columns
    │ Row │ x1       │ x2       │ x3       │
    │     │ Float64  │ Float64  │ Float64  │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 0.236033 │ 0.644883 │ 0.440897 │
    ⋮
    │ 24  │ 0.278582 │ 0.241591 │ 0.990741 │
    │ 25  │ 0.751313 │ 0.884837 │ 0.550334 │"""

    io = IOContext(IOBuffer(), :displaysize=>(11,40), :limit=>true)
    show(io, df_big, allcols=true)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame
    │ Row │ x1       │ x2       │ x3       │
    │     │ Float64  │ Float64  │ Float64  │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 0.236033 │ 0.644883 │ 0.440897 │
    ⋮
    │ 24  │ 0.278582 │ 0.241591 │ 0.990741 │
    │ 25  │ 0.751313 │ 0.884837 │ 0.550334 │

    │ Row │ x4       │ x5       │
    │     │ Float64  │ Float64  │
    ├─────┼──────────┼──────────┤
    │ 1   │ 0.580782 │ 0.138763 │
    ⋮
    │ 24  │ 0.762276 │ 0.755415 │
    │ 25  │ 0.339081 │ 0.649056 │"""

    io = IOContext(IOBuffer(), :displaysize=>(11,40), :limit=>true)
    show(io, df_big, allrows=true, allcols=true)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame
    │ Row │ x1         │ x2        │
    │     │ Float64    │ Float64   │
    ├─────┼────────────┼───────────┤
    │ 1   │ 0.236033   │ 0.644883  │
    │ 2   │ 0.346517   │ 0.0778264 │
    │ 3   │ 0.312707   │ 0.848185  │
    │ 4   │ 0.00790928 │ 0.0856352 │
    │ 5   │ 0.488613   │ 0.553206  │
    │ 6   │ 0.210968   │ 0.46335   │
    │ 7   │ 0.951916   │ 0.185821  │
    │ 8   │ 0.999905   │ 0.111981  │
    │ 9   │ 0.251662   │ 0.976312  │
    │ 10  │ 0.986666   │ 0.0516146 │
    │ 11  │ 0.555751   │ 0.53803   │
    │ 12  │ 0.437108   │ 0.455692  │
    │ 13  │ 0.424718   │ 0.279395  │
    │ 14  │ 0.773223   │ 0.178246  │
    │ 15  │ 0.28119    │ 0.548983  │
    │ 16  │ 0.209472   │ 0.370971  │
    │ 17  │ 0.251379   │ 0.894166  │
    │ 18  │ 0.0203749  │ 0.648054  │
    │ 19  │ 0.287702   │ 0.417039  │
    │ 20  │ 0.859512   │ 0.144566  │
    │ 21  │ 0.0769509  │ 0.622403  │
    │ 22  │ 0.640396   │ 0.872334  │
    │ 23  │ 0.873544   │ 0.524975  │
    │ 24  │ 0.278582   │ 0.241591  │
    │ 25  │ 0.751313   │ 0.884837  │

    │ Row │ x3        │ x4        │
    │     │ Float64   │ Float64   │
    ├─────┼───────────┼───────────┤
    │ 1   │ 0.440897  │ 0.580782  │
    │ 2   │ 0.404673  │ 0.768359  │
    │ 3   │ 0.736787  │ 0.519525  │
    │ 4   │ 0.953803  │ 0.514863  │
    │ 5   │ 0.0951856 │ 0.998136  │
    │ 6   │ 0.519675  │ 0.603682  │
    │ 7   │ 0.0135403 │ 0.758775  │
    │ 8   │ 0.303399  │ 0.590953  │
    │ 9   │ 0.702557  │ 0.722086  │
    │ 10  │ 0.596537  │ 0.953207  │
    │ 11  │ 0.638935  │ 0.384411  │
    │ 12  │ 0.872347  │ 0.320011  │
    │ 13  │ 0.548635  │ 0.865625  │
    │ 14  │ 0.262992  │ 0.45457   │
    │ 15  │ 0.526443  │ 0.420287  │
    │ 16  │ 0.465019  │ 0.225151  │
    │ 17  │ 0.275519  │ 0.286169  │
    │ 18  │ 0.461823  │ 0.309144  │
    │ 19  │ 0.951861  │ 0.170391  │
    │ 20  │ 0.288737  │ 0.147162  │
    │ 21  │ 0.661232  │ 0.230063  │
    │ 22  │ 0.194568  │ 0.0929292 │
    │ 23  │ 0.393193  │ 0.681415  │
    │ 24  │ 0.990741  │ 0.762276  │
    │ 25  │ 0.550334  │ 0.339081  │

    │ Row │ x5        │
    │     │ Float64   │
    ├─────┼───────────┤
    │ 1   │ 0.138763  │
    │ 2   │ 0.456446  │
    │ 3   │ 0.739918  │
    │ 4   │ 0.816004  │
    │ 5   │ 0.114529  │
    │ 6   │ 0.748928  │
    │ 7   │ 0.878108  │
    │ 8   │ 0.930481  │
    │ 9   │ 0.896291  │
    │ 10  │ 0.663145  │
    │ 11  │ 0.472799  │
    │ 12  │ 0.880525  │
    │ 13  │ 0.0141033 │
    │ 14  │ 0.502774  │
    │ 15  │ 0.224851  │
    │ 16  │ 0.287858  │
    │ 17  │ 0.104033  │
    │ 18  │ 0.475749  │
    │ 19  │ 0.416681  │
    │ 20  │ 0.521387  │
    │ 21  │ 0.908499  │
    │ 22  │ 0.102832  │
    │ 23  │ 0.670421  │
    │ 24  │ 0.755415  │
    │ 25  │ 0.649056  │"""

    io = IOContext(IOBuffer(), :displaysize=>(11,40), :limit=>true)
    show(io, df_big, allrows=true, allcols=false)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame. Omitted printing of 3 columns
    │ Row │ x1         │ x2        │
    │     │ Float64    │ Float64   │
    ├─────┼────────────┼───────────┤
    │ 1   │ 0.236033   │ 0.644883  │
    │ 2   │ 0.346517   │ 0.0778264 │
    │ 3   │ 0.312707   │ 0.848185  │
    │ 4   │ 0.00790928 │ 0.0856352 │
    │ 5   │ 0.488613   │ 0.553206  │
    │ 6   │ 0.210968   │ 0.46335   │
    │ 7   │ 0.951916   │ 0.185821  │
    │ 8   │ 0.999905   │ 0.111981  │
    │ 9   │ 0.251662   │ 0.976312  │
    │ 10  │ 0.986666   │ 0.0516146 │
    │ 11  │ 0.555751   │ 0.53803   │
    │ 12  │ 0.437108   │ 0.455692  │
    │ 13  │ 0.424718   │ 0.279395  │
    │ 14  │ 0.773223   │ 0.178246  │
    │ 15  │ 0.28119    │ 0.548983  │
    │ 16  │ 0.209472   │ 0.370971  │
    │ 17  │ 0.251379   │ 0.894166  │
    │ 18  │ 0.0203749  │ 0.648054  │
    │ 19  │ 0.287702   │ 0.417039  │
    │ 20  │ 0.859512   │ 0.144566  │
    │ 21  │ 0.0769509  │ 0.622403  │
    │ 22  │ 0.640396   │ 0.872334  │
    │ 23  │ 0.873544   │ 0.524975  │
    │ 24  │ 0.278582   │ 0.241591  │
    │ 25  │ 0.751313   │ 0.884837  │"""
end

@testset "IOContext parameters test" begin
    df = DataFrame(A = Int64[1:4;], B = ["x\"", "∀ε>0: x+ε>x", "z\$", "A\nC"],
                   C = Float32[1.0, 2.0, 3.0, 4.0])
    str1, size = capture_stdout() do
        show(df)
    end
    io = IOContext(IOBuffer(), :limit=>true, :displaysize=>size)
    show(io, df)
    str2 = String(take!(io.io))
    @test str1 == str2

    Random.seed!(1)
    df_big = DataFrame(rand(25,5))
    str1, size = capture_stdout() do
        show(df_big)
    end
    io = IOContext(IOBuffer(), :limit=>true, :displaysize=>size)
    show(io, df_big)
    str2 = String(take!(io.io))
    @test str1 == str2
end

@testset "SubDataFrame show test" begin
    df = DataFrame(A = Int64[1:4;], B = ["x\"", "∀ε>0: x+ε>x", "z\$", "A\nC"],
                   C = Float32[1.0, 2.0, 3.0, 4.0])
    subdf = view(df, [2, 3], :)
    io = IOBuffer()
    show(io, subdf, allrows=true, allcols=false)
    str = String(take!(io))
    @test str == """
    2×3 SubDataFrame
    │ Row │ A     │ B           │ C       │
    │     │ Int64 │ String      │ Float32 │
    ├─────┼───────┼─────────────┼─────────┤
    │ 1   │ 2     │ ∀ε>0: x+ε>x │ 2.0     │
    │ 2   │ 3     │ z\$          │ 3.0     │"""
    show(io, subdf, allrows=true)
    show(io, subdf, allcols=true)
    show(io, subdf, allcols=true, allrows=true)
end

@testset "Test showing StackedVector and RepeatedVector" begin
    A = DataFrames.StackedVector(Any[[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    @test sprint(show, A) == "[1, 2, 3, 4, 5, 6, 7, 8, 9]"
    A = DataFrames.RepeatedVector([1, 2, 3], 5, 1)
    @test sprint(show, A) == "[1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3]"
    A = DataFrames.RepeatedVector([1, 2, 3], 1, 5)
    @test sprint(show, A) == "[1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3]"
end

@testset "Test colors and non-standard values: missing and nothing" begin
    df = DataFrame(Fish = ["Suzy", "Amir"], Mass = [1.5, missing])
    @test sprint(show, df, context=:color=>true) == """
        2×2 DataFrame
        │ Row │ Fish   │ Mass     │
        │     │ \e[90mString\e[39m │ \e[90mFloat64⍰\e[39m │
        ├─────┼────────┼──────────┤
        │ 1   │ Suzy   │ 1.5      │
        │ 2   │ Amir   │ \e[90mmissing\e[39m  │"""

    df = DataFrame(A = [:Symbol, missing, :missing],
                   B = [missing, "String", "missing"],
                   C = [:missing, "missing", missing])
    @test sprint(show, df, context=:color=>true) == """
        3×3 DataFrame
        │ Row │ A       │ B       │ C       │
        │     │ \e[90mSymbol⍰\e[39m │ \e[90mString⍰\e[39m │ \e[90mAny\e[39m     │
        ├─────┼─────────┼─────────┼─────────┤
        │ 1   │ Symbol  │ \e[90mmissing\e[39m │ missing │
        │ 2   │ \e[90mmissing\e[39m │ String  │ missing │
        │ 3   │ missing │ missing │ \e[90mmissing\e[39m │"""

    df_nothing = DataFrame(A = [1.0, 2.0, 3.0], B = ["g", "g", nothing])
    @test sprint(show, df_nothing) == """
    3×2 DataFrame
    │ Row │ A       │ B      │
    │     │ Float64 │ Union… │
    ├─────┼─────────┼────────┤
    │ 1   │ 1.0     │ g      │
    │ 2   │ 2.0     │ g      │
    │ 3   │ 3.0     │        │"""
end

@testset "Test correct width computation" begin
    df = DataFrame([["a"]], [:x])
    @test sprint(show, df) == """
    1×1 DataFrame
    │ Row │ x      │
    │     │ String │
    ├─────┼────────┤
    │ 1   │ a      │"""
end

@testset "Test showing special types: strings with escapes, categorical and BigFloat" begin
    df = DataFrame(a = ["1\n1", "2\t2", "3\r3", "4\$4", "5\"5", "6\\6"])
    @test sprint(show, df) == """
    6×1 DataFrame
    │ Row │ a      │
    │     │ String │
    ├─────┼────────┤
    │ 1   │ 1\\n1   │
    │ 2   │ 2\\t2   │
    │ 3   │ 3\\r3   │
    │ 4   │ 4\$4    │
    │ 5   │ 5"5    │
    │ 6   │ 6\\\\6   │"""

    df = DataFrame(a = categorical([1,2,3]), b = categorical(["a", "b", missing]))
    @test sprint(show, df) == """
    3×2 DataFrame
    │ Row │ a            │ b             │
    │     │ Categorical… │ Categorical…⍰ │
    ├─────┼──────────────┼───────────────┤
    │ 1   │ 1            │ a             │
    │ 2   │ 2            │ b             │
    │ 3   │ 3            │ missing       │"""

    df = DataFrame(a = [big(1.0), missing])
    @test sprint(show, df) == """
    2×1 DataFrame
    │ Row │ a         │
    │     │ BigFloat⍰ │
    ├─────┼───────────┤
    │ 1   │ 1.0       │
    │ 2   │ missing   │"""
end

@testset "Test using :compact parameter of IOContext" begin
    df = DataFrame(x = [float(pi)])
    @test sprint(show, df) == """
        1×1 DataFrame
        │ Row │ x       │
        │     │ Float64 │
        ├─────┼─────────┤
        │ 1   │ 3.14159 │"""

    @test sprint(show, df, context=:compact=>false) == """
        1×1 DataFrame
        │ Row │ x                 │
        │     │ Float64           │
        ├─────┼───────────────────┤
        │ 1   │ 3.141592653589793 │"""
end

@testset "Test empty data frame and DataFrameRow" begin
    df = DataFrame(x = [float(pi)])
    @test sprint(show, df[:, 2:1]) == "0×0 DataFrame\n"
    @test sprint(show, @view df[:, 2:1]) == "0×0 SubDataFrame\n"
    @test sprint(show, df[1, 2:1]) == "DataFrameRow"
end

@testset "consistency" begin
    df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(df.c, 5)
    @test_throws AssertionError sprint(show, df)

    df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(DataFrames._columns(df), df[:, :a])
    @test_throws AssertionError sprint(show, df)
end

end # module
