# this needs to be defined outside of the module to make
# Julia print type name without module name when displaying it
struct ⛵⛵⛵⛵⛵
end
Base.show(io::IO, ::⛵⛵⛵⛵⛵) = show(io, "⛵")

module TestShow

using DataFrames, Dates, Random, Test

import Main: ⛵⛵⛵⛵⛵

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
                   C = Float32[1.0, 2.0, 3.0, 4.0], D = ['\'', '∀', '\$', '\n'])

    refstr = """
    4×4 DataFrame
    │ Row │ A     │ B           │ C       │ D    │
    │     │ Int64 │ String      │ Float32 │ Char │
    ├─────┼───────┼─────────────┼─────────┼──────┤
    │ 1   │ 1     │ x"          │ 1.0     │ '\\'' │
    │ 2   │ 2     │ ∀ε>0: x+ε>x │ 2.0     │ '∀'  │
    │ 3   │ 3     │ z\$          │ 3.0     │ '\$'  │
    │ 4   │ 4     │ A\\nC        │ 4.0     │ '\\n' │"""

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
    df_big = DataFrame(reshape(Int64(10000001):Int64(10000000+25*5), 25, 5))

    io = IOContext(IOBuffer(), :displaysize=>(11, 40), :limit=>true)
    show(io, df_big)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame. Omitted printing of 2 columns
    │ Row │ x1       │ x2       │ x3       │
    │     │ Int64    │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 10000001 │ 10000026 │ 10000051 │
    ⋮
    │ 24  │ 10000024 │ 10000049 │ 10000074 │
    │ 25  │ 10000025 │ 10000050 │ 10000075 │"""

    io = IOContext(IOBuffer(), :displaysize=>(11, 40), :limit=>true)
    show(io, df_big, allcols=true)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame
    │ Row │ x1       │ x2       │ x3       │
    │     │ Int64    │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 10000001 │ 10000026 │ 10000051 │
    ⋮
    │ 24  │ 10000024 │ 10000049 │ 10000074 │
    │ 25  │ 10000025 │ 10000050 │ 10000075 │

    │ Row │ x4       │ x5       │
    │     │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┤
    │ 1   │ 10000076 │ 10000101 │
    ⋮
    │ 24  │ 10000099 │ 10000124 │
    │ 25  │ 10000100 │ 10000125 │"""

    io = IOContext(IOBuffer(), :displaysize=>(11, 40), :limit=>true)
    show(io, df_big, allrows=true, allcols=true)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame
    │ Row │ x1       │ x2       │ x3       │
    │     │ Int64    │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 10000001 │ 10000026 │ 10000051 │
    │ 2   │ 10000002 │ 10000027 │ 10000052 │
    │ 3   │ 10000003 │ 10000028 │ 10000053 │
    │ 4   │ 10000004 │ 10000029 │ 10000054 │
    │ 5   │ 10000005 │ 10000030 │ 10000055 │
    │ 6   │ 10000006 │ 10000031 │ 10000056 │
    │ 7   │ 10000007 │ 10000032 │ 10000057 │
    │ 8   │ 10000008 │ 10000033 │ 10000058 │
    │ 9   │ 10000009 │ 10000034 │ 10000059 │
    │ 10  │ 10000010 │ 10000035 │ 10000060 │
    │ 11  │ 10000011 │ 10000036 │ 10000061 │
    │ 12  │ 10000012 │ 10000037 │ 10000062 │
    │ 13  │ 10000013 │ 10000038 │ 10000063 │
    │ 14  │ 10000014 │ 10000039 │ 10000064 │
    │ 15  │ 10000015 │ 10000040 │ 10000065 │
    │ 16  │ 10000016 │ 10000041 │ 10000066 │
    │ 17  │ 10000017 │ 10000042 │ 10000067 │
    │ 18  │ 10000018 │ 10000043 │ 10000068 │
    │ 19  │ 10000019 │ 10000044 │ 10000069 │
    │ 20  │ 10000020 │ 10000045 │ 10000070 │
    │ 21  │ 10000021 │ 10000046 │ 10000071 │
    │ 22  │ 10000022 │ 10000047 │ 10000072 │
    │ 23  │ 10000023 │ 10000048 │ 10000073 │
    │ 24  │ 10000024 │ 10000049 │ 10000074 │
    │ 25  │ 10000025 │ 10000050 │ 10000075 │

    │ Row │ x4       │ x5       │
    │     │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┤
    │ 1   │ 10000076 │ 10000101 │
    │ 2   │ 10000077 │ 10000102 │
    │ 3   │ 10000078 │ 10000103 │
    │ 4   │ 10000079 │ 10000104 │
    │ 5   │ 10000080 │ 10000105 │
    │ 6   │ 10000081 │ 10000106 │
    │ 7   │ 10000082 │ 10000107 │
    │ 8   │ 10000083 │ 10000108 │
    │ 9   │ 10000084 │ 10000109 │
    │ 10  │ 10000085 │ 10000110 │
    │ 11  │ 10000086 │ 10000111 │
    │ 12  │ 10000087 │ 10000112 │
    │ 13  │ 10000088 │ 10000113 │
    │ 14  │ 10000089 │ 10000114 │
    │ 15  │ 10000090 │ 10000115 │
    │ 16  │ 10000091 │ 10000116 │
    │ 17  │ 10000092 │ 10000117 │
    │ 18  │ 10000093 │ 10000118 │
    │ 19  │ 10000094 │ 10000119 │
    │ 20  │ 10000095 │ 10000120 │
    │ 21  │ 10000096 │ 10000121 │
    │ 22  │ 10000097 │ 10000122 │
    │ 23  │ 10000098 │ 10000123 │
    │ 24  │ 10000099 │ 10000124 │
    │ 25  │ 10000100 │ 10000125 │"""

    io = IOContext(IOBuffer(), :displaysize=>(11, 40), :limit=>true)
    show(io, df_big, allrows=true, allcols=false)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame. Omitted printing of 2 columns
    │ Row │ x1       │ x2       │ x3       │
    │     │ Int64    │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 10000001 │ 10000026 │ 10000051 │
    │ 2   │ 10000002 │ 10000027 │ 10000052 │
    │ 3   │ 10000003 │ 10000028 │ 10000053 │
    │ 4   │ 10000004 │ 10000029 │ 10000054 │
    │ 5   │ 10000005 │ 10000030 │ 10000055 │
    │ 6   │ 10000006 │ 10000031 │ 10000056 │
    │ 7   │ 10000007 │ 10000032 │ 10000057 │
    │ 8   │ 10000008 │ 10000033 │ 10000058 │
    │ 9   │ 10000009 │ 10000034 │ 10000059 │
    │ 10  │ 10000010 │ 10000035 │ 10000060 │
    │ 11  │ 10000011 │ 10000036 │ 10000061 │
    │ 12  │ 10000012 │ 10000037 │ 10000062 │
    │ 13  │ 10000013 │ 10000038 │ 10000063 │
    │ 14  │ 10000014 │ 10000039 │ 10000064 │
    │ 15  │ 10000015 │ 10000040 │ 10000065 │
    │ 16  │ 10000016 │ 10000041 │ 10000066 │
    │ 17  │ 10000017 │ 10000042 │ 10000067 │
    │ 18  │ 10000018 │ 10000043 │ 10000068 │
    │ 19  │ 10000019 │ 10000044 │ 10000069 │
    │ 20  │ 10000020 │ 10000045 │ 10000070 │
    │ 21  │ 10000021 │ 10000046 │ 10000071 │
    │ 22  │ 10000022 │ 10000047 │ 10000072 │
    │ 23  │ 10000023 │ 10000048 │ 10000073 │
    │ 24  │ 10000024 │ 10000049 │ 10000074 │
    │ 25  │ 10000025 │ 10000050 │ 10000075 │"""
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
    df_big = DataFrame(rand(25, 5))
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
        │     │ \e[90mString\e[39m │ \e[90mFloat64?\e[39m │
        ├─────┼────────┼──────────┤
        │ 1   │ Suzy   │ 1.5      │
        │ 2   │ Amir   │ \e[90mmissing\e[39m  │"""

    df = DataFrame(A = [:Symbol, missing, :missing],
                   B = [missing, "String", "missing"],
                   C = [:missing, "missing", missing])
    @test sprint(show, df, context=:color=>true) == """
        3×3 DataFrame
        │ Row │ A       │ B       │ C       │
        │     │ \e[90mSymbol?\e[39m │ \e[90mString?\e[39m │ \e[90mAny\e[39m     │
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

@testset "Test showing special types" begin
    # strings with escapes
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

    # categorical
    df = DataFrame(a = categorical([1, 2, 3]), b = categorical(["a", "b", missing]))
    @test sprint(show, df) == """
    3×2 DataFrame
    │ Row │ a    │ b       │
    │     │ Cat… │ Cat…?   │
    ├─────┼──────┼─────────┤
    │ 1   │ 1    │ a       │
    │ 2   │ 2    │ b       │
    │ 3   │ 3    │ missing │"""

    # BigFloat
    df = DataFrame(a = [big(1.0), missing])
    @test sprint(show, df) == """
    2×1 DataFrame
    │ Row │ a         │
    │     │ BigFloat? │
    ├─────┼───────────┤
    │ 1   │ 1.0       │
    │ 2   │ missing   │"""

    # date types
    df = DataFrame(a = Date(2020, 2, 11), b = DateTime(2020, 2, 11, 15), c = Day(1))
    @test sprint(show, df) == """
    1×3 DataFrame
    │ Row │ a          │ b                   │ c     │
    │     │ Date       │ DateTime            │ Day   │
    ├─────┼────────────┼─────────────────────┼───────┤
    │ 1   │ 2020-02-11 │ 2020-02-11T15:00:00 │ 1 day │"""

    # Irrational
    df = DataFrame(a = π)
    if VERSION < v"1.2.0-DEV.276"
        @test sprint(show, df) == """
        1×1 DataFrame
        │ Row │ a                      │
        │     │ Irrational{:π}         │
        ├─────┼────────────────────────┤
        │ 1   │ π = 3.1415926535897... │"""
    else
        @test sprint(show, df) == """
        1×1 DataFrame
        │ Row │ a        │
        │     │ Irratio… │
        ├─────┼──────────┤
        │ 1   │ π        │"""
    end
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

@testset "Test of DataFrameRows and DataFrameColumns" begin
    df = DataFrame(x = [float(pi)])
    @test sprint(show, eachrow(df)) == """
        1×1 DataFrameRows
        │ Row │ x       │
        │     │ Float64 │
        ├─────┼─────────┤
        │ 1   │ 3.14159 │"""

    @test sprint((io, x) -> show(io, x, summary=false), eachrow(df)) == """

        │ Row │ x       │
        │     │ Float64 │
        ├─────┼─────────┤
        │ 1   │ 3.14159 │"""

    @test sprint(show, eachcol(df)) == """
        1×1 DataFrameColumns
        │ Row │ x       │
        │     │ Float64 │
        ├─────┼─────────┤
        │ 1   │ 3.14159 │"""

    @test sprint((io, x) -> show(io, x, summary=false), eachcol(df)) == """

        │ Row │ x       │
        │     │ Float64 │
        ├─────┼─────────┤
        │ 1   │ 3.14159 │"""
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

@testset "wide type name" begin
    @test sprint(show, DataFrame(a=⛵⛵⛵⛵⛵())) == """
    1×1 DataFrame
    │ Row │ a       │
    │     │ ⛵⛵⛵… │
    ├─────┼─────────┤
    │ 1   │ "⛵"    │"""

    @test sprint(show, DataFrame(a=categorical([Int64(2)^54]))) == """
    1×1 DataFrame
    │ Row │ a                 │
    │     │ CategoricalValue… │
    ├─────┼───────────────────┤
    │ 1   │ 18014398509481984 │"""

    @test sprint(show, DataFrame(a=categorical([Int64(2)^53]))) == """
    1×1 DataFrame
    │ Row │ a                │
    │     │ Categorical…     │
    ├─────┼──────────────────┤
    │ 1   │ 9007199254740992 │"""

    @test sprint(show, DataFrame(a=categorical([Int64(2)^37]))) == """
    1×1 DataFrame
    │ Row │ a            │
    │     │ Categorical… │
    ├─────┼──────────────┤
    │ 1   │ 137438953472 │"""

    @test sprint(show, DataFrame(a=categorical([Int64(2)^36]))) == """
    1×1 DataFrame
    │ Row │ a           │
    │     │ Cat…        │
    ├─────┼─────────────┤
    │ 1   │ 68719476736 │"""

    @test sprint(show, DataFrame(a=Union{Function, Missing}[missing])) == """
    1×1 DataFrame
    │ Row │ a         │
    │     │ Function? │
    ├─────┼───────────┤
    │ 1   │ missing   │"""
end

@testset "wide type name" begin
    df = DataFrame(A = Int32.(1:3), B = ["x", "y", "z"])

    io = IOBuffer()
    show(io, df, eltypes=true)
    str = String(take!(io))
    @test str == """
    3×2 DataFrame
    │ Row │ A     │ B      │
    │     │ Int32 │ String │
    ├─────┼───────┼────────┤
    │ 1   │ 1     │ x      │
    │ 2   │ 2     │ y      │
    │ 3   │ 3     │ z      │"""

    io = IOBuffer()
    show(io, df, eltypes=false)
    str = String(take!(io))
    @test str == """
    3×2 DataFrame
    │ Row │ A │ B │
    ├─────┼───┼───┤
    │ 1   │ 1 │ x │
    │ 2   │ 2 │ y │
    │ 3   │ 3 │ z │"""
end

@testset "UnionAll" begin
    df = DataFrame(x=AbstractVector[1:2])

    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
    1×1 DataFrame
    │ Row │ x        │
    │     │ Abstrac… │
    ├─────┼──────────┤
    │ 1   │ 1:2      │"""
end

@testset "wide output and column trimming" begin
    df = DataFrame(x = "0123456789"^4)
    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
    1×1 DataFrame
    │ Row │ x                                 │
    │     │ String                            │
    ├─────┼───────────────────────────────────┤
    │ 1   │ 01234567890123456789012345678901… │"""

    io = IOContext(IOBuffer(), :displaysize=>(10, 10), :limit=>true)
    show(io, df)
    str = String(take!(io.io))
    @test str === "1×1 DataFrame. Omitted printing of all columns as they do not fit the display size"

    df = DataFrame(x = "0123456789"^4, y = "0123456789"^4)
    io = IOContext(IOBuffer(), :displaysize=>(10, 10), :limit=>true)
    show(io, df, splitcols=true, allcols=true)
    str = String(take!(io.io))
    @test str === """
    1×2 DataFrame
    │ Row │ x                                 │
    │     │ String                            │
    ├─────┼───────────────────────────────────┤
    │ 1   │ 01234567890123456789012345678901… │

    │ Row │ y                                 │
    │     │ String                            │
    ├─────┼───────────────────────────────────┤
    │ 1   │ 01234567890123456789012345678901… │"""

    df = DataFrame(x = "😄"^20)
        io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str === """
    1×1 DataFrame
    │ Row │ x                                 │
    │     │ String                            │
    ├─────┼───────────────────────────────────┤
    │ 1   │ 😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄… │"""
end

end # module
