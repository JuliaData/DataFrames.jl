# this needs to be defined outside of the module to make
# Julia print type name without module name when displaying it
struct ⛵⛵⛵⛵⛵
end
Base.show(io::IO, ::⛵⛵⛵⛵⛵) = show(io, "⛵")

struct F<:AbstractFloat
    i::Int
end

Base.show(io::IO, f::F) = show(io, f.i)

module TestShow

using DataFrames, Dates, Random, Test, CategoricalArrays, InlineStrings

import Main: ⛵⛵⛵⛵⛵, F

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
    df = DataFrame(A=Int64[1:4;], B=["x\"", "∀ε>0: x+ε>x", "z\$", "A\nC"],
                   C=Float32[1.0, 2.0, 3.0, 4.0], D=['\'', '∀', '$', '\n'])

    refstr = """
    4×4 DataFrame
     Row │ A      B            C        D
         │ Int64  String       Float32  Char
    ─────┼───────────────────────────────────
       1 │     1  x"               1.0  '
       2 │     2  ∀ε>0: x+ε>x      2.0  ∀
       3 │     3  z\$               3.0  \$
       4 │     4  A\\nC             4.0  \\n"""

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

    df = DataFrame(A=Vector{String}(undef, 3))
    @test sprint(show, df) == """
        3×1 DataFrame
         Row │ A
             │ String
        ─────┼────────
           1 │ #undef
           2 │ #undef
           3 │ #undef"""
end

@testset "displaysize test" begin
    df_big = DataFrame(reshape(Int64(10000001):Int64(10000000+25*5), 25, 5),
                       :auto)

    io = IOContext(IOBuffer(), :displaysize=>(11, 40), :limit=>true)
    show(io, df_big)
    str = String(take!(io.io))
    @test str == """
        25×5 DataFrame
         Row │ x1        x2        x3        x ⋯
             │ Int64     Int64     Int64     I ⋯
        ─────┼──────────────────────────────────
           1 │ 10000001  10000026  10000051  1 ⋯
          ⋮  │    ⋮         ⋮         ⋮        ⋱
          25 │ 10000025  10000050  10000075  1
                   2 columns and 23 rows omitted"""

    io = IOContext(IOBuffer(), :displaysize=>(11, 40), :limit=>true)
    show(io, df_big, allcols=true)
    str = String(take!(io.io))
    @test str == """
        25×5 DataFrame
         Row │ x1        x2        x3        x4        x5
             │ Int64     Int64     Int64     Int64     Int64
        ─────┼──────────────────────────────────────────────────
           1 │ 10000001  10000026  10000051  10000076  10000101
          ⋮  │    ⋮         ⋮         ⋮         ⋮         ⋮
          25 │ 10000025  10000050  10000075  10000100  10000125
                                                 23 rows omitted"""

    io = IOContext(IOBuffer(), :displaysize=>(11, 40), :limit=>true)
    show(io, df_big, allrows=true, allcols=true)
    str = String(take!(io.io))
    @test str == """
        25×5 DataFrame
         Row │ x1        x2        x3        x4        x5
             │ Int64     Int64     Int64     Int64     Int64
        ─────┼──────────────────────────────────────────────────
           1 │ 10000001  10000026  10000051  10000076  10000101
           2 │ 10000002  10000027  10000052  10000077  10000102
           3 │ 10000003  10000028  10000053  10000078  10000103
           4 │ 10000004  10000029  10000054  10000079  10000104
           5 │ 10000005  10000030  10000055  10000080  10000105
           6 │ 10000006  10000031  10000056  10000081  10000106
           7 │ 10000007  10000032  10000057  10000082  10000107
           8 │ 10000008  10000033  10000058  10000083  10000108
           9 │ 10000009  10000034  10000059  10000084  10000109
          10 │ 10000010  10000035  10000060  10000085  10000110
          11 │ 10000011  10000036  10000061  10000086  10000111
          12 │ 10000012  10000037  10000062  10000087  10000112
          13 │ 10000013  10000038  10000063  10000088  10000113
          14 │ 10000014  10000039  10000064  10000089  10000114
          15 │ 10000015  10000040  10000065  10000090  10000115
          16 │ 10000016  10000041  10000066  10000091  10000116
          17 │ 10000017  10000042  10000067  10000092  10000117
          18 │ 10000018  10000043  10000068  10000093  10000118
          19 │ 10000019  10000044  10000069  10000094  10000119
          20 │ 10000020  10000045  10000070  10000095  10000120
          21 │ 10000021  10000046  10000071  10000096  10000121
          22 │ 10000022  10000047  10000072  10000097  10000122
          23 │ 10000023  10000048  10000073  10000098  10000123
          24 │ 10000024  10000049  10000074  10000099  10000124
          25 │ 10000025  10000050  10000075  10000100  10000125"""

    io = IOContext(IOBuffer(), :displaysize=>(11, 40), :limit=>true)
    show(io, df_big, allrows=true, allcols=false)
    str = String(take!(io.io))
    @test str == """
        25×5 DataFrame
         Row │ x1        x2        x3        x ⋯
             │ Int64     Int64     Int64     I ⋯
        ─────┼──────────────────────────────────
           1 │ 10000001  10000026  10000051  1 ⋯
           2 │ 10000002  10000027  10000052  1
           3 │ 10000003  10000028  10000053  1
           4 │ 10000004  10000029  10000054  1
           5 │ 10000005  10000030  10000055  1 ⋯
           6 │ 10000006  10000031  10000056  1
           7 │ 10000007  10000032  10000057  1
           8 │ 10000008  10000033  10000058  1
           9 │ 10000009  10000034  10000059  1 ⋯
          10 │ 10000010  10000035  10000060  1
          11 │ 10000011  10000036  10000061  1
          12 │ 10000012  10000037  10000062  1
          13 │ 10000013  10000038  10000063  1 ⋯
          14 │ 10000014  10000039  10000064  1
          15 │ 10000015  10000040  10000065  1
          16 │ 10000016  10000041  10000066  1
          17 │ 10000017  10000042  10000067  1 ⋯
          18 │ 10000018  10000043  10000068  1
          19 │ 10000019  10000044  10000069  1
          20 │ 10000020  10000045  10000070  1
          21 │ 10000021  10000046  10000071  1 ⋯
          22 │ 10000022  10000047  10000072  1
          23 │ 10000023  10000048  10000073  1
          24 │ 10000024  10000049  10000074  1
          25 │ 10000025  10000050  10000075  1 ⋯
                               2 columns omitted"""
end

@testset "GroupedDataFrame displaysize test" begin
    df = DataFrame(x = (1:50) .> 5, y = (1:50) .> 25, z = (1:50) .> 45)
    io = IOContext(IOBuffer(), :displaysize=>(30, 40), :limit=>true)

    show(io, groupby(df, :x), allcols=true)
    str = String(take!(io.io))
    @test str == """
        GroupedDataFrame with 2 groups based on key: x
        First Group (5 rows): x = false
         Row │ x      y      z
             │ Bool   Bool   Bool
        ─────┼─────────────────────
           1 │ false  false  false
           2 │ false  false  false
           3 │ false  false  false
           4 │ false  false  false
           5 │ false  false  false
        ⋮
        Last Group (45 rows): x = true
         Row │ x     y      z
             │ Bool  Bool   Bool
        ─────┼────────────────────
           1 │ true  false  false
           2 │ true  false  false
           3 │ true  false  false
           4 │ true  false  false
           5 │ true  false  false
          ⋮  │  ⋮      ⋮      ⋮
          41 │ true   true   true
          42 │ true   true   true
          43 │ true   true   true
          44 │ true   true   true
          45 │ true   true   true
                   35 rows omitted"""

    show(io, groupby(df, :y), allcols=true)
    str = String(take!(io.io))
    @test str == """
        GroupedDataFrame with 2 groups based on key: y
        First Group (25 rows): y = false
         Row │ x      y      z
             │ Bool   Bool   Bool
        ─────┼─────────────────────
           1 │ false  false  false
           2 │ false  false  false
           3 │ false  false  false
           4 │ false  false  false
          ⋮  │   ⋮      ⋮      ⋮
          23 │  true  false  false
          24 │  true  false  false
          25 │  true  false  false
                    18 rows omitted
        ⋮
        Last Group (25 rows): y = true
         Row │ x     y     z
             │ Bool  Bool  Bool
        ─────┼───────────────────
           1 │ true  true  false
           2 │ true  true  false
           3 │ true  true  false
          ⋮  │  ⋮     ⋮      ⋮
          23 │ true  true   true
          24 │ true  true   true
          25 │ true  true   true
                  19 rows omitted"""

    show(io, groupby(df, :z), allcols=true)
    str = String(take!(io.io))
    @test str == """
        GroupedDataFrame with 2 groups based on key: z
        First Group (45 rows): z = false
         Row │ x      y      z
             │ Bool   Bool   Bool
        ─────┼─────────────────────
           1 │ false  false  false
           2 │ false  false  false
           3 │ false  false  false
           4 │ false  false  false
           5 │ false  false  false
          ⋮  │   ⋮      ⋮      ⋮
          41 │  true   true  false
          42 │  true   true  false
          43 │  true   true  false
          44 │  true   true  false
          45 │  true   true  false
                    35 rows omitted
        ⋮
        Last Group (5 rows): z = true
         Row │ x     y     z
             │ Bool  Bool  Bool
        ─────┼──────────────────
           1 │ true  true  true
           2 │ true  true  true
           3 │ true  true  true
           4 │ true  true  true
           5 │ true  true  true"""

    show(io, groupby(df, :x), allcols=true, allrows=true)
    str = String(take!(io.io))
    @test str == """
        GroupedDataFrame with 2 groups based on key: x
        First Group (5 rows): x = false
         Row │ x      y      z
             │ Bool   Bool   Bool
        ─────┼─────────────────────
           1 │ false  false  false
           2 │ false  false  false
           3 │ false  false  false
           4 │ false  false  false
           5 │ false  false  false
        ⋮
        Last Group (45 rows): x = true
         Row │ x     y      z
             │ Bool  Bool   Bool
        ─────┼────────────────────
           1 │ true  false  false
           2 │ true  false  false
           3 │ true  false  false
           4 │ true  false  false
           5 │ true  false  false
           6 │ true  false  false
           7 │ true  false  false
           8 │ true  false  false
           9 │ true  false  false
          10 │ true  false  false
          11 │ true  false  false
          12 │ true  false  false
          13 │ true  false  false
          14 │ true  false  false
          15 │ true  false  false
          16 │ true  false  false
          17 │ true  false  false
          18 │ true  false  false
          19 │ true  false  false
          20 │ true  false  false
          21 │ true   true  false
          22 │ true   true  false
          23 │ true   true  false
          24 │ true   true  false
          25 │ true   true  false
          26 │ true   true  false
          27 │ true   true  false
          28 │ true   true  false
          29 │ true   true  false
          30 │ true   true  false
          31 │ true   true  false
          32 │ true   true  false
          33 │ true   true  false
          34 │ true   true  false
          35 │ true   true  false
          36 │ true   true  false
          37 │ true   true  false
          38 │ true   true  false
          39 │ true   true  false
          40 │ true   true  false
          41 │ true   true   true
          42 │ true   true   true
          43 │ true   true   true
          44 │ true   true   true
          45 │ true   true   true"""

    # height is zero or invalid -> print all rows
    for h in -1:0
        io = IOContext(IOBuffer(), :displaysize=>(h, 40), :limit=>true)
        show(io, groupby(df, :x), allcols=true)
        str_hrows = String(take!(io.io))
        show(io, groupby(df, :x), allcols=true, allrows=true)
        str_allrows = String(take!(io.io))
        @test str_hrows == str_allrows
    end

    # printed height always matches desired height, above a reasonable minimum
    for a in 1:50, b in 1:50, h in 15:40
        df = DataFrame(x = [fill(1, a); fill(2, b)])
        io = IOContext(IOBuffer(), :displaysize=>(h, 40), :limit=>true)
        show(io, groupby(df, :x), allcols=true)
        str = String(take!(io.io))
        nlines = length(split(str, '\n'))
        # leave one line for last REPL prompt at top, two for new prompt
        # (this is the same behavior as ungrouped data frames)
        desired = min(a + b + 10, h - 3)
        @test nlines == desired
    end

    for a in 1:50, h in 15:40
        df = DataFrame(x = fill(1, a))
        io = IOContext(IOBuffer(), :displaysize=>(h, 40), :limit=>true)
        show(io, groupby(df, :x), allcols=true)
        str = String(take!(io.io))
        nlines = length(split(str, '\n'))
        # leave one line for last REPL prompt at top, two for new prompt
        # (this is the same behavior as ungrouped data frames)
        desired = min(a + 5, h - 3)
        @test nlines == desired
    end

    # one group
    io = IOContext(IOBuffer(), :displaysize=>(15, 40), :limit=>true)
    df = DataFrame(x = Int64.(1:15), y = Int64(1))
    show(io, groupby(df, :y))
    str = String(take!(io.io))
    @test str == """
        GroupedDataFrame with 1 group based on key: y
        First Group (15 rows): y = 1
         Row │ x      y
             │ Int64  Int64
        ─────┼──────────────
           1 │     1      1
           2 │     2      1
           3 │     3      1
          ⋮  │   ⋮      ⋮
          14 │    14      1
          15 │    15      1
             10 rows omitted"""

    # zero groups
    io = IOContext(IOBuffer())
    df = DataFrame(x=[], y=Int[])
    show(io, groupby(df, :x))
    str = String(take!(io.io))
    @test str == "GroupedDataFrame with 0 groups based on key: x"
end

@testset "IOContext parameters test" begin
    df = DataFrame(A=Int64[1:4;], B=["x\"", "∀ε>0: x+ε>x", "z\$", "A\nC"],
                   C=Float32[1.0, 2.0, 3.0, 4.0])
    str1, size = capture_stdout() do
        show(df)
    end
    io = IOContext(IOBuffer(), :limit=>true, :displaysize=>size)
    show(io, df)
    str2 = String(take!(io.io))
    @test str1 == str2

    Random.seed!(1)
    df_big = DataFrame(rand(25, 5), :auto)
    str1, size = capture_stdout() do
        show(df_big)
    end
    io = IOContext(IOBuffer(), :limit=>true, :displaysize=>size)
    show(io, df_big)
    str2 = String(take!(io.io))
    @test str1 == str2
end

@testset "SubDataFrame show test" begin
    df = DataFrame(A=Int64[1:4;], B=["x\"", "∀ε>0: x+ε>x", "z\$", "A\nC"],
                   C=Float32[1.0, 2.0, 3.0, 4.0])
    subdf = view(df, [2, 3], :)
    io = IOBuffer()
    show(io, subdf, allrows=true, allcols=false)
    str = String(take!(io))
    @test str == """
        2×3 SubDataFrame
         Row │ A      B            C
             │ Int64  String       Float32
        ─────┼─────────────────────────────
           1 │     2  ∀ε>0: x+ε>x      2.0
           2 │     3  z\$               3.0"""
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
    # TODO: update when https://github.com/KristofferC/Crayons.jl/issues/47 is resolved
    if Base.get_have_color()
        df = DataFrame(Fish=["Suzy", "Amir"], Mass=[1.5, missing])
        @test sprint(show, df, context=:color=>true) == """
            \e[1m2×2 DataFrame\e[0m
            \e[1m Row \e[0m│\e[1m Fish   \e[0m\e[1m Mass      \e[0m
                 │\e[90m String \e[0m\e[90m Float64?  \e[0m
            ─────┼───────────────────
               1 │ Suzy          1.5
               2 │ Amir   \e[90m missing   \e[0m"""

        df = DataFrame(A=[:Symbol, missing, :missing],
                       B=[missing, "String", "missing"],
                       C=[:missing, "missing", missing])
        @test sprint(show, df, context=:color=>true) == """
            \e[1m3×3 DataFrame\e[0m
            \e[1m Row \e[0m│\e[1m A       \e[0m\e[1m B       \e[0m\e[1m C       \e[0m
                 │\e[90m Symbol? \e[0m\e[90m String? \e[0m\e[90m Any     \e[0m
            ─────┼───────────────────────────
               1 │ Symbol  \e[90m missing \e[0m missing
               2 │\e[90m missing \e[0m String   missing
               3 │ missing  missing \e[90m missing \e[0m"""
    end

    df_nothing = DataFrame(A=[1.0, 2.0, 3.0], B=["g", "g", nothing])
    @test sprint(show, df_nothing) == """
        3×2 DataFrame
         Row │ A        B
             │ Float64  Union…
        ─────┼─────────────────
           1 │     1.0  g
           2 │     2.0  g
           3 │     3.0"""
end

@testset "Test correct width computation" begin
    df = DataFrame([["a"]], [:x])
    @test sprint(show, df) == """
        1×1 DataFrame
         Row │ x
             │ String
        ─────┼────────
           1 │ a"""
end

@testset "Test showing special types" begin
    # strings with escapes
    df = DataFrame(a=["1\n1", "2\t2", "3\r3", "4\$4", "5\"5", "6\\6"])
    @test sprint(show, df) == """
        6×1 DataFrame
         Row │ a
             │ String
        ─────┼────────
           1 │ 1\\n1
           2 │ 2\\t2
           3 │ 3\\r3
           4 │ 4\$4
           5 │ 5"5
           6 │ 6\\\\6"""

    # categorical
    df = DataFrame(a=categorical([1, 2, 3]), b=categorical(["a", "b", missing]))
    @test sprint(show, df) == """
        3×2 DataFrame
         Row │ a     b
             │ Cat…  Cat…?
        ─────┼───────────────
           1 │ 1     a
           2 │ 2     b
           3 │ 3     missing"""

    # BigFloat
    df = DataFrame(a=[big(1.0), missing])
    @test sprint(show, df) == """
        2×1 DataFrame
         Row │ a
             │ BigFloat?
        ─────┼───────────
           1 │       1.0
           2 │ missing"""

    # date types
    df = DataFrame(a=Date(2020, 2, 11), b=DateTime(2020, 2, 11, 15), c=Day(1))
    @test sprint(show, df) == """
        1×3 DataFrame
         Row │ a           b                    c
             │ Date        DateTime             Day
        ─────┼────────────────────────────────────────
           1 │ 2020-02-11  2020-02-11T15:00:00  1 day"""

    # Irrational
    df = DataFrame(a=π)
    @test sprint(show, df) == """
        1×1 DataFrame
         Row │ a
             │ Irration…
        ─────┼───────────
           1 │         π"""
end

@testset "Test using :compact parameter of IOContext" begin
    df = DataFrame(x=[float(pi)])
    @test sprint(show, df) == """
        1×1 DataFrame
         Row │ x
             │ Float64
        ─────┼─────────
           1 │ 3.14159"""

    @test sprint(show, df, context=:compact=>false) == """
        1×1 DataFrame
         Row │ x
             │ Float64
        ─────┼───────────────────
           1 │ 3.141592653589793"""
end

@testset "Test of DataFrameRows and DataFrameColumns" begin
    df = DataFrame(x=[float(pi)])
    @test sprint(show, eachrow(df)) == """
        1×1 DataFrameRows
         Row │ x
             │ Float64
        ─────┼─────────
           1 │ 3.14159"""

    @test sprint((io, x) -> show(io, x, summary=false), eachrow(df)) == """
         Row │ x
             │ Float64
        ─────┼─────────
           1 │ 3.14159"""

    @test sprint(show, eachcol(df)) == """
        1×1 DataFrameColumns
         Row │ x
             │ Float64
        ─────┼─────────
           1 │ 3.14159"""

    @test sprint((io, x) -> show(io, x, summary=false), eachcol(df)) == """
         Row │ x
             │ Float64
        ─────┼─────────
           1 │ 3.14159"""
end

@testset "Test empty data frame and DataFrameRow" begin
    df = DataFrame(x=[float(pi)])
    @test sprint(show, df[:, 2:1]) == "0×0 DataFrame"
    @test sprint(show, @view df[:, 2:1]) == "0×0 SubDataFrame"
    @test sprint(show, df[1, 2:1]) == "DataFrameRow"
end

@testset "consistency" begin
    df = DataFrame(a=[1, 1, 2, 2], b=[5, 6, 7, 8], c=1:4)
    push!(df.c, 5)
    @test_throws AssertionError sprint(show, df)

    df = DataFrame(a=[1, 1, 2, 2], b=[5, 6, 7, 8], c=1:4)
    push!(DataFrames._columns(df), df[:, :a])
    @test_throws AssertionError sprint(show, df)
end

@testset "wide type name" begin
    @test sprint(show, DataFrame(a=⛵⛵⛵⛵⛵())) == """
        1×1 DataFrame
         Row │ a
             │ ⛵⛵⛵⛵…
        ─────┼───────────
           1 │ "⛵\""""

    @test sprint(show, DataFrame(a=categorical([Int64(2)^54]))) == """
        1×1 DataFrame
         Row │ a
             │ Cat…
        ─────┼───────────────────
           1 │ 18014398509481984"""

    @test sprint(show, DataFrame(a=categorical([Int64(2)^53]))) == """
        1×1 DataFrame
         Row │ a
             │ Cat…
        ─────┼──────────────────
           1 │ 9007199254740992"""

    @test sprint(show, DataFrame(a=categorical([Int64(2)^37]))) == """
        1×1 DataFrame
         Row │ a
             │ Cat…
        ─────┼──────────────
           1 │ 137438953472"""

    @test sprint(show, DataFrame(a=categorical([Int64(2)^36]))) == """
        1×1 DataFrame
         Row │ a
             │ Cat…
        ─────┼─────────────
           1 │ 68719476736"""

    @test sprint(show, DataFrame(a=Union{Function, Missing}[missing])) == """
        1×1 DataFrame
         Row │ a
             │ Function?
        ─────┼───────────
           1 │ missing"""
end

@testset "wide type name" begin
    df = DataFrame(A=Int32.(1:3), B=["x", "y", "z"])

    io = IOBuffer()
    show(io, df, eltypes=true)
    str = String(take!(io))
    @test str == """
        3×2 DataFrame
         Row │ A      B
             │ Int32  String
        ─────┼───────────────
           1 │     1  x
           2 │     2  y
           3 │     3  z"""

    io = IOBuffer()
    show(io, df, eltypes=false)
    str = String(take!(io))
    @test str == """
        3×2 DataFrame
         Row │ A  B
        ─────┼──────
           1 │ 1  x
           2 │ 2  y
           3 │ 3  z"""
end

@testset "UnionAll" begin
    df = DataFrame(x=AbstractVector[1:2])

    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
        1×1 DataFrame
         Row │ x
             │ Abstract…
        ─────┼───────────
           1 │ 1:2"""
end

@testset "wide output and column trimming" begin
    df = DataFrame(x="0123456789"^4)
    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
        1×1 DataFrame
         Row │ x
             │ String
        ─────┼───────────────────────────────────
           1 │ 01234567890123456789012345678901…"""

    io = IOContext(IOBuffer(), :displaysize=>(10, 10), :limit=>true)
    show(io, df)
    str = String(take!(io.io))
    @test str === """
        1×1 DataF…
         Row │ x ⋯
             │ S ⋯
        ─────┼────
           1 │ 0 ⋯
        1 column omitted"""

    df = DataFrame(x="😄"^20)
    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str === """
        1×1 DataFrame
         Row │ x
             │ String
        ─────┼───────────────────────────────────
           1 │ 😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄😄…"""
end

@testset "Floating point alignment" begin
    df = DataFrame(a=[i == 2 ? missing : 10^i for i = -7:1.0:7],
                   b=Int64.(1:1:15),
                   c=[i % 2 == 0 for i = 1:15],
                   d=[i == 2 ? "test" : 10^i for i = -7:1.0:7],
                   e=[i == 2 ? -0.0 : i == 3 ? +0.0 : 10^i for i = -7:1.0:7])

    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
        15×5 DataFrame
         Row │ a             b      c      d         e
             │ Float64?      Int64  Bool   Any       Float64
        ─────┼───────────────────────────────────────────────────
           1 │       1.0e-7      1  false  1.0e-7         1.0e-7
           2 │       1.0e-6      2   true  1.0e-6         1.0e-6
           3 │       1.0e-5      3  false  1.0e-5         1.0e-5
           4 │       0.0001      4   true  0.0001         0.0001
           5 │       0.001       5  false  0.001          0.001
           6 │       0.01        6   true  0.01           0.01
           7 │       0.1         7  false  0.1            0.1
           8 │       1.0         8   true  1.0            1.0
           9 │      10.0         9  false  10.0          10.0
          10 │ missing          10   true  test          -0.0
          11 │    1000.0        11  false  1000.0         0.0
          12 │   10000.0        12   true  10000.0    10000.0
          13 │  100000.0        13  false  100000.0  100000.0
          14 │       1.0e6      14   true  1.0e6          1.0e6
          15 │       1.0e7      15  false  1.0e7          1.0e7"""


    io = IOBuffer()
    show(io, df, eltypes = false)
    str = String(take!(io))
    @test str == """
        15×5 DataFrame
         Row │ a             b   c      d         e
        ─────┼────────────────────────────────────────────────
           1 │       1.0e-7   1  false  1.0e-7         1.0e-7
           2 │       1.0e-6   2   true  1.0e-6         1.0e-6
           3 │       1.0e-5   3  false  1.0e-5         1.0e-5
           4 │       0.0001   4   true  0.0001         0.0001
           5 │       0.001    5  false  0.001          0.001
           6 │       0.01     6   true  0.01           0.01
           7 │       0.1      7  false  0.1            0.1
           8 │       1.0      8   true  1.0            1.0
           9 │      10.0      9  false  10.0          10.0
          10 │ missing       10   true  test          -0.0
          11 │    1000.0     11  false  1000.0         0.0
          12 │   10000.0     12   true  10000.0    10000.0
          13 │  100000.0     13  false  100000.0  100000.0
          14 │       1.0e6   14   true  1.0e6          1.0e6
          15 │       1.0e7   15  false  1.0e7          1.0e7"""

    df = DataFrame(This_is_a_very_big_name=1.0, b=2.0, c=3.0)

    io = IOBuffer()
    show(io, df, eltypes = false)
    str = String(take!(io))
    @test str == """
        1×3 DataFrame
         Row │ This_is_a_very_big_name  b    c
        ─────┼───────────────────────────────────
           1 │                     1.0  2.0  3.0"""

    df = DataFrame(This_is_a_very_big_name=[10.0^i for i = -5:1:5],
                   This_is_smaller=1.0:2:22,
                   T=100001:1.0:100011)

    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
        11×3 DataFrame
         Row │ This_is_a_very_big_name  This_is_smaller  T
             │ Float64                  Float64          Float64
        ─────┼────────────────────────────────────────────────────
           1 │                  1.0e-5              1.0  100001.0
           2 │                  0.0001              3.0  100002.0
           3 │                  0.001               5.0  100003.0
           4 │                  0.01                7.0  100004.0
           5 │                  0.1                 9.0  100005.0
           6 │                  1.0                11.0  100006.0
           7 │                 10.0                13.0  100007.0
           8 │                100.0                15.0  100008.0
           9 │               1000.0                17.0  100009.0
          10 │              10000.0                19.0  100010.0
          11 │             100000.0                21.0  100011.0"""

    df = DataFrame(😄😄😄😄😄😄😄=[10.0^i for i = -5:1:5],
                   🚀🚀🚀🚀🚀=1.0:2:22)

    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
        11×2 DataFrame
         Row │ 😄😄😄😄😄😄😄  🚀🚀🚀🚀🚀
             │ Float64         Float64
        ─────┼────────────────────────────
           1 │         1.0e-5         1.0
           2 │         0.0001         3.0
           3 │         0.001          5.0
           4 │         0.01           7.0
           5 │         0.1            9.0
           6 │         1.0           11.0
           7 │        10.0           13.0
           8 │       100.0           15.0
           9 │      1000.0           17.0
          10 │     10000.0           19.0
          11 │    100000.0           21.0"""

    df = DataFrame(😄😄😄😄😄😄😄😄😄=[10.0^i + pi for i = -5:1:5],
                   🚀🚀🚀🚀🚀🚀🚀=collect(1.0:2:22) .+ pi)

    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
        11×2 DataFrame
         Row │ 😄😄😄😄😄😄😄😄😄  🚀🚀🚀🚀🚀🚀🚀
             │ Float64             Float64
        ─────┼────────────────────────────────────
           1 │          3.1416            4.14159
           2 │          3.14169           6.14159
           3 │          3.14259           8.14159
           4 │          3.15159          10.1416
           5 │          3.24159          12.1416
           6 │          4.14159          14.1416
           7 │         13.1416           16.1416
           8 │        103.142            18.1416
           9 │       1003.14             20.1416
          10 │      10003.1              22.1416
          11 │          1.00003e5        24.1416"""

    io = IOContext(IOBuffer(), :compact => false)
    show(io, df)
    str = String(take!(io.io))
    @test str == """
        11×2 DataFrame
         Row │ 😄😄😄😄😄😄😄😄😄       🚀🚀🚀🚀🚀🚀🚀
             │ Float64                  Float64
        ─────┼─────────────────────────────────────────────
           1 │      3.141602653589793    4.141592653589793
           2 │      3.1416926535897933   6.141592653589793
           3 │      3.142592653589793    8.141592653589793
           4 │      3.151592653589793   10.141592653589793
           5 │      3.241592653589793   12.141592653589793
           6 │      4.141592653589793   14.141592653589793
           7 │     13.141592653589793   16.141592653589793
           8 │    103.1415926535898     18.141592653589793
           9 │   1003.1415926535898     20.141592653589793
          10 │  10003.14159265359       22.141592653589793
          11 │ 100003.14159265358       24.141592653589793"""

    df = DataFrame(This_is_a_very_long_header =
                   Union{F, Float64, Missing}[i == 6 ? F(1234567) : i == 4 ? missing : 10.0^i for i = -6:1:6])

    io = IOContext(IOBuffer())
    show(io, df)
    str = String(take!(io.io))
    @test str == """
        13×1 DataFrame
         Row │ This_is_a_very_long_header
             │ Union{Missing, Float64, F}
        ─────┼────────────────────────────
           1 │                     1.0e-6
           2 │                     1.0e-5
           3 │                     0.0001
           4 │                     0.001
           5 │                     0.01
           6 │                     0.1
           7 │                     1.0
           8 │                    10.0
           9 │                   100.0
          10 │                  1000.0
          11 │               missing
          12 │                100000.0
          13 │               1234567"""

end

@testset "Complex number alignment" begin
    a = Union{Missing, Number}[
        im,
        1 + im,
        -1 - im,
        1.123 + 123im,
        -2.323 - 32im,
        -1//3 - 4//5im,
        1028.23123,
        missing,
        -0.304105,
        2123123,
        +0.304105,
        1.3123e-10 + 1.123e-5im
    ]
    b = Int64[i ≤ 6 ? -1 * 10^(i - 1) : 10^(i - 7) for i = 1:12]
    c = Union{Missing, Float64}[i == 5 ? missing : 10.0^(i-4) for i = 1:12]
    d = ComplexF64[(-1)^(i % 3 == 0) * 10.0^(i-6) + (-1)^i * (10.0)^(11-i-6) * im for i = 1:12]
    df = DataFrame(very_big_column_name_1=a,
                   very_big_column_name_2=b,
                   very_big_column_name_3=c,
                   very_big_column_name_4=d)

    io = IOContext(IOBuffer())
    show(io, df)
    str = String(take!(io.io))

    @test str == """
        12×4 DataFrame
         Row │ very_big_column_name_1  very_big_column_name_2  very_big_column_name_3  very_big_column_name_4
             │ Union{Missing, Number}  Int64                   Float64?                $(Complex{Float64})
        ─────┼────────────────────────────────────────────────────────────────────────────────────────────────
           1 │                     im                      -1                   0.001        1.0e-5-10000.0im
           2 │                  1+1im                     -10                   0.01         0.0001+1000.0im
           3 │                 -1-1im                    -100                   0.1          -0.001-100.0im
           4 │          1.123+123.0im                   -1000                   1.0            0.01+10.0im
           5 │          -2.323-32.0im                  -10000             missing               0.1-1.0im
           6 │          -1//3+4//5*im                 -100000                 100.0            -1.0+0.1im
           7 │                1028.23                       1                1000.0            10.0-0.01im
           8 │                missing                      10               10000.0           100.0+0.001im
           9 │              -0.304105                     100              100000.0         -1000.0-0.0001im
          10 │                2123123                    1000                   1.0e6       10000.0+1.0e-5im
          11 │               0.304105                   10000                   1.0e7      100000.0-1.0e-6im
          12 │  1.3123e-10+1.123e-5im                  100000                   1.0e8        -1.0e6+1.0e-7im"""
end

@testset "Invalid keywords in text mode" begin
    df = DataFrame(a=[1, 1, 2, 2], b=[5, 6, 7, 8], c=1:4)
    @test_throws ArgumentError show(stdout, df, max_column_width="100px")
    @test_throws ArgumentError show(stdout, MIME("text/plain"), df, max_column_width="100px")
end

@testset "Issue #2673 - Vertical line when not showing row numbers" begin
    df = DataFrame(a=Int64[10, 20], b=Int64[30, 40], c=Int64[50, 60])

    io = IOContext(IOBuffer())
    show(io, df)
    str = String(take!(io.io))
    @test str == """
        2×3 DataFrame
         Row │ a      b      c
             │ Int64  Int64  Int64
        ─────┼─────────────────────
           1 │    10     30     50
           2 │    20     40     60"""


    io = IOContext(IOBuffer())
    show(io, df, show_row_number=false)
    str = String(take!(io.io))
    @test str == """
        2×3 DataFrame
         a      b      c
         Int64  Int64  Int64
        ─────────────────────
            10     30     50
            20     40     60"""

    io = IOContext(IOBuffer())
    show(io, df[2, :])
    str = String(take!(io.io))
    @test str == """
        DataFrameRow
         Row │ a      b      c
             │ Int64  Int64  Int64
        ─────┼─────────────────────
           2 │    20     40     60"""


    io = IOContext(IOBuffer())
    show(io, df[2, :], show_row_number=false)
    str = String(take!(io.io))
    @test str == """
        DataFrameRow
         a      b      c
         Int64  Int64  Int64
        ─────────────────────
            20     40     60"""
end

@testset "Issue #3346 - Showing Base.UUID" begin
    rand_uuid = Base.UUID("36cea533-4597-132e-557c-08487c42ef97")
    df = DataFrame(
        integer = [Int64(10)^n for n in 1:10],
        uuid    = [i % 2 == 0 ? missing : rand_uuid for i in 1:10],
        float   = [10.0^n for n in 1:10], string = ["A"^100 for _ in 1:10]
    )
    io = IOContext(IOBuffer())
    show(io, df)
    str = String(take!(io.io))

    @test str == """
        10×4 DataFrame
         Row │ integer      uuid                                  float        string
             │ Int64        Base.UUID?                            Float64      String
        ─────┼───────────────────────────────────────────────────────────────────────────────────────────────────
           1 │          10  36cea533-4597-132e-557c-08487c42ef97      10.0     AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…
           2 │         100  missing                                  100.0     AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…
           3 │        1000  36cea533-4597-132e-557c-08487c42ef97    1000.0     AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…
           4 │       10000  missing                                10000.0     AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…
           5 │      100000  36cea533-4597-132e-557c-08487c42ef97  100000.0     AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…
           6 │     1000000  missing                                    1.0e6   AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…
           7 │    10000000  36cea533-4597-132e-557c-08487c42ef97       1.0e7   AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…
           8 │   100000000  missing                                    1.0e8   AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…
           9 │  1000000000  36cea533-4597-132e-557c-08487c42ef97       1.0e9   AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…
          10 │ 10000000000  missing                                    1.0e10  AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA…"""
end

@testset "cover all corner cases of compacttype" begin
    df = DataFrame(x2345678901234567=categorical(["1"]))
    @test sprint(show, df) === """
        1×1 DataFrame
         Row │ x2345678901234567
             │ CategoricalValue…
        ─────┼───────────────────
           1 │ 1"""
end

@testset "InlineStrings with GroupedDataFrame" begin
    df = DataFrame(id=inlinestrings(["a", "b", "c"]), value=collect(Int64, 1:3))

    io = IOContext(IOBuffer(), :limit=>true)
    show(io, groupby(df, :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 3 groups based on key: id
        First Group (1 row): id = "a"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ a            1
        ⋮
        Last Group (1 row): id = "c"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ c            3"""

    io = IOContext(IOBuffer(), :limit=>true)
    show(io, MIME("text/plain"), groupby(df, :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 3 groups based on key: id
        First Group (1 row): id = "a"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ a            1
        ⋮
        Last Group (1 row): id = "c"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ c            3"""

    io = IOContext(IOBuffer(), :limit=>false)
    show(io, groupby(df, :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 3 groups based on key: id
        Group 1 (1 row): id = "a"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ a            1
        Group 2 (1 row): id = "b"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ b            2
        Group 3 (1 row): id = "c"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ c            3"""


    io = IOContext(IOBuffer(), :limit=>false)
    show(io, MIME("text/plain"), groupby(df, :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 3 groups based on key: id
        Group 1 (1 row): id = "a"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ a            1
        Group 2 (1 row): id = "b"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ b            2
        Group 3 (1 row): id = "c"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ c            3"""

    io = IOContext(IOBuffer(), :limit=>true)
    show(io, groupby(df[1:1, :], :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 1 group based on key: id
        First Group (1 row): id = "a"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ a            1"""

    io = IOContext(IOBuffer(), :limit=>false)
    show(io, groupby(df[1:1, :], :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 1 group based on key: id
        Group 1 (1 row): id = "a"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ a            1"""

    io = IOContext(IOBuffer(), :limit=>true)
    show(io, MIME("text/plain"), groupby(df[1:1, :], :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 1 group based on key: id
        First Group (1 row): id = "a"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ a            1"""

    io = IOContext(IOBuffer(), :limit=>false)
    show(io, MIME("text/plain"), groupby(df[1:1, :], :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 1 group based on key: id
        Group 1 (1 row): id = "a"
         Row │ id       value
             │ String1  Int64
        ─────┼────────────────
           1 │ a            1"""

    io = IOContext(IOBuffer())
    show(io, MIME("text/html"), groupby(df, :id))
    @test String(take!(io.io)) === "<p><b>GroupedDataFrame with 3 groups based on key: id</b></p>" *
        "<div><div style = \"float: left;\"><span>First Group (1 row): id = &quot;a&quot;</span></div>" *
        "<div style = \"clear: both;\"></div></div>" *
        "<div class = \"data-frame\" style = \"overflow-x: scroll;\">" *
        "<table class = \"data-frame\" style = \"margin-bottom: 6px;\">" *
        "<thead><tr class = \"header\"><th class = \"rowNumber\" " *
        "style = \"font-weight: bold; text-align: right;\">Row</th>" *
        "<th style = \"text-align: left;\">id</th><th style = \"text-align: left;\">value</th></tr>" *
        "<tr class = \"subheader headerLastRow\"><th class = \"rowNumber\" " *
        "style = \"font-weight: bold; text-align: right;\"></th>" *
        "<th title = \"String1\" style = \"text-align: left;\">String1</th>" *
        "<th title = \"Int64\" style = \"text-align: left;\">Int64</th></tr></thead>" *
        "<tbody><tr><td class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">1</td>" *
        "<td style = \"text-align: left;\">a</td><td style = \"text-align: right;\">1</td></tr></tbody>" *
        "</table></div><p>&vellip;</p><div><div style = \"float: left;\">" *
        "<span>Last Group (1 row): id = &quot;c&quot;</span></div>" *
        "<div style = \"clear: both;\"></div></div><div class = \"data-frame\" " *
        "style = \"overflow-x: scroll;\"><table class = \"data-frame\" " *
        "style = \"margin-bottom: 6px;\"><thead><tr class = \"header\">" *
        "<th class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">Row</th>" *
        "<th style = \"text-align: left;\">id</th><th style = \"text-align: left;\">value</th></tr>" *
        "<tr class = \"subheader headerLastRow\"><th class = \"rowNumber\" " *
        "style = \"font-weight: bold; text-align: right;\"></th>" *
        "<th title = \"String1\" style = \"text-align: left;\">String1</th>" *
        "<th title = \"Int64\" style = \"text-align: left;\">Int64</th></tr></thead>" *
        "<tbody><tr><td class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">1</td>" *
        "<td style = \"text-align: left;\">c</td><td style = \"text-align: right;\">3</td></tr></tbody></table></div>"

    io = IOContext(IOBuffer())
    show(io, MIME("text/html"), groupby(df[1:1, :], :id))
    @test String(take!(io.io)) === "<p><b>GroupedDataFrame with 1 group based on key: id</b></p><div>" *
        "<div style = \"float: left;\"><span>First Group (1 row): id = &quot;a&quot;</span></div>" *
        "<div style = \"clear: both;\"></div></div><div class = \"data-frame\" " *
        "style = \"overflow-x: scroll;\"><table class = \"data-frame\" " *
        "style = \"margin-bottom: 6px;\"><thead><tr class = \"header\">" *
        "<th class = \"rowNumber\" style = \"font-weight: bold; text-align: right;\">Row</th>" *
        "<th style = \"text-align: left;\">id</th><th style = \"text-align: left;\">value</th></tr>" *
        "<tr class = \"subheader headerLastRow\"><th class = \"rowNumber\" " *
        "style = \"font-weight: bold; text-align: right;\"></th>" *
        "<th title = \"String1\" style = \"text-align: left;\">String1</th>" *
        "<th title = \"Int64\" style = \"text-align: left;\">Int64</th></tr></thead>" *
        "<tbody><tr><td class = \"rowNumber\" style = \"font-weight: bold; " *
        "text-align: right;\">1</td><td style = \"text-align: left;\">a</td>" *
        "<td style = \"text-align: right;\">1</td></tr></tbody></table></div>"

    io = IOContext(IOBuffer())
    show(io, MIME("text/latex"), groupby(df, :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 3 groups based on key: id

        First Group (1 row): id = "a"

        \\begin{tabular}{r|cc}
        \t& id & value\\\\
        \t\\hline
        \t& String1 & Int64\\\\
        \t\\hline
        \t1 & a & 1 \\\\
        \\end{tabular}

        \$\\dots\$

        Last Group (1 row): id = "c"

        \\begin{tabular}{r|cc}
        \t& id & value\\\\
        \t\\hline
        \t& String1 & Int64\\\\
        \t\\hline
        \t1 & c & 3 \\\\
        \\end{tabular}
        """

    io = IOContext(IOBuffer())
    show(io, MIME("text/latex"), groupby(df[1:1, :], :id))
    @test String(take!(io.io)) === """
        GroupedDataFrame with 1 group based on key: id

        First Group (1 row): id = "a"

        \\begin{tabular}{r|cc}
        \t& id & value\\\\
        \t\\hline
        \t& String1 & Int64\\\\
        \t\\hline
        \t1 & a & 1 \\\\
        \\end{tabular}
        """
end

end # module
