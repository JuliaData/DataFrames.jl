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
    df_big = DataFrame(rand(Int64(10000000):Int64(20000000), 25,5))

    io = IOContext(IOBuffer(), :displaysize=>(11,40), :limit=>true)
    show(io, df_big)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame. Omitted printing of 2 columns
    │ Row │ x1       │ x2       │ x3       │
    │     │ Int64    │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 11569985 │ 12178109 │ 17315979 │
    ⋮
    │ 24  │ 14660095 │ 13529407 │ 19204569 │
    │ 25  │ 16992761 │ 15379139 │ 13043102 │"""

    io = IOContext(IOBuffer(), :displaysize=>(11,40), :limit=>true)
    show(io, df_big, allcols=true)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame
    │ Row │ x1       │ x2       │ x3       │
    │     │ Int64    │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 11569985 │ 12178109 │ 17315979 │
    ⋮
    │ 24  │ 14660095 │ 13529407 │ 19204569 │
    │ 25  │ 16992761 │ 15379139 │ 13043102 │

    │ Row │ x4       │ x5       │
    │     │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┤
    │ 1   │ 10701540 │ 17870314 │
    ⋮
    │ 24  │ 19137290 │ 16313933 │
    │ 25  │ 12314843 │ 17754964 │"""

    io = IOContext(IOBuffer(), :displaysize=>(11,40), :limit=>true)
    show(io, df_big, allrows=true, allcols=true)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame
    │ Row │ x1       │ x2       │ x3       │
    │     │ Int64    │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 11569985 │ 12178109 │ 17315979 │
    │ 2   │ 19192686 │ 15027856 │ 16050701 │
    │ 3   │ 11140411 │ 13293718 │ 12677906 │
    │ 4   │ 11985459 │ 15660106 │ 13057018 │
    │ 5   │ 19201756 │ 11621062 │ 12785196 │
    │ 6   │ 18146532 │ 15031398 │ 17264273 │
    │ 7   │ 13797788 │ 12442579 │ 14445511 │
    │ 8   │ 10315853 │ 15868969 │ 17213493 │
    │ 9   │ 12916299 │ 18460443 │ 12300445 │
    │ 10  │ 13157897 │ 10639979 │ 10106305 │
    │ 11  │ 16871306 │ 17775325 │ 12544912 │
    │ 12  │ 15251225 │ 10795979 │ 17976274 │
    │ 13  │ 13028558 │ 12220954 │ 17347524 │
    │ 14  │ 13057734 │ 17805919 │ 18183984 │
    │ 15  │ 16120716 │ 12095491 │ 12250720 │
    │ 16  │ 17157319 │ 12697043 │ 11145594 │
    │ 17  │ 19264603 │ 16981136 │ 12984027 │
    │ 18  │ 11292293 │ 19779844 │ 18722894 │
    │ 19  │ 12360733 │ 13117985 │ 11836582 │
    │ 20  │ 13975865 │ 14529221 │ 18458601 │
    │ 21  │ 11416596 │ 17150526 │ 14731764 │
    │ 22  │ 19587351 │ 15978326 │ 17757430 │
    │ 23  │ 10802633 │ 17029758 │ 12105159 │
    │ 24  │ 14660095 │ 13529407 │ 19204569 │
    │ 25  │ 16992761 │ 15379139 │ 13043102 │

    │ Row │ x4       │ x5       │
    │     │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┤
    │ 1   │ 10701540 │ 17870314 │
    │ 2   │ 15507419 │ 16954480 │
    │ 3   │ 16317941 │ 10996749 │
    │ 4   │ 16740306 │ 18240586 │
    │ 5   │ 14628017 │ 14818074 │
    │ 6   │ 14239854 │ 12226254 │
    │ 7   │ 16972931 │ 17422692 │
    │ 8   │ 16291319 │ 18748371 │
    │ 9   │ 11568636 │ 19668632 │
    │ 10  │ 12073431 │ 11550526 │
    │ 11  │ 14037727 │ 19458202 │
    │ 12  │ 12911112 │ 17083732 │
    │ 13  │ 11111638 │ 18826082 │
    │ 14  │ 14534511 │ 18412780 │
    │ 15  │ 19921217 │ 12593752 │
    │ 16  │ 12449162 │ 19192439 │
    │ 17  │ 11091987 │ 17326521 │
    │ 18  │ 16884903 │ 19680264 │
    │ 19  │ 18348781 │ 16089361 │
    │ 20  │ 10189747 │ 13978329 │
    │ 21  │ 19285347 │ 11039190 │
    │ 22  │ 14664380 │ 17880065 │
    │ 23  │ 18398749 │ 12751655 │
    │ 24  │ 19137290 │ 16313933 │
    │ 25  │ 12314843 │ 17754964 │"""

    io = IOContext(IOBuffer(), :displaysize=>(11,40), :limit=>true)
    show(io, df_big, allrows=true, allcols=false)
    str = String(take!(io.io))
    @test str == """
    25×5 DataFrame. Omitted printing of 2 columns
    │ Row │ x1       │ x2       │ x3       │
    │     │ Int64    │ Int64    │ Int64    │
    ├─────┼──────────┼──────────┼──────────┤
    │ 1   │ 11569985 │ 12178109 │ 17315979 │
    │ 2   │ 19192686 │ 15027856 │ 16050701 │
    │ 3   │ 11140411 │ 13293718 │ 12677906 │
    │ 4   │ 11985459 │ 15660106 │ 13057018 │
    │ 5   │ 19201756 │ 11621062 │ 12785196 │
    │ 6   │ 18146532 │ 15031398 │ 17264273 │
    │ 7   │ 13797788 │ 12442579 │ 14445511 │
    │ 8   │ 10315853 │ 15868969 │ 17213493 │
    │ 9   │ 12916299 │ 18460443 │ 12300445 │
    │ 10  │ 13157897 │ 10639979 │ 10106305 │
    │ 11  │ 16871306 │ 17775325 │ 12544912 │
    │ 12  │ 15251225 │ 10795979 │ 17976274 │
    │ 13  │ 13028558 │ 12220954 │ 17347524 │
    │ 14  │ 13057734 │ 17805919 │ 18183984 │
    │ 15  │ 16120716 │ 12095491 │ 12250720 │
    │ 16  │ 17157319 │ 12697043 │ 11145594 │
    │ 17  │ 19264603 │ 16981136 │ 12984027 │
    │ 18  │ 11292293 │ 19779844 │ 18722894 │
    │ 19  │ 12360733 │ 13117985 │ 11836582 │
    │ 20  │ 13975865 │ 14529221 │ 18458601 │
    │ 21  │ 11416596 │ 17150526 │ 14731764 │
    │ 22  │ 19587351 │ 15978326 │ 17757430 │
    │ 23  │ 10802633 │ 17029758 │ 12105159 │
    │ 24  │ 14660095 │ 13529407 │ 19204569 │
    │ 25  │ 16992761 │ 15379139 │ 13043102 │"""
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
        1×1 DataFrameColumns (with names=false)
        │ Row │ x       │
        │     │ Float64 │
        ├─────┼─────────┤
        │ 1   │ 3.14159 │"""

    @test sprint((io, x) -> show(io, x, summary=false), eachcol(df)) == """

        │ Row │ x       │
        │     │ Float64 │
        ├─────┼─────────┤
        │ 1   │ 3.14159 │"""

    @test sprint(show, eachcol(df, true)) == """
        1×1 DataFrameColumns (with names=true)
        │ Row │ x       │
        │     │ Float64 │
        ├─────┼─────────┤
        │ 1   │ 3.14159 │"""

    @test sprint((io, x) -> show(io, x, summary=false), eachcol(df, true)) == """

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

end # module
