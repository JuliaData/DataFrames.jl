module TestIO

using Test, DataFrames, CategoricalArrays, Dates

# Test LaTeX export
@testset "LaTeX export" begin
    df = DataFrame(A = 1:4,
                B = ["\$10.0", "M&F", "A~B", "\\alpha"],
                C = ["A", "B", "C", "S"],
                D = [1.0, 2.0, missing, 3.0],
                E = CategoricalArray(["a", missing, "c", "d"]),
                F = Vector{String}(undef, 4)
                )
    str = """
        \\begin{tabular}{r|cccccc}
        \t& A & B & C & D & E & F\\\\
        \t\\hline
        \t& $(Int) & String & String & Float64? & Cat…? & String\\\\
        \t\\hline
        \t1 & 1 & \\\$10.0 & A & 1.0 & a & \\emph{\\#undef} \\\\
        \t2 & 2 & M\\&F & B & 2.0 & \\emph{missing} & \\emph{\\#undef} \\\\
        \t3 & 3 & A\\textasciitilde{}B & C & \\emph{missing} & c & \\emph{\\#undef} \\\\
        \t4 & 4 & \\textbackslash{}\\textbackslash{}alpha & S & 3.0 & d & \\emph{\\#undef} \\\\
        \\end{tabular}
        """
    @test repr(MIME("text/latex"), df) == str
    @test repr(MIME("text/latex"), eachcol(df)) == str
    @test repr(MIME("text/latex"), eachrow(df)) == str

    @test_throws ArgumentError DataFrames._show(stdout, MIME("text/latex"),
                                                DataFrame(ones(2,2)), rowid=10)
end

@testset "Huge LaTeX export" begin
    df = DataFrame(a=1:1000)
    ioc = IOContext(IOBuffer(), :displaysize => (10, 10), :limit => false)
    show(ioc, "text/latex", df)
    @test length(String(take!(ioc.io))) > 10000

    ioc = IOContext(IOBuffer(), :displaysize => (10, 10), :limit => true)
    show(ioc, "text/latex", df)
    @test length(String(take!(ioc.io))) < 10000
end

#Test HTML output for IJulia and similar
@testset "HTML output" begin
    df = DataFrame(Fish = ["Suzy", "Amir"], Mass = [1.5, missing])
    io = IOBuffer()
    show(io, "text/html", df)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th>String</th><th>Float64?</th></tr></thead><tbody>" *
                 "<p>2 rows × 2 columns</p>" *
                 "<tr><th>1</th><td>Suzy</td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td>Amir</td><td><em>missing</em></td></tr></tbody></table>"

    df = DataFrame(Fish = Vector{String}(undef, 2), Mass = [1.5, missing])
    io = IOBuffer()
    show(io, "text/html", df)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th>String</th><th>Float64?</th></tr></thead><tbody>" *
                 "<p>2 rows × 2 columns</p>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table>"

    io = IOBuffer()
    show(io, "text/html", eachrow(df))
    str = String(take!(io))
    @test str == "<p>2×2 DataFrameRows</p>" *
                 "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th>String</th><th>Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table>"

    io = IOBuffer()
    show(io, "text/html", eachcol(df))
    str = String(take!(io))
    @test str == "<p>2×2 DataFrameColumns</p>" *
                 "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th>String</th><th>Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table>"

    io = IOBuffer()
    show(io, "text/html", df[1, :])
    str = String(take!(io))
    @test str == "<p>DataFrameRow (2 columns)</p><table class=\"data-frame\">" *
                 "<thead><tr><th></th><th>Fish</th><th>Mass</th></tr><tr><th></th>" *
                 "<th>String</th><th>Float64?</th></tr></thead><tbody><tr><th>1</th>" *
                 "<td><em>#undef</em></td><td>1.5</td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME"text/html"(), df, summary=false)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th>String</th><th>Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME"text/html"(), eachrow(df), summary=false)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th>String</th><th>Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME"text/html"(), eachcol(df), summary=false)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th>String</th><th>Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME"text/html"(), df[1, :], summary=false)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><thead><tr><th></th><th>Fish</th>" *
                 "<th>Mass</th></tr><tr><th></th><th>String</th><th>Float64?</th></tr></thead>" *
                 "<tbody><tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr></tbody></table>"

    @test_throws ArgumentError DataFrames._show(stdout, MIME("text/html"),
                                                DataFrame(ones(2,2)), rowid=10)
end

# test limit attribute of IOContext is used
@testset "limit attribute" begin
    df = DataFrame(a=1:1000)
    ioc = IOContext(IOBuffer(), :displaysize => (10, 10), :limit => false)
    show(ioc, "text/html", df)
    @test length(String(take!(ioc.io))) > 10000

    ioc = IOContext(IOBuffer(), :displaysize => (10, 10), :limit => true)
    show(ioc, "text/html", df)
    @test length(String(take!(ioc.io))) < 10000
end

@testset "printtable" begin
    df = DataFrame(A = 1:3,
                B = 'a':'c',
                C = ["A", "B", "C"],
                D = CategoricalArray(string.('a':'c')),
                E = CategoricalArray(["A", "B", missing]),
                F = Vector{Union{Int, Missing}}(1:3),
                G = missings(3),
                H = fill(missing, 3))

    @test sprint(DataFrames.printtable, df) ==
        """
        "A","B","C","D","E","F","G","H"
        1,"'a'","A","a","A","1",missing,missing
        2,"'b'","B","b","B","2",missing,missing
        3,"'c'","C","c",missing,"3",missing,missing
        """
end

@testset "csv/tsv output" begin
    df = DataFrame(a = [1,2], b = [1.0, 2.0])

    for x in [df, eachcol(df), eachrow(df)]
        @test sprint(show, "text/csv", x) == """
        "a","b"
        1,1.0
        2,2.0
        """
        @test sprint(show, "text/tab-separated-values", x) == """
        "a"\t"b"
        1\t1.0
        2\t2.0
        """
    end
end

@testset "empty data frame and DataFrameRow" begin
    df = DataFrame(a = [1,2], b = [1.0, 2.0])

    @test sprint(show, "text/csv", df[:, 2:1]) == ""
    @test sprint(show, "text/tab-separated-values", df[:, 2:1]) == ""
    @test sprint(show, "text/html", df[:, 2:1]) ==
          "<table class=\"data-frame\"><thead><tr><th></th></tr><tr><th></th></tr>" *
          "</thead><tbody><p>0 rows × 0 columns</p></tbody></table>"
    @test sprint(show, "text/latex", df[:, 2:1]) ==
          "\\begin{tabular}{r|}\n\t& \\\\\n\t\\hline\n\t& \\\\\n\t\\hline\n\\end{tabular}\n"

    @test sprint(show, "text/csv", @view df[:, 2:1]) == ""
    @test sprint(show, "text/tab-separated-values", @view df[:, 2:1]) == ""
    @test sprint(show, "text/html", @view df[:, 2:1]) ==
          "<table class=\"data-frame\"><thead><tr><th></th></tr><tr><th></th></tr>" *
          "</thead><tbody><p>0 rows × 0 columns</p></tbody></table>"
    @test sprint(show, "text/latex", @view df[:, 2:1]) ==
          "\\begin{tabular}{r|}\n\t& \\\\\n\t\\hline\n\t& \\\\\n\t\\hline\n\\end{tabular}\n"

    @test sprint(show, "text/csv", df[1, 2:1]) == ""
    @test sprint(show, "text/tab-separated-values", df[1, 2:1]) == ""
    @test sprint(show, "text/html", df[1, 2:1]) ==
          "<p>DataFrameRow (0 columns)</p><table class=\"data-frame\">" *
          "<thead><tr><th></th></tr><tr><th></th></tr></thead><tbody></tbody></table>"
    @test sprint(show, "text/latex", df[1, 2:1]) ==
          "\\begin{tabular}{r|}\n\t& \\\\\n\t\\hline\n\t& \\\\\n\t\\hline\n\\end{tabular}\n"
end

@testset "consistency" begin
    df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(df.c, 5)
    @test_throws AssertionError sprint(show, "text/html", df)
    @test_throws AssertionError sprint(show, "text/latex", df)
    @test_throws AssertionError sprint(show, "text/csv", df)
    @test_throws AssertionError sprint(show, "text/tab-separated-values", df)

    df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8], c = 1:4)
    push!(DataFrames._columns(df), df[:, :a])
    @test_throws AssertionError sprint(show, "text/html", df)
    @test_throws AssertionError sprint(show, "text/latex", df)
    @test_throws AssertionError sprint(show, "text/csv", df)
    @test_throws AssertionError sprint(show, "text/tab-separated-values", df)
end

@testset "summary tests" begin
    df = DataFrame(ones(2,3))

    for (v, s) in [(df, "2×3 DataFrame"),
                   (view(df, :, :), "2×3 SubDataFrame"),
                   (df[1, :], "3-element DataFrameRow"),
                   (DataFrames.index(df), "data frame with 3 columns"),
                   (DataFrames.index(df[1:1, 1:1]), "data frame with 1 column"),
                   (DataFrames.index(view(df, 1:1, 1:1)), "data frame with 1 column"),
                   (eachrow(df), "2-element DataFrameRows"),
                   (eachcol(df), "3-element DataFrameColumns")]
        @test summary(v) == s
        io = IOBuffer()
        summary(io, v)
        @test String(take!(io)) == s
    end
end

@testset "eltypes tests" begin
    df = DataFrame(A = Int32.(1:3), B = ["x", "y", "z"])

    io = IOBuffer()
    show(io, MIME("text/plain"), df, eltypes=true)
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
    show(io, MIME("text/plain"), eachcol(df), eltypes=true)
    str = String(take!(io))
    @test str == """
    3×2 DataFrameColumns
    │ Row │ A     │ B      │
    │     │ Int32 │ String │
    ├─────┼───────┼────────┤
    │ 1   │ 1     │ x      │
    │ 2   │ 2     │ y      │
    │ 3   │ 3     │ z      │"""

    io = IOBuffer()
    show(io, MIME("text/plain"), eachrow(df), eltypes=true)
    str = String(take!(io))
    @test str == """
    3×2 DataFrameRows
    │ Row │ A     │ B      │
    │     │ Int32 │ String │
    ├─────┼───────┼────────┤
    │ 1   │ 1     │ x      │
    │ 2   │ 2     │ y      │
    │ 3   │ 3     │ z      │"""

    io = IOBuffer()
    show(io, MIME("text/plain"), df, eltypes=false)
    str = String(take!(io))
    @test str == """
    3×2 DataFrame
    │ Row │ A │ B │
    ├─────┼───┼───┤
    │ 1   │ 1 │ x │
    │ 2   │ 2 │ y │
    │ 3   │ 3 │ z │"""

    io = IOBuffer()
    show(io, MIME("text/plain"), eachcol(df), eltypes=false)
    str = String(take!(io))
    @test str == """
    3×2 DataFrameColumns
    │ Row │ A │ B │
    ├─────┼───┼───┤
    │ 1   │ 1 │ x │
    │ 2   │ 2 │ y │
    │ 3   │ 3 │ z │"""

    io = IOBuffer()
    show(io, MIME("text/plain"), eachrow(df), eltypes=false)
    str = String(take!(io))
    @test str == """
    3×2 DataFrameRows
    │ Row │ A │ B │
    ├─────┼───┼───┤
    │ 1   │ 1 │ x │
    │ 2   │ 2 │ y │
    │ 3   │ 3 │ z │"""

    io = IOBuffer()
    show(io, MIME("text/html"), df, eltypes=true)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr>" *
                 "<tr><th></th><th>Int32</th><th>String</th></tr></thead><tbody>" *
                 "<p>3 rows × 2 columns</p>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME("text/html"), eachcol(df), eltypes=true)
    str = String(take!(io))
    @test str == "<p>3×2 DataFrameColumns</p><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr>" *
                 "<tr><th></th><th>Int32</th><th>String</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME("text/html"), eachrow(df), eltypes=true)
    str = String(take!(io))
    @test str == "<p>3×2 DataFrameRows</p><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr>" *
                 "<tr><th></th><th>Int32</th><th>String</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME("text/html"), df, eltypes=false)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr></thead><tbody>" *
                 "<p>3 rows × 2 columns</p>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME("text/html"), eachcol(df), eltypes=false)
    str = String(take!(io))
    @test str == "<p>3×2 DataFrameColumns</p><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME("text/html"), eachrow(df), eltypes=false)
    str = String(take!(io))
    @test str == "<p>3×2 DataFrameRows</p><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table>"

    for x in [df, eachcol(df), eachrow(df)]
        io = IOBuffer()
        show(io, MIME("text/latex"), x, eltypes=true)
        str = String(take!(io))
        @test str == """
        \\begin{tabular}{r|cc}
        \t& A & B\\\\
        \t\\hline
        \t& Int32 & String\\\\
        \t\\hline
        \t1 & 1 & x \\\\
        \t2 & 2 & y \\\\
        \t3 & 3 & z \\\\
        \\end{tabular}
        """

        io = IOBuffer()
        show(io, MIME("text/latex"), x, eltypes=false)
        str = String(take!(io))
        @test str == """
        \\begin{tabular}{r|cc}
        \t& A & B\\\\
        \t\\hline
        \t1 & 1 & x \\\\
        \t2 & 2 & y \\\\
        \t3 & 3 & z \\\\
        \\end{tabular}
        """
    end
end

@testset "improved printing of special types" begin
    df = DataFrame(A=Int64.(1:9), B = Vector{Any}(undef, 9))
    df.B[1:8] = [df, # DataFrame
                 df[1,:], # DataFrameRow
                 view(df,1:1, :), # SubDataFrame
                 eachrow(df), # DataFrameColumns
                 eachcol(df), # DataFrameRows
                 groupby(df, :A),missing,nothing] # GroupedDataFrame

    io = IOBuffer()
    show(io, df)
    str = String(take!(io))

    @test str == """
    9×2 DataFrame
    │ Row │ A     │ B                                              │
    │     │ Int64 │ Any                                            │
    ├─────┼───────┼────────────────────────────────────────────────┤
    │ 1   │ 1     │ 9×2 DataFrame                                  │
    │ 2   │ 2     │ 2-element DataFrameRow                         │
    │ 3   │ 3     │ 1×2 SubDataFrame                               │
    │ 4   │ 4     │ 9-element DataFrameRows                        │
    │ 5   │ 5     │ 2-element DataFrameColumns                     │
    │ 6   │ 6     │ GroupedDataFrame with 9 groups based on key: A │
    │ 7   │ 7     │ missing                                        │
    │ 8   │ 8     │ nothing                                        │
    │ 9   │ 9     │ #undef                                         │"""


    io = IOBuffer()
    show(IOContext(io, :color => true), df)
    str = String(take!(io))
    @test str == """
    9×2 DataFrame
    │ Row │ A     │ B                                              │
    │     │ \e[90mInt64\e[39m │ \e[90mAny\e[39m                                            │
    ├─────┼───────┼────────────────────────────────────────────────┤
    │ 1   │ 1     │ \e[90m9×2 DataFrame\e[39m                                  │
    │ 2   │ 2     │ \e[90m2-element DataFrameRow\e[39m                         │
    │ 3   │ 3     │ \e[90m1×2 SubDataFrame\e[39m                               │
    │ 4   │ 4     │ \e[90m9-element DataFrameRows\e[39m                        │
    │ 5   │ 5     │ \e[90m2-element DataFrameColumns\e[39m                     │
    │ 6   │ 6     │ \e[90mGroupedDataFrame with 9 groups based on key: A\e[39m │
    │ 7   │ 7     │ \e[90mmissing\e[39m                                        │
    │ 8   │ 8     │ \e[90mnothing\e[39m                                        │
    │ 9   │ 9     │ \e[90m#undef\e[39m                                         │"""


    io = IOBuffer()
    show(io, MIME("text/html"), df)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><thead><tr><th></th><th>A</th><th>B</th></tr>" *
                 "<tr><th></th><th>Int64</th><th>Any</th></tr></thead>" *
                 "<tbody><p>9 rows × 2 columns</p>" *
                 "<tr><th>1</th><td>1</td><td><em>9×2 DataFrame</em></td></tr>" *
                 "<tr><th>2</th><td>2</td><td><em>2-element DataFrameRow</em></td></tr>" *
                 "<tr><th>3</th><td>3</td><td><em>1×2 SubDataFrame</em></td></tr>" *
                 "<tr><th>4</th><td>4</td><td><em>9-element DataFrameRows</em></td></tr>" *
                 "<tr><th>5</th><td>5</td><td><em>2-element DataFrameColumns</em></td></tr>" *
                 "<tr><th>6</th><td>6</td><td><em>GroupedDataFrame with 9 groups based on key: A</em></td></tr>" *
                 "<tr><th>7</th><td>7</td><td><em>missing</em></td></tr>" *
                 "<tr><th>8</th><td>8</td><td><em>nothing</em></td></tr>" *
                 "<tr><th>9</th><td>9</td><td><em>#undef</em></td></tr></tbody></table>"

    io = IOBuffer()
    show(io, MIME("text/latex"), df)
    str = String(take!(io))
    @test str == """
    \\begin{tabular}{r|cc}
    \t& A & B\\\\
    \t\\hline
    \t& Int64 & Any\\\\
    \t\\hline
    \t1 & 1 & \\emph{9×2 DataFrame} \\\\
    \t2 & 2 & \\emph{2-element DataFrameRow} \\\\
    \t3 & 3 & \\emph{1×2 SubDataFrame} \\\\
    \t4 & 4 & \\emph{9-element DataFrameRows} \\\\
    \t5 & 5 & \\emph{2-element DataFrameColumns} \\\\
    \t6 & 6 & \\emph{GroupedDataFrame with 9 groups based on key: A} \\\\
    \t7 & 7 & \\emph{missing} \\\\
    \t8 & 8 & \\emph{nothing} \\\\
    \t9 & 9 & \\emph{\\#undef} \\\\
    \\end{tabular}
    """

    @test_throws UndefRefError show(io, MIME("text/csv"), df)
    @test_throws UndefRefError show(io, MIME("text/tab-separated-values"), df)

    io = IOBuffer()
    show(io, MIME("text/csv"), df[1:end-1, :])
    str = String(take!(io))
    @test str == """
    "A","B"
    1,"9×2 DataFrame"
    2,"2-element DataFrameRow"
    3,"1×2 SubDataFrame"
    4,"9-element DataFrameRows"
    5,"2-element DataFrameColumns"
    6,"GroupedDataFrame with 9 groups based on key: A"
    7,missing
    8,nothing
    """

    io = IOBuffer()
    show(io, MIME("text/tab-separated-values"), df[1:end-1, :])
    str = String(take!(io))
    @test str == """
    "A"\t"B"
    1\t"9×2 DataFrame"
    2\t"2-element DataFrameRow"
    3\t"1×2 SubDataFrame"
    4\t"9-element DataFrameRows"
    5\t"2-element DataFrameColumns"
    6\t"GroupedDataFrame with 9 groups based on key: A"
    7\tmissing
    8\tnothing
    """
end

end # module
