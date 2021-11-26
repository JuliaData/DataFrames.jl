# This type is for testing the escaping of quotes in HTML output. It needs to be
# outside of the module so that Julia 1.0 would not complain about the type
# being defined in local scope.
struct QuoteTestType{T} end

module TestIO

using Test, DataFrames, CategoricalArrays, Dates, Markdown
import Main: QuoteTestType

# Test LaTeX export
@testset "LaTeX export" begin
    df = DataFrame(A=Int64.( 1:4 ),
                   B=["\$10.0", "M&F", "A~B", "\\alpha"],
                   C=["A", "B", "C", "S"],
                   D=[1.0, 2.0, missing, 3.0],
                   E=CategoricalArray(["a", missing, "c", "d"]),
                   F=Vector{String}(undef, 4),
                   G=[ md"[DataFrames.jl](http://juliadata.github.io/DataFrames.jl)", md"###A", md"``\frac{A}{B}``", md"*A*b**A**"]
                  )
    str = """
        \\begin{tabular}{r|ccccccc}
        \t& A & B & C & D & E & F & G\\\\
        \t\\hline
        \t& Int64 & String & String & Float64? & Cat…? & String & MD\\\\
        \t\\hline
        \t1 & 1 & \\\$10.0 & A & 1.0 & a & \\emph{\\#undef} & \\href{http://juliadata.github.io/DataFrames.jl}{DataFrames.jl} \\\\
        \t2 & 2 & M\\&F & B & 2.0 & \\emph{missing} & \\emph{\\#undef} & \\#\\#\\#A \\\\
        \t3 & 3 & A\\textasciitilde{}B & C & \\emph{missing} & c & \\emph{\\#undef} & \$\\frac{A}{B}\$ \\\\
        \t4 & 4 & \\textbackslash{}\\textbackslash{}alpha & S & 3.0 & d & \\emph{\\#undef} & \\emph{A}b\\textbf{A} \\\\
        \\end{tabular}
        """

    @test repr(MIME("text/latex"), df) == str
    @test repr(MIME("text/latex"), eachcol(df)) == str
    @test repr(MIME("text/latex"), eachrow(df)) == str

    @test_throws ArgumentError DataFrames._show(stdout, MIME("text/latex"),
                                                DataFrame(ones(2,2), :auto), rowid=10)
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
    df = DataFrame(Fish=["Suzy", "Amir"], Mass=[1.5, missing])
    io = IOBuffer()
    show(io, "text/html", df)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\">" *
                 "<p>2 rows × 2 columns</p>" *
                 "<table class=\"data-frame\">" *
                 "<thead><tr><th></th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th title=\"String\">String</th>" *
                 "<th title=\"Union{Missing, Float64}\">Float64?</th></tr></thead>" *
                 "<tbody><tr><th>1</th><td>Suzy</td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td>Amir</td><td><em>missing</em></td></tr>" *
                 "</tbody></table></div>"

    df = DataFrame(Fish=Vector{String}(undef, 2), Mass=[1.5, missing])
    io = IOBuffer()
    show(io, "text/html", df)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\">" *
                 "<p>2 rows × 2 columns</p>" *
                 "<table class=\"data-frame\">" *
                 "<thead><tr><th></th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th title=\"String\">String</th>" *
                 "<th title=\"Union{Missing, Float64}\">Float64?</th></tr></thead>" *
                 "<tbody><tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr>" *
                 "</tbody></table></div>"

    io = IOBuffer()
    show(io, "text/html", eachrow(df))
    str = String(take!(io))
    @test str == "<p>2×2 DataFrameRows</p>" *
                 "<div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th title=\"String\">String</th><th title=\"Union{Missing, Float64}\">Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, "text/html", eachcol(df))
    str = String(take!(io))
    @test str == "<p>2×2 DataFrameColumns</p>" *
                 "<div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th title=\"String\">String</th><th title=\"Union{Missing, Float64}\">Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, "text/html", df[1, :])
    str = String(take!(io))
    @test str == "<p>DataFrameRow (2 columns)</p>" *
                 "<div class=\"data-frame\"><table class=\"data-frame\">" *
                 "<thead><tr><th></th><th>Fish</th><th>Mass</th></tr><tr><th></th>" *
                 "<th title=\"String\">String</th><th title=\"Union{Missing, Float64}\">Float64?</th></tr></thead><tbody><tr><th>1</th>" *
                 "<td><em>#undef</em></td><td>1.5</td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, MIME"text/html"(), df, summary=false)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th title=\"String\">String</th><th title=\"Union{Missing, Float64}\">Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, MIME"text/html"(), eachrow(df), summary=false)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th title=\"String\">String</th><th title=\"Union{Missing, Float64}\">Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, MIME"text/html"(), eachcol(df), summary=false)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th></th><th title=\"String\">String</th><th title=\"Union{Missing, Float64}\">Float64?</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td><em>#undef</em></td><td><em>missing</em></td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, MIME"text/html"(), df[1, :], summary=false)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th></th><th>Fish</th>" *
                 "<th>Mass</th></tr><tr><th></th><th title=\"String\">String</th><th title=\"Union{Missing, Float64}\">Float64?</th></tr></thead>" *
                 "<tbody><tr><th>1</th><td><em>#undef</em></td><td>1.5</td></tr></tbody></table></div>"

    io = IOBuffer()
    show(IOContext(io, :limit => true, :displaysize => (10, 10)), MIME"text/html"(),
         DataFrame(Int64[1 2 3 4 5 6 7 8 9], :auto))
    str = String(take!(io))
    @test str == "<div class=\"data-frame\"><p>1 rows × 9 columns (omitted printing of 7 columns)</p>" *
                 "<table class=\"data-frame\"><thead><tr><th></th><th>x1</th><th>x2</th></tr><tr><th></th>" *
                 "<th title=\"Int64\">Int64</th><th title=\"Int64\">Int64</th></tr></thead>" *
                 "<tbody><tr><th>1</th><td>1</td><td>2</td></tr></tbody></table></div>"

    @test_throws ArgumentError DataFrames._show(stdout, MIME("text/html"),
                                                DataFrame(ones(2,2), :auto), rowid=10)

    df = DataFrame(
        A=Int64[1,4,9,16],
        B = [
            md"[DataFrames.jl](http://juliadata.github.io/DataFrames.jl)",
            md"###A",
            md"``\frac{A}{B}``",
            md"*A*b**A**" ]
    )

    @test repr(MIME("text/html"), df) ==
    "<div class=\"data-frame\"><p>4 rows × 2 columns</p>" *
    "<table class=\"data-frame\"><thead><tr><th></th><th>A</th><th>B</th></tr><tr><th></th>" *
    "<th title=\"Int64\">Int64</th><th title=\"Markdown.MD\">MD</th></tr></thead>" *
    "<tbody><tr><th>1</th><td>1</td>" *
    "<td><div class=\"markdown\"><p><a href=\"http://juliadata.github.io/DataFrames.jl\">DataFrames.jl</a></p>\n</div></td></tr>" *
    "<tr><th>2</th><td>4</td><td><div class=\"markdown\"><p>###A</p>\n</div></td></tr>" *
    "<tr><th>3</th><td>9</td><td><div class=\"markdown\"><p>&#36;\\frac&#123;A&#125;&#123;B&#125;&#36;</p>\n</div></td></tr>" *
    "<tr><th>4</th><td>16</td><td><div class=\"markdown\"><p><em>A</em>b<strong>A</strong></p>\n</div></td></tr>" *
    "</tbody></table></div>"

    # Test that single and double quotes get escaped properly
    df = DataFrame(
        xs = ["'", "\"", "<foo>'</bar>"],
        ys = [QuoteTestType{'\''}(), QuoteTestType{'"'}, QuoteTestType{Symbol("\"'")}()],
        zs = QuoteTestType{'"'}[QuoteTestType{'"'}(), QuoteTestType{'"'}(), QuoteTestType{'"'}()],
    )
    io = IOBuffer()
    show(io, "text/html", df)
    str = String(take!(io))
    @test str ==
        "<div class=\"data-frame\"><p>3 rows × 3 columns</p>" *
        "<table class=\"data-frame\"><thead>" *
            "<tr>" *
                "<th></th>" *
                "<th>xs</th>" *
                "<th>ys</th>" *
                "<th>zs</th>" *
            "</tr><tr>" *
                "<th></th>" *
                "<th title=\"String\">String</th>" *
                "<th title=\"Any\">Any</th>" *
                "<th title=\"QuoteTestType{&apos;&quot;&apos;}\">QuoteTes…</th>" *
            "</tr>" *
        "</thead><tbody>" *
            "<tr>" *
                "<th>1</th>" *
                "<td>&apos;</td>" *
                "<td>QuoteTestType{&apos;\\\\&apos;&apos;}()</td>" *
                "<td>QuoteTestType{&apos;&quot;&apos;}()</td>" *
            "</tr><tr>" *
                "<th>2</th>" *
                "<td>&quot;</td>" *
                "<td>QuoteTestType{&apos;&quot;&apos;}</td>" *
                "<td>QuoteTestType{&apos;&quot;&apos;}()</td>" *
            "</tr><tr>" *
                "<th>3</th>" *
                "<td>&lt;foo&gt;&apos;&lt;/bar&gt;</td>" *
                "<td>QuoteTestType{Symbol(&quot;\\\\&quot;&apos;&quot;)}()</td>" *
                "<td>QuoteTestType{&apos;&quot;&apos;}()</td>" *
            "</tr>" *
        "</tbody></table></div>"
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
    df = DataFrame(A=1:3,
                   B='a':'c',
                   C=["A", "B", "C"],
                   D=CategoricalArray(string.('a':'c')),
                   E=CategoricalArray(["A", "B", missing]),
                   F=Vector{Union{Int, Missing}}(1:3),
                   G=missings(3),
                   H=fill(missing, 3)
                  )

    @test sprint(DataFrames.printtable, df) ==
        """
        "A","B","C","D","E","F","G","H"
        1,"a","A","a","A","1",missing,missing
        2,"b","B","b","B","2",missing,missing
        3,"c","C","c",missing,"3",missing,missing
        """
end

@testset "csv/tsv output" begin
    df = DataFrame(a=[1,2], b=[1.0, 2.0])

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

@testset "Markdown as text/plain and as text/csv" begin
    df = DataFrame(
        A=Int64[1,4,9,16,25,36,49,64],
        B = [
            md"[DataFrames.jl](http://juliadata.github.io/DataFrames.jl)",
            md"``\frac{x^2}{x^2+y^2}``",
            md"# Header",
            md"This is *very*, **very**, very, very, very, very, very, very, very long line" ,
            md"",
            Markdown.parse("∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0∫αγ∞1∫αγ∞2∫αγ∞3"),
            Markdown.parse("∫αγ∞1∫αγ∞\n"*
                           "  * 2∫αγ∞3∫αγ∞4\n"*
                           "  * ∫αγ∞5∫αγ\n"*
                           "  * ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0"),
            Markdown.parse("∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0∫α\n"*
                           "  * γ∞1∫α\n"*
                           "  * γ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0"),
        ]
    )
    if VERSION < v"1.6.0-DEV"
        @test sprint(show, "text/plain", df) ==
            """
            8×2 DataFrame
             Row │ A      B
                 │ Int64  MD
            ─────┼──────────────────────────────────────────
               1 │     1    DataFrames.jl (http://juliadat…
               2 │     4    \\frac{x^2}{x^2+y^2}
               3 │     9    Header\\n  ≡≡≡≡≡≡≡≡
               4 │    16    This is very, very, very, very…
               5 │    25
               6 │    36    ∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6…
               7 │    49    ∫αγ∞1∫αγ∞\\n\\n    •    2∫αγ∞3∫α…
               8 │    64    ∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6…"""
    else
        @test sprint(show, "text/plain", df) ==
            """
            8×2 DataFrame
             Row │ A      B
                 │ Int64  MD
            ─────┼──────────────────────────────────────────
               1 │     1    DataFrames.jl (http://juliadat…
               2 │     4    \\frac{x^2}{x^2+y^2}
               3 │     9    Header\\n  ≡≡≡≡≡≡≡≡
               4 │    16    This is very, very, very, very…
               5 │    25
               6 │    36    ∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6…
               7 │    49    ∫αγ∞1∫αγ∞\\n\\n    •  2∫αγ∞3∫αγ∞…
               8 │    64    ∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6…"""
    end

    @test sprint(show, "text/csv", df) ==
        """
        \"A\",\"B\"
        1,\"[DataFrames.jl](http://juliadata.github.io/DataFrames.jl)\"
        4,\"\$\\\\frac{x^2}{x^2+y^2}\$\"
        9,\"# Header\"
        16,\"This is *very*, **very**, very, very, very, very, very, very, very long line\"
        25,\"\"
        36,\"∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0∫αγ∞1∫αγ∞2∫αγ∞3\"
        49,\"∫αγ∞1∫αγ∞\\n\\n  * 2∫αγ∞3∫αγ∞4\\n  * ∫αγ∞5∫αγ\\n  * ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0\"
        64,\"∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0∫α\\n\\n  * γ∞1∫α\\n  * γ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0\"
        """
end

@testset "Markdown as HTML" begin
    df = DataFrame(
        A=Int64[1,4,9,16,25,36,49,64],
        B = [
            md"[DataFrames.jl](http://juliadata.github.io/DataFrames.jl)",
            md"``\frac{x^2}{x^2+y^2}``",
            md"# Header",
            md"This is *very*, **very**, very, very, very, very, very, very, very long line" ,
            md"",
            Markdown.parse("∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0" *
                           "∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0"),
            Markdown.parse("∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ\n"*
                           "  * ∞7∫αγ\n"*
                           "  * ∞8∫αγ\n"*
                           "  * ∞9∫αγ∞0∫α\nγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0"),
            Markdown.parse("∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0∫α\n"*
                           "  * γ∞1∫α\n"*
                           "  * γ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0"),
        ]
    )
    @test sprint(show,"text/html",df) ==
        "<div class=\"data-frame\"><p>8 rows × 2 columns</p>" *
        "<table class=\"data-frame\"><thead>" *
            "<tr><th></th><th>A</th><th>B</th></tr>" *
            "<tr><th></th><th title=\"Int64\">Int64</th><th title=\"Markdown.MD\">MD</th></tr>" *
        "</thead>" *
        "<tbody>" *
        "<tr><th>1</th><td>1</td><td><div class=\"markdown\">" *
            "<p><a href=\"http://juliadata.github.io/DataFrames.jl\">DataFrames.jl</a></p>\n</div></td></tr>" *
        "<tr><th>2</th><td>4</td><td><div class=\"markdown\"><p>&#36;\\frac&#123;x^2&#125;&#123;x^2&#43;y^2&#125;&#36;</p>\n</div></td></tr>" *
        "<tr><th>3</th><td>9</td><td><div class=\"markdown\"><h1>Header</h1>\n</div></td></tr>" *
        "<tr><th>4</th><td>16</td><td><div class=\"markdown\">" *
            "<p>This is <em>very</em>, <strong>very</strong>, very, very, very, very, very, very, very long line</p>\n" *
        "</div></td></tr>" *
        "<tr><th>5</th><td>25</td><td><div class=\"markdown\"></div></td></tr>" *
        "<tr><th>6</th><td>36</td><td><div class=\"markdown\">" *
            "<p>∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0</p>\n" *
        "</div></td></tr>" *
        "<tr><th>7</th><td>49</td><td><div class=\"markdown\">" *
            "<p>∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ</p>\n<ul>\n<li><p>∞7∫αγ</p>\n</li>\n<li><p>∞8∫αγ</p>\n</li>\n<li><p>∞9∫αγ∞0∫α</p>\n</li>\n</ul>\n<p>γ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0</p>\n" *
        "</div></td></tr>" *
        "<tr><th>8</th><td>64</td><td><div class=\"markdown\">" *
            "<p>∫αγ∞1∫αγ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0∫α</p>" *
            "\n<ul>\n" *
                "<li><p>γ∞1∫α</p>\n</li>\n" *
                "<li><p>γ∞2∫αγ∞3∫αγ∞4∫αγ∞5∫αγ∞6∫αγ∞7∫αγ∞8∫αγ∞9∫αγ∞0</p>\n</li>\n" *
        "</ul>\n" * "</div></td></tr></tbody></table></div>"
end

@testset "empty data frame and DataFrameRow" begin
    df = DataFrame(a=[1,2], b=[1.0, 2.0])

    @test sprint(show, "text/csv", df[:, 2:1]) == ""
    @test sprint(show, "text/tab-separated-values", df[:, 2:1]) == ""
    @test sprint(show, "text/html", df[:, 2:1]) ==
          "<div class=\"data-frame\"><p>0 rows × 0 columns</p>" *
          "<table class=\"data-frame\"><thead><tr><th></th></tr><tr><th></th></tr>" *
          "</thead><tbody></tbody></table></div>"
    @test sprint(show, "text/latex", df[:, 2:1]) ==
          "\\begin{tabular}{r|}\n\t& \\\\\n\t\\hline\n\t& \\\\\n\t\\hline\n\\end{tabular}\n"

    @test sprint(show, "text/csv", @view df[:, 2:1]) == ""
    @test sprint(show, "text/tab-separated-values", @view df[:, 2:1]) == ""
    @test sprint(show, "text/html", @view df[:, 2:1]) ==
          "<div class=\"data-frame\"><p>0 rows × 0 columns</p>" *
          "<table class=\"data-frame\"><thead><tr><th></th></tr><tr><th></th></tr>" *
          "</thead><tbody></tbody></table></div>"
    @test sprint(show, "text/latex", @view df[:, 2:1]) ==
          "\\begin{tabular}{r|}\n\t& \\\\\n\t\\hline\n\t& \\\\\n\t\\hline\n\\end{tabular}\n"

    @test sprint(show, "text/csv", df[1, 2:1]) == ""
    @test sprint(show, "text/tab-separated-values", df[1, 2:1]) == ""
    @test sprint(show, "text/html", df[1, 2:1]) ==
          "<p>DataFrameRow (0 columns)</p><div class=\"data-frame\"><table class=\"data-frame\">" *
          "<thead><tr><th></th></tr><tr><th></th></tr></thead><tbody></tbody></table></div>"
    @test sprint(show, "text/latex", df[1, 2:1]) ==
          "\\begin{tabular}{r|}\n\t& \\\\\n\t\\hline\n\t& \\\\\n\t\\hline\n\\end{tabular}\n"
end

@testset "consistency" begin
    df = DataFrame(a=[1, 1, 2, 2], b=[5, 6, 7, 8], c=1:4)
    push!(df.c, 5)
    @test_throws AssertionError sprint(show, "text/html", df)
    @test_throws AssertionError sprint(show, "text/latex", df)
    @test_throws AssertionError sprint(show, "text/csv", df)
    @test_throws AssertionError sprint(show, "text/tab-separated-values", df)

    df = DataFrame(a=[1, 1, 2, 2], b=[5, 6, 7, 8], c=1:4)
    push!(DataFrames._columns(df), df[:, :a])
    @test_throws AssertionError sprint(show, "text/html", df)
    @test_throws AssertionError sprint(show, "text/latex", df)
    @test_throws AssertionError sprint(show, "text/csv", df)
    @test_throws AssertionError sprint(show, "text/tab-separated-values", df)
end

@testset "summary tests" begin
    df = DataFrame(ones(2,3), :auto)

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
    df = DataFrame(A=Int32.(1:3), B=["x", "y", "z"])

    io = IOBuffer()
    show(io, MIME("text/plain"), df, eltypes=true)
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
    show(io, MIME("text/plain"), eachcol(df), eltypes=true)
    str = String(take!(io))
    @test str == """
        3×2 DataFrameColumns
         Row │ A      B
             │ Int32  String
        ─────┼───────────────
           1 │     1  x
           2 │     2  y
           3 │     3  z"""

    io = IOBuffer()
    show(io, MIME("text/plain"), eachrow(df), eltypes=true)
    str = String(take!(io))
    @test str == """
        3×2 DataFrameRows
         Row │ A      B
             │ Int32  String
        ─────┼───────────────
           1 │     1  x
           2 │     2  y
           3 │     3  z"""

    io = IOBuffer()
    show(io, MIME("text/plain"), df, eltypes=false)
    str = String(take!(io))
    @test str == """
        3×2 DataFrame
         Row │ A  B
        ─────┼──────
           1 │ 1  x
           2 │ 2  y
           3 │ 3  z"""

    io = IOBuffer()
    show(io, MIME("text/plain"), eachcol(df), eltypes=false)
    str = String(take!(io))
    @test str == """
        3×2 DataFrameColumns
         Row │ A  B
        ─────┼──────
           1 │ 1  x
           2 │ 2  y
           3 │ 3  z"""

    io = IOBuffer()
    show(io, MIME("text/plain"), eachrow(df), eltypes=false)
    str = String(take!(io))
    @test str == """
        3×2 DataFrameRows
         Row │ A  B
        ─────┼──────
           1 │ 1  x
           2 │ 2  y
           3 │ 3  z"""

    io = IOBuffer()
    show(io, MIME("text/html"), df, eltypes=true)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\"><p>3 rows × 2 columns</p>" *
                 "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr>" *
                 "<tr><th></th><th title=\"Int32\">Int32</th><th title=\"String\">String</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, MIME("text/html"), eachcol(df), eltypes=true)
    str = String(take!(io))
    @test str == "<p>3×2 DataFrameColumns</p><div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr>" *
                 "<tr><th></th><th title=\"Int32\">Int32</th><th title=\"String\">String</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, MIME("text/html"), eachrow(df), eltypes=true)
    str = String(take!(io))
    @test str == "<p>3×2 DataFrameRows</p><div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr>" *
                 "<tr><th></th><th title=\"Int32\">Int32</th><th title=\"String\">String</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, MIME("text/html"), df, eltypes=false)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\"><p>3 rows × 2 columns</p>" *
                 "<table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, MIME("text/html"), eachcol(df), eltypes=false)
    str = String(take!(io))
    @test str == "<p>3×2 DataFrameColumns</p><div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table></div>"

    io = IOBuffer()
    show(io, MIME("text/html"), eachrow(df), eltypes=false)
    str = String(take!(io))
    @test str == "<p>3×2 DataFrameRows</p><div class=\"data-frame\"><table class=\"data-frame\"><thead><tr><th>" *
                 "</th><th>A</th><th>B</th></tr></thead><tbody>" *
                 "<tr><th>1</th><td>1</td><td>x</td></tr>" *
                 "<tr><th>2</th><td>2</td><td>y</td></tr><tr><th>3</th><td>3</td><td>z</td></tr></tbody></table></div>"

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
    df = DataFrame(A=Int64.(1:9), B=Vector{Any}(undef, 9))
    df.B[1:8] = [df, # DataFrame
                 df[1,:], # DataFrameRow
                 view(df,1:1, :), # SubDataFrame
                 eachrow(df), # DataFrameColumns
                 eachcol(df), # DataFrameRows
                 groupby(df, :A), # GroupedDataFrame
                 missing,
                 nothing]

    io = IOBuffer()
    show(io, df)
    str = String(take!(io))

    @test str == """
    9×2 DataFrame
     Row │ A      B
         │ Int64  Any
    ─────┼──────────────────────────────────────────
       1 │     1  9×2 DataFrame
       2 │     2  2-element DataFrameRow
       3 │     3  1×2 SubDataFrame
       4 │     4  9-element DataFrameRows
       5 │     5  2-element DataFrameColumns
       6 │     6  GroupedDataFrame with 9 groups b…
       7 │     7  missing
       8 │     8
       9 │     9  #undef"""

    # TODO: update when https://github.com/KristofferC/Crayons.jl/issues/47 is resolved
    if VERSION >= v"1.6" && Base.get_have_color()
        io = IOBuffer()
        show(IOContext(io, :color => true), df)
        str = String(take!(io))
        @test str == """
        \e[1m9×2 DataFrame\e[0m
        \e[1m Row \e[0m│\e[1m A     \e[0m\e[1m B                                 \e[0m
        \e[1m     \e[0m│\e[90m Int64 \e[0m\e[90m Any                               \e[0m
        ─────┼──────────────────────────────────────────
           1 │     1 \e[90m 9×2 DataFrame                     \e[0m
           2 │     2 \e[90m 2-element DataFrameRow            \e[0m
           3 │     3 \e[90m 1×2 SubDataFrame                  \e[0m
           4 │     4 \e[90m 9-element DataFrameRows           \e[0m
           5 │     5 \e[90m 2-element DataFrameColumns        \e[0m
           6 │     6 \e[90m GroupedDataFrame with 9 groups b… \e[0m
           7 │     7 \e[90m missing                           \e[0m
           8 │     8 \e[90m                                   \e[0m
           9 │     9 \e[90m #undef                            \e[0m"""
    end

    io = IOBuffer()
    show(io, MIME("text/html"), df)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\"><p>9 rows × 2 columns</p>" *
                 "<table class=\"data-frame\"><thead><tr><th></th><th>A</th><th>B</th></tr>" *
                 "<tr><th></th><th title=\"Int64\">Int64</th><th title=\"Any\">Any</th></tr></thead>" *
                 "<tbody>" *
                 "<tr><th>1</th><td>1</td><td><em>9×2 DataFrame</em></td></tr>" *
                 "<tr><th>2</th><td>2</td><td><em>2-element DataFrameRow</em></td></tr>" *
                 "<tr><th>3</th><td>3</td><td><em>1×2 SubDataFrame</em></td></tr>" *
                 "<tr><th>4</th><td>4</td><td><em>9-element DataFrameRows</em></td></tr>" *
                 "<tr><th>5</th><td>5</td><td><em>2-element DataFrameColumns</em></td></tr>" *
                 "<tr><th>6</th><td>6</td><td><em>GroupedDataFrame with 9 groups based on key: A</em></td></tr>" *
                 "<tr><th>7</th><td>7</td><td><em>missing</em></td></tr>" *
                 "<tr><th>8</th><td>8</td><td></td></tr>" *
                 "<tr><th>9</th><td>9</td><td><em>#undef</em></td></tr></tbody></table></div>"

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
    \t8 & 8 &  \\\\
    \t9 & 9 & \\emph{\\#undef} \\\\
    \\end{tabular}
    """

    @test_throws UndefRefError show(io, MIME("text/csv"), df)
    @test_throws UndefRefError show(io, MIME("text/tab-separated-values"), df)

    df[end, 2] = "\""
    push!(df, (10, Symbol("\"")))
    push!(df, (11, '"'))
    df.B[6] = groupby(df, :A)
    io = IOBuffer()
    show(io, MIME("text/csv"), df)
    str = String(take!(io))
    @test str == """
    "A","B"
    1,"11×2 DataFrame"
    2,"2-element DataFrameRow"
    3,"1×2 SubDataFrame"
    4,"11-element DataFrameRows"
    5,"2-element DataFrameColumns"
    6,"GroupedDataFrame with 11 groups based on key: A"
    7,missing
    8,nothing
    9,"\\""
    10,"\\""
    11,"\\""
    """

    io = IOBuffer()
    show(io, MIME("text/tab-separated-values"), df)
    str = String(take!(io))
    @test str == """
    "A"\t"B"
    1\t"11×2 DataFrame"
    2\t"2-element DataFrameRow"
    3\t"1×2 SubDataFrame"
    4\t"11-element DataFrameRows"
    5\t"2-element DataFrameColumns"
    6\t"GroupedDataFrame with 11 groups based on key: A"
    7\tmissing
    8\tnothing
    9\t"\\""
    10\t"\\""
    11\t"\\""
    """
end

@testset "check truncate keyword argument" begin
    df = DataFrame(x="0123456789"^10)

    # no truncation
    io = IOBuffer()
    show(io, MIME("text/html"), df)
    str = String(take!(io))
    @test str == "<div class=\"data-frame\"><p>1 rows × 1 columns</p>" *
                 "<table class=\"data-frame\"><thead><tr><th></th><th>x</th></tr>" *
                 "<tr><th></th><th title=\"String\">String</th></tr></thead>" *
                 "<tbody><tr><th>1</th>" *
                 "<td>01234567890123456789012345678901234567890123456789" *
                 "01234567890123456789012345678901234567890123456789</td>"*
                 "</tr></tbody></table></div>"

    # no truncation
    io = IOBuffer()
    show(io, MIME("text/latex"), df)
    str = String(take!(io))
    @test str == """
    \\begin{tabular}{r|c}
    \t& x\\\\
    \t\\hline
    \t& String\\\\
    \t\\hline
    \t1 & 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789 \\\\
    \\end{tabular}
    """

    # no truncation
    io = IOBuffer()
    show(io, MIME("text/csv"), df)
    str = String(take!(io))
    @test str == "\"x\"\n\"01234567890123456789012345678901234567890123456789" *
                 "01234567890123456789012345678901234567890123456789\"\n"

    # no truncation
    io = IOBuffer()
    show(io, MIME("text/tab-separated-values"), df)
    str = String(take!(io))
    @test str == "\"x\"\n\"01234567890123456789012345678901234567890123456789" *
                 "01234567890123456789012345678901234567890123456789\"\n"

    # default truncation
    io = IOBuffer()
    show(io, MIME("text/plain"), df)
    str = String(take!(io))
    @test str == """
        1×1 DataFrame
         Row │ x
             │ String
        ─────┼───────────────────────────────────
           1 │ 01234567890123456789012345678901…"""

    io = IOBuffer()
    show(io, df)
    str = String(take!(io))
    @test str == """
        1×1 DataFrame
         Row │ x
             │ String
        ─────┼───────────────────────────────────
           1 │ 01234567890123456789012345678901…"""

    # no truncation
    io = IOBuffer()
    show(io, df, truncate=0)
    str = String(take!(io))
    @test str == """
        1×1 DataFrame
         Row │ x
             │ String
        ─────┼──────────────────────────────────────────────────────────────────────────────────────────────────────
           1 │ 0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"""

    # custom truncation
    io = IOBuffer()
    show(io, df, truncate=1)
    str = String(take!(io))
    @test str == """
        1×1 DataFrame
         Row │ x
             │ String
        ─────┼────────
           1 │ 01234…"""


    df = DataFrame(x12345678901234567890="0123456789"^10)
    io = IOBuffer()
    show(io, df, truncate=1, rowlabel=:r12345678901234567890)
    str = String(take!(io))
    @test str == """
        1×1 DataFrame
         r12345678901234567890 │ x12345678901234567890
                               │ String
        ───────────────────────┼───────────────────────
                             1 │ 01234567890123456789…"""

end

end # module
