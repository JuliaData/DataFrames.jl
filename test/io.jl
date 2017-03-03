module TestIO
    using Base.Test
    using DataTables
    using CSV
    using LaTeXStrings

    #test_group("We can read various file types.")

    data = joinpath(dirname(@__FILE__), "data")

    filenames = ["$data/blanklines/blanklines.csv",
                 "$data/headeronly/headeronly.csv",
                 "$data/newlines/embedded_os9.csv",
                 "$data/newlines/embedded_osx.csv",
                 "$data/newlines/embedded_windows.csv",
                 "$data/padding/space_after_delimiter.csv",
                 "$data/padding/space_around_delimiter.csv",
                 "$data/padding/space_before_delimiter.csv",
                 "$data/quoting/empty.csv",
                 "$data/quoting/escaping.csv",
                 "$data/quoting/quotedcommas.csv",
                 "$data/scaling/10000rows.csv",
                 "$data/utf8/corrupt_utf8.csv",
                 "$data/utf8/short_corrupt_utf8.csv",
                 "$data/utf8/utf8.csv"]

    for filename in filenames
        try
            dt = CSV.read(filename)
        catch
            error(@sprintf "Failed to read %s\n" filename)
        end
    end

    #test_group("We get the right size, types, values for a basic csv.")

    filename = "$data/scaling/movies.csv"
    io = open(filename)
    dt = CSV.read(io, null="NA", weakrefstrings=false)
    @test isopen(io)
    close(io)

    @test size(dt) == (58788, 25)

    @test dt[1, 1] === Nullable(1)
    @test isequal(dt[1, 2], Nullable("\$"))
    @test dt[1, 3] === Nullable(1971)
    @test dt[1, 4] === Nullable(121)
    @test isnull(dt[1, 5])
    @test dt[1, 6] === Nullable(6.4)
    @test dt[1, 7] === Nullable(348)
    @test dt[1, 8] === Nullable(4.5)
    @test dt[1, 9] === Nullable(4.5)
    @test dt[1, 10] === Nullable(4.5)
    @test dt[1, 11] === Nullable(4.5)
    @test dt[1, 12] === Nullable(14.5)
    @test dt[1, 13] === Nullable(24.5)
    @test dt[1, 14] === Nullable(24.5)
    @test dt[1, 15] === Nullable(14.5)
    @test dt[1, 16] === Nullable(4.5)
    @test dt[1, 17] === Nullable(4.5)
    @test isequal(dt[1, 18], Nullable{String}())
    @test dt[1, 19] === Nullable(0)
    @test dt[1, 20] === Nullable(0)
    @test dt[1, 21] === Nullable(1)
    @test dt[1, 22] === Nullable(1)
    @test dt[1, 23] === Nullable(0)
    @test dt[1, 24] === Nullable(0)
    @test dt[1, 25] === Nullable(0)

    @test dt[end, 1] === Nullable(58788)
    @test isequal(dt[end, 2], Nullable("xXx: State of the Union"))
    @test dt[end, 3] === Nullable(2005)
    @test dt[end, 4] === Nullable(101)
    @test dt[end, 5] === Nullable(87000000)
    @test dt[end, 6] === Nullable(3.9)
    @test dt[end, 7] === Nullable(1584)
    @test dt[end, 8] === Nullable(24.5)
    @test dt[end, 9] === Nullable(4.5)
    @test dt[end, 10] === Nullable(4.5)
    @test dt[end, 11] === Nullable(4.5)
    @test dt[end, 12] === Nullable(4.5)
    @test dt[end, 13] === Nullable(14.5)
    @test dt[end, 14] === Nullable(4.5)
    @test dt[end, 15] === Nullable(4.5)
    @test dt[end, 16] === Nullable(4.5)
    @test dt[end, 17] === Nullable(14.5)
    @test isequal(dt[end, 18], Nullable("PG-13"))
    @test dt[end, 19] === Nullable(1)
    @test dt[end, 20] === Nullable(0)
    @test dt[end, 21] === Nullable(0)
    @test dt[end, 22] === Nullable(0)
    @test dt[end, 23] === Nullable(0)
    @test dt[end, 24] === Nullable(0)
    @test dt[end, 25] === Nullable(0)

    #test_group("CSV.read handles common separators and infers them from extensions.")

    dt1 = CSV.read("$data/separators/sample_data.csv")
    dt2 = CSV.read("$data/separators/sample_data.tsv", delim='\t')
    dt3 = CSV.read("$data/separators/sample_data.wsv", delim=' ')

    @test isequal(dt1, dt2)
    @test isequal(dt2, dt3)

    #test_group("CSV.read handles common newlines.")

    dt = CSV.read("$data/newlines/os9.csv")
    @test isequal(CSV.read("$data/newlines/osx.csv"), dt)
    @test isequal(CSV.read("$data/newlines/windows.csv"), dt)

    @test isequal(dt, CSV.read("$data/newlines/os9.csv"))
    @test isequal(dt, CSV.read("$data/newlines/osx.csv"))
    @test isequal(dt, CSV.read("$data/newlines/windows.csv"))

    #test_group("CSV.read treats rows as specified.")

    dt1 = CSV.read("$data/skiplines/skipfront.csv", datarow = 4)
    dt2 = CSV.read("$data/skiplines/skipfront_windows.csv", datarow = 4)

    @test isequal(dt1, dt2)

    #test_group("CSV.read handles custom delimiters.")
    dt = CSV.read("$data/decimal/period.csv")
    @test isequal(dt[2, :A], Nullable(0.3))
    @test isequal(dt[2, :B], Nullable(4.0))

    #test_group("CSV.read column names.")
    ns = [:Var1, :Var2, :Var3, :Var4, :Var5]
    dt = CSV.read("$data/typeinference/mixedtypes.csv")
    names!(dt, ns)
    @test isequal(dt, CSV.read("$data/typeinference/mixedtypes.csv", datarow=2, header = ns))

    dt = CSV.read("$data/separators/sample_data.csv", datarow = 1, header = ns[1:3])
    @test isequal(dt[1, :Var1], Nullable(0))

    #test_group("Properties of data frames returned by CSV.read method.")

    filename = "$data/typeinference/standardtypes.csv"
    dt = CSV.read(filename, weakrefstrings=false)
    @test isa(dt[:IntColumn], NullableArray{Int,1})
    @test isa(dt[:IntlikeColumn], NullableArray{Float64,1})
    @test isa(dt[:FloatColumn], NullableArray{Float64,1})
    @test isa(dt[:BoolColumn], NullableArray{String,1})
    @test isa(dt[:StringColumn], NullableArray{String,1})

    filename = "$data/typeinference/mixedtypes.csv"
    dt = CSV.read(filename, weakrefstrings=false)
    @test isa(dt[:c1], NullableArray{String,1})
    @test isequal(dt[:c1][1], Nullable("1"))
    @test isequal(dt[:c1][2], Nullable("2.0"))
    @test isequal(dt[:c1][3], Nullable("true"))
    @test isa(dt[:c2], NullableArray{Float64,1})
    @test isequal(dt[:c2][1], Nullable(1.0))
    @test isequal(dt[:c2][2], Nullable(3.0))
    @test isequal(dt[:c2][3], Nullable(4.5))
    @test isa(dt[:c3], NullableArray{String,1})
    @test isequal(dt[:c3][1], Nullable("0"))
    @test isequal(dt[:c3][2], Nullable("1"))
    @test isequal(dt[:c3][3], Nullable("f"))
    @test isa(dt[:c4], NullableArray{String,1})
    @test isequal(dt[:c4][1], Nullable("t"))
    @test isequal(dt[:c4][2], Nullable("F"))
    @test isequal(dt[:c4][3], Nullable("TRUE"))
    @test isa(dt[:c5], NullableArray{String,1})
    @test isequal(dt[:c5][1], Nullable("False"))
    @test isequal(dt[:c5][2], Nullable("true"))
    @test isequal(dt[:c5][3], Nullable("true"))

    # CSV.read defining column types
    filename = "$data/definedtypes/mixedvartypes.csv"

    dt = CSV.read(filename, weakrefstrings=false)
    @test isa(dt[:n], NullableArray{Int,1})
    @test isequal(dt[:n][1], Nullable(1))
    @test isa(dt[:s], NullableArray{String,1})
    @test isequal(dt[:s][1], Nullable("text"))
    @test isa(dt[:f], NullableArray{Float64,1})
    @test isequal(dt[:f][1], Nullable(2.3))
    @test isa(dt[:b], NullableArray{String,1})
    @test isequal(dt[:b][1], Nullable("T"))

    dt = CSV.read(filename, types = [Int64, String, Float64, String])
    @test isa(dt[:n], NullableArray{Int64,1})
    @test isequal(dt[:n][1], Nullable(1))
    @test isa(dt[:s], NullableArray{String,1})
    @test isequal(dt[:s][1], Nullable("text"))
    @test isequal(dt[:s][4], Nullable("text ole"))
    @test isa(dt[:f], NullableArray{Float64,1})
    @test isequal(dt[:f][1], Nullable(2.3))
    @test isa(dt[:b], NullableArray{String,1})
    @test isequal(dt[:b][1], Nullable("T"))
    @test isequal(dt[:b][2], Nullable("FALSE"))
    @test isequal(dt[:n][1], Nullable(1.0))
    @test isnull(dt[:s][3])
    @test isequal(dt[:f][2], Nullable(0.2))
    @test isequal(dt[:f][3], Nullable(5.7))

    # Test CSV.read with Nullable() and compare to the results
    tf = tempname()
    isfile(tf) && rm(tf)
    dt = DataTable(A = NullableArray(Nullable{Int}[1,Nullable()]),
                   B = NullableArray(Nullable{String}["b", Nullable()]))
    CSV.write(tf, dt)
    @test CSV.read(tf) == dt

    # Test LaTeX export
    dt = DataTable(A = 1:4,
                   B = ["\$10.0", "M&F", "A~B", "\\alpha"],
                   C = [L"\alpha", L"\beta", L"\gamma", L"\sum_{i=1}^n \delta_i"],
                   D = [1.0, 2.0, Nullable(), 3.0]
                   )
    str = """
        \\begin{tabular}{r|cccc}
        \t& A & B & C & D\\\\
        \t\\hline
        \t1 & 1 & \\\$10.0 & \$\\alpha\$ & 1.0 \\\\
        \t2 & 2 & M\\&F & \$\\beta\$ & 2.0 \\\\
        \t3 & 3 & A\\textasciitilde{}B & \$\\gamma\$ &  \\\\
        \t4 & 4 & \\textbackslash{}alpha & \$\\sum_{i=1}^n \\delta_i\$ & 3.0 \\\\
        \\end{tabular}
        """
    @test reprmime(MIME("text/latex"), dt) == str

    #Test HTML output for IJulia and similar
    dt = DataTable(Fish = ["Suzy", "Amir"], Mass = [1.5, Nullable()])
    io = IOBuffer()
    show(io, "text/html", dt)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\"><tr><th></th><th>Fish</th><th>Mass</th></tr><tr><th>1</th><td>Suzy</td><td>1.5</td></tr><tr><th>2</th><td>Amir</td><td>#NULL</td></tr></table>"

    # test limit attribute of IOContext is used
    dt = DataTable(a=collect(1:1000))
    ioc = IOContext(IOBuffer(), displaysize=(10, 10), limit=false)
    show(ioc, "text/html", dt)
    @test length(String(take!(ioc.io))) > 10000

    io = IOBuffer()
    show(io, "text/html", dt)
    @test length(String(take!(io))) < 10000

end
