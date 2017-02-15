module TestIO
    using Base.Test
    using DataTables, Compat
    using LaTeXStrings

    #test_group("We can read various file types.")

    data = joinpath(dirname(@__FILE__), "data")

    filenames = ["$data/blanklines/blanklines.csv",
                 "$data/compressed/movies.csv.gz",
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
            dt = readtable(filename)
        catch
            error(@sprintf "Failed to read %s\n" filename)
        end
    end

    #test_group("We get the right size, types, values for a basic csv.")

    filename = "$data/scaling/movies.csv"
    io = open(filename)
    dt = readtable(io)
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
    @test isequal(dt[1, 18], Nullable(""))
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

    #test_group("readtable handles common separators and infers them from extensions.")

    dt1 = readtable("$data/separators/sample_data.csv")
    dt2 = readtable("$data/separators/sample_data.tsv")
    dt3 = readtable("$data/separators/sample_data.wsv")
    dt4 = readtable("$data/separators/sample_data_white.txt", separator = ' ')

    @test isequal(dt1, dt2)
    @test isequal(dt2, dt3)
    @test isequal(dt3, dt4)

    readtable("$data/quoting/quotedwhitespace.txt", separator = ' ')

    #test_group("readtable handles common newlines.")

    dt = readtable("$data/newlines/os9.csv")
    @test isequal(readtable("$data/newlines/osx.csv"), dt)
    @test isequal(readtable("$data/newlines/windows.csv"), dt)

    @test isequal(dt, readtable("$data/newlines/os9.csv", skipblanks = false))
    @test isequal(dt, readtable("$data/newlines/osx.csv", skipblanks = false))
    @test isequal(dt, readtable("$data/newlines/windows.csv", skipblanks = false))

    #test_group("readtable treats rows as specified.")

    dt1 = readtable("$data/comments/before_after_data.csv", allowcomments = true)
    dt2 = readtable("$data/comments/middata.csv", allowcomments = true)
    dt3 = readtable("$data/skiplines/skipfront.csv", skipstart = 3)
    dt4 = readtable("$data/skiplines/skipfront.csv", skipstart = 4, header = false)
    names!(dt4, names(dt1))
    dt5 = readtable("$data/comments/before_after_data_windows.csv", allowcomments = true)
    dt6 = readtable("$data/comments/middata_windows.csv", allowcomments = true)
    dt7 = readtable("$data/skiplines/skipfront_windows.csv", skipstart = 3)
    dt8 = readtable("$data/skiplines/skipfront_windows.csv", skipstart = 4, header = false)
    names!(dt8, names(dt1))
    # dt9 = readtable("$data/skiplines/skipfront.csv", skipstart = 3, skiprows = 5:6)
    # dt10 = readtable("$data/skiplines/skipfront.csv", skipstart = 3, header = false, skiprows = [4, 6])
    # names!(dt10, names(dt1))

    @test isequal(dt2, dt1)
    @test isequal(dt3, dt1)
    @test isequal(dt4, dt1)

    # Windows EOLS
    @test isequal(dt5, dt1)
    @test isequal(dt6, dt1)
    @test isequal(dt7, dt1)
    @test isequal(dt8, dt1)

    # @test isequal(dt9, dt1[3:end])
    # @test isequal(dt10, dt1[[1, 3:end]])

    function normalize_eol!(dt)
        for (name, col) in eachcol(dt)
            if eltype(col) <: AbstractString ||
               (isa(col, NullableArray) && eltype(eltype(col)) <: AbstractString)
                dt[name] = map(s -> replace(s, "\r\n", "\n"), col)
            end
        end
        dt
    end

    osxpath = "$data/skiplines/complex_osx.csv"
    winpath = "$data/skiplines/complex_windows.csv"

    opts1 = @compat Dict{Any,Any}(:allowcomments => true)
    opts2 = @compat Dict{Any,Any}(:skipstart => 4, :skiprows => [6, 7, 12, 14, 17], :skipblanks => false)

    dt1 = readtable(osxpath; opts1...)
    # dt2 = readtable(osxpath; opts2...)
    dt1w = readtable(winpath; opts1...)
    # dt2w = readtable(winpath; opts2...)

    # Normalize line endings in both and test equality
    @test isequal(normalize_eol!(dt1w), normalize_eol!(dt1))
    # @test normalize_eol!(dt2w) == dt1

    opts1[:nrows] = 3
    opts2[:nrows] = 3

    @test isequal(normalize_eol!(readtable(osxpath; opts1...)), dt1[1:3, :])
    # @test isequalreadtable(osxpath; opts2...), dt1[1:3, :]
    @test isequal(normalize_eol!(readtable(winpath; opts1...)), dt1[1:3, :])
    # @test isequalreadtable(winpath; opts2...), dt1[1:3, :])

    #test_group("readtable handles custom delimiters.")

    readtable("$data/skiplines/skipfront.csv", allowcomments = true, commentmark = '%')

    readtable("$data/separators/sample_data.csv", quotemark = Char[])
    @test_throws ErrorException readtable("$data/newlines/embedded_osx.csv", quotemark = Char[])
    dt = readtable("$data/quoting/single.csv", quotemark = ['\''])
    @test isequal(dt, readtable("$data/quoting/mixed.csv", quotemark = ['\'', '"']))

    # dt = readtable("$data/decimal/period.csv")
    # @test isequaldt[2, :A], 0.3)
    # @test isequaldt[2, :B], 4.0)

    # @test isequal(dt, readtable("$data/decimal/comma.tsv", decimal = ','))

    #test_group("readtable column names.")

    ns = [:Var1, :Var2, :Var3, :Var4, :Var5]
    dt = readtable("$data/typeinference/mixedtypes.csv")
    names!(dt, ns)
    @test isequal(dt, readtable("$data/typeinference/mixedtypes.csv", names = ns))

    dt = readtable("$data/separators/sample_data.csv", header = false, names = ns[1:3])
    @test isequal(dt[1, :Var1], Nullable(0))
    dt = readtable("$data/separators/sample_data.csv", names = ns[1:3])
    @test isequal(dt[1, :Var1], Nullable(1))

    #test_group("Properties of data frames returned by readtable method.")

    # Readtable ignorepadding
    io = IOBuffer("A , \tB  , C\n1 , \t2, 3\n")
    @test isequal(readtable(io, ignorepadding = true), DataTable(A = 1, B = 2, C = 3))

    # Readtable c-style escape options

    dt = readtable("$data/escapes/escapes.csv", allowescapes = true)
    @test isequal(dt[1, :V], Nullable("\t\r\n"))
    @test isequal(dt[2, :V], Nullable("\\\\t"))
    @test isequal(dt[3, :V], Nullable("\\\""))

    dt = readtable("$data/escapes/escapes.csv")
    @test isequal(dt[1, :V], Nullable("\\t\\r\\n"))
    @test isequal(dt[2, :V], Nullable("\\\\t"))
    @test isequal(dt[3, :V], Nullable("\\\""))

    # dt = readtable("$data/escapes/escapes.csv", escapechars = ['"'], nrows = 2)
    # @test isequal(dt[1, :V], "\\t\\r\\n")
    # @test isequal(dt[2, :V], "\\\\\\\\t")

    # Readtable with makefactors active should only make factors from columns
    # of strings.
    filename = "$data/factors/mixedvartypes.csv"
    dt = readtable(filename, makefactors = true)

    @test isa(dt[:factorvar], NullableCategoricalArray{Compat.UTF8String,1})
    @test isa(dt[:floatvar], NullableArray{Float64,1})

    # Readtable shouldn't silently drop data when reading highly compressed gz.
    dt = readtable("$data/compressed/1000x2.csv.gz")
    @test size(dt) == (1000, 2)

    # Readtable type inference
    filename = "$data/typeinference/bool.csv"
    dt = readtable(filename)
    @test isa(dt[:Name], NullableArray{Compat.UTF8String,1})
    @test isa(dt[:IsMale], NullableArray{Bool,1})
    @test get(dt[:IsMale][1])
    @test !get(dt[:IsMale][4])

    filename = "$data/typeinference/standardtypes.csv"
    dt = readtable(filename)
    @test isa(dt[:IntColumn], NullableArray{Int,1})
    @test isa(dt[:IntlikeColumn], NullableArray{Float64,1})
    @test isa(dt[:FloatColumn], NullableArray{Float64,1})
    @test isa(dt[:BoolColumn], NullableArray{Bool,1})
    @test isa(dt[:StringColumn], NullableArray{Compat.UTF8String,1})

    filename = "$data/typeinference/mixedtypes.csv"
    dt = readtable(filename)
    @test isa(dt[:c1], NullableArray{Compat.UTF8String,1})
    @test isequal(dt[:c1][1], Nullable("1"))
    @test isequal(dt[:c1][2], Nullable("2.0"))
    @test isequal(dt[:c1][3], Nullable("true"))
    @test isa(dt[:c2], NullableArray{Float64,1})
    @test isequal(dt[:c2][1], Nullable(1.0))
    @test isequal(dt[:c2][2], Nullable(3.0))
    @test isequal(dt[:c2][3], Nullable(4.5))
    @test isa(dt[:c3], NullableArray{Compat.UTF8String,1})
    @test isequal(dt[:c3][1], Nullable("0"))
    @test isequal(dt[:c3][2], Nullable("1"))
    @test isequal(dt[:c3][3], Nullable("f"))
    @test isa(dt[:c4], NullableArray{Bool,1})
    @test isequal(dt[:c4][1], Nullable(true))
    @test isequal(dt[:c4][2], Nullable(false))
    @test isequal(dt[:c4][3], Nullable(true))
    @test isa(dt[:c5], NullableArray{Compat.UTF8String,1})
    @test isequal(dt[:c5][1], Nullable("False"))
    @test isequal(dt[:c5][2], Nullable("true"))
    @test isequal(dt[:c5][3], Nullable("true"))

    # Readtable defining column types
    filename = "$data/definedtypes/mixedvartypes.csv"

    dt = readtable(filename)
    @test isa(dt[:n], NullableArray{Int,1})
    @test isequal(dt[:n][1], Nullable(1))
    @test isa(dt[:s], NullableArray{Compat.UTF8String,1})
    @test isequal(dt[:s][1], Nullable("text"))
    @test isa(dt[:f], NullableArray{Float64,1})
    @test isequal(dt[:f][1], Nullable(2.3))
    @test isa(dt[:b], NullableArray{Bool,1})
    @test isequal(dt[:b][1], Nullable(true))

    dt = readtable(filename, eltypes = [Int64, Compat.UTF8String, Float64, Bool])
    @test isa(dt[:n], NullableArray{Int64,1})
    @test isequal(dt[:n][1], Nullable(1))
    @test isa(dt[:s], NullableArray{Compat.UTF8String,1})
    @test isequal(dt[:s][1], Nullable("text"))
    @test isequal(dt[:s][4], Nullable("text ole"))
    @test isa(dt[:f], NullableArray{Float64,1})
    @test isequal(dt[:f][1], Nullable(2.3))
    @test isa(dt[:b], NullableArray{Bool,1})
    @test isequal(dt[:b][1], Nullable(true))
    @test isequal(dt[:b][2], Nullable(false))

    dt = readtable(filename, eltypes = [Int64, Compat.UTF8String, Float64, Compat.UTF8String])
    @test isa(dt[:n], NullableArray{Int64,1})
    @test isequal(dt[:n][1], Nullable(1.0))
    @test isnull(dt[:s][3])
    @test isa(dt[:f], NullableArray{Float64,1})
    # Float are not converted to int
    @test isequal(dt[:f][1], Nullable(2.3))
    @test isequal(dt[:f][2], Nullable(0.2))
    @test isequal(dt[:f][3], Nullable(5.7))
    @test isa(dt[:b], NullableArray{Compat.UTF8String,1})
    @test isequal(dt[:b][1], Nullable("T"))
    @test isequal(dt[:b][2], Nullable("FALSE"))

    # Readtable name normalization
    abnormal = "\u212b"
    ns = [:Å, :_B_C_, :_end]
    @test !in(Symbol(abnormal), ns)

    io = IOBuffer(abnormal*",%_B*\tC*,end\n1,2,3\n")
    @test names(readtable(io)) == ns

    # With normalization disabled
    io = IOBuffer(abnormal*",%_B*\tC*,end\n1,2,3\n")
    @test names(readtable(io, normalizenames=false)) == [Symbol(abnormal),Symbol("%_B*\tC*"),:end]

    # Test writetable with Nullable() and compare to the results
    tf = tempname()
    isfile(tf) && rm(tf)
    dt = DataTable(A = NullableArray(Nullable{Int}[1,Nullable()]),
                   B = NullableArray(Nullable{String}["b", Nullable()]))
    writetable(tf, dt)
    @test readcsv(tf) == ["A" "B"; 1 "b"; "NULL" "NULL"]

    # Test writetable with nastring set and compare to the results
    isfile(tf) && rm(tf)
    writetable(tf, dt, nastring="none")
    @test readcsv(tf) == ["A" "B"; 1 "b"; "none" "none"]
    rm(tf)

    # Test writetable with append
    dt1 = DataTable(a = NullableArray([1, 2, 3]), b = NullableArray([4, 5, 6]))
    dt2 = DataTable(a = NullableArray([1, 2, 3]), b = NullableArray([4, 5, 6]))
    dt3 = DataTable(a = NullableArray([1, 2, 3]), c = NullableArray([4, 5, 6])) # 2nd column mismatch
    dt3b = DataTable(a = NullableArray([1, 2, 3]), b = NullableArray([4, 5, 6]), c = NullableArray([4, 5, 6])) # number of columns mismatch


    # Would use joinpath(tempdir(), randstring()) to get around tempname
    # creating a file on Windows, but Julia 0.3 has no srand() to unset the
    # seed set in test/data.jl -- annoying for local testing.
    tf = tempname()
    isfile(tf) && rm(tf)

    # Written as normal if file doesn't exist
    writetable(tf, dt1, append = true)
    @test isequal(readtable(tf), dt1)

    # Written as normal if file is empty
    open(io -> print(io, ""), tf, "w")
    writetable(tf, dt1, append = true)
    @test isequal(readtable(tf), dt1)

    # Appends to existing file if append == true
    writetable(tf, dt1)
    writetable(tf, dt2, header = false, append = true)
    @test isequal(readtable(tf), vcat(dt1, dt2))

    # Overwrites file if append == false
    writetable(tf, dt1)
    writetable(tf, dt2)
    @test isequal(readtable(tf), dt2)

    # Enforces matching column names iff append == true && header == true
    writetable(tf, dt2)
    @test_throws KeyError writetable(tf, dt3, append = true)
    writetable(tf, dt3, header = false, append = true)

    # Enforces matching column count if append == true
    writetable(tf, dt3)
    @test_throws DimensionMismatch writetable(tf, dt3b, header = false, append = true)
    rm(tf)

    # Quotemarks are escaped
    tf = tempname()
    isfile(tf) && rm(tf)

    dt = DataTable(a = ["who's"]) # We have a ' in our string

    # Make sure the ' doesn't get escaped for no reason
    writetable(tf, dt)
    @test isequal(readtable(tf), dt)

    # Make sure the ' does get escaped when needed
    writetable(tf, dt, quotemark='\'')
    @test readstring(tf) == "'a'\n'who\\'s'\n"
    rm(tf)

    ### Tests for nonstandard string literals
    # Test basic @csv_str usage
    dt1 = csv"""
        name,  age, squidPerWeek
        Alice,  36,         3.14
        Bob,    24,         0
        Carol,  58,         2.71
        Eve,    49,         7.77
        """
    @test size(dt1) == (4, 3)
    @test names(dt1) == [:name, :age, :squidPerWeek]
    @test isequal(dt1[1], NullableArray(["Alice","Bob","Carol","Eve"]))
    @test isequal(dt1[2], NullableArray([36,24,58,49]))
    @test isequal(dt1[3], NullableArray([3.14,0,2.71,7.77]))
    @test isa(dt1[1], NullableArray{Compat.UTF8String,1})

    # Test @wsv_str
    dt2 = wsv"""
        name  age squidPerWeek
        Alice  36         3.14
        Bob    24         0
        Carol  58         2.71
        Eve    49         7.77
        """
    @test isequal(dt2, dt1)

    # Test @tsv_str
    dt3 = tsv"""
        name	age	squidPerWeek
        Alice	36	3.14
        Bob	24	0
        Carol	58	2.71
        Eve	49	7.77
        """
    @test isequal(dt3, dt1)

    # csv2 can't be tested until non-'.' decimals are implemented
    #dt4 = csv2"""
    #    name;  age; squidPerWeek
    #    Alice;  36;         3,14
    #    Bob;    24;         0
    #    Carol;  58;         2,71
    #    Eve;    49;         7,77
    #    """
    #@test isequal(dt4, dt1)

    # Test 'f' flag
    dt5 = csv"""
        name,  age, squidPerWeek
        Alice,  36,         3.14
        Bob,    24,         0
        Carol,  58,         2.71
        Eve,    49,         7.77
        """f
    @test isa(dt5[1], NullableCategoricalArray{Compat.UTF8String,1})

    # Test 'c' flag
    dt6 = csv"""
        name,  age, squidPerWeek
        Alice,  36,         3.14
        Bob,    24,         0
        #Carol,  58,         2.71
        Eve,    49,         7.77
        """c
    @test isequal(dt6, dt1[[1,2,4],:])

    # Test 'H' flag
    dt7 = csv"""
        Alice,  36,         3.14
        Bob,    24,         0
        Carol,  58,         2.71
        Eve,    49,         7.77
        """H
    @test names(dt7) == [:x1,:x2,:x3]
    names!(dt7, names(dt1))
    @test isequal(dt7, dt1)

    # Test multiple flags at once
    dt8 = csv"""
        Alice,  36,         3.14
        Bob,    24,         0
        #Carol,  58,         2.71
        Eve,    49,         7.77
        """fcH
    @test isa(dt8[1], NullableCategoricalArray{Compat.UTF8String,1})
    @test names(dt8) == [:x1,:x2,:x3]
    names!(dt8, names(dt1))
    @test isequal(dt8, dt1[[1,2,4],:])

    # Test invalid flag
    # Need to wrap macro call inside eval to prevent the error from being
    # thrown prematurely
    @test_throws ArgumentError eval(:(csv"foo,bar"a))

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
