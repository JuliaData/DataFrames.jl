module TestIO
    using Base.Test
    using DataFrames, Compat

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
            df = readtable(filename)
        catch
            error(@sprintf "Failed to read %s\n" filename)
        end
    end

    #test_group("We get the right size, types, values for a basic csv.")

    filename = "$data/scaling/movies.csv"
    df = readtable(filename)

    @test size(df) == (58788, 25)

    @test df[1, 1] === 1
    @test df[1, 2] == "\$"
    @test df[1, 3] === 1971
    @test df[1, 4] === 121
    @test df[1, 5] === NA
    @test df[1, 6] === 6.4
    @test df[1, 7] === 348
    @test df[1, 8] === 4.5
    @test df[1, 9] === 4.5
    @test df[1, 10] === 4.5
    @test df[1, 11] === 4.5
    @test df[1, 12] === 14.5
    @test df[1, 13] === 24.5
    @test df[1, 14] === 24.5
    @test df[1, 15] === 14.5
    @test df[1, 16] === 4.5
    @test df[1, 17] === 4.5
    @test df[1, 18] == ""
    @test df[1, 19] === 0
    @test df[1, 20] === 0
    @test df[1, 21] === 1
    @test df[1, 22] === 1
    @test df[1, 23] === 0
    @test df[1, 24] === 0
    @test df[1, 25] === 0

    @test df[end, 1] === 58788
    @test df[end, 2] == "xXx: State of the Union"
    @test df[end, 3] === 2005
    @test df[end, 4] === 101
    @test df[end, 5] === 87000000
    @test df[end, 6] === 3.9
    @test df[end, 7] === 1584
    @test df[end, 8] === 24.5
    @test df[end, 9] === 4.5
    @test df[end, 10] === 4.5
    @test df[end, 11] === 4.5
    @test df[end, 12] === 4.5
    @test df[end, 13] === 14.5
    @test df[end, 14] === 4.5
    @test df[end, 15] === 4.5
    @test df[end, 16] === 4.5
    @test df[end, 17] === 14.5
    @test df[end, 18] == "PG-13"
    @test df[end, 19] === 1
    @test df[end, 20] === 0
    @test df[end, 21] === 0
    @test df[end, 22] === 0
    @test df[end, 23] === 0
    @test df[end, 24] === 0
    @test df[end, 25] === 0

    #test_group("readtable handles common separators and infers them from extensions.")

    df1 = readtable("$data/separators/sample_data.csv")
    df2 = readtable("$data/separators/sample_data.tsv")
    df3 = readtable("$data/separators/sample_data.wsv")
    df4 = readtable("$data/separators/sample_data_white.txt", separator = ' ')

    @test df1 == df2
    @test df2 == df3
    @test df3 == df4

    readtable("$data/quoting/quotedwhitespace.txt", separator = ' ')

    #test_group("readtable handles common newlines.")

    df = readtable("$data/newlines/os9.csv")
    @test isequal(readtable("$data/newlines/osx.csv"), df)
    @test isequal(readtable("$data/newlines/windows.csv"), df)

    @test isequal(df, readtable("$data/newlines/os9.csv", skipblanks = false))
    @test isequal(df, readtable("$data/newlines/osx.csv", skipblanks = false))
    @test isequal(df, readtable("$data/newlines/windows.csv", skipblanks = false))

    #test_group("readtable treats rows as specified.")

    df1 = readtable("$data/comments/before_after_data.csv", allowcomments = true)
    df2 = readtable("$data/comments/middata.csv", allowcomments = true)
    df3 = readtable("$data/skiplines/skipfront.csv", skipstart = 3)
    df4 = readtable("$data/skiplines/skipfront.csv", skipstart = 4, header = false)
    names!(df4, names(df1))
    df5 = readtable("$data/comments/before_after_data_windows.csv", allowcomments = true)
    df6 = readtable("$data/comments/middata_windows.csv", allowcomments = true)
    df7 = readtable("$data/skiplines/skipfront_windows.csv", skipstart = 3)
    df8 = readtable("$data/skiplines/skipfront_windows.csv", skipstart = 4, header = false)
    names!(df8, names(df1))
    # df9 = readtable("$data/skiplines/skipfront.csv", skipstart = 3, skiprows = 5:6)
    # df10 = readtable("$data/skiplines/skipfront.csv", skipstart = 3, header = false, skiprows = [4, 6])
    # names!(df10, names(df1))

    @test df2 == df1
    @test df3 == df1
    @test df4 == df1

    # Windows EOLS
    @test df5 == df1
    @test df6 == df1
    @test df7 == df1
    @test df8 == df1

    # @test df9 == df1[3:end]
    # @test df10 == df1[[1, 3:end]]

    function normalize_eol!(df)
        for (name, col) in eachcol(df)
            if eltype(col) <: AbstractString
                df[name] = map(s -> replace(s, "\r\n", "\n"), col)
            end
        end
        df
    end

    osxpath = "$data/skiplines/complex_osx.csv"
    winpath = "$data/skiplines/complex_windows.csv"

    opts1 = Dict{Any,Any}(:allowcomments => true)
    opts2 = Dict{Any,Any}(:skipstart => 4, :skiprows => [6, 7, 12, 14, 17], :skipblanks => false)

    df1 = readtable(osxpath; opts1...)
    # df2 = readtable(osxpath; opts2...)
    df1w = readtable(winpath; opts1...)
    # df2w = readtable(winpath; opts2...)

    # Normalize line endings in both and test equality
    @test normalize_eol!(df1w) == normalize_eol!(df1)
    # @test normalize_eol!(df2w) == df1

    opts1[:nrows] = 3
    opts2[:nrows] = 3

    @test normalize_eol!(readtable(osxpath; opts1...)) == df1[1:3, :]
    # @test readtable(osxpath; opts2...) == df1[1:3, :]
    @test normalize_eol!(readtable(winpath; opts1...)) == df1[1:3, :]
    # @test readtable(winpath; opts2...) == df1[1:3, :]

    #test_group("readtable handles custom delimiters.")

    readtable("$data/skiplines/skipfront.csv", allowcomments = true, commentmark = '%')

    readtable("$data/separators/sample_data.csv", quotemark = Char[])
    @test_throws ErrorException readtable("$data/newlines/embedded_osx.csv", quotemark = Char[])
    df = readtable("$data/quoting/single.csv", quotemark = ['\''])
    @test df == readtable("$data/quoting/mixed.csv", quotemark = ['\'', '"'])

    # df = readtable("$data/decimal/period.csv")
    # @test df[2, :A] == 0.3
    # @test df[2, :B] == 4.0

    # @test df == readtable("$data/decimal/comma.tsv", decimal = ',')

    #test_group("readtable column names.")

    ns = [:Var1, :Var2, :Var3, :Var4, :Var5]
    df = readtable("$data/typeinference/mixedtypes.csv")
    names!(df, ns)
    @test df == readtable("$data/typeinference/mixedtypes.csv", names = ns)

    df = readtable("$data/separators/sample_data.csv", header = false, names = ns[1:3])
    @test df[1, :Var1] == 0
    df = readtable("$data/separators/sample_data.csv", names = ns[1:3])
    @test df[1, :Var1] == 1

    #test_group("Properties of data frames returned by readtable method.")

    # Readtable ignorepadding
    io = IOBuffer("A , \tB  , C\n1 , \t2, 3\n")
    @test readtable(io, ignorepadding = true) == DataFrame(A = 1, B = 2, C = 3)

    # Readtable c-style escape options

    df = readtable("$data/escapes/escapes.csv", allowescapes = true)
    @test df[1, :V] == "\t\r\n"
    @test df[2, :V] == "\\\\t"
    @test df[3, :V] == "\\\""

    df = readtable("$data/escapes/escapes.csv")
    @test df[1, :V] == "\\t\\r\\n"
    @test df[2, :V] == "\\\\t"
    @test df[3, :V] == "\\\""

    # df = readtable("$data/escapes/escapes.csv", escapechars = ['"'], nrows = 2)
    # @test df[1, :V] == "\\t\\r\\n"
    # @test df[2, :V] == "\\\\\\\\t"

    # Readtable with makefactors active should only make factors from columns
    # of strings.
    filename = "$data/factors/mixedvartypes.csv"
    df = readtable(filename, makefactors = true)

    @test typeof(df[:factorvar]) == PooledDataArray{Compat.UTF8String,UInt32,1}
    @test typeof(df[:floatvar]) == DataArray{Float64,1}

    # Readtable shouldn't silently drop data when reading highly compressed gz.
    df = readtable("$data/compressed/1000x2.csv.gz")
    @test size(df) == (1000, 2)

    # Readtable type inference
    filename = "$data/typeinference/bool.csv"
    df = readtable(filename)
    @test typeof(df[:Name]) == DataArray{Compat.UTF8String,1}
    @test typeof(df[:IsMale]) == DataArray{Bool,1}
    @test df[:IsMale][1] == true
    @test df[:IsMale][4] == false

    filename = "$data/typeinference/standardtypes.csv"
    df = readtable(filename)
    @test typeof(df[:IntColumn]) == DataArray{Int,1}
    @test typeof(df[:IntlikeColumn]) == DataArray{Float64,1}
    @test typeof(df[:FloatColumn]) == DataArray{Float64,1}
    @test typeof(df[:BoolColumn]) == DataArray{Bool,1}
    @test typeof(df[:StringColumn]) == DataArray{Compat.UTF8String,1}

    filename = "$data/typeinference/mixedtypes.csv"
    df = readtable(filename)
    @test typeof(df[:c1]) == DataArray{Compat.UTF8String,1}
    @test df[:c1][1] == "1"
    @test df[:c1][2] == "2.0"
    @test df[:c1][3] == "true"
    @test typeof(df[:c2]) == DataArray{Float64,1}
    @test df[:c2][1] == 1.0
    @test df[:c2][2] == 3.0
    @test df[:c2][3] == 4.5
    @test typeof(df[:c3]) == DataArray{Compat.UTF8String,1}
    @test df[:c3][1] == "0"
    @test df[:c3][2] == "1"
    @test df[:c3][3] == "f"
    @test typeof(df[:c4]) == DataArray{Bool,1}
    @test df[:c4][1] == true
    @test df[:c4][2] == false
    @test df[:c4][3] == true
    @test typeof(df[:c5]) == DataArray{Compat.UTF8String,1}
    @test df[:c5][1] == "False"
    @test df[:c5][2] == "true"
    @test df[:c5][3] == "true"

    # Readtable defining column types
    filename = "$data/definedtypes/mixedvartypes.csv"

    df = readtable(filename)
    @test typeof(df[:n]) == DataArray{Int,1}
    @test df[:n][1] == 1
    @test typeof(df[:s]) == DataArray{Compat.UTF8String,1}
    @test df[:s][1] == "text"
    @test typeof(df[:f]) == DataArray{Float64,1}
    @test df[:f][1] == 2.3
    @test typeof(df[:b]) == DataArray{Bool,1}
    @test df[:b][1] == true

    df = readtable(filename, eltypes = [Int64, Compat.UTF8String, Float64, Bool])
    @test typeof(df[:n]) == DataArray{Int64,1}
    @test df[:n][1] == 1
    @test typeof(df[:s]) == DataArray{Compat.UTF8String,1}
    @test df[:s][1] == "text"
    @test df[:s][4] == "text ole"
    @test typeof(df[:f]) == DataArray{Float64,1}
    @test df[:f][1] == 2.3
    @test typeof(df[:b]) == DataArray{Bool,1}
    @test df[:b][1] == true
    @test df[:b][2] == false

    df = readtable(filename, eltypes = [Int64, Compat.UTF8String, Float64, Compat.UTF8String])
    @test typeof(df[:n]) == DataArray{Int64,1}
    @test df[:n][1] == 1.0
    @test isna(df[:s][3])
    @test typeof(df[:f]) == DataArray{Float64,1}
    # Float are not converted to int
    @test df[:f][1] == 2.3
    @test df[:f][2] == 0.2
    @test df[:f][3] == 5.7
    @test typeof(df[:b]) == DataArray{Compat.UTF8String,1}
    @test df[:b][1] == "T"
    @test df[:b][2] == "FALSE"

    # Readtable name normalization
    abnormal = "\u212b"
    ns = [:Å, :_B_C_, :_end]
    @test !in(Symbol(abnormal), ns)

    io = IOBuffer(abnormal*",%_B*\tC*,end\n1,2,3\n")
    @test names(readtable(io)) == ns

    # With normalization disabled
    io = IOBuffer(abnormal*",%_B*\tC*,end\n1,2,3\n")
    @test names(readtable(io, normalizenames=false)) == [Symbol(abnormal),Symbol("%_B*\tC*"),:end]

    # Test writetable with NA and compare to the results
    tf = tempname()
    isfile(tf) && rm(tf)
    df = DataFrame(A = @data([1,NA]), B = @data(["b", NA]))
    writetable(tf, df)
    @test readcsv(tf) == ["A" "B"; 1 "b"; "NA" "NA"]

    # Test writetable with nastring set and compare to the results
    isfile(tf) && rm(tf)
    writetable(tf, df, nastring="none")
    @test readcsv(tf) == ["A" "B"; 1 "b"; "none" "none"]
    rm(tf)

    # Test writetable with append
    df1 = DataFrame(a = @data([1, 2, 3]), b = @data([4, 5, 6]))
    df2 = DataFrame(a = @data([1, 2, 3]), b = @data([4, 5, 6]))
    df3 = DataFrame(a = @data([1, 2, 3]), c = @data([4, 5, 6])) # 2nd column mismatch
    df3b = DataFrame(a = @data([1, 2, 3]), b = @data([4, 5, 6]), c = @data([4, 5, 6])) # number of columns mismatch


    # Would use joinpath(tempdir(), randstring()) to get around tempname
    # creating a file on Windows, but Julia 0.3 has no srand() to unset the
    # seed set in test/data.jl -- annoying for local testing.
    tf = tempname()
    isfile(tf) && rm(tf)

    # Written as normal if file doesn't exist
    writetable(tf, df1, append = true)
    @test readtable(tf) == df1

    # Written as normal if file is empty
    open(io -> print(io, ""), tf, "w")
    writetable(tf, df1, append = true)
    @test readtable(tf) == df1

    # Appends to existing file if append == true
    writetable(tf, df1)
    writetable(tf, df2, header = false, append = true)
    @test readtable(tf) == vcat(df1, df2)

    # Overwrites file if append == false
    writetable(tf, df1)
    writetable(tf, df2)
    @test readtable(tf) == df2

    # Enforces matching column names iff append == true && header == true
    writetable(tf, df2)
    @test_throws KeyError writetable(tf, df3, append = true)
    writetable(tf, df3, header = false, append = true)

    # Enforces matching column count if append == true
    writetable(tf, df3)
    @test_throws DimensionMismatch writetable(tf, df3b, header = false, append = true)
    rm(tf)

    # Quotemarks are escaped
    tf = tempname()
    isfile(tf) && rm(tf)

    df = DataFrame(a = ["who's"]) # We have a ' in our string

    # Make sure the ' doesn't get escaped for no reason
    writetable(tf, df)
    @test readtable(tf) == df

    # Make sure the ' does get escaped when needed
    writetable(tf, df, quotemark='\'')
    @test readstring(tf) == "'a'\n'who\\'s'\n"
    rm(tf)

    ### Tests for nonstandard string literals
    # Test basic @csv_str usage
    df1 = csv"""
        name,  age, squidPerWeek
        Alice,  36,         3.14
        Bob,    24,         0
        Carol,  58,         2.71
        Eve,    49,         7.77
        """
    @test size(df1) == (4, 3)
    @test names(df1) == [:name, :age, :squidPerWeek]
    @test df1[1] == ["Alice","Bob","Carol","Eve"]
    @test df1[2] == [36,24,58,49]
    @test df1[3] == [3.14,0,2.71,7.77]
    @test typeof(df1[1]) <: DataArray

    # Test @wsv_str
    df2 = wsv"""
        name  age squidPerWeek
        Alice  36         3.14
        Bob    24         0
        Carol  58         2.71
        Eve    49         7.77
        """
    @test df2 == df1

    # Test @tsv_str
    df3 = tsv"""
        name	age	squidPerWeek
        Alice	36	3.14
        Bob	24	0
        Carol	58	2.71
        Eve	49	7.77
        """
    @test df3 == df1

    # csv2 can't be tested until non-'.' decimals are implemented
    #df4 = csv2"""
    #    name;  age; squidPerWeek
    #    Alice;  36;         3,14
    #    Bob;    24;         0
    #    Carol;  58;         2,71
    #    Eve;    49;         7,77
    #    """
    #@test df4 == df1

    # Test 'f' flag
    df5 = csv"""
        name,  age, squidPerWeek
        Alice,  36,         3.14
        Bob,    24,         0
        Carol,  58,         2.71
        Eve,    49,         7.77
        """f
    @test typeof(df5[1]) <: PooledDataArray

    # Test 'c' flag
    df6 = csv"""
        name,  age, squidPerWeek
        Alice,  36,         3.14
        Bob,    24,         0
        #Carol,  58,         2.71
        Eve,    49,         7.77
        """c
    @test df6 == df1[[1,2,4],:]

    # Test 'H' flag
    df7 = csv"""
        Alice,  36,         3.14
        Bob,    24,         0
        Carol,  58,         2.71
        Eve,    49,         7.77
        """H
    @test names(df7) == [:x1,:x2,:x3]
    @test Array(df7) == Array(df1)

    # Test multiple flags at once
    df8 = csv"""
        Alice,  36,         3.14
        Bob,    24,         0
        #Carol,  58,         2.71
        Eve,    49,         7.77
        """fcH
    @test typeof(df8[1]) <: PooledDataArray
    @test names(df8) == [:x1,:x2,:x3]
    @test Array(df8) == Array(df1[[1,2,4],:])

    # Test invalid flag
    # Need to wrap macro call inside eval to prevent the error from being
    # thrown prematurely
    @test_throws ArgumentError eval(:(csv"foo,bar"a))
end
