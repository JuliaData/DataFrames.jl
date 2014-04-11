module TestIO
    using Base.Test
    using DataFrames

    #test_group("Confirm that we can read various file types.")
    testdir = dirname(@__FILE__)

    filenames = ["$testdir/data/blanklines/blanklines.csv",
                 "$testdir/data/compressed/movies.csv.gz",
                 "$testdir/data/newlines/os9.csv",
                 "$testdir/data/newlines/osx.csv",
                 "$testdir/data/newlines/windows.csv",
                 "$testdir/data/newlines/embedded_os9.csv",
                 "$testdir/data/newlines/embedded_osx.csv",
                 "$testdir/data/newlines/embedded_windows.csv",
                 "$testdir/data/padding/space_after_delimiter.csv",
                 "$testdir/data/padding/space_around_delimiter.csv",
                 "$testdir/data/padding/space_before_delimiter.csv",
                 "$testdir/data/quoting/empty.csv",
                 "$testdir/data/quoting/escaping.csv",
                 "$testdir/data/quoting/quotedcommas.csv",
                 "$testdir/data/scaling/10000rows.csv",
                 "$testdir/data/scaling/movies.csv",
                 "$testdir/data/separators/sample_data.csv",
                 "$testdir/data/separators/sample_data.tsv",
                 "$testdir/data/separators/sample_data.wsv",
                 "$testdir/data/typeinference/bool.csv",
                 "$testdir/data/typeinference/standardtypes.csv",
                 "$testdir/data/utf8/corrupt_utf8.csv",
                 "$testdir/data/utf8/short_corrupt_utf8.csv",
                 "$testdir/data/utf8/utf8.csv"]

    for filename in filenames
        try
            df = readtable(filename)
        catch
            error(@sprintf "Failed to read %s\n" filename)
        end
    end

    # Spot check movies.csv file
    filename = "$testdir/data/scaling/movies.csv"
    df = readtable(filename)
    @test df[1, 1] === 1
    # TODO: Figure out why strict equality won't work here
    #       Doesn't seem to be UTF8String vs. ASCIIString
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
    # TODO: Figure out why strict equality won't work here
    #       Doesn't seem to be UTF8String vs. ASCIIString
    @test df[1, 18] == ""
    @test df[1, 19] === 0
    @test df[1, 20] === 0
    @test df[1, 21] === 1
    @test df[1, 22] === 1
    @test df[1, 23] === 0
    @test df[1, 24] === 0
    @test df[1, 24] === 0

    @test df[end, 1] === 58788
    # TODO: Figure out why strict equality won't work here
    #       Doesn't seem to be UTF8String vs. ASCIIString
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
    # TODO: Figure out why strict equality won't work here
    #       Doesn't seem to be UTF8String vs. ASCIIString
    @test df[end, 18] == "PG-13"
    @test df[end, 19] === 1
    @test df[end, 20] === 0
    @test df[end, 21] === 0
    @test df[end, 22] === 0
    @test df[end, 23] === 0
    @test df[end, 24] === 0
    @test df[end, 25] === 0

    df1 = readtable("$testdir/data/comments/before_after_data.csv", allowcomments = true)
    df2 = readtable("$testdir/data/comments/middata.csv", allowcomments = true)
    df3 = readtable("$testdir/data/skiplines/skipfront.csv", skipstart = 3)
    
    @test df1 == df2
    @test df2 == df3

    df1 = readtable("$testdir/data/separators/sample_data.csv")
    df2 = readtable("$testdir/data/separators/sample_data.tsv")
    df3 = readtable("$testdir/data/separators/sample_data.wsv")
    df4 = readtable("$testdir/data/separators/sample_data_white.txt", separator = ' ')

    @test df1 == df2
    @test df2 == df3
    @test df3 == df4

    readtable("$testdir/data/quoting/quotedwhitespace.txt", separator = ' ')

    # TODO: Implement skipping lines at specified row positions
    # readtable("$testdir/data/skiplines/skipbottom.csv", skiprows = [1, 2, 3])

    # TODO: Implement skipping lines at bottom
    # readtable("$testdir/data/skiplines/skipbottom.csv", skipstartlines = 4)

    #test_group("Confirm that we can read a large file.")

    df = DataFrame()

    nrows, ncols = 100_000, 10

    for j in 1:(ncols - 1)
        df[j] = randn(nrows)
    end
    df[ncols] = "A"

    filename = tempname()

    writetable(filename, df, separator = ',')

    df1 = readtable(filename, separator = ',')

    @test isequal(df, df1)

    rm(filename)

    #
    # Lots of rows
    #

    df = DataFrame()

    nrows, ncols = 1_000_000, 1

    for j in 1:ncols
        df[j] = randn(nrows)
    end

    filename = tempname()

    writetable(filename, df, separator = ',')

    df1 = readtable(filename, separator = ',')

    @test isequal(df, df1)

    rm(filename)


    #test_group("Properties of data frames returned by readtable method.")

    # Readtable with makefactors active should only make factors from columns
    # of strings.
    filename = "$testdir/data/factors/mixedvartypes.csv"
    df = readtable(filename, makefactors = true)

    @test typeof(df[:factorvar]) == PooledDataArray{UTF8String,Uint32,1}
    @test typeof(df[:floatvar]) == DataArray{Float64,1}

    # Readtable shouldn't silently drop data when reading highly compressed gz.
    df = readtable("$testdir/data/compressed/1000x2.csv.gz")
    @test size(df) == (1000, 2)

    # Readtable type inference
    filename = "$testdir/data/typeinference/bool.csv"
    df = readtable(filename)
    @test typeof(df[:Name]) == DataArray{UTF8String,1}
    @test typeof(df[:IsMale]) == DataArray{Bool,1}
    @test df[:IsMale][1] == true
    @test df[:IsMale][4] == false
 
    filename = "$testdir/data/typeinference/standardtypes.csv"
    df = readtable(filename)
    @test typeof(df[:IntColumn]) == DataArray{Int,1}
    @test typeof(df[:IntlikeColumn]) == DataArray{Float64,1}
    @test typeof(df[:FloatColumn]) == DataArray{Float64,1}
    @test typeof(df[:BoolColumn]) == DataArray{Bool,1}
    @test typeof(df[:StringColumn]) == DataArray{UTF8String,1}
 
    filename = "$testdir/data/typeinference/mixedtypes.csv"
    df = readtable(filename)
    @test typeof(df[:c1]) == DataArray{UTF8String,1}
    @test df[:c1][1] == "1" 
    @test df[:c1][2] == "2.0" 
    @test df[:c1][3] == "true" 
    @test typeof(df[:c2]) == DataArray{Float64,1}
    @test df[:c2][1] == 1.0 
    @test df[:c2][2] == 3.0 
    @test df[:c2][3] == 4.5 
    @test typeof(df[:c3]) == DataArray{UTF8String,1}
    @test df[:c3][1] == "0" 
    @test df[:c3][2] == "1" 
    @test df[:c3][3] == "f" 
    @test typeof(df[:c4]) == DataArray{Bool,1}
    @test df[:c4][1] == true
    @test df[:c4][2] == false
    @test df[:c4][3] == true
    @test typeof(df[:c5]) == DataArray{UTF8String,1}
    @test df[:c5][1] == "False"
    @test df[:c5][2] == "true"
    @test df[:c5][3] == "true"
 
    # Readtable defining column types
    filename = "$testdir/data/definedtypes/mixedvartypes.csv"
 
    df = readtable(filename)
    @test typeof(df[:n]) == DataArray{Int,1}
    @test df[:n][1] == 1
    @test typeof(df[:s]) == DataArray{UTF8String,1}
    @test df[:s][1] == "text"
    @test typeof(df[:f]) == DataArray{Float64,1}
    @test df[:f][1] == 2.3
    @test typeof(df[:b]) == DataArray{Bool,1}
    @test df[:b][1] == true

    df = readtable(filename,eltypes=[Int64, UTF8String, Float64, Bool])
    @test typeof(df[:n]) == DataArray{Int64,1}
    @test df[:n][1] == 1
    @test typeof(df[:s]) == DataArray{UTF8String,1}
    @test df[:s][1] == "text"
    @test df[:s][4] == "text ole"
    @test typeof(df[:f]) == DataArray{Float64,1}
    @test df[:f][1] == 2.3
    @test typeof(df[:b]) == DataArray{Bool,1}
    @test df[:b][1] == true
    @test df[:b][2] == false

    df = readtable(filename,eltypes=[Int64, UTF8String, Float64, UTF8String])
    @test typeof(df[:n]) == DataArray{Int64,1}
    @test df[:n][1] == 1.0
    @test isna(df[:s][3])
    @test typeof(df[:f]) == DataArray{Float64,1}
    # Float are not converted to int
    @test df[:f][1] == 2.3
    @test df[:f][2] == 0.2
    @test df[:f][3] == 5.7
    @test typeof(df[:b]) == DataArray{UTF8String,1}
    @test df[:b][1] == "T"
    @test df[:b][2] == "FALSE"

    # Readtable ignorepadding argument
    io = IOBuffer("A , \tB  , C\n1 , \t2, 3\n")
    @test readtable(io, ignorepadding=true) == DataFrame(A=1, B=2, C=3)

    # Readtable name normalization
    abnormal = "\u212b"
    ns = [:Å, :B_C, :_end]
    @test !in(symbol(abnormal), ns)

    io = IOBuffer(abnormal*",%_B*\tC*,end\n1,2,3\n")
    @test names(readtable(io)) == ns
end
