module TestIO
    using Base.Test
    using DataArrays
    using DataFrames

    #test_group("Confirm that we can read various file types.")

    filenames = ["test/data/blanklines/blanklines.csv",
                 "test/data/compressed/movies.csv.gz",
                 "test/data/newlines/os9.csv",
                 "test/data/newlines/osx.csv",
                 "test/data/newlines/windows.csv",
                 "test/data/newlines/embedded_os9.csv",
                 "test/data/newlines/embedded_osx.csv",
                 "test/data/newlines/embedded_windows.csv",
                 "test/data/padding/space_after_delimiter.csv",
                 "test/data/padding/space_around_delimiter.csv",
                 "test/data/padding/space_before_delimiter.csv",
                 "test/data/quoting/empty.csv",
                 "test/data/quoting/escaping.csv",
                 "test/data/quoting/quotedcommas.csv",
                 "test/data/scaling/10000rows.csv",
                 "test/data/scaling/movies.csv",
                 "test/data/separators/sample_data.csv",
                 "test/data/separators/sample_data.tsv",
                 "test/data/separators/sample_data.wsv",
                 "test/data/typeinference/bool.csv",
                 "test/data/typeinference/standardtypes.csv",
                 "test/data/utf8/corrupt_utf8.csv",
                 "test/data/utf8/short_corrupt_utf8.csv",
                 "test/data/utf8/utf8.csv"]

    for filename in filenames
        try
            df = readtable(filename)
        catch
            error(@sprintf "Failed to read %s\n" filename)
        end
    end

    readtable("test/data/comments/before_after_data.csv", allowcomments = true)
    readtable("test/data/comments/middata.csv", allowcomments = true)
    readtable("test/data/skiplines/skipfront.csv", skipstart = 3)
    
    readtable("test/data/separators/sample_data_white.txt",separator=' ')
    readtable("test/data/quoting/quotedwhitespace.txt", separator=' ')

    # TODO: Implement skipping lines at specified row positions
    # readtable("test/data/skiplines/skipbottom.csv", skiprows = [1, 2, 3])

    # TODO: Implement skipping lines at bottom
    # readtable("test/data/skiplines/skipbottom.csv", skipstartlines = 4)

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

    @assert isequal(df, df1)

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

    @assert isequal(df, df1)

    rm(filename)


    #test_group("Properties of data frames returned by readtable method.")

    # Readtable with makefactors active should only make factors from columns
    # of strings.
    filename = "test/data/factors/mixedvartypes.csv"
    df = readtable(filename, makefactors = true)

    @assert typeof(df["factorvar"]) == PooledDataArray{UTF8String,Uint32,1}
    @assert typeof(df["floatvar"]) == DataArray{Float64,1}

    # Readtable shouldn't silently drop data when reading highly compressed gz.
    df = readtable("test/data/compressed/1000x2.csv.gz")
    @assert size(df) == (1000, 2)

    # Readtable type inference
    filename = "test/data/typeinference/bool.csv"
    df = readtable(filename)
    @assert typeof(df["Name"]) == DataArray{UTF8String,1}
    @assert typeof(df["IsMale"]) == DataArray{Bool,1}
    @assert df["IsMale"][1] == true
    @assert df["IsMale"][4] == false
 
    filename = "test/data/typeinference/standardtypes.csv"
    df = readtable(filename)
    @assert typeof(df["IntColumn"]) == DataArray{Int64,1}
    @assert typeof(df["IntlikeColumn"]) == DataArray{Float64,1}
    @assert typeof(df["FloatColumn"]) == DataArray{Float64,1}
    @assert typeof(df["BoolColumn"]) == DataArray{Bool,1}
    @assert typeof(df["StringColumn"]) == DataArray{UTF8String,1}
 
    filename = "test/data/typeinference/mixedtypes.csv"
    df = readtable(filename)
    @assert typeof(df["c1"]) == DataArray{UTF8String,1}
    @assert df["c1"][1] == "1" 
    @assert df["c1"][2] == "2.0" 
    @assert df["c1"][3] == "true" 
    @assert typeof(df["c2"]) == DataArray{Float64,1}
    @assert df["c2"][1] == 1.0 
    @assert df["c2"][2] == 3.0 
    @assert df["c2"][3] == 4.5 
    @assert typeof(df["c3"]) == DataArray{UTF8String,1}
    @assert df["c3"][1] == "0" 
    @assert df["c3"][2] == "1" 
    @assert df["c3"][3] == "f" 
    @assert typeof(df["c4"]) == DataArray{Bool,1}
    @assert df["c4"][1] == true
    @assert df["c4"][2] == false
    @assert df["c4"][3] == true
    @assert typeof(df["c5"]) == DataArray{UTF8String,1}
    @assert df["c5"][1] == "False"
    @assert df["c5"][2] == "true"
    @assert df["c5"][3] == "true"
 
    # Readtable defining column types
    filename = "test/data/definedtypes/mixedvartypes.csv"
 
    df = readtable(filename)
    @assert typeof(df["n"]) == DataArray{Int64,1}
    @assert df["n"][1] == 1
    @assert typeof(df["s"]) == DataArray{UTF8String,1}
    @assert df["s"][1] == "text"
    @assert typeof(df["f"]) == DataArray{Float64,1}
    @assert df["f"][1] == 2.3
    @assert typeof(df["b"]) == DataArray{Bool,1}
    @assert df["b"][1] == true

    df = readtable(filename,coltypes=[Int64, UTF8String, Float64, Bool])
    @assert typeof(df["n"]) == DataArray{Int64,1}
    @assert df["n"][1] == 1
    @assert typeof(df["s"]) == DataArray{UTF8String,1}
    @assert df["s"][1] == "text"
    @assert df["s"][4] == "text ole"
    @assert typeof(df["f"]) == DataArray{Float64,1}
    @assert df["f"][1] == 2.3
    @assert typeof(df["b"]) == DataArray{Bool,1}
    @assert df["b"][1] == true
    @assert df["b"][2] == false

    df = readtable(filename,coltypes=[Int64, UTF8String, Float64, UTF8String])
    @assert typeof(df["n"]) == DataArray{Int64,1}
    @assert df["n"][1] == 1.0
    @assert isna(df["s"][3])
    @assert typeof(df["f"]) == DataArray{Float64,1}
    # Float are not converted to int
    @assert df["f"][1] == 2.3
    @assert df["f"][2] == 0.2
    @assert df["f"][3] == 5.7
    @assert typeof(df["b"]) == DataArray{UTF8String,1}
    @assert df["b"][1] == "T"
    @assert df["b"][2] == "FALSE"
end
