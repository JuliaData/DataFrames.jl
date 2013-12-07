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

end
