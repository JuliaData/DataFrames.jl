#' @exported
#' @description
#'
#' Returns a string summary of an AbstractDataFrame in a standardized
#' form. For example, a standard DataFrame with 10 rows and 5 columns
#' will be summarized as "10x5 DataFrame".
#'
#' @param adf::AbstractDataFrame The AbstractDataFrame to be summarized.
#'
#' @returns res::UTF8String The summary of `adf`.
#'
#' @examples
#'
#' summary(DataFrame(A = 1:10))
function Base.summary(adf::AbstractDataFrame) # -> UTF8String
    nrows, ncols = size(adf)
    return utf8(@sprintf "%dx%d %s" nrows ncols typeof(adf))
end

#' @description
#'
#' Determine the number of UTF8 characters that would be used to
#' render a value.
#'
#' @param x::Any A value whose string width will be computed.
#'
#' @returns w::Int The width of the string.
#'
#' @examples
#'
#' ourstrwidth("abc")
#' ourstrwidth(10000)
begin
    local io = IOBuffer(Array(Uint8, 80), true, true)
    global ourstrwidth
    function ourstrwidth(x::Any) # -> Int
        truncate(io, 0)
        ourshowcompact(io, x)
        return position(io)
    end
    ourstrwidth(x::String) = strwidth(x) + 2 # -> Int
    ourstrwidth(s::Symbol) = int(ccall(:u8_strwidth,
                                       Csize_t,
                                       (Ptr{Uint8}, ),
                                       convert(Ptr{Uint8}, s)))
end

#' @description
#'
#' Render a value to an IO object in a compact format. Unlike
#' Base.showcompact, we render strings without surrounding quote
#' marks.
#'
#' @param io::IO An IO object to be printed to.
#' @param x::Any A value to be printed.
#'
#' @returns x::Nothing A `nothing` value.
#'
#' @examples
#'
#' ourshowcompact(STDOUT, "abc")
#' ourshowcompact(STDOUT, 10000)
ourshowcompact(io::IO, x::Any) = showcompact(io, x) # -> Nothing
ourshowcompact(io::IO, x::String) = showcompact(io, x) # -> Nothing
ourshowcompact(io::IO, x::Symbol) = print(io, x) # -> Nothing

#' @description
#'
#' Calculates, for each column of an AbstractDataFrame, the maximum
#' string width used to render either the name of that column or the
#' longest entry in that column -- among the rows of the AbstractDataFrame
#' will be rendered to IO. The widths for all columns are returned as a
#' vector.
#'
#' NOTE: The last entry of the result vector is the string width of the
#'       implicit row ID column contained in every AbstractDataFrame.
#'
#' @param adf::AbstractDataFrame The AbstractDataFrame whose columns will be
#'        printed.
#' @param rowindices1::AbstractVector{Int} A set of indices of the first
#'        chunk of the AbstractDataFrame that would be rendered to IO.
#' @param rowindices2::AbstractVector{Int} A set of indices of the second
#'        chunk of the AbstractDataFrame that would be rendered to IO. Can
#'        be empty if the AbstractDataFrame would be printed without any
#'        ellipses.
#' @param rowlabel::String The label that will be used when rendered the
#'        numeric ID's of each row. Typically, this will be set to "Row #".
#'
#' @returns widths::Vector{Int} The maximum string widths required to render
#'          each column, including that column's name.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "yy", "z"])
#' maxwidths = getmaxwidths(df, 1:1, 3:3, "Row #")
function getmaxwidths(adf::AbstractDataFrame,
                      rowindices1::AbstractVector{Int},
                      rowindices2::AbstractVector{Int},
                      rowlabel::Symbol) # -> Vector{Int}
    ncols = size(adf, 2)
    cnames = names(adf)
    maxwidths = Array(Int, ncols + 1)

    # TODO: Move this definition somewhere else
    NAstrwidth = 2
    undefstrwidth = ourstrwidth(Base.undef_ref_str)

    for j in 1:ncols
        # (1) Consider length of column name
        maxwidths[j] = ourstrwidth(cnames[j])

        # (2) Consider length of longest entry in that column
        col = adf[j]
        for indices in (rowindices1, rowindices2)
            for i in indices
                if isna(col, i)
                    maxwidths[j] = max(maxwidths[j], NAstrwidth)
                else
                    try
                        maxwidths[j] = max(maxwidths[j], ourstrwidth(col[i]))
                    catch
                        maxwidths[j] = max(maxwidths[j], undefstrwidth)
                    end
                end
            end
        end
    end

    rowmaxwidth1 = isempty(rowindices1) ? 0 : ndigits(maximum(rowindices1))
    rowmaxwidth2 = isempty(rowindices2) ? 0 : ndigits(maximum(rowindices2))

    maxwidths[ncols + 1] = max(max(rowmaxwidth1,
                                   rowmaxwidth2),
                               ourstrwidth(rowlabel))

    return maxwidths
end

#' @description
#'
#' Given the maximum widths required to render each column of an
#' AbstractDataFrame, this returns the total number of UTF8 characters
#' that would be required to render an entire row to an IO system.
#'
#' NOTE: This width includes the whitespace and special characters used to
#'       pretty print the AbstractDataFrame.
#'
#' @param maxwidths::Vector{Int} The maximum width needed to render each
#'        column of an AbstractDataFrame.
#'
#' @returns totalwidth::Int The total width required to render a complete row
#'          of the AbstractDataFrame for which `maxwidths` was computed.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "yy", "z"])
#' maxwidths = getmaxwidths(df, 1:1, 3:3, "Row #")
#' totalwidth = getprintedwidth(maxwidths))
function getprintedwidth(maxwidths::Vector{Int}) # -> Int
    # Include length of line-initial |
    totalwidth = 1
    for i in 1:length(maxwidths)
        # Include length of field + 2 spaces + trailing |
        totalwidth += maxwidths[i] + 3
    end
    return totalwidth
end

#' @description
#'
#' When rendering an AbstractDataFrame to a REPL window in chunks, each of
#' which will fit within the width of the REPL window, this function will
#' return the indices of the columns that should be included in each chunk.
#'
#' NOTE: The resulting bounds should be interpreted as follows: the
#'       i-th chunk bound is the index MINUS 1 of the first column in the
#'       i-th chunk. The (i + 1)-th chunk bound is the EXACT index of the
#'       last column in the i-th chunk. For example, the bounds [0, 3, 5]
#'       imply that the first chunk contains columns 1-3 and the second chunk
#'       contains columns 4-5.
#'
#' @param maxwidths::Vector{Int} The maximum width needed to render each
#'        column of an AbstractDataFrame.
#' @param splitchunks::Bool Should the output be split into chunks at all or
#'        should only one chunk be constructed for the entire
#'        AbstractDataFrame?
#'
#' @returns chunkbounds::Vector{Int} The bounds of each chunk of columns.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "yy", "z"])
#' maxwidths = getmaxwidths(df, 1:1, 3:3, "Row #")
#' chunkbounds = getchunkbounds(maxwidths, true)
function getchunkbounds(maxwidths::Vector{Int},
                        splitchunks::Bool) # -> Vector{Int}
    ncols = length(maxwidths) - 1
    rowmaxwidth = maxwidths[ncols + 1]
    _, availablewidth = Base.tty_size()
    if splitchunks
        chunkbounds = [0]
        # Include 2 spaces + 2 | characters for row/col label
        totalwidth = rowmaxwidth + 4
        for j in 1:ncols
            # Include 2 spaces + | character in per-column character count
            totalwidth += maxwidths[j] + 3
            if totalwidth > availablewidth
                push!(chunkbounds, j - 1)
                totalwidth = rowmaxwidth + 4 + maxwidths[j] + 3
            end
        end
        push!(chunkbounds, ncols)
    else
        chunkbounds = [0, ncols]
    end
    return chunkbounds
end

#' @description
#'
#' Render a subset of rows and columns of an AbstractDataFrame to an
#' IO system. For chunked printing, this function is used to print a
#' single chunk, starting from the first indicated column and ending with
#' the last indicated column. Assumes that the maximum string widths
#' required for printing have been precomputed.
#'
#' @param io::IO The IO system to which `adf` will be printed.
#' @param adf::AbstractDataFrame An AbstractDataFrame.
#' @param rowindices::AbstractVector{Int} The indices of the subset of rows
#'        that will be rendered to `io`.
#' @param maxwidths::Vector{Int} The pre-computed maximum string width
#'        required to render each column.
#' @param leftcol::Int The index of the first column in a chunk to be
#'        rendered.
#' @param rightcol::Int The index of the last column in a chunk to be
#'        rendered.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' showrowindices(STDOUT, df, 1:2, [1, 1, 5], 1, 2)
function showrowindices(io::IO,
                        adf::AbstractDataFrame,
                        rowindices::AbstractVector{Int},
                        maxwidths::Vector{Int},
                        leftcol::Int,
                        rightcol::Int) # -> Nothing
    rowmaxwidth = maxwidths[end]

    for i in rowindices
        # Print row ID
        @printf io "| %d" i
        padding = rowmaxwidth - ndigits(i)
        for itr in 1:padding
            write(io, ' ')
        end
        print(io, " | ")
        # Print DataFrame entry
        for j in leftcol:rightcol
            strlen = 0
            try
                strlen = ourstrwidth(adf[i, j])
                ourshowcompact(io, adf[i, j])
            catch
                strlen = ourstrwidth(Base.undef_ref_str)
                ourshowcompact(io, Base.undef_ref_str)
            end
            padding = maxwidths[j] - strlen
            for itr in 1:padding
                write(io, ' ')
            end
            if j == rightcol
                if i == rowindices[end]
                    print(io, " |")
                else
                    print(io, " |\n")
                end
            else
                print(io, " | ")
            end
        end
    end
    return
end

#' @description
#'
#' Render a subset of rows (possibly in chunks) of an AbstractDataFrame to an
#' IO system. Users can control
#'
#' NOTE: The value of `maxwidths[end]` must be the string width of
#' `rowlabel`.
#'
#' @param io::IO The IO system to which `adf` will be printed.
#' @param adf::AbstractDataFrame An AbstractDataFrame.
#' @param rowindices1::AbstractVector{Int} The indices of the first subset
#'        of rows to be rendered.
#' @param rowindices2::AbstractVector{Int} The indices of the second subset
#'        of rows to be rendered. An ellipsis will be printed before
#'        rendering this second subset of rows.
#' @param maxwidths::Vector{Int} The pre-computed maximum string width
#'        required to render each column.
#' @param splitchunks::Bool Should the printing of the AbstractDataFrame
#'        be done in chunks? Defaults to `false`.
#' @param rowlabel::Symbol What label should be printed when rendering the
#'        numeric ID's of each row? Defaults to `"Row #"`.
#' @param displaysummary::Bool Should a brief string summary of the
#'        AbstractDataFrame be rendered to the IO system before printing the
#'        contents of the renderable rows? Defaults to `true`.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' showrows(STDOUT, df, 1:2, 3:3, [1, 1, 5], false, "Row #", true)
function showrows(io::IO,
                  adf::AbstractDataFrame,
                  rowindices1::AbstractVector{Int},
                  rowindices2::AbstractVector{Int},
                  maxwidths::Vector{Int},
                  splitchunks::Bool = false,
                  rowlabel::Symbol = symbol("Row #"),
                  displaysummary::Bool = true) # -> Nothing
    ncols = size(adf, 2)
    cnames = names(adf)

    if displaysummary
        println(io, summary(adf))
    end

    if isempty(rowindices1)
        return
    end

    rowmaxwidth = maxwidths[ncols + 1]
    chunkbounds = getchunkbounds(maxwidths, splitchunks)
    nchunks = length(chunkbounds) - 1

    for chunkindex in 1:nchunks
        leftcol = chunkbounds[chunkindex] + 1
        rightcol = chunkbounds[chunkindex + 1]

        # Print table bounding line
        write(io, '|')
        for itr in 1:(rowmaxwidth + 2)
            write(io, '-')
        end
        write(io, '|')
        for j in leftcol:rightcol
            for itr in 1:(maxwidths[j] + 2)
                write(io, '-')
            end
            write(io, '|')
        end
        write(io, '\n')

        # Print column names
        @printf io "| %s" rowlabel
        padding = rowmaxwidth - ourstrwidth(rowlabel)
        for itr in 1:padding
            write(io, ' ')
        end
        @printf io " | "
        for j in leftcol:rightcol
            s = cnames[j]
            ourshowcompact(io, s)
            padding = maxwidths[j] - ourstrwidth(s)
            for itr in 1:padding
                write(io, ' ')
            end
            if j == rightcol
                print(io, " |\n")
            else
                print(io, " | ")
            end
        end

        # Print main table body, potentially in two abbreviated sections
        showrowindices(io,
                       adf,
                       rowindices1,
                       maxwidths,
                       leftcol,
                       rightcol)
        if !isempty(rowindices2)
            print(io, "\n⋮\n")
            showrowindices(io,
                           adf,
                           rowindices2,
                           maxwidths,
                           leftcol,
                           rightcol)
        end

        # Print newlines to separate chunks
        if chunkindex < nchunks
            print(io, "\n\n")
        end
    end

    return
end

#' @exported
#' @description
#'
#' Render an AbstractDataFrame to an IO system. The specific visual
#' representation chosen depends on the width of the REPL window
#' from which the call to `show` derives. If the DataFrame could not
#' be rendered without splitting the output into chunks, a summary of the
#' columns is rendered instead of rendering the raw data. This dynamic
#' response to screen width can be configured using the argument
#' `splitchunks`.
#'
#' @param io::IO The IO system to which `adf` will be printed.
#' @param adf::AbstractDataFrame An AbstractDataFrame.
#' @param splitchunks::Bool Should the printing of the AbstractDataFrame
#'        be done in chunks? Defaults to `false`.
#' @param rowlabel::Symbol What label should be printed when rendering the
#'        numeric ID's of each row? Defaults to `"Row #"`.
#' @param displaysummary::Bool Should a brief string summary of the
#'        AbstractDataFrame be rendered to the IO system before printing the
#'        contents of the renderable rows? Defaults to `true`.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' show(STDOUT, df, false, "Row #", true)
function Base.show(io::IO,
                   adf::AbstractDataFrame,
                   splitchunks::Bool = true,
                   rowlabel::Symbol = symbol("Row #"),
                   displaysummary::Bool = true) # -> Nothing
    nrows = size(adf, 1)
    tty_rows, tty_cols = Base.tty_size()
    availableheight = tty_rows - 5
    nrowssubset = fld(availableheight, 2)
    bound = min(nrowssubset - 1, nrows)
    if nrows <= availableheight
        rowindices1 = 1:nrows
        rowindices2 = 1:0
    else
        rowindices1 = 1:bound
        rowindices2 = max(bound + 1, nrows - nrowssubset + 1):nrows
    end
    maxwidths = getmaxwidths(adf, rowindices1, rowindices2, rowlabel)
    width = getprintedwidth(maxwidths)
    if width > tty_cols && !splitchunks
        showcols(io, adf)
    else
        showrows(io,
                 adf,
                 rowindices1,
                 rowindices2,
                 maxwidths,
                 splitchunks,
                 rowlabel,
                 displaysummary)
    end
    return
end

#' @exported
#' @description
#'
#' Render an AbstractDataFrame to STDOUT with or without chunking. See
#' other `show` documentation for details. This is mainly used to force
#' showing the AbstractDataFrame in chunks.
#'
#' @param adf::AbstractDataFrame An AbstractDataFrame.
#' @param splitchunks::Bool Should the printing of the AbstractDataFrame
#'        be done in chunks? Defaults to `false`.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' show(df, true)
function Base.show(adf::AbstractDataFrame,
                   splitchunks::Bool = true) # -> Nothing
    return show(STDOUT, adf, splitchunks)
end

#' @exported
#' @description
#'
#' Render a DataFrameRow to an IO system. Each column of the DataFrameRow
#' is printed on a separate line.
#'
#' @param io::IO The IO system where rendering will take place.
#' @param r::DataFrameRow The DataFrameRow to be rendered to `io`.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' for r in eachrow(df)
#'     show(STDOUT, r)
#' end
function Base.show(io::IO, r::DataFrameRow)
    labelwidth = mapreduce(n -> length(string(n)), max, names(r)) + 2
    @printf(io, "DataFrameRow (row %d)\n", r.row)
    for (label, value) in r
        println(io, rpad(label, labelwidth, ' '), value)
    end
end

#' @exported
#' @description
#'
#' Render a DataFrameRow to STDOUT. See other `show` documentation for
#' details.
#'
#' @param r::DataFrameRow The DataFrameRow to be rendered to `io`.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' for r in eachrow(df)
#'     show(r)
#' end
Base.show(row::DataFrameRow) = show(STDOUT, row)

#' @exported
#' @description
#'
#' Render all of the rows of an AbstractDataFrame to an IO system. See
#' `show` documentation for details.
#'
#' @param io::IO The IO system to which `adf` will be printed.
#' @param adf::AbstractDataFrame An AbstractDataFrame.
#' @param splitchunks::Bool Should the printing of the AbstractDataFrame
#'        be done in chunks? Defaults to `false`.
#' @param rowlabel::Symbol What label should be printed when rendering the
#'        numeric ID's of each row? Defaults to `"Row #"`.
#' @param displaysummary::Bool Should a brief string summary of the
#'        AbstractDataFrame be rendered to the IO system before printing the
#'        contents of the renderable rows? Defaults to `true`.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' showall(STDOUT, df, false, "Row #", true)
function Base.showall(io::IO,
                      adf::AbstractDataFrame,
                      splitchunks::Bool = false,
                      rowlabel::Symbol = symbol("Row #"),
                      displaysummary::Bool = true) # -> Nothing
    rowindices1 = 1:size(adf, 1)
    rowindices2 = 1:0
    maxwidths = getmaxwidths(adf, rowindices1, rowindices2, rowlabel)
    width = getprintedwidth(maxwidths)
    showrows(io,
             adf,
             rowindices1,
             rowindices2,
             maxwidths,
             splitchunks,
             rowlabel,
             displaysummary)
    return
end

#' @exported
#' @description
#'
#' Render all of the rows of an AbstractDataFrame to STDOUT. See
#' `showall` documentation for details.
#'
#' @param adf::AbstractDataFrame An AbstractDataFrame.
#' @param splitchunks::Bool Should the printing of the AbstractDataFrame
#'        be done in chunks? Defaults to `false`.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' showall(df, true)
function Base.showall(adf::AbstractDataFrame,
                      splitchunks::Bool = false) # -> Nothing
    showall(STDOUT, adf, splitchunks)
    return
end

#' @description
#'
#' Render a summary of the column names, column types and column missingness
#' count.
#'
#' @param io::IO The `io` to be rendered to.
#' @param adf::AbstractDataFrame An AbstractDataFrame.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' showcols(df, true)
function showcols(io::IO, adf::AbstractDataFrame) # -> Nothing
    println(io, summary(adf))
    metadata = DataFrame(Name = names(adf),
                         Eltype = eltypes(adf),
                         Missing = colmissing(adf))
    showall(io, metadata, true, symbol("Col #"), false)
    return
end

showcols(adf::AbstractDataFrame) = showcols(STDOUT, adf) # -> Nothing

#' @exported
#' @description
#'
#' Print an AbstractDataFrame to an IO system with an added newline.
#'
#' @param io::IO The `io` system to be rendered to.
#' @param adf::AbstractDataFrame An AbstractDataFrame.
#'
#' @returns o::Nothing A `nothing` value.
#'
#' @examples
#'
#' df = DataFrame(A = 1:3, B = ["x", "y", "z"])
#' print(STDOUT, df)
# TODO: Determine if this method is strictly necessary.
function Base.print(io::IO, adf::AbstractDataFrame)
    show(io, adf)
    print(io, '\n')
end
