function Base.summary(adf::AbstractDataFrame) # -> UTF8String
    nrows, ncols = size(adf)
    return @sprintf "%dx%d %s" nrows ncols typeof(adf)
end

begin
    local io = IOBuffer(Array(Uint8, 80), true, true)
    global ourstrwidth
    function ourstrwidth(x::Any)
        truncate(io, 0)
        ourshowcompact(io, x)
        return position(io)
    end
end

ourshowcompact(io::IO, x::Any) = showcompact(io, x)
ourshowcompact(io::IO, x::String) = print(io, x)

# Determine the maximum string length of any entry in each DataFrame column
function getmaxwidths(adf::AbstractDataFrame,
                      rowindices1::AbstractVector{Int},
                      rowindices2::AbstractVector{Int},
                      rowlabel::String) # -> Vector{Int}
    ncols = size(adf, 2)
    cnames = names(adf)
    maxwidths = Array(Int, ncols + 1)
    for j in 1:ncols
        # (1) Consider length of column name
        maxwidths[j] = ourstrwidth(cnames[j])

        # (2) Consider length of longest entry in that column
        for i in rowindices1
            maxwidths[j] = max(maxwidths[j], ourstrwidth(adf[i, j]))
        end
        for i in rowindices2
            maxwidths[j] = max(maxwidths[j], ourstrwidth(adf[i, j]))
        end
    end
    rowmaxwidth1 = isempty(rowindices1) ? 0 : ndigits(maximum(rowindices1))
    rowmaxwidth2 = isempty(rowindices2) ? 0 : ndigits(maximum(rowindices2))
    maxwidths[ncols + 1] = max(max(rowmaxwidth1, rowmaxwidth2),
                               ourstrwidth(rowlabel))
    return maxwidths
end

# Determine width of printing DataFrame in a single chunk
function getprintedwidth(maxwidths::Vector{Int}) # -> Int
    n = length(maxwidths)
    # Length of line-initial |
    width = 1
    for i in 1:n
        # Length of field + 2 spaces + trailing |
        width += maxwidths[i] + 3
    end
    return width
end

# Given available space, split the columns into a set of chunks
# each of which should fit on the screen at once
function getchunkbounds(maxwidths::Vector{Int},
                        splitchunks::Bool) # -> Vector{Int}
    ncols = length(maxwidths) - 1
    rowmaxwidth = maxwidths[ncols + 1]
    availablewidth = Base.tty_cols()
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

function showrowindices(io::IO,
                        adf::AbstractDataFrame,
                        rowindices::AbstractVector{Int},
                        rowmaxwidth::Int,
                        maxwidths::Vector{Int},
                        leftcol::Int,
                        rightcol::Int) # -> Nothing
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
            strlen = ourstrwidth(adf[i, j])
            ourshowcompact(io, adf[i, j])
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

function showrows(io::IO,
                  adf::AbstractDataFrame,
                  rowindices1::AbstractVector{Int},
                  rowindices2::AbstractVector{Int},
                  maxwidths::Vector{Int},
                  splitchunks::Bool = false,
                  rowlabel::String = "Row #",
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
                       rowmaxwidth,
                       maxwidths,
                       leftcol,
                       rightcol)
        if !isempty(rowindices2)
            print(io, "\n⋮\n")
            showrowindices(io,
                           adf,
                           rowindices2,
                           rowmaxwidth,
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

function Base.show(io::IO,
                   adf::AbstractDataFrame,
                   splitchunks::Bool = false,
                   rowlabel::String = "Row #",
                   displaysummary::Bool = true) # -> Nothing
    nrows = size(adf, 1)
    availableheight = Base.tty_rows() - 5
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
    if width > Base.tty_cols() && !splitchunks
        column_summary(io, adf)
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

function Base.show(adf::AbstractDataFrame,
                   splitchunks::Bool = false) # -> Nothing
    show(STDOUT, adf, splitchunks)
end

function Base.showall(io::IO,
                      adf::AbstractDataFrame,
                      splitchunks::Bool = false,
                      rowlabel::String = "Row #",
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

function Base.showall(adf::AbstractDataFrame,
                      splitchunks::Bool = false) # -> Nothing
    showall(STDOUT, adf, splitchunks)
    return
end

function countna(da::DataArray)
    n = length(da)
    res = 0
    for i in 1:n
        if da.na[i]
            res += 1
        end
    end
    return res
end

function countna(da::PooledDataArray)
    n = length(da)
    res = 0
    for i in 1:n
        if da.refs[i] == 0
            res += 1
        end
    end
    return res
end

function colmissing(adf::AbstractDataFrame) # -> Vector{Int}
    nrows, ncols = size(adf)
    missing = zeros(Int, ncols)
    for j in 1:ncols
        missing[j] = countna(adf[j])
    end
    return missing
end

function column_summary(io::IO, adf::AbstractDataFrame) # -> Nothing
    println(io, summary(adf))
    metadata = DataFrame(Name = names(adf),
                         Type = types(adf),
                         Missing = colmissing(adf))
    showall(io, metadata, true, "Col #", false)
    return
end

function Base.print(io::IO, adf::AbstractDataFrame)
    show(io, adf)
    print(io, '\n')
end
