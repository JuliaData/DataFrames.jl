function Base.summary(adf::AbstractDataFrame) # -> UTF8String
    nrows, ncols = size(adf)
    return @sprintf "%dx%d %s" nrows ncols typeof(adf)
end

# Determine the maximum string length of any entry in each DataFrame column
function getmaxwidths(adf::AbstractDataFrame,
                      rowindices1::AbstractVector{Int},
                      rowindices2::AbstractVector{Int},
                      rowlabel::String) # -> Vector{Int}
    ncols = size(adf, 2)
    names = colnames(adf)
    maxwidths = Array(Int, ncols + 1)
    for j in 1:ncols
        # (1) Consider length of column name
        maxwidths[j] = length(names[j])

        # (2) Consider length of longest entry in that column
        for i in rowindices1
            maxwidths[j] = max(maxwidths[j],
                               length(string(adf[i, j])))
        end
        for i in rowindices2
            maxwidths[j] = max(maxwidths[j],
                               length(string(adf[i, j])))
        end
    end
    m1 = isempty(rowindices1) ? 0 : maximum(rowindices1)
    m2 = isempty(rowindices2) ? 0 : maximum(rowindices2)
    maxwidths[ncols + 1] = max(max(ndigits(m1), ndigits(m2)),
                               length(rowlabel))
    return maxwidths
end

# Determine width of printing DataFrame in a single chunk
function getprintedwidth(maxwidths::Vector{Int}) # -> Int
    n = length(maxwidths)
    width = 1 # Length of line-initial |
    for i in 1:n
        width += maxwidths[i] + 3 # Length of field + 2 spaces + trailing |
    end
    return width
end

# Given available space, split the columns into a set of chunks
# each of which should fit on the screen at once
function getchunkbounds(maxwidths::Vector{Int},
                        splitchunks::Bool) # -> Vector{Int}
    ncols = length(maxwidths) - 1
    rowmaxwidth = maxwidths[ncols + 1]
    availablespace = Base.tty_cols() - (rowmaxwidth + 3)
    if splitchunks
        chunkbounds = Array(Int, 0)
        currentchunk = 0
        totalchars = 0
        push!(chunkbounds, 0)
        for j in 1:ncols
            # Include 2 spaces + | character in per-column character count
            totalchars += maxwidths[j] + 3
            if fld(totalchars, availablespace) != currentchunk
                currentchunk += 1
                push!(chunkbounds, j - 1)
            end
        end
        if isempty(chunkbounds)
            chunkbounds = [ncols]
        end
        if chunkbounds[end] != ncols
            push!(chunkbounds, ncols)
        end
    else
        chunkbounds = [0, ncols]
    end
    return chunkbounds, availablespace
end

function showrowindices(io::IO,
                        adf::AbstractDataFrame,
                        rowindices::AbstractVector{Int},
                        rowmaxwidth::Int,
                        maxwidths::Vector{Int},
                        leftcol::Int,
                        rightcol::Int)
    for i in rowindices
        # Row ID
        @printf io "| %d" i
        if i == 1
            for addedspace in 1:(rowmaxwidth - 1)
                write(io, ' ')
            end
        else
            for addedspace in 1:(rowmaxwidth - ndigits(i))
                write(io, ' ')
            end
        end
        print(io, " | ")
        # DataFrame entry
        for j in leftcol:rightcol
            s = string(adf[i, j])
            @printf io "%s" s
            for addedspace in 1:(maxwidths[j] - length(s))
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
                  rowlabel::String = "Row #") # -> Nothing
    ncols = size(adf, 2)
    names = colnames(adf)

    println(io, summary(adf))

    maxwidths = getmaxwidths(adf, rowindices1, rowindices2, rowlabel)
    rowmaxwidth = maxwidths[ncols + 1]
    chunkbounds, availablespace = getchunkbounds(maxwidths, splitchunks)

    for chunkindex in 1:(length(chunkbounds) - 1)
        leftcol = chunkbounds[chunkindex] + 1
        rightcol = chunkbounds[chunkindex + 1]

        # Header bounding line
        write(io, '|')
        for ind in 1:(rowmaxwidth + 2)
            write(io, '-')
        end
        write(io, '|')
        for j in leftcol:rightcol
            for ind in 1:(maxwidths[j] + 2)
                write(io, '-')
            end
            write(io, '|')
        end
        write(io, '\n')

        # Header column names
        @printf io "| %s | " rowlabel
        for j in leftcol:rightcol
            s = names[j]
            print(io, s)
            for addedspace in 1:(maxwidths[j] - length(s))
                write(io, ' ')
            end
            if j == rightcol
                print(io, " |\n")
            else
                print(io, " | ")
            end
        end

        # Main table body
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

        if chunkindex < length(chunkbounds) - 1
            print(io, "\n\n")
        end
    end

    return
end

function Base.show(io::IO,
                   adf::AbstractDataFrame,
                   splitchunks::Bool = false,
                   rowlabel::String = "Row #")
    nrows = size(adf, 1)
    availablespace = Base.tty_rows() - 5
    regionsize = fld(availablespace, 2)
    bound = min(regionsize - 1, nrows)
    if nrows <= availablespace
        rowindices1 = 1:nrows
        rowindices2 = 1:0
    else
        rowindices1 = 1:bound
        rowindices2 = max(bound + 1, nrows - regionsize + 1):nrows
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
                 rowlabel)
    end
    return
end

function Base.show(adf::AbstractDataFrame, splitchunks::Bool = false)
    show(STDOUT, adf, splitchunks)
end

function Base.showall(io::IO,
                      adf::AbstractDataFrame,
                      splitchunks::Bool = false,
                      rowlabel::String = "Row #")
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
             rowlabel)
    return
end

function Base.showall(adf::AbstractDataFrame,
                      splitchunks::Bool = false)
    showall(STDOUT, adf, splitchunks)
    return
end

function column_summary(io::IO, adf::AbstractDataFrame)
    ncols = size(adf, 2)
    metadata = DataFrame(Name = colnames(adf),
                         Type = coltypes(adf),
                         Missing = [sum(isna(adf[j])) for j in 1:ncols])
    showall(io, metadata, true, "Col #")
    return
end
