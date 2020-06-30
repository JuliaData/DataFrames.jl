Base.summary(df::AbstractDataFrame) =
    @sprintf("%d×%d %s", size(df)..., nameof(typeof(df)))
Base.summary(io::IO, df::AbstractDataFrame) = print(io, summary(df))

"""
    DataFrames.ourstrwidth(io::IO, x::Any, buffer)

Determine the number of characters that would be used to print a value.
"""
function ourstrwidth(io::IO, x::Any, buffer::IOBuffer)
    truncate(buffer, 0)
    ourshow(IOContext(buffer, :compact=>get(io, :compact, true)), x)
    return textwidth(String(take!(buffer)))
end

"""
    DataFrames.ourshow(io::IO, x::Any)

Render a value to an `IO` object compactly and omitting type information, by
calling 3-argument `show`, or 2-argument `show` if the former contains line breaks.
Unlike `show`, render strings without surrounding quote marks.
"""
function ourshow(io::IO, x::Any; styled::Bool=false)
    io_ctx = IOContext(io, :compact=>get(io, :compact, true), :typeinfo=>typeof(x))

    # This mirrors the behavior of Base.print_matrix_row
    # First try 3-arg show
    sx = sprint(show, "text/plain", x, context=io_ctx)

    # If the output contains line breaks, try 2-arg show instead.
    if occursin('\n', sx)
        sx = sprint(show, x, context=io_ctx)
    end

    # strings should have " stripped here
    if x isa AbstractString
        @assert sx[1] == sx[end] == '"'
        sx = escape_string(chop(sx, head=1, tail=1), "")
    end

    if styled
        printstyled(io_ctx, sx, color=:light_black)
    else
        print(io_ctx, sx)
    end
end

const SHOW_TABULAR_TYPES = Union{AbstractDataFrame, DataFrameRow, DataFrameRows,
                                 DataFrameColumns, GroupedDataFrame}

ourshow(io::IO, x::AbstractString) = escape_string(io, x, "")
ourshow(io::IO, x::CategoricalValue{<:AbstractString}) = escape_string(io, get(x), "")
ourshow(io::IO, x::Symbol) = ourshow(io, string(x))
ourshow(io::IO, x::Nothing; styled::Bool=false) = ourshow(io, "", styled=styled)
ourshow(io::IO, x::SHOW_TABULAR_TYPES; styled::Bool=false) =
    ourshow(io, summary(x), styled=styled)

# AbstractChar: https://github.com/JuliaLang/julia/pull/34730 (1.5.0-DEV.261)
# Irrational: https://github.com/JuliaLang/julia/pull/34741 (1.5.0-DEV.266)
if VERSION < v"1.5.0-DEV.261" || VERSION < v"1.5.0-DEV.266"
    function ourshow(io::IO, x::T) where T <: Union{AbstractChar, Irrational}
        io = IOContext(io, :compact=>get(io, :compact, true), :typeinfo=>typeof(x))
        show(io, x)
    end
end

"""Return compact string representation of type T"""
function compacttype(T::Type, maxwidth::Int=8, initial::Bool=true)
    maxwidth = max(8, maxwidth)

    T === Any && return "Any"
    T === Missing && return "Missing"

    sT = string(T)
    textwidth(sT) ≤ maxwidth && return sT

    if T >: Missing
        T = nonmissingtype(T)
        sT = string(T)
        suffix = "?"
        # ignore "?" for initial width counting but respect it for display
        initial || (maxwidth -= 1)
        textwidth(sT) ≤ maxwidth && return sT * suffix
    else
        suffix = ""
    end

    maxwidth -= 1 # we will add "…" at the end

    if T <: CategoricalValue
        sT = string(nameof(T))
        if textwidth(sT) ≤ maxwidth
            return sT * "…" * suffix
        else
            return (maxwidth ≥ 11 ? "Categorical…" : "Cat…") * suffix
        end
    elseif T isa Union
        return "Union…" * suffix
    else
        sT = string(nameof(T))
    end

    cumwidth = 0
    stop = 0
    for (i, c) in enumerate(sT)
        cumwidth += textwidth(c)
        if cumwidth ≤ maxwidth
            stop = i
        else
            break
        end
    end
    return first(sT, stop) * "…" * suffix
end

"""
    DataFrames.getmaxwidths(df::AbstractDataFrame,
                            io::IO,
                            rowindices1::AbstractVector{Int},
                            rowindices2::AbstractVector{Int},
                            rowlabel::Symbol,
                            rowid::Union{Integer, Nothing},
                            show_eltype::Bool,
                            buffer::IOBuffer)

Calculate, for each column of an AbstractDataFrame, the maximum
string width used to render the name of that column, its type, and the
longest entry in that column -- among the rows of the data frame
will be rendered to IO. The widths for all columns are returned as a
vector.

Return a `Vector{Int}` giving the maximum string widths required to render
each column, including that column's name and type.

NOTE: The last entry of the result vector is the string width of the
implicit row ID column contained in every `AbstractDataFrame`.

# Arguments
- `df::AbstractDataFrame`: The data frame whose columns will be printed.
- `io::IO`: The `IO` to which `df` is to be printed
- `rowindices1::AbstractVector{Int}: A set of indices of the first
  chunk of the AbstractDataFrame that would be rendered to IO.
- `rowindices2::AbstractVector{Int}: A set of indices of the second
  chunk of the AbstractDataFrame that would be rendered to IO. Can
  be empty if the AbstractDataFrame would be printed without any
  ellipses.
- `rowlabel::AbstractString`: The label that will be used when rendered the
  numeric ID's of each row. Typically, this will be set to "Row".
- `rowid`: Used to handle showing `DataFrameRow`.
- `show_eltype`: Whether to print the column type
   under the column name in the heading.
- `buffer`: buffer passed around to avoid reallocations in `ourstrwidth`
```
"""
function getmaxwidths(df::AbstractDataFrame,
                      io::IO,
                      rowindices1::AbstractVector{Int},
                      rowindices2::AbstractVector{Int},
                      rowlabel::Symbol,
                      rowid::Union{Integer, Nothing},
                      show_eltype::Bool,
                      buffer::IOBuffer)
    maxwidths = Vector{Int}(undef, size(df, 2) + 1)

    undefstrwidth = ourstrwidth(io, "#undef", buffer)

    j = 1
    for (name, col) in pairs(eachcol(df))
        # (1) Consider length of column name
        maxwidth = ourstrwidth(io, name, buffer)

        # (2) Consider length of longest entry in that column
        for indices in (rowindices1, rowindices2), i in indices
            if isassigned(col, i)
                maxwidth = max(maxwidth, ourstrwidth(io, col[i], buffer))
            else
                maxwidth = max(maxwidth, undefstrwidth)
            end
        end
        if show_eltype
            maxwidths[j] = max(maxwidth, ourstrwidth(io, compacttype(eltype(col)), buffer))
        else
            maxwidths[j] = maxwidth
        end
        j += 1
    end

    if rowid isa Nothing
        rowmaxwidth1 = isempty(rowindices1) ? 0 : ndigits(maximum(rowindices1))
        rowmaxwidth2 = isempty(rowindices2) ? 0 : ndigits(maximum(rowindices2))
        maxwidths[j] = max(max(rowmaxwidth1, rowmaxwidth2), ourstrwidth(io, rowlabel, buffer))
    else
        maxwidths[j] = max(ndigits(rowid), ourstrwidth(io, rowlabel, buffer))
    end

    return maxwidths
end

"""
    DataFrames.getprintedwidth(maxwidths::Vector{Int})

Given the maximum widths required to render each column of an
`AbstractDataFrame`, return the total number of characters
that would be required to render an entire row to an I/O stream.

NOTE: This width includes the whitespace and special characters used to
pretty print the `AbstractDataFrame`.

# Arguments
- `maxwidths::Vector{Int}`: The maximum width needed to render each
  column of an `AbstractDataFrame`.
```
"""
function getprintedwidth(maxwidths::Vector{Int})
    # Include length of line-initial |
    totalwidth = 1
    for i in 1:length(maxwidths)
        # Include length of field + 2 spaces + trailing |
        totalwidth += maxwidths[i] + 3
    end
    return totalwidth
end

"""
    getchunkbounds(maxwidths::Vector{Int},
                   splitcols::Bool,
                   availablewidth::Int)

When rendering an `AbstractDataFrame` to a REPL window in chunks, each of
which will fit within the width of the REPL window, this function will
return the indices of the columns that should be included in each chunk.

NOTE: The resulting bounds should be interpreted as follows: the
i-th chunk bound is the index MINUS 1 of the first column in the
i-th chunk. The (i + 1)-th chunk bound is the EXACT index of the
last column in the i-th chunk. For example, the bounds [0, 3, 5]
imply that the first chunk contains columns 1-3 and the second chunk
contains columns 4-5.

# Arguments
- `maxwidths::Vector{Int}`: The maximum width needed to render each
  column of an AbstractDataFrame.
- `splitcols::Bool`: Whether to split printing in chunks of columns
  fitting the screen width rather than printing all columns in the same block.
- `availablewidth::Int`: The available width in the REPL.
```
"""
function getchunkbounds(maxwidths::Vector{Int},
                        splitcols::Bool,
                        availablewidth::Int)
    ncols = length(maxwidths) - 1
    rowmaxwidth = maxwidths[ncols + 1]
    if splitcols
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

"""
    showrowindices(io::IO,
                   df::AbstractDataFrame,
                   rowindices::AbstractVector{Int},
                   maxwidths::Vector{Int},
                   leftcol::Int,
                   rightcol::Int,
                   rowid::Union{Int,Nothing},
                   buffer::IOBuffer)

Render a subset of rows and columns of an `AbstractDataFrame` to an
I/O stream. For chunked printing, this function is used to print a
single chunk, starting from the first indicated column and ending with
the last indicated column. Assumes that the maximum string widths
required for printing have been precomputed.

# Arguments
- `io::IO`: The I/O stream to which `df` will be printed.
- `df::AbstractDataFrame`: An AbstractDataFrame.
- `rowindices::AbstractVector{Int}`: The indices of the subset of rows
  that will be rendered to `io`.
- `maxwidths::Vector{Int}`: The pre-computed maximum string width
  required to render each column.
- `leftcol::Int`: The index of the first column in a chunk to be rendered.
- `rightcol::Int`: The index of the last column in a chunk to be rendered.
- `rowid`: Used to handle showing `DataFrameRow`.
- `buffer`: buffer passed around to avoid reallocations in `ourstrwidth`

# Examples
```jldoctest
julia> using DataFrames

julia> df = DataFrame(A = 1:3, B = ["x", "y", "z"]);

julia> DataFrames.showrowindices(stdout, df, 1:2, [1, 1, 5], 1, 2)
│ 1     │ 1 │ x │
│ 2     │ 2 │ y │
```
"""
function showrowindices(io::IO,
                        df::AbstractDataFrame,
                        rowindices::AbstractVector{Int},
                        maxwidths::Vector{Int},
                        leftcol::Int,
                        rightcol::Int,
                        rowid::Union{Integer, Nothing},
                        buffer::IOBuffer)
    rowmaxwidth = maxwidths[end]

    for i in rowindices
        # Print row ID
        if rowid isa Nothing
            @printf io "│ %d" i
        else
            @printf io "│ %d" rowid
        end
        padding = rowmaxwidth - ndigits(rowid isa Nothing ? i : rowid)
        for _ in 1:padding
            write(io, ' ')
        end
        print(io, " │ ")
        # Print DataFrame entry
        for j in leftcol:rightcol
            strlen = 0
            if isassigned(df[!, j], i)
                s = df[i, j]
                strlen = ourstrwidth(io, s, buffer)
                if ismissing(s) || s isa SHOW_TABULAR_TYPES
                    ourshow(io, s, styled=true)
                else
                    ourshow(io, s)
                end
            else
                strlen = ourstrwidth(io, "#undef", buffer)
                ourshow(io, "#undef", styled=true)
            end
            padding = maxwidths[j] - strlen
            for _ in 1:padding
                write(io, ' ')
            end
            if j == rightcol
                if i == rowindices[end]
                    print(io, " │")
                else
                    print(io, " │\n")
                end
            else
                print(io, " │ ")
            end
        end
    end
    return
end

"""
    showrows(io::IO,
             df::AbstractDataFrame,
             rowindices1::AbstractVector{Int},
             rowindices2::AbstractVector{Int},
             maxwidths::Vector{Int},
             splitcols::Bool,
             allcols::Bool,
             rowlabel::Symbol,
             displaysummary::Bool,
             eltypes::Bool,
             rowid::Union{Integer, Nothing},
             buffer::IOBuffer)

Render a subset of rows (possibly in chunks) of an `AbstractDataFrame` to an
I/O stream.

NOTE: The value of `maxwidths[end]` must be the string width of
`rowlabel`.

# Arguments
- `io::IO`: The I/O stream to which `df` will be printed.
- `df::AbstractDataFrame`: An AbstractDataFrame.
- `rowindices1::AbstractVector{Int}`: The indices of the first subset
  of rows to be rendered.
- `rowindices2::AbstractVector{Int}`: The indices of the second subset
  of rows to be rendered. An ellipsis will be printed before
  rendering this second subset of rows.
- `maxwidths::Vector{Int}`: The pre-computed maximum string width
  required to render each column.
- `allcols::Bool = false`: Whether to print all columns, rather than
  a subset that fits the device width.
- `splitcols::Bool`: Whether to split printing in chunks of columns fitting the
  screen width rather than printing all columns in the same block.
- `rowlabel::Symbol`: What label should be printed when rendering the
  numeric ID's of each row? Defaults to `:Row`.
- `displaysummary::Bool`: Should a brief string summary of the
  AbstractDataFrame be rendered to the I/O stream before printing the
  contents of the renderable rows? Defaults to `true`.
- `eltypes::Bool = true`: Whether to print the column type
   under the column name in the heading. Defaults to `true`.
- `rowid::Union{Integer, Nothing} = nothing`: Used to handle showing `DataFrameRow`
- `buffer::IOBuffer`: buffer passed around to avoid reallocations in `ourstrwidth`

# Examples

```jldoctest
julia> using DataFrames

julia> df = DataFrame(A = 1:3, B = ["x", "y", "z"]);

julia> DataFrames.showrows(stdout, df, 1:2, 3:3, [5, 6, 3], false, true, :Row, true)
3×2 DataFrame
│ Row │ A     │ B      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ x      │
│ 2   │ 2     │ y      │
⋮
│ 3   │ 3     │ z      │
```
"""
function showrows(io::IO,
                  df::AbstractDataFrame,
                  rowindices1::AbstractVector{Int},
                  rowindices2::AbstractVector{Int},
                  maxwidths::Vector{Int},
                  splitcols::Bool,
                  allcols::Bool,
                  rowlabel::Symbol,
                  displaysummary::Bool,
                  eltypes::Bool,
                  rowid::Union{Integer, Nothing},
                  buffer::IOBuffer)

    ncols = size(df, 2)

    if isempty(rowindices1)
        if displaysummary
            println(io, summary(df))
        end
        return
    end

    rowmaxwidth = maxwidths[ncols + 1]
    chunkbounds = getchunkbounds(maxwidths, splitcols, displaysize(io)[2])
    nchunks = allcols ? length(chunkbounds) - 1 : min(length(chunkbounds) - 1, 1)

    header = displaysummary ? summary(df) : ""
    if !allcols && length(chunkbounds) > 2
        header *= ". Omitted printing of $(chunkbounds[end] - chunkbounds[2]) columns"
    end
    println(io, header)

    for chunkindex in 1:nchunks
        leftcol = chunkbounds[chunkindex] + 1
        rightcol = chunkbounds[chunkindex + 1]

        # Print column names
        @printf io "│ %s" rowlabel
        padding = rowmaxwidth - ourstrwidth(io, rowlabel, buffer)
        for itr in 1:padding
            write(io, ' ')
        end
        print(io, " │ ")
        for j in leftcol:rightcol
            s = _names(df)[j]
            ourshow(io, s)
            padding = maxwidths[j] - ourstrwidth(io, s, buffer)
            for itr in 1:padding
                write(io, ' ')
            end
            if j == rightcol
                print(io, " │\n")
            else
                print(io, " │ ")
            end
        end

        # Print column types
        if eltypes
            print(io, "│ ")
            padding = rowmaxwidth
            for itr in 1:padding
                write(io, ' ')
            end
            print(io, " │ ")
            for j in leftcol:rightcol
                s = compacttype(eltype(df[!, j]), maxwidths[j], false)
                printstyled(io, s, color=:light_black)
                padding = maxwidths[j] - ourstrwidth(io, s, buffer)
                for itr in 1:padding
                    write(io, ' ')
                end
                if j == rightcol
                    print(io, " │\n")
                else
                    print(io, " │ ")
                end
            end
        end

        # Print table bounding line
        write(io, '├')
        for itr in 1:(rowmaxwidth + 2)
            write(io, '─')
        end
        write(io, '┼')
        for j in leftcol:rightcol
            for itr in 1:(maxwidths[j] + 2)
                write(io, '─')
            end
            if j < rightcol
                write(io, '┼')
            else
                write(io, '┤')
            end
        end
        write(io, '\n')

        # Print main table body, potentially in two abbreviated sections
        showrowindices(io,
                       df,
                       rowindices1,
                       maxwidths,
                       leftcol,
                       rightcol,
                       rowid, buffer)

        if !isempty(rowindices2)
            print(io, "\n⋮\n")
            showrowindices(io,
                           df,
                           rowindices2,
                           maxwidths,
                           leftcol,
                           rightcol,
                           rowid, buffer)
        end

        # Print newlines to separate chunks
        if chunkindex < nchunks
            print(io, "\n\n")
        end
    end

    return
end

function _show(io::IO,
               df::AbstractDataFrame;
               allrows::Bool = !get(io, :limit, false),
               allcols::Bool = !get(io, :limit, false),
               splitcols = get(io, :limit, false),
               rowlabel::Symbol = :Row,
               summary::Bool = true,
               eltypes::Bool = true,
               rowid=nothing)
    _check_consistency(df)

    # we will pass around this buffer to avoid its reallocation in ourstrwidth
    buffer = IOBuffer(Vector{UInt8}(undef, 80), read=true, write=true)

    nrows = size(df, 1)
    if rowid !== nothing
        if size(df, 2) == 0
            rowid = nothing
        elseif nrows != 1
            throw(ArgumentError("rowid may be passed only with a single row data frame"))
        end
    end
    dsize = displaysize(io)
    availableheight = dsize[1] - 7
    nrowssubset = fld(availableheight, 2)
    bound = min(nrowssubset - 1, nrows)
    if allrows || nrows <= availableheight
        rowindices1 = 1:nrows
        rowindices2 = 1:0
    else
        rowindices1 = 1:bound
        rowindices2 = max(bound + 1, nrows - nrowssubset + 1):nrows
    end
    maxwidths = getmaxwidths(df, io, rowindices1, rowindices2, rowlabel, rowid, eltypes, buffer)
    width = getprintedwidth(maxwidths)
    showrows(io,
             df,
             rowindices1,
             rowindices2,
             maxwidths,
             splitcols,
             allcols,
             rowlabel,
             summary,
             eltypes,
             rowid, buffer)
    return
end

"""
    show([io::IO,] df::AbstractDataFrame;
         allrows::Bool = !get(io, :limit, false),
         allcols::Bool = !get(io, :limit, false),
         allgroups::Bool = !get(io, :limit, false),
         splitcols::Bool = get(io, :limit, false),
         rowlabel::Symbol = :Row,
         summary::Bool = true,
         eltypes::Bool = true)

Render a data frame to an I/O stream. The specific visual
representation chosen depends on the width of the display.

If `io` is omitted, the result is printed to `stdout`,
and `allrows`, `allcols` and `allgroups` default to `false`
while `splitcols` defaults to `true`.

# Arguments
- `io::IO`: The I/O stream to which `df` will be printed.
- `df::AbstractDataFrame`: The data frame to print.
- `allrows::Bool `: Whether to print all rows, rather than
  a subset that fits the device height. By default this is the case only if
  `io` does not have the `IOContext` property `limit` set.
- `allcols::Bool`: Whether to print all columns, rather than
  a subset that fits the device width. By default this is the case only if
  `io` does not have the `IOContext` property `limit` set.
- `allgroups::Bool`: Whether to print all groups rather than
  the first and last, when `df` is a `GroupedDataFrame`.
  By default this is the case only if `io` does not have the `IOContext` property
  `limit` set.
- `splitcols::Bool`: Whether to split printing in chunks of columns fitting the
  screen width rather than printing all columns in the same block. Only applies
  if `allcols` is `true`.
  By default this is the case only if `io` has the `IOContext` property `limit` set.
- `rowlabel::Symbol = :Row`: The label to use for the column containing row numbers.
- `summary::Bool = true`: Whether to print a brief string summary of the data frame.
- `eltypes::Bool = true`: Whether to print the column types under column names.

# Examples
```jldoctest
julia> using DataFrames

julia> df = DataFrame(A = 1:3, B = ["x", "y", "z"]);

julia> show(df, allcols=true)
3×2 DataFrame
│ Row │ A     │ B      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ x      │
│ 2   │ 2     │ y      │
│ 3   │ 3     │ z      │
```
"""
Base.show(io::IO,
          df::AbstractDataFrame;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          splitcols = get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true) =
    _show(io, df, allrows=allrows, allcols=allcols, splitcols=splitcols,
          rowlabel=rowlabel, summary=summary, eltypes=eltypes)

Base.show(df::AbstractDataFrame;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          splitcols = get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true) =
    show(stdout, df,
         allrows=allrows, allcols=allcols, splitcols=splitcols,
         rowlabel=rowlabel, summary=summary, eltypes=eltypes)
