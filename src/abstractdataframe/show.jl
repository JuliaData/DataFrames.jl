Base.summary(df::AbstractDataFrame) =
    @sprintf("%d×%d %s", size(df)..., nameof(typeof(df)))
Base.summary(io::IO, df::AbstractDataFrame) = print(io, summary(df))

"""
    DataFrames.ourstrwidth(io::IO, x::Any, buffer::IOBuffer, truncstring::Int)

Determine the number of characters that would be used to print a value.
"""
function ourstrwidth(io::IO, x::Any, buffer::IOBuffer, truncstring::Int)
    truncate(buffer, 0)
    ourshow(IOContext(buffer, :compact=>get(io, :compact, true)), x, truncstring)
    return textwidth(String(take!(buffer)))
end

function truncatestring(s::AbstractString, truncstring::Int)
    truncstring <= 0 && return s
    totalwidth = 0
    for (i, c) in enumerate(s)
        totalwidth += textwidth(c)
        if totalwidth > truncstring
            return first(s, i-1) * '…'
        end
    end
    return s
end

"""
    DataFrames.ourshow(io::IO, x::Any, truncstring::Int)

Render a value to an `IO` object compactly using print.
`truncstring` indicates the approximate number of text characters width to truncate
the output (if it is a non-positive value then no truncation is applied).
"""
function ourshow(io::IO, x::Any, truncstring::Int; styled::Bool=false)
    io_ctx = IOContext(io, :compact=>get(io, :compact, true), :typeinfo=>typeof(x))
    sx = sprint(print, x, context=io_ctx)
    sx = escape_string(sx, ()) # do not escape "
    sx = truncatestring(sx, truncstring)
    styled ? printstyled(io_ctx, sx, color=:light_black) : print(io_ctx, sx)
end

const SHOW_TABULAR_TYPES = Union{AbstractDataFrame, DataFrameRow, DataFrameRows,
                                 DataFrameColumns, GroupedDataFrame}

# workaround Julia 1.0 for Char
ourshow(io::IO, x::Char, truncstring::Int; styled::Bool=false) =
    ourshow(io, string(x), styled=styled, truncstring)

ourshow(io::IO, x::Nothing, truncstring::Int; styled::Bool=false) =
    ourshow(io, "", styled=styled, truncstring)
ourshow(io::IO, x::SHOW_TABULAR_TYPES, truncstring::Int; styled::Bool=false) =
    ourshow(io, summary(x), truncstring, styled=styled)

function ourshow(io::IO, x::Markdown.MD, truncstring::Int)
    r = repr(x)
    truncstring <= 0 && return chomp(truncstring)
    len = min(length(r, 1, something(findfirst(==('\n'), r), lastindex(r)+1)-1), truncstring)
    return print(io, len < length(r) - 1 ? first(r, len)*'…' : first(r, len))
end

# AbstractChar: https://github.com/JuliaLang/julia/pull/34730 (1.5.0-DEV.261)
# Irrational: https://github.com/JuliaLang/julia/pull/34741 (1.5.0-DEV.266)
if VERSION < v"1.5.0-DEV.261" || VERSION < v"1.5.0-DEV.266"
    function ourshow(io::IO, x::T, truncstring::Int) where T <: Union{AbstractChar, Irrational}
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

    # This is only type display shortening so we
    # are OK with any T whose name starts with CategoricalValue here
    if startswith(sT, "CategoricalValue") || startswith(sT, "CategoricalArrays.CategoricalValue")
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
                      buffer::IOBuffer,
                      truncstring::Int)
    maxwidths = Vector{Int}(undef, size(df, 2) + 1)

    undefstrwidth = ourstrwidth(io, "#undef", buffer, truncstring)

    j = 1
    for (name, col) in pairs(eachcol(df))
        # (1) Consider length of column name
        # do not truncate column name
        maxwidth = ourstrwidth(io, name, buffer, 0)

        # (2) Consider length of longest entry in that column
        for indices in (rowindices1, rowindices2), i in indices
            if isassigned(col, i)
                maxwidth = max(maxwidth, ourstrwidth(io, col[i], buffer, truncstring))
            else
                maxwidth = max(maxwidth, undefstrwidth)
            end
        end
        if show_eltype
            # do not truncate eltype name
            maxwidths[j] = max(maxwidth, ourstrwidth(io, compacttype(eltype(col)), buffer, 0))
        else
            maxwidths[j] = maxwidth
        end
        j += 1
    end

    # do not truncate rowlabel
    if rowid isa Nothing
        rowmaxwidth1 = isempty(rowindices1) ? 0 : ndigits(maximum(rowindices1))
        rowmaxwidth2 = isempty(rowindices2) ? 0 : ndigits(maximum(rowindices2))
        maxwidths[j] = max(max(rowmaxwidth1, rowmaxwidth2),
                           ourstrwidth(io, rowlabel, buffer, 0))
    else
        maxwidths[j] = max(ndigits(rowid), ourstrwidth(io, rowlabel, buffer, 0))
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
                        buffer::IOBuffer,
                        truncstring::Int)
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
                strlen = ourstrwidth(io, s, buffer, truncstring)
                if ismissing(s) || s isa SHOW_TABULAR_TYPES
                    ourshow(io, s, truncstring, styled=true)
                else
                    ourshow(io, s, truncstring)
                end
            else
                strlen = ourstrwidth(io, "#undef", buffer, truncstring)
                ourshow(io, "#undef", truncstring, styled=true)
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
                  buffer::IOBuffer,
                  truncstring::Int)

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
    cols_other_chunks = chunkbounds[end] - chunkbounds[2]
    if !allcols && length(chunkbounds) > 2
        # if we print only one chunk and it does not fit the screen give up
        if cols_other_chunks == ncols
            print(io, header * ". Omitted printing of all columns as they do " *
                  "not fit the display size")
            return
        end
        header *= ". Omitted printing of $cols_other_chunks columns"
    end

    println(io, header)

    for chunkindex in 1:nchunks
        leftcol = chunkbounds[chunkindex] + 1
        rightcol = chunkbounds[chunkindex + 1]

        # nothing to print in this chunk
        leftcol > rightcol && continue

        # Print column names
        @printf io "│ %s" rowlabel
        # do not truncate rowlabel
        padding = rowmaxwidth - ourstrwidth(io, rowlabel, buffer, 0)
        for itr in 1:padding
            write(io, ' ')
        end
        print(io, " │ ")
        for j in leftcol:rightcol
            s = _names(df)[j]
            # do not truncate column names
            ourshow(io, s, 0)
            padding = maxwidths[j] - ourstrwidth(io, s, buffer, 0)
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
                # do not truncate eltype
                padding = maxwidths[j] - ourstrwidth(io, s, buffer, 0)
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
        showrowindices(io, df, rowindices1, maxwidths, leftcol, rightcol,
                       rowid, buffer, truncstring)

        if !isempty(rowindices2)
            print(io, "\n⋮\n")
            showrowindices(io, df, rowindices2, maxwidths, leftcol, rightcol,
                           rowid, buffer, truncstring)
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
               rowlabel::Symbol = :Row,
               summary::Bool = true,
               eltypes::Bool = true,
               rowid = nothing,
               truncate::Int = 32,
               kwargs...)

    _check_consistency(df)

    aux = names(df)
    names_len = textwidth.(aux)
    maxwidth = max.(9, names_len)
    names_mat = permutedims(aux)
    types = eltype.(eachcol(df))

    # NOTE: If we use `type` here, the time to print the first table is 2x more.
    # This should be something related to type inference.
    types_str = compacttype.(eltype.(eachcol(df)), maxwidth) |> permutedims

    if allcols && allrows
        crop = :none
    elseif allcols
        crop = :vertical
    elseif allrows
        crop = :horizontal
    else
        crop = :both
    end

    compact_printing::Bool = get(io, :compact, true)

    # This vector stores the column indices that are only floats. In this case,
    # the printed numbers will be aligned on the decimal point.
    float_cols = Int[]

    # These vectors contain the number of the row and the padding that must be
    # applied so that the float number is aligned with on the decimal point
    indices = Vector{Int}[]
    padding = Vector{Int}[]

    # If the screen is limited, we do not need to process all the numbers.
    dsize = displaysize(io)
    num_rows, num_cols = size(df)

    if !allcols
        # Given the spacing, there is no way to fit more than W/9 rows of
        # floating numbers in the screen, where W is the display width.
        Δc = clamp(div(dsize[2], 9), 0, num_cols)
    else
        Δc = num_cols
    end

    if !allrows
        # Get the maximum number of lines that we can display given the screen size.
        Δr = clamp(dsize[1] - 4, 0, num_rows)
    else
        Δr = num_rows
    end

    Δr_lim = cld(Δr, 2)

    # Do not align the numbers if there are more than 500 rows.
    if Δr ≤ 500
        for i = 1:Δc
            # Analyze the order of the number to compute the maximum padding
            # that must be applied to align the numbers at the decimal point.
            if nonmissingtype(types[i]) <: AbstractFloat
                max_pad_i = 0
                order_i = zeros(Δr)
                indices_i = zeros(Δr)

                for k = 1:Δr
                    # We need to process the top and bottom of the table because
                    # we are cropping in the middle.

                    kr =  k ≤ Δr_lim ? k : num_rows - (k - Δr_lim) + 1

                    v = df[kr, i]

                    order_v::Int = 0

                    if v isa Number
                        abs_v = abs(v)
                        log_v::Int = (!isinf(v) && !isnan(v) && abs_v > 1) ? floor(Int, log10(abs_v)) : 0

                        # If the order is higher than 5, then we print using
                        # scientific notation.
                        order_v = log_v > 5 ? 0 : floor(Int, log_v)

                        # If the number is negative, we need to add an additional
                        # padding to print the sign.
                        v < 0 && (order_v += 1)
                    end

                    order_i[k] = order_v
                    indices_i[k] = kr

                    order_v > max_pad_i && (max_pad_i = order_v)
                end

                push!(float_cols, i)
                push!(indices, indices_i)
                push!(padding, max_pad_i .- order_i)
            end
        end
    end

    # Create the formatter for floating point columns.
    ft_float = (v, i, j)->_pretty_tables_float_formatter(v, i, j, float_cols,
                                                         indices, padding,
                                                         compact_printing)

    # Make sure that `truncate` does not hide the type and the column name.
    maximum_columns_width = [truncate == 0 ? 0 : max(truncate + 1, l, textwidth(t))
                             for (l, t) in zip(names_len, types_str)]

    # Check if the user wants to display a summary about the DataFrame that is
    # being printed. This will be shown using the `title` option of
    # `pretty_table`.
    title = summary ? Base.summary(df) : ""

    # If `rowid` is not `nothing`, then we are printing a data row. In this
    # case, we will add this information using the row name column of
    # PrettyTables.jl. Otherwise, we can just use the row number column.
    if (rowid === nothing) || (ncol(df) == 0)
        show_row_number = true
        row_names = nothing
    else
        nrow(df) != 1 &&
            throw(ArgumentError("rowid may be passed only with a single row data frame"))
        show_row_number = false
        row_names = [string(rowid)]
    end

    # Print the table with the selected options.
    pretty_table(io, df, vcat(names_mat, types_str);
                 alignment                   = :l,
                 compact_printing            = compact_printing,
                 continuation_row_alignment  = :l,
                 crop                        = crop,
                 crop_num_lines_at_beginning = 2,
                 ellipsis_line_skip          = 3,
                 formatters                  = (_pretty_tables_general_formatter,
                                                ft_float),
                 hlines                      = [:header],
                 highlighters                = (_PRETTY_TABLES_HIGHLIGHTER,),
                 maximum_columns_width       = maximum_columns_width,
                 newline_at_end              = false,
                 nosubheader                 = !eltypes,
                 row_name_alignment          = :r,
                 row_name_crayon             = Crayon(),
                 row_name_column_title       = string(rowlabel),
                 row_names                   = row_names,
                 row_number_alignment        = :r,
                 row_number_column_title     = string(rowlabel),
                 show_row_number             = show_row_number,
                 title                       = title,
                 vcrop_mode                  = :middle,
                 vlines                      = [1],
                 kwargs...)

    return nothing
end

"""
    show([io::IO,] df::AbstractDataFrame;
         allrows::Bool = !get(io, :limit, false),
         allcols::Bool = !get(io, :limit, false),
         allgroups::Bool = !get(io, :limit, false),
         rowlabel::Symbol = :Row,
         summary::Bool = true,
         eltypes::Bool = true,
         truncate::Int = 32,
         kwargs...)

Render a data frame to an I/O stream. The specific visual
representation chosen depends on the width of the display.

If `io` is omitted, the result is printed to `stdout`,
and `allrows`, `allcols` and `allgroups` default to `false`.

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
- `rowlabel::Symbol = :Row`: The label to use for the column containing row numbers.
- `summary::Bool = true`: Whether to print a brief string summary of the data frame.
- `eltypes::Bool = true`: Whether to print the column types under column names.
- `truncate::Int = 32`: the maximal display width the output can use before
  being truncated (in the `textwidth` sense, excluding `…`).
  If `truncate` is 0 or less, no truncation is applied.
- `kwargs...`: Any keyword argument supported by the function `pretty_table` of
  PrettyTables.jl can be passed here to customize the output.

# Examples
```jldoctest
julia> using DataFrames

julia> df = DataFrame(A = 1:3, B = ["x", "y", "z"]);

julia> show(df, allcols=true)
3×2 DataFrame
 Row │ A      B
     │ Int64  String
─────┼───────────────
   1 │ 1      x
   2 │ 2      y
   3 │ 3      z
```
"""
Base.show(io::IO,
          df::AbstractDataFrame;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    _show(io, df; allrows=allrows, allcols=allcols, rowlabel=rowlabel,
          summary=summary, eltypes=eltypes, truncate=truncate, kwargs...)

Base.show(df::AbstractDataFrame;
          allrows::Bool = !get(stdout, :limit, true),
          allcols::Bool = !get(stdout, :limit, true),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true,
          truncate::Int = 32,
          kwargs...) =
    show(stdout, df;
         allrows=allrows, allcols=allcols, rowlabel=rowlabel, summary=summary,
         eltypes=eltypes, truncate=truncate, kwargs...)
