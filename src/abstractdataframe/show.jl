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

    # NOTE: If we reuse `types` here, the time to print the first table is 2x more.
    # This should be something related to type inference.
    types_str = permutedims(compacttype.(eltype.(eachcol(df)), maxwidth))

    if allcols && allrows
        crop = :none
    elseif allcols
        crop = :vertical
    elseif allrows
        crop = :horizontal
    else
        crop = :both
    end

    # For consistency, if `kwargs` has `compact_printng`, we must use it.
    compact_printing::Bool = get(kwargs, :compact_printing, get(io, :compact, true))

    num_rows, num_cols = size(df)

    # By default, we align the columns to the left unless they are numbers,
    # which is checked in the following.
    alignment = fill(:l, num_cols)

    # Create the dictionary with the anchor regex that is used to align the
    # floating points.
    alignment_anchor_regex = Dict{Int, Vector{Regex}}()

    # Columns composed of numbers are printed aligned to the right.
    alignment_regex_vec = [r"\."]

    for i = 1:num_cols
        type_i = nonmissingtype(types[i])

        if type_i <: Number
            alignment_anchor_regex[i] = alignment_regex_vec
            alignment[i] = :r
        end
    end

    # Make sure that `truncate` does not hide the type and the column name.
    maximum_columns_width = Int[truncate == 0 ? 0 : max(truncate + 1, l, textwidth(t))
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
                 alignment                   = alignment,
                 alignment_anchor_fallback   = :r,
                 alignment_anchor_regex      = alignment_anchor_regex,
                 compact_printing            = compact_printing,
                 crop                        = crop,
                 crop_num_lines_at_beginning = 2,
                 ellipsis_line_skip          = 3,
                 formatters                  = (_pretty_tables_general_formatter,),
                 header_alignment            = :l,
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
    show([io::IO, ]df::AbstractDataFrame;
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

julia> show(df, show_row_number=false)
3×2 DataFrame
 A     │ B
 Int64 │ String
───────┼────────
     1 │ x
     2 │ y
     3 │ z
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
