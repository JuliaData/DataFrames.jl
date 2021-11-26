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

# For most data frames, especially wide, columns having the same element type
# occur multiple times. batch_compacttype ensures that we compute string
# representation of a specific column element type only once and then reuse it.

function batch_compacttype(types::Vector{Any}, maxwidths::Vector{Int})
    @assert length(types) == length(maxwidths)
    cache = Dict{Any, String}()
    return map(types, maxwidths) do T, maxwidth
        get!(cache, T) do
            compacttype(T, maxwidth)
        end
    end
end

function batch_compacttype(types::Vector{Any}, maxwidth::Int)
    cache = Dict{Type, String}()
    return map(types) do T
        get!(cache, T) do
            compacttype(T, maxwidth)
        end
    end
end

"""
    compacttype(T::Type, maxwidth::Int=8, initial::Bool=true)

Return compact string representation of type `T`.

For displaying data frame we do not want string representation of type to be
longer than `maxwidth`. This function implements rules how type names are
cropped if they are longer than `maxwidth`.
"""
function compacttype(T::Type, maxwidth::Int)
    maxwidth = max(8, maxwidth)

    T === Any && return "Any"
    T === Missing && return "Missing"

    sT = string(T)
    textwidth(sT) ≤ maxwidth && return sT

    if T >: Missing
        T = nonmissingtype(T)
        sT = string(T)
        suffix = "?"
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
        sTfull = sT
        sT = string(nameof(T))
    end

    # handle the case when the type printed is not parametric but string(T)
    # prefixed it with the module name which caused it to be overlong
    textwidth(sT) ≤ maxwidth + 1 && endswith(sTfull, sT) && return sT

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

    names_str = names(df)
    names_len = Int[textwidth(n) for n in names_str]
    maxwidth = Int[max(9, nl) for nl in names_len]
    types = Any[eltype(c) for c in eachcol(df)]
    types_str = batch_compacttype(types, maxwidth)

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

    # Regex to align real numbers.
    alignment_regex_real = [r"\."]

    # Regex for columns with complex numbers.
    #
    # Here we are matching `+` or `-` unless it is not at the beginning of the
    # string or an `e` precedes it.
    alignment_regex_complex = [r"(?<!^)(?<!e)[+-]"]

    for i = 1:num_cols
        type_i = nonmissingtype(types[i])

        if type_i <: Complex
            alignment_anchor_regex[i] = alignment_regex_complex
            alignment[i] = :r
        elseif type_i <: Real
            alignment_anchor_regex[i] = alignment_regex_real
            alignment[i] = :r
        elseif type_i <: Number
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
        show_row_number::Bool = get(kwargs, :show_row_number, true)
        row_names = nothing

        # If the columns with row numbers is not shown, then we should not
        # display a vertical line after the first column.
        vlines = fill(1, show_row_number)
    else
        nrow(df) != 1 &&
            throw(ArgumentError("rowid may be passed only with a single row data frame"))

        # In this case, if the user does not want to show the row number, then
        # we must hide the row name column, which is used to display the
        # `rowid`.
        if !get(kwargs, :show_row_number, true)
            row_names = nothing
            vlines = Int[]
        else
            row_names = [string(rowid)]
            vlines = Int[1]
        end

        show_row_number = false
    end

    # Print the table with the selected options.
    pretty_table(io, df;
                 alignment                   = alignment,
                 alignment_anchor_fallback   = :r,
                 alignment_anchor_regex      = alignment_anchor_regex,
                 compact_printing            = compact_printing,
                 crop                        = crop,
                 crop_num_lines_at_beginning = 2,
                 ellipsis_line_skip          = 3,
                 formatters                  = (_pretty_tables_general_formatter,),
                 header                      = (names_str, types_str),
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
                 vlines                      = vlines,
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

julia> df = DataFrame(A=1:3, B=["x", "y", "z"]);

julia> show(df, show_row_number=false)
3×2 DataFrame
 A      B
 Int64  String
───────────────
     1  x
     2  y
     3  z
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
