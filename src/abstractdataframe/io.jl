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

    ct = show_eltype ? batch_compacttype(Any[eltype(c) for c in eachcol(df)], 9) : String[]
    j = 1
    for (col_idx, (name, col)) in enumerate(pairs(eachcol(df)))
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
            maxwidths[j] = max(maxwidth, ourstrwidth(io, ct[col_idx], buffer, 0))
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
    show(io::IO, mime::MIME, df::AbstractDataFrame)

Render a data frame to an I/O stream in MIME type `mime`.

# Arguments
- `io::IO`: The I/O stream to which `df` will be printed.
- `mime::MIME`: supported MIME types are: `"text/plain"`, `"text/html"`, `"text/latex"`,
  `"text/csv"`, `"text/tab-separated-values"` (the last two MIME types do not support
   showing `#undef` values)
- `df::AbstractDataFrame`: The data frame to print.

Additionally selected MIME types support passing the following keyword arguments:
- MIME type `"text/plain"` accepts all listed keyword arguments and their behavior
  is identical as for `show(::IO, ::AbstractDataFrame)`
- MIME type `"text/html"` accepts the following keyword arguments:
    - `eltypes::Bool = true`: Whether to print the column types under column names.
    - `summary::Bool = true`: Whether to print a brief string summary of the data frame.
    - `max_column_width::AbstractString = ""`: The maximum column width. It must
          be a string containing a valid CSS length. For example, passing
          "100px" will limit the width of all columns to 100 pixels. If empty,
          the columns will be rendered without limits.
    - `kwargs...`: Any keyword argument supported by the function `pretty_table`
      of PrettyTables.jl can be passed here to customize the output.

# Examples
```jldoctest
julia> show(stdout, MIME("text/latex"), DataFrame(A=1:3, B=["x", "y", "z"]))
\\begin{tabular}{r|cc}
\t& A & B\\\\
\t\\hline
\t& Int64 & String\\\\
\t\\hline
\t1 & 1 & x \\\\
\t2 & 2 & y \\\\
\t3 & 3 & z \\\\
\\end{tabular}
14

julia> show(stdout, MIME("text/csv"), DataFrame(A=1:3, B=["x", "y", "z"]))
"A","B"
1,"x"
2,"y"
3,"z"
```
"""
Base.show(io::IO, mime::MIME, df::AbstractDataFrame)
function Base.show(io::IO, mime::MIME"text/html", df::AbstractDataFrame;
                   summary::Bool=true, eltypes::Bool=true,
                   max_column_width::AbstractString="", kwargs...)
    _verify_kwargs_for_html(; kwargs...)
    return _show(io, mime, df; summary=summary, eltypes=eltypes,
                 max_column_width=max_column_width, kwargs...)
end

Base.show(io::IO, mime::MIME"text/latex", df::AbstractDataFrame; eltypes::Bool=true) =
    _show(io, mime, df, eltypes=eltypes)
Base.show(io::IO, mime::MIME"text/csv", df::AbstractDataFrame) =
    printtable(io, df, header = true, separator = ',')
Base.show(io::IO, mime::MIME"text/tab-separated-values", df::AbstractDataFrame) =
    printtable(io, df, header = true, separator = '\t')
Base.show(io::IO, mime::MIME"text/plain", df::AbstractDataFrame; kwargs...) =
    show(io, df; kwargs...)

##############################################################################
#
# HTML output
#
##############################################################################

function html_escape(cell::AbstractString)
    cell = replace(cell, "&"=>"&amp;")
    cell = replace(cell, "<"=>"&lt;")
    cell = replace(cell, ">"=>"&gt;")
    # Replace quotes so that the resulting string could also be used in the attributes of
    # HTML tags
    cell = replace(cell, "\""=>"&quot;")
    cell = replace(cell, "'"=>"&apos;")
    return cell
end

function _show(io::IO,
               ::MIME"text/html",
               df::AbstractDataFrame;
               summary::Bool=true,
               eltypes::Bool=true,
               rowid::Union{Int, Nothing}=nothing,
               title::AbstractString="",
               max_column_width::AbstractString="",
               kwargs...)
    _check_consistency(df)

    names_str = names(df)
    types = Any[eltype(c) for c in eachcol(df)]
    types_str = batch_compacttype(types, 9)
    types_str_complete = batch_compacttype(types, 256)

    # For consistency, if `kwargs` has `compact_printing`, we must use it.
    compact_printing::Bool = get(kwargs, :compact_printing, get(io, :compact, true))

    num_rows, num_cols = size(df)

    # By default, we align the columns to the left unless they are numbers,
    # which is checked in the following.
    alignment = fill(:l, num_cols)

    for i = 1:num_cols
        type_i = nonmissingtype(types[i])

        if type_i <: Number
            alignment[i] = :r
        end
    end

    if get(io, :limit, false)
        # Obtain the maximum number of rows and columns that we can print from
        # environment variables.
        mxrow = something(tryparse(Int, get(ENV, "DATAFRAMES_ROWS", "25")), 25)
        mxcol = something(tryparse(Int, get(ENV, "DATAFRAMES_COLUMNS", "100")), 100)
    else
        mxrow = -1
        mxcol = -1
    end

    # Check if the user wants to display a summary about the DataFrame that is
    # being printed. This will be shown using the `title` option of
    # `pretty_table`.
    if summary
        if isempty(title)
            title = Base.summary(df)
        end
    else
        title = ""
    end

    # If `rowid` is not `nothing`, then we are printing a data row. In this
    # case, we will add this information using the row name column of
    # PrettyTables.jl. Otherwise, we can just use the row number column.
    if (rowid === nothing) || (ncol(df) == 0)
        show_row_number::Bool = get(kwargs, :show_row_number, true)
        row_labels = nothing

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
            row_labels = nothing
            vlines = Int[]
        else
            row_labels = [string(rowid)]
            vlines = Int[1]
        end

        show_row_number = false
    end

    pretty_table(io, df;
                 alignment                 = alignment,
                 backend                   = Val(:html),
                 compact_printing          = compact_printing,
                 formatters                = (_pretty_tables_general_formatter,),
                 header                    = (names_str, types_str),
                 header_alignment          = :l,
                 header_cell_titles        = (nothing, types_str_complete),
                 highlighters              = (_PRETTY_TABLES_HTML_HIGHLIGHTER,),
                 max_num_of_columns        = mxcol,
                 max_num_of_rows           = mxrow,
                 maximum_columns_width     = max_column_width,
                 minify                    = true,
                 row_label_column_title    = "Row",
                 row_labels                = row_labels,
                 row_number_alignment      = :r,
                 row_number_column_title   = "Row",
                 show_omitted_cell_summary = true,
                 show_row_number           = show_row_number,
                 show_subheader            = eltypes,
                 standalone                = false,
                 table_class               = "data-frame",
                 table_div_class           = "data-frame",
                 table_style               = _PRETTY_TABLES_HTML_TABLE_STYLE,
                 top_left_str              = String(title),
                 top_right_str_decoration  = HtmlDecoration(font_style = "italic"),
                 vcrop_mode                = :middle,
                 wrap_table_in_div         = true,
                 kwargs...)

    return nothing
end

function Base.show(io::IO, mime::MIME"text/html", dfrs::DataFrameRows; kwargs...)
    _verify_kwargs_for_html(; kwargs...)
    df = parent(dfrs)
    title = "$(nrow(df))×$(ncol(df)) DataFrameRows"
    _show(io, mime, df; title=title, kwargs...)
end

function Base.show(io::IO, mime::MIME"text/html", dfcs::DataFrameColumns; kwargs...)
    _verify_kwargs_for_html(; kwargs...)
    df = parent(dfcs)
    title = "$(nrow(df))×$(ncol(df)) DataFrameColumns"
    _show(io, mime, df; title=title, kwargs...)
end

# Internal function to verify the keywords in show functions using the HTML
# backend.
function _verify_kwargs_for_html(; kwargs...)
    haskey(kwargs, :rowid) &&
        throw(ArgumentError("Keyword argument `rowid` is reserved and must not be used."))

    haskey(kwargs, :title) &&
        throw(ArgumentError("Use the `top_left_str` keyword argument instead of `title` " *
                            "to change the label above the data frame."))

    haskey(kwargs, :truncate) &&
        throw(ArgumentError("`truncate` is not supported in HTML. " *
                            "Use `max_column_width` to limit the size of the columns in this case."))

    return nothing
end

##############################################################################
#
# LaTeX output
#
##############################################################################

function latex_char_escape(char::Char)
    if char == '\\'
        return "\\textbackslash{}"
    elseif char == '~'
        return "\\textasciitilde{}"
    else
        return string('\\', char)
    end
end

function latex_escape(cell::AbstractString)
    replace(cell, ['\\','~', '#', '$', '%', '&', '_', '^', '{', '}']=>latex_char_escape)
end

function _show(io::IO, ::MIME"text/latex", df::AbstractDataFrame;
               eltypes::Bool=true, rowid=nothing)
    _check_consistency(df)

    # we will pass around this buffer to avoid its reallocation in ourstrwidth
    buffer = IOBuffer(Vector{UInt8}(undef, 80), read=true, write=true)

    if rowid !== nothing
        if size(df, 2) == 0
            rowid = nothing
        elseif size(df, 1) != 1
            throw(ArgumentError("rowid may be passed only with a single row data frame"))
        end
    end

    mxrow, mxcol = size(df)
    if get(io, :limit, false)
        tty_rows, tty_cols = get(io, :displaysize, displaysize(io))
        mxrow = min(mxrow, tty_rows)
        maxwidths = getmaxwidths(df, io, 1:mxrow, 0:-1, :X, nothing, true, buffer, 0) .+ 2
        mxcol = min(mxcol, searchsortedfirst(cumsum(maxwidths), tty_cols))
    end

    cnames = _names(df)[1:mxcol]
    alignment = repeat("c", mxcol)
    write(io, "\\begin{tabular}{r|")
    write(io, alignment)
    mxcol < size(df, 2) && write(io, "c")
    write(io, "}\n")
    write(io, "\t& ")
    header = join(map(c -> latex_escape(string(c)), cnames), " & ")
    write(io, header)
    mxcol < size(df, 2) && write(io, " & ")
    write(io, "\\\\\n")
    write(io, "\t\\hline\n")
    if eltypes
        write(io, "\t& ")
        ct = batch_compacttype(Any[eltype(df[!, idx]) for idx in 1:mxcol], 9)
        header = join(latex_escape.(ct), " & ")
        write(io, header)
        mxcol < size(df, 2) && write(io, " & ")
        write(io, "\\\\\n")
        write(io, "\t\\hline\n")
    end
    for row in 1:mxrow
        write(io, "\t")
        write(io, @sprintf("%d", rowid === nothing ? row : rowid))
        for col in 1:mxcol
            write(io, " & ")
            if !isassigned(df[!, col], row)
                print(io, "\\emph{\\#undef}")
            else
                cell = df[row, col]
                if ismissing(cell)
                    print(io, "\\emph{missing}")
                elseif cell isa Markdown.MD
                    print(io, strip(repr(MIME("text/latex"), cell)))
                elseif cell isa SHOW_TABULAR_TYPES
                    print(io, "\\emph{")
                    print(io, latex_escape(sprint(ourshow, cell, 0, context=io)))
                    print(io, "}")
                else
                    if showable(MIME("text/latex"), cell)
                        show(io, MIME("text/latex"), cell)
                    else
                        print(io, latex_escape(sprint(ourshow, cell, 0, context=io)))
                    end
                end
            end
        end
        mxcol < size(df, 2) && write(io, " & \$\\dots\$")
        write(io, " \\\\\n")
    end
    if size(df, 1) > mxrow
        write(io, "\t\$\\dots\$")
        for col in 1:mxcol
            write(io, " & \$\\dots\$")
        end
        mxcol < size(df, 2) && write(io, " & ")
        write(io, " \\\\\n")
    end
    write(io, "\\end{tabular}\n")
end

Base.show(io::IO, mime::MIME"text/latex", dfrs::DataFrameRows; eltypes::Bool=true) =
	_show(io, mime, parent(dfrs), eltypes=eltypes)
Base.show(io::IO, mime::MIME"text/latex", dfcs::DataFrameColumns; eltypes::Bool=true) =
	_show(io, mime, parent(dfcs), eltypes=eltypes)

##############################################################################
#
# MIME: text/csv and text/tab-separated-values
#
##############################################################################

escapedprint(io::IO, x::SHOW_TABULAR_TYPES, escapes::AbstractString) =
    escapedprint(io, summary(x), escapes)
escapedprint(io::IO, x::Any, escapes::AbstractString) =
    escapedprint(io, sprint(print, x), escapes)
escapedprint(io::IO, x::AbstractString, escapes::AbstractString) =
    escape_string(io, x, escapes)

function printtable(io::IO,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    missingstring::AbstractString = "missing",
                    nothingstring::AbstractString = "nothing")
    _check_consistency(df)
    n, p = size(df)
    etypes = eltype.(eachcol(df))
    if header
        cnames = _names(df)
        for j in 1:p
            print(io, quotemark)
            print(io, cnames[j])
            print(io, quotemark)
            if j < p
                print(io, separator)
            else
                print(io, '\n')
            end
        end
    end
    quotestr = string(quotemark)
    for i in 1:n
        for j in 1:p
            cell = df[i, j]
            if ismissing(cell)
                print(io, missingstring)
            elseif isnothing(cell)
                print(io, nothingstring)
            else
                if cell isa Markdown.MD
                    print(io, quotemark)
                    r = repr(cell)
                    escapedprint(io, chomp(r), quotestr)
                    print(io, quotemark)
                elseif !(etypes[j] <: Real)
                    print(io, quotemark)
                    escapedprint(io, cell, quotestr)
                    print(io, quotemark)
                else
                    print(io, cell)
                end
            end
            if j < p
                print(io, separator)
            else
                print(io, '\n')
            end
        end
    end
    nothing
end

Base.show(io::IO, mime::MIME"text/csv",
          dfs::Union{DataFrameRows, DataFrameColumns}) =
    show(io, mime, parent(dfs))
Base.show(io::IO, mime::MIME"text/tab-separated-values",
          dfs::Union{DataFrameRows, DataFrameColumns}) =
    show(io, mime, parent(dfs))
