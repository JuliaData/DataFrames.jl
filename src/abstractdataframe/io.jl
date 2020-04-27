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
- MIME type `"text/plain"` accepts all listed keyword arguments and therir behavior
  is identical as for `show(::IO, ::AbstractDataFrame)`
- MIME type `"text/html"` accepts `summary` keyword argument which
  allows to choose whether to print a brief string summary of the data frame.

# Examples
```jldoctest
julia> show(stdout, MIME("text/latex"), DataFrame(A = 1:3, B = ["x", "y", "z"]))
\\begin{tabular}{r|cc}
        & A & B\\\\
        \\hline
        & Int64 & String\\\\
        \\hline
        1 & 1 & x \\\\
        2 & 2 & y \\\\
        3 & 3 & z \\\\
\\end{tabular}
14

julia> show(stdout, MIME("text/csv"), DataFrame(A = 1:3, B = ["x", "y", "z"]))
"A","B"
1,"x"
2,"y"
3,"z"
```
"""
Base.show(io::IO, mime::MIME, df::AbstractDataFrame)
Base.show(io::IO, mime::MIME"text/html", df::AbstractDataFrame;
          summary::Bool=true, eltypes::Bool=true) =
    _show(io, mime, df, summary=summary, eltypes=eltypes)
Base.show(io::IO, mime::MIME"text/latex", df::AbstractDataFrame; eltypes::Bool=true) =
    _show(io, mime, df, eltypes=eltypes)
Base.show(io::IO, mime::MIME"text/csv", df::AbstractDataFrame) =
    printtable(io, df, header = true, separator = ',')
Base.show(io::IO, mime::MIME"text/tab-separated-values", df::AbstractDataFrame) =
    printtable(io, df, header = true, separator = '\t')
Base.show(io::IO, mime::MIME"text/plain", df::AbstractDataFrame;
          allrows::Bool = !get(io, :limit, false),
          allcols::Bool = !get(io, :limit, false),
          splitcols = get(io, :limit, false),
          rowlabel::Symbol = :Row,
          summary::Bool = true,
          eltypes::Bool = true) =
    show(io, df, allrows=allrows, allcols=allcols,
         splitcols=splitcols, rowlabel=rowlabel, summary=summary, eltypes=eltypes)

##############################################################################
#
# HTML output
#
##############################################################################

function digitsep(value::Integer)
    # Adapted from https://github.com/IainNZ/Humanize.jl
    value = string(abs(value))
    group_ends = reverse(collect(length(value):-3:1))
    groups = [value[max(end_index - 2, 1):end_index]
              for end_index in group_ends]
    return join(groups, ',')
end

function html_escape(cell::AbstractString)
    cell = replace(cell, "&"=>"&amp;")
    cell = replace(cell, "<"=>"&lt;")
    cell = replace(cell, ">"=>"&gt;")
    return cell
end

function _show(io::IO, ::MIME"text/html", df::AbstractDataFrame;
               summary::Bool=true, eltypes::Bool=true, rowid::Union{Int,Nothing}=nothing)
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
        tty_rows, tty_cols = displaysize(io)
        mxrow = min(mxrow, tty_rows)
        maxwidths = getmaxwidths(df, io, 1:mxrow, 0:-1, :X, nothing, true, buffer) .+ 2
        mxcol = min(mxcol, searchsortedfirst(cumsum(maxwidths), tty_cols))
    end

    cnames = _names(df)[1:mxcol]
    write(io, "<table class=\"data-frame\">")
    write(io, "<thead>")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$(html_escape(String(column_name)))</th>")
    end
    write(io, "</tr>")
    if eltypes
        write(io, "<tr>")
        write(io, "<th></th>")
        for j in 1:mxcol
            s = html_escape(compacttype(eltype(df[!, j])))
            write(io, "<th>$s</th>")
        end
        write(io, "</tr>")
    end
    write(io, "</thead>")
    write(io, "<tbody>")
    if summary
        omitmsg = if mxcol < size(df, 2)
                      " (omitted printing of $(size(df, 2)-mxcol) columns)"
                  else
                      ""
                  end
        write(io, "<p>$(digitsep(nrow(df))) rows × $(digitsep(ncol(df))) columns$omitmsg</p>")
    end
    for row in 1:mxrow
        write(io, "<tr>")
        if rowid === nothing
            write(io, "<th>$row</th>")
        else
            write(io, "<th>$rowid</th>")
        end
        for column_name in cnames
            if isassigned(df[!, column_name], row)
                cell_val = df[row, column_name]
                if ismissing(cell_val)
                    write(io, "<td><em>missing</em></td>")
                elseif cell_val isa SHOW_TABULAR_TYPES
                    write(io, "<td><em>")
                    cell = sprint(ourshow, cell_val)
                    write(io, html_escape(cell))
                    write(io, "</em></td>")
                else
                    cell = sprint(ourshow, cell_val)
                    write(io, "<td>$(html_escape(cell))</td>")
                end
            else
                write(io, "<td><em>#undef</em></td>")
            end
        end
        write(io, "</tr>")
    end
    if size(df, 1) > mxrow
        write(io, "<tr>")
        write(io, "<th>&vellip;</th>")
        for column_name in cnames
            write(io, "<td>&vellip;</td>")
        end
        write(io, "</tr>")
    end
    write(io, "</tbody>")
    write(io, "</table>")
end

function Base.show(io::IO, mime::MIME"text/html", dfr::DataFrameRow;
                   summary::Bool=true, eltypes::Bool=true)
    r, c = parentindices(dfr)
    summary && write(io, "<p>DataFrameRow ($(length(dfr)) columns)</p>")
    _show(io, mime, view(parent(dfr), [r], c), summary=false, eltypes=eltypes, rowid=r)
end

function Base.show(io::IO, mime::MIME"text/html", dfrs::DataFrameRows;
                   summary::Bool=true, eltypes::Bool=true)
    df = parent(dfrs)
    summary && write(io, "<p>$(nrow(df))×$(ncol(df)) DataFrameRows</p>")
    _show(io, mime, df, summary=false, eltypes=eltypes)
end

function Base.show(io::IO, mime::MIME"text/html", dfcs::DataFrameColumns;
                   summary::Bool=true, eltypes::Bool=true)
    df = parent(dfcs)
    if summary
        write(io, "<p>$(nrow(df))×$(ncol(df)) DataFrameColumns</p>")
    end
    _show(io, mime, df, summary=false, eltypes=eltypes)
end

function Base.show(io::IO, mime::MIME"text/html", gd::GroupedDataFrame)
    N = length(gd)
    parent_names = _names(gd)
    keys = html_escape(join(string.(groupcols(gd)), ", "))
    keystr = length(gd.cols) > 1 ? "keys" : "key"
    groupstr = N > 1 ? "groups" : "group"
    write(io, "<p><b>$(typeof(gd).name) with $N $groupstr based on $keystr: $keys</b></p>")
    if N > 0
        nrows = size(gd[1], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [html_escape(string(parent_names[col], " = ",
                                                repr(first(gd[1][!, col]))))
                             for col in gd.cols]

        write(io, "<p><i>First Group ($nrows $rows): ")
        join(io, identified_groups, ", ")
        write(io, "</i></p>")
        show(io, mime, gd[1], summary=false)
    end
    if N > 1
        nrows = size(gd[N], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [html_escape(string(parent_names[col], " = ",
                                                repr(first(gd[N][!, col]))))
                             for col in gd.cols]

        write(io, "<p>&vellip;</p>")
        write(io, "<p><i>Last Group ($nrows $rows): ")
        join(io, identified_groups, ", ")
        write(io, "</i></p>")
        show(io, mime, gd[N], summary=false)
    end
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
    replace(cell, ['\\','~','#','$','%','&','_','^','{','}']=>latex_char_escape)
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
        maxwidths = getmaxwidths(df, io, 1:mxrow, 0:-1, :X, nothing, true, buffer) .+ 2
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
        header = join(map(c -> latex_escape(string(compacttype(c))),
                          eltype.(eachcol(df)[1:mxcol])), " & ")
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
                cell = df[row,col]
                if ismissing(cell)
                    print(io, "\\emph{missing}")
                elseif cell isa SHOW_TABULAR_TYPES
                    print(io, "\\emph{")
                    print(io, latex_escape(sprint(ourshow, cell, context=io)))
                    print(io, "}")
                else
                    if showable(MIME("text/latex"), cell)
                        show(io, MIME("text/latex"), cell)
                    else
                        print(io, latex_escape(sprint(ourshow, cell, context=io)))
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

function Base.show(io::IO, mime::MIME"text/latex", dfr::DataFrameRow; eltypes::Bool=true)
    r, c = parentindices(dfr)
    _show(io, mime, view(parent(dfr), [r], c), eltypes=eltypes, rowid=r)
end

Base.show(io::IO, mime::MIME"text/latex", dfrs::DataFrameRows; eltypes::Bool=true) =
	_show(io, mime, parent(dfrs), eltypes=eltypes)
Base.show(io::IO, mime::MIME"text/latex", dfcs::DataFrameColumns; eltypes::Bool=true) =
	_show(io, mime, parent(dfcs), eltypes=eltypes)

function Base.show(io::IO, mime::MIME"text/latex", gd::GroupedDataFrame)
    N = length(gd)
    parent_names = _names(gd)
    keys = join(latex_escape.(string.(groupcols(gd))), ", ")
    keystr = length(gd.cols) > 1 ? "keys" : "key"
    groupstr = N > 1 ? "groups" : "group"
    write(io, "$(typeof(gd).name) with $N $groupstr based on $keystr: $keys\n\n")
    if N > 0
        nrows = size(gd[1], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [latex_escape(string(parent_names[col], " = ",
                                                 repr(first(gd[1][!, col]))))
                             for col in gd.cols]

        write(io, "First Group ($nrows $rows): ")
        join(io, identified_groups, ", ")
        write(io, "\n\n")
        show(io, mime, gd[1])
    end
    if N > 1
        nrows = size(gd[N], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [latex_escape(string(parent_names[col], " = ",
                                                 repr(first(gd[N][!, col]))))
                             for col in gd.cols]

        write(io, "\n\$\\dots\$\n\n")
        write(io, "Last Group ($nrows $rows): ")
        join(io, identified_groups, ", ")
        write(io, "\n\n")
        show(io, mime, gd[N])
    end
end

##############################################################################
#
# MIME: text/csv and text/tab-separated-values
#
##############################################################################

escapedprint(io::IO, x::Any, escapes::AbstractString) = ourshow(io, x)
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
            if ismissing(df[i, j])
                print(io, missingstring)
            elseif isnothing(df[i, j])
                print(io, nothingstring)
            else
                if ! (etypes[j] <: Real)
                    print(io, quotemark)
                    escapedprint(io, df[i, j], quotestr)
                    print(io, quotemark)
                else
                    print(io, df[i, j])
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

function Base.show(io::IO, mime::MIME"text/csv", dfr::DataFrameRow)
    r, c = parentindices(dfr)
    show(io, mime, view(parent(dfr), [r], c))
end

function Base.show(io::IO, mime::MIME"text/tab-separated-values", dfr::DataFrameRow)
    r, c = parentindices(dfr)
    show(io, mime, view(parent(dfr), [r], c))
end

Base.show(io::IO, mime::MIME"text/csv",
          dfs::Union{DataFrameRows, DataFrameColumns}) =
    show(io, mime, parent(dfs))
Base.show(io::IO, mime::MIME"text/tab-separated-values",
          dfs::Union{DataFrameRows, DataFrameColumns}) =
    show(io, mime, parent(dfs))

function Base.show(io::IO, mime::MIME"text/csv", gd::GroupedDataFrame)
    isfirst = true
    for sdf in gd
        printtable(io, sdf, header = isfirst, separator = ',')
        isfirst && (isfirst = false)
    end
end

function Base.show(io::IO, mime::MIME"text/tab-separated-values", gd::GroupedDataFrame)
    isfirst = true
    for sdf in gd
        printtable(io, sdf, header = isfirst, separator = '\t')
        isfirst && (isfirst = false)
    end
end
