##############################################################################
#
# Text output
#
##############################################################################

function escapedprint(io::IO, x::Any, escapes::AbstractString)
    ourshowcompact(io, x)
end

function escapedprint(io::IO, x::AbstractString, escapes::AbstractString)
    escape_string(io, x, escapes)
end

function digitsep(value::Integer)
    # Adapted from https://github.com/IainNZ/Humanize.jl
    value = string(abs(value))
    group_ends = reverse(collect(length(value):-3:1))
    groups = [value[max(end_index - 2, 1):end_index]
              for end_index in group_ends]
    return join(groups, ',')
end

function printtable(io::IO,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    missingstring::AbstractString = "missing")
    n, p = size(df)
    etypes = eltypes(df)
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
            if !ismissing(df[j][i])
                if ! (etypes[j] <: Real)
                    print(io, quotemark)
                    escapedprint(io, df[i, j], quotestr)
                    print(io, quotemark)
                else
                    print(io, df[i, j])
                end
            else
                print(io, missingstring)
            end
            if j < p
                print(io, separator)
            else
                print(io, '\n')
            end
        end
    end
    return
end

function printtable(df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    missingstring::AbstractString = "missing")
    printtable(stdout,
               df,
               header = header,
               separator = separator,
               quotemark = quotemark,
               missingstring = missingstring)
    return
end
##############################################################################
#
# HTML output
#
##############################################################################

function html_escape(cell::AbstractString)
    cell = replace(cell, "&"=>"&amp;")
    cell = replace(cell, "<"=>"&lt;")
    cell = replace(cell, ">"=>"&gt;")
    return cell
end

Base.show(io::IO, mime::MIME"text/html", df::AbstractDataFrame; summary::Bool=true) =
    _show(io, mime, df, summary=summary)

function _show(io::IO, ::MIME"text/html", df::AbstractDataFrame;
               summary::Bool=true, rowid::Union{Int,Nothing}=nothing)
    n = size(df, 1)
    if rowid !== nothing && n != 1
        throw(ArgumentError("rowid may be passed only with a single row data frame"))
    end
    cnames = _names(df)
    write(io, "<table class=\"data-frame\">")
    write(io, "<thead>")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$(html_escape(String(column_name)))</th>")
    end
    write(io, "</tr>")
    write(io, "<tr>")
    write(io, "<th></th>")
    for j in 1:ncol(df)
        s = html_escape(compacttype(eltype(df[j])))
        write(io, "<th>$s</th>")
    end
    write(io, "</tr>")
    write(io, "</thead>")
    write(io, "<tbody>")
    haslimit = get(io, :limit, true)
    if haslimit
        tty_rows, tty_cols = displaysize(io)
        mxrow = min(n,tty_rows)
    else
        mxrow = n
    end
    if summary
        write(io, "<p>$(digitsep(n)) rows × $(digitsep(ncol(df))) columns</p>")
    end
    for row in 1:mxrow
        write(io, "<tr>")
        if rowid === nothing
            write(io, "<th>$row</th>")
        else
            write(io, "<th>$rowid</th>")
        end
        for column_name in cnames
            if isassigned(df[column_name], row)
                cell = sprint(ourshowcompact, df[row, column_name])
            else
                cell = sprint(ourshowcompact, Base.undef_ref_str)
            end
            write(io, "<td>$(html_escape(cell))</td>")
        end
        write(io, "</tr>")
    end
    if n > mxrow
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

function Base.show(io::IO, mime::MIME"text/html", dfr::DataFrameRow; summary::Bool=true)
    r, c = parentindices(dfr)
    write(io, "<p>DataFrameRow</p>")
    _show(io, mime, view(parent(dfr), [r], c), summary=summary, rowid=r)
end

function Base.show(io::IO, mime::MIME"text/html", gd::GroupedDataFrame)
    N = length(gd)
    keynames = names(gd.parent)[gd.cols]
    parent_names = names(gd.parent)
    keys = join(':' .* string.(keynames), ", ")
    keystr = length(gd.cols) > 1 ? "keys" : "key"
    groupstr = N > 1 ? "groups" : "group"
    write(io, "<p><b>$(typeof(gd).name) with $N $groupstr based on $keystr: $keys</b></p>")
    if N > 0
        nrows = size(gd[1], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [':' * string(parent_names[col], " = ", first(gd[1][col]))
                             for col in gd.cols]

        write(io, "<p><i>First Group ($nrows $rows): ")
        join(io, identified_groups, ", ")
        write(io, "</i></p>")
        show(io, mime, gd[1], summary=false)
    end
    if N > 1
        nrows = size(gd[N], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [':' * string(parent_names[col], " = ", first(gd[N][col]))
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

Base.show(io::IO, mime::MIME"text/latex", df::AbstractDataFrame) =
    _show(io, mime, df)

function _show(io::IO, ::MIME"text/latex", df::AbstractDataFrame; rowid=nothing)
    nrows = size(df, 1)
    ncols = size(df, 2)

    if rowid !== nothing && nrows != 1
        throw(ArgumentError("rowid may be passed only with a single row data frame"))
    end

    haslimit = get(io, :limit, true)
    if haslimit
        tty_rows, tty_cols = displaysize(io)
        mxrow = min(nrows,tty_rows)
    else
        mxrow = nrows
    end

    cnames = _names(df)
    alignment = repeat("c", ncols)
    write(io, "\\begin{tabular}{r|")
    write(io, alignment)
    write(io, "}\n")
    write(io, "\t& ")
    header = join(map(c -> latex_escape(string(c)), cnames), " & ")
    write(io, header)
    write(io, "\\\\\n")
    write(io, "\t\\hline\n")
    write(io, "\t& ")
    header = join(map(c -> latex_escape(string(compacttype(c))), eltypes(df)), " & ")
    write(io, header)
    write(io, "\\\\\n")
    write(io, "\t\\hline\n")
    for row in 1:mxrow
        write(io, "\t")
        write(io, @sprintf("%d", rowid === nothing ? row : rowid))
        for col in 1:ncols
            write(io, " & ")
            cell = isassigned(df[col], row) ? df[row,col] : Base.undef_ref_str
            if !ismissing(cell)
                if showable(MIME("text/latex"), cell)
                    show(io, MIME("text/latex"), cell)
                else
                    print(io, latex_escape(sprint(ourshowcompact, cell)))
                end
            end
        end
        write(io, " \\\\\n")
    end
    if nrows > mxrow
        write(io, "\t\$\\dots\$")
        for col in 1:ncols
            write(io, " & \$\\dots\$")
        end
        write(io, " \\\\\n")
    end
    write(io, "\\end{tabular}\n")
end

function Base.show(io::IO, mime::MIME"text/latex", dfr::DataFrameRow)
    r, c = parentindices(dfr)
    _show(io, mime, view(parent(dfr), [r], c), rowid=r)
end

function Base.show(io::IO, mime::MIME"text/latex", gd::GroupedDataFrame)
    N = length(gd)
    keynames = names(gd.parent)[gd.cols]
    parent_names = names(gd.parent)
    keys = join(latex_escape.(':' .* string.(keynames)), ", ")
    keystr = length(gd.cols) > 1 ? "keys" : "key"
    groupstr = N > 1 ? "groups" : "group"
    write(io, "$(typeof(gd).name) with $N $groupstr based on $keystr: $keys\n\n")
    if N > 0
        nrows = size(gd[1], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [latex_escape(':' * string(parent_names[col], " = ",
                                                       first(gd[1][col])))
                             for col in gd.cols]

        write(io, "First Group ($nrows $rows): ")
        join(io, identified_groups, ", ")
        write(io, "\n\n")
        show(io, mime, gd[1])
    end
    if N > 1
        nrows = size(gd[N], 1)
        rows = nrows > 1 ? "rows" : "row"

        identified_groups = [latex_escape(':' * string(parent_names[col], " = ",
                                                       first(gd[N][col])))
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
# MIME
#
##############################################################################

function Base.show(io::IO, mime::MIME"text/csv", dfr::DataFrameRow)
    r, c = parentindices(dfr)
    show(io, mime, view(parent(dfr), [r], c))
end

function Base.show(io::IO, mime::MIME"text/tab-separated-values", dfr::DataFrameRow)
    r, c = parentindices(dfr)
    show(io, mime, view(parent(dfr), [r], c))
end

function Base.show(io::IO, ::MIME"text/csv", df::AbstractDataFrame)
    printtable(io, df, header = true, separator = ',')
end

function Base.show(io::IO, ::MIME"text/tab-separated-values", df::AbstractDataFrame)
    printtable(io, df, header = true, separator = '\t')
end

##############################################################################
#
# DataStreams-based IO
#
##############################################################################

using DataStreams, WeakRefStrings

struct DataFrameStream{T}
    columns::T
    header::Vector{String}
end
DataFrameStream(df::DataFrame) = DataFrameStream(Tuple(_columns(df)), string.(names(df)))

# DataFrame Data.Source implementation
Data.schema(df::DataFrame) =
    Data.Schema(Type[eltype(A) for A in _columns(df)], string.(names(df)), size(df, 1))

Data.isdone(source::DataFrame, row, col, rows, cols) = row > rows || col > cols
function Data.isdone(source::DataFrame, row, col)
    cols = length(source)
    return Data.isdone(source, row, col, cols == 0 ? 0 : length(source.columns[1]), cols)
end

Data.streamtype(::Type{DataFrame}, ::Type{Data.Column}) = true
Data.streamtype(::Type{DataFrame}, ::Type{Data.Field}) = true

Data.streamfrom(source::DataFrame, ::Type{Data.Column}, ::Type{T}, row, col) where {T} =
    source[col]
Data.streamfrom(source::DataFrame, ::Type{Data.Field}, ::Type{T}, row, col) where {T} =
    source[col][row]

# DataFrame Data.Sink implementation
Data.streamtypes(::Type{DataFrame}) = [Data.Column, Data.Field]
Data.weakrefstrings(::Type{DataFrame}) = true

allocate(::Type{T}, rows, ref) where {T} = Vector{T}(undef, rows)
allocate(::Type{CategoricalString{R}}, rows, ref) where {R} =
    CategoricalArray{String, 1, R}(undef, rows)
allocate(::Type{Union{CategoricalString{R}, Missing}}, rows, ref) where {R} =
    CategoricalArray{Union{String, Missing}, 1, R}(undef, rows)
allocate(::Type{CategoricalValue{T, R}}, rows, ref) where {T, R} =
    CategoricalArray{T, 1, R}(undef, rows)
allocate(::Type{Union{Missing, CategoricalValue{T, R}}}, rows, ref) where {T, R} =
    CategoricalArray{Union{Missing, T}, 1, R}(undef, rows)
allocate(::Type{WeakRefString{T}}, rows, ref) where {T} =
    WeakRefStringArray(ref, WeakRefString{T}, rows)
allocate(::Type{Union{Missing, WeakRefString{T}}}, rows, ref) where {T} =
    WeakRefStringArray(ref, Union{Missing, WeakRefString{T}}, rows)
allocate(::Type{Missing}, rows, ref) = missings(rows)

# Construct or modify a DataFrame to be ready to stream data from a source with `sch`
function DataFrame(sch::Data.Schema{R}, ::Type{S}=Data.Field,
                   append::Bool=false, args...;
                   reference::Vector{UInt8}=UInt8[]) where {R, S <: Data.StreamType}
    types = Data.types(sch)
    if !isempty(args) && args[1] isa DataFrame && types == Data.types(Data.schema(args[1]))
        # passing in an existing DataFrame Sink w/ same types as source
        sink = args[1]
        sinkrows = size(Data.schema(sink), 1)
        # are we appending and either column-streaming or there are an unknown # of rows
        if append && (S == Data.Column || !R)
            sch.rows = sinkrows
            # dont' need to do anything because:
              # for Data.Column, we just append columns anyway (see Data.streamto! below)
              # for Data.Field, unknown # of source rows, so we'll just push! in streamto!
        else
            # need to adjust the existing sink
            # similar to above, for Data.Column or unknown # of rows for Data.Field,
                # we'll append!/push! in streamto!, so we empty! the columns
            # if appending, we want to grow our columns to be able to include every row
                # in source (sinkrows + sch.rows)
            # if not appending, we're just "re-using" a sink, so we just resize it
                # to the # of rows in the source
            newsize = ifelse(S == Data.Column || !R, 0,
                        ifelse(append, sinkrows + sch.rows, sch.rows))
            foreach(col->resize!(col, newsize), _columns(sink))
            sch.rows = newsize
        end
        # take care of a possible reference from source by addint to WeakRefStringArrays
        if !isempty(reference)
            foreach(col-> col isa WeakRefStringArray && push!(col.data, reference),
                    _columns(sink))
        end
        return DataFrameStream(sink)
    else
        # allocating a fresh DataFrame Sink; append is irrelevant
        # for Data.Column or unknown # of rows in Data.Field, we only ever append!,
            # so just allocate empty columns
        rows = ifelse(S == Data.Column, 0, ifelse(!R, 0, sch.rows))
        names = Data.header(sch)
        sch.rows = rows
        return DataFrameStream(Tuple(allocate(types[i], rows, reference)
                                     for i = 1:length(types)), names)
    end
end

DataFrame(sink, sch::Data.Schema, ::Type{S}, append::Bool;
          reference::Vector{UInt8}=UInt8[]) where {S} =
    DataFrame(sch, S, append, sink; reference=reference)

@inline Data.streamto!(sink::DataFrameStream, ::Type{Data.Field}, val,
                      row, col::Int) =
    (A = sink.columns[col]; row > length(A) ? push!(A, val) : setindex!(A, val, row))
@inline Data.streamto!(sink::DataFrameStream, ::Type{Data.Field}, val,
                       row, col::Int, ::Type{Val{false}}) =
    push!(sink.columns[col], val)
@inline Data.streamto!(sink::DataFrameStream, ::Type{Data.Field}, val,
                       row, col::Int, ::Type{Val{true}}) =
    sink.columns[col][row] = val
@inline function Data.streamto!(sink::DataFrameStream, ::Type{Data.Column}, column,
                       row, col::Int, knownrows)
    append!(sink.columns[col], column)
end

Data.close!(df::DataFrameStream) =
    DataFrame(collect(AbstractVector, df.columns), Symbol.(df.header), makeunique=true)
