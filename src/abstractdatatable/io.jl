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

function printtable(io::IO,
                    dt::AbstractDataTable;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    nastring::AbstractString = "NULL")
    n, p = size(dt)
    etypes = eltypes(dt)
    if header
        cnames = _names(dt)
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
            if !isnull(dt[j][i])
                if ! (etypes[j] <: Real)
                    print(io, quotemark)
                    x = unsafe_get(dt[i, j])
                    escapedprint(io, x, quotestr)
                    print(io, quotemark)
                else
                    print(io, dt[i, j])
                end
            else
                print(io, nastring)
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

function printtable(dt::AbstractDataTable;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    nastring::AbstractString = "NULL")
    printtable(STDOUT,
               dt,
               header = header,
               separator = separator,
               quotemark = quotemark,
               nastring = nastring)
    return
end
##############################################################################
#
# HTML output
#
##############################################################################

function html_escape(cell::AbstractString)
    cell = replace(cell, "&", "&amp;")
    cell = replace(cell, "<", "&lt;")
    cell = replace(cell, ">", "&gt;")
    return cell
end

@compat function Base.show(io::IO, ::MIME"text/html", dt::AbstractDataTable)
    cnames = _names(dt)
    write(io, "<table class=\"data-frame\">")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$column_name</th>")
    end
    write(io, "</tr>")
    haslimit = get(io, :limit, true)
    n = size(dt, 1)
    if haslimit
        tty_rows, tty_cols = _displaysize(io)
        mxrow = min(n,tty_rows)
    else
        mxrow = n
    end
    for row in 1:mxrow
        write(io, "<tr>")
        write(io, "<th>$row</th>")
        for column_name in cnames
            cell = sprint(ourshowcompact, dt[row, column_name])
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
    write(io, "</table>")
end

##############################################################################
#
# LaTeX output
#
##############################################################################

function latex_char_escape(char::AbstractString)
    if char == "\\"
        return "\\textbackslash{}"
    elseif char == "~"
        return "\\textasciitilde{}"
    else
        return string("\\", char)
    end
end

function latex_escape(cell::AbstractString)
    cell = replace(cell, ['\\','~','#','$','%','&','_','^','{','}'], latex_char_escape)
    return cell
end

function Base.show(io::IO, ::MIME"text/latex", dt::AbstractDataTable)
    nrows = size(dt, 1)
    ncols = size(dt, 2)
    cnames = _names(dt)
    alignment = repeat("c", ncols)
    write(io, "\\begin{tabular}{r|")
    write(io, alignment)
    write(io, "}\n")
    write(io, "\t& ")
    header = join(map(c -> latex_escape(string(c)), cnames), " & ")
    write(io, header)
    write(io, "\\\\\n")
    write(io, "\t\\hline\n")
    for row in 1:nrows
        write(io, "\t")
        write(io, @sprintf("%d", row))
        for col in 1:ncols
            write(io, " & ")
            cell = dt[row,col]
            if !isnull(cell)
                content = unsafe_get(cell)
                if mimewritable(MIME("text/latex"), content)
                    show(io, MIME("text/latex"), content)
                else
                    print(io, latex_escape(sprint(ourshowcompact, content)))
                end
            end
        end
        write(io, " \\\\\n")
    end
    write(io, "\\end{tabular}\n")
end

##############################################################################
#
# MIME
#
##############################################################################

@compat function Base.show(io::IO, ::MIME"text/csv", dt::AbstractDataTable)
    printtable(io, dt, true, ',')
end

@compat function Base.show(io::IO, ::MIME"text/tab-separated-values", dt::AbstractDataTable)
    printtable(io, dt, true, '\t')
end

##############################################################################
#
# DataStreams-based IO
#
##############################################################################

importall DataStreams
using WeakRefStrings

# DataTables DataStreams implementation
function Data.schema(df::DataTable, ::Type{Data.Column})
    return Data.Schema(map(string, names(df)),
                       DataType[typeof(A) for A in df.columns], size(df, 1))
end

# DataTable as a Data.Source
function Data.isdone(source::DataTable, row, col)
    rows, cols = size(source)
    return row > rows || col > cols
end

Data.streamtype(::Type{DataTable}, ::Type{Data.Column}) = true
Data.streamtype(::Type{DataTable}, ::Type{Data.Field}) = true

Data.streamfrom{T <: AbstractVector}(source::DataTable, ::Type{Data.Column}, ::Type{T}, col) =
    (@inbounds A = source.columns[col]::T; return A)
Data.streamfrom{T}(source::DataTable, ::Type{Data.Column}, ::Type{T}, col) =
    (@inbounds A = source.columns[col]; return A)
Data.streamfrom{T}(source::DataTable, ::Type{Data.Field}, ::Type{T}, row, col) =
    (@inbounds A = Data.streamfrom(source, Data.Column, T, col); return A[row]::T)

# DataTable as a Data.Sink
allocate{T}(::Type{T}, rows, ref) = Array{T}(rows)
allocate{T}(::Type{Vector{T}}, rows, ref) = Array{T}(rows)

allocate{T}(::Type{Nullable{T}}, rows, ref) =
    NullableArray{T, 1}(Array{T}(rows), fill(true, rows), isempty(ref) ? UInt8[] : ref)
allocate{T}(::Type{NullableVector{T}}, rows, ref) =
    NullableArray{T, 1}(Array{T}(rows), fill(true, rows), isempty(ref) ? UInt8[] : ref)

allocate{S,R}(::Type{CategoricalArrays.CategoricalValue{S,R}}, rows, ref) =
    CategoricalArray{S,1,R}(rows)
allocate{S,R}(::Type{CategoricalVector{S,R}}, rows, ref) =
    CategoricalArray{S,1,R}(rows)

allocate{S,R}(::Type{Nullable{CategoricalArrays.CategoricalValue{S,R}}}, rows, ref) =
    NullableCategoricalArray{S,1,R}(rows)
allocate{S,R}(::Type{NullableCategoricalVector{S,R}}, rows, ref) =
    NullableCategoricalArray{S,1,R}(rows)

function DataTable{T <: Data.StreamType}(sch::Data.Schema,
                                         ::Type{T}=Data.Field,
                                         append::Bool=false,
                                         ref::Vector{UInt8}=UInt8[], args...)
    rows, cols = size(sch)
    rows = max(0, T <: Data.Column ? 0 : rows) # don't pre-allocate for Column streaming
    columns = Vector{Any}(cols)
    types = Data.types(sch)
    for i = 1:cols
        columns[i] = allocate(types[i], rows, ref)
    end
    return DataTable(columns, map(Symbol, Data.header(sch)))
end

# given an existing DataTable (`sink`), make any necessary changes for streaming source
# with Data.Schema `sch` to it, given we know if we'll be `appending` or not
function DataTable(sink, sch::Data.Schema, ::Type{Data.Field}, append::Bool,
                   ref::Vector{UInt8})
    rows, cols = size(sch)
    newsize = max(0, rows) + (append ? size(sink, 1) : 0)
    # need to make sure we don't break a NullableVector{WeakRefString{UInt8}} when appending
    if append
        for (i, T) in enumerate(Data.types(sch))
            if T <: Nullable{WeakRefString{UInt8}}
                sink.columns[i] = NullableArray(String[string(get(x, "")) for x in sink.columns[i]])
                sch.types[i] = Nullable{String}
            end
        end
    end
    newsize != size(sink, 1) && foreach(x->resize!(x, newsize), sink.columns)
    sch.rows = newsize
    return sink
end
function DataTable(sink, sch::Data.Schema, ::Type{Data.Column}, append::Bool, ref::Vector{UInt8})
    rows, cols = size(sch)
    append ? (sch.rows += size(sink, 1)) : foreach(empty!, sink.columns)
    return sink
end

Data.streamtypes(::Type{DataTable}) = [Data.Column, Data.Field]

Data.streamto!{T}(sink::DataTable, ::Type{Data.Field}, val::T, row, col, sch::Data.Schema{false}) =
    push!(sink.columns[col]::Vector{T}, val)
Data.streamto!{T}(sink::DataTable, ::Type{Data.Field}, val::Nullable{T}, row, col, sch::Data.Schema{false}) =
    push!(sink.columns[col]::NullableVector{T}, val)
Data.streamto!{T, R}(sink::DataTable, ::Type{Data.Field}, val::CategoricalValue{T, R}, row, col, sch::Data.Schema{false}) =
    push!(sink.columns[col]::CategoricalVector{T, R}, val)
Data.streamto!{T, R}(sink::DataTable, ::Type{Data.Field}, val::Nullable{CategoricalValue{T, R}}, row, col, sch::Data.Schema{false}) =
    push!(sink.columns[col]::NullableCategoricalVector{T, R}, val)
Data.streamto!{T}(sink::DataTable, ::Type{Data.Field}, val::T, row, col, sch::Data.Schema{true}) =
    (sink.columns[col]::Vector{T})[row] = val
Data.streamto!{T}(sink::DataTable, ::Type{Data.Field}, val::Nullable{T}, row, col, sch::Data.Schema{true}) =
    (sink.columns[col]::NullableVector{T})[row] = val
Data.streamto!(sink::DataTable, ::Type{Data.Field}, val::Nullable{WeakRefString{UInt8}}, row, col, sch::Data.Schema{true}) =
    sink.columns[col][row] = val
Data.streamto!{T, R}(sink::DataTable, ::Type{Data.Field}, val::CategoricalValue{T, R}, row, col, sch::Data.Schema{true}) =
    (sink.columns[col]::CategoricalVector{T, R})[row] = val
Data.streamto!{T, R}(sink::DataTable, ::Type{Data.Field}, val::Nullable{CategoricalValue{T, R}}, row, col, sch::Data.Schema{true}) =
    (sink.columns[col]::NullableCategoricalVector{T, R})[row] = val

function Data.streamto!{T}(sink::DataTable, ::Type{Data.Column}, column::T, row, col, sch::Data.Schema)
    if row == 0
        sink.columns[col] = column
    else
        append!(sink.columns[col]::T, column)
    end
    return length(column)
end

function Base.append!{T}(dest::NullableVector{WeakRefString{T}}, column::NullableVector{WeakRefString{T}})
    offset = length(dest.values)
    parentoffset = length(dest.parent)
    append!(dest.isnull, column.isnull)
    append!(dest.parent, column.parent)
    # appending new data to `dest` would invalid all existing WeakRefString pointers
    resize!(dest.values, length(dest) + length(column))
    for i = 1:offset
        old = dest.values[i]
        dest.values[i] = WeakRefString{T}(pointer(dest.parent, old.ind), old.len, old.ind)
    end
    for i = 1:length(column)
        old = column.values[i]
        dest.values[offset + i] = WeakRefString{T}(pointer(dest.parent, parentoffset + old.ind), old.len, parentoffset + old.ind)
    end
    return length(dest)
end
