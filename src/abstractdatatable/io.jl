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
                    nastring::AbstractString = "null")
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
                    nastring::AbstractString = "null")
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

using DataStreams, WeakRefStrings

struct DataTableStream{T}
    columns::T
    header::Vector{String}
end
DataTableStream(dt::DataTable) = DataTableStream(Tuple(dt.columns), string.(names(dt)))

# DataTable Data.Source implementation
function Data.schema(dt::DataTable)
    return Data.Schema(Type[eltype(A) for A in dt.columns],
                       string.(names(dt)), length(dt) == 0 ? 0 : length(dt.columns[1]))
end

Data.isdone(source::DataTable, row, col, rows, cols) = row > rows || col > cols
function Data.isdone(source::DataTable, row, col)
    cols = length(source)
    return Data.isdone(source, row, col, cols == 0 ? 0 : length(dt.columns[1]), cols)
end

Data.streamtype(::Type{DataTable}, ::Type{Data.Column}) = true
Data.streamtype(::Type{DataTable}, ::Type{Data.Field}) = true

Data.streamfrom(source::DataTable, ::Type{Data.Column}, ::Type{T}, row, col) where {T} =
    source[col]
Data.streamfrom(source::DataTable, ::Type{Data.Field}, ::Type{T}, row, col) where {T} =
    source[col][row]

# DataTable Data.Sink implementation
Data.streamtypes(::Type{DataTable}) = [Data.Column, Data.Field]
Data.weakrefstrings(::Type{DataTable}) = true

allocate(::Type{T}, rows, ref) where {T} = Vector{T}(rows)
allocate(::Type{T}, rows, ref) where {T <: Union{WeakRefString, Null}} =
    WeakRefStringArray(ref, T, rows)

# Construct or modify a DataTable to be ready to stream data from a source with `sch`
function DataTable(sch::Data.Schema{R}, ::Type{S}=Data.Field,
                   append::Bool=false, args...;
                   reference::Vector{UInt8}=UInt8[]) where {R, S <: Data.StreamType}
    types = Data.types(sch)
    if !isempty(args) && args[1] isa DataTable && types == Data.types(Data.schema(args[1]))
        # passing in an existing DataTable Sink w/ same types as source
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
            foreach(col->resize!(col, newsize), sink.columns)
            sch.rows = newsize
        end
        # take care of a possible reference from source by addint to WeakRefStringArrays
        if !isempty(reference)
            foreach(col-> col isa WeakRefStringArray && push!(col.data, reference),
                sink.columns)
        end
        sink = DataTableStream(sink)
    else
        # allocating a fresh DataTable Sink; append is irrelevant
        # for Data.Column or unknown # of rows in Data.Field, we only ever append!,
            # so just allocate empty columns
        rows = ifelse(S == Data.Column, 0, ifelse(!R, 0, sch.rows))
        names = Data.header(sch)
        sink = DataTableStream(
                Tuple(allocate(types[i], rows, reference) for i = 1:length(types)), names)
        sch.rows = rows
    end
    return sink
end

DataTable(sink, sch::Data.Schema, ::Type{S}, append::Bool;
          reference::Vector{UInt8}=UInt8[]) where {S} =
    DataTable(sch, S, append, sink; reference=reference)

@inline Data.streamto!(sink::DataTableStream, ::Type{Data.Field}, val,
                      row, col::Int) =
    (A = sink.columns[col]; row > length(A) ? push!(A, val) : setindex!(A, val, row))
@inline Data.streamto!(sink::DataTableStream, ::Type{Data.Field}, val,
                       row, col::Int, ::Type{Val{false}}) =
    push!(sink.columns[col], val)
@inline Data.streamto!(sink::DataTableStream, ::Type{Data.Field}, val,
                       row, col::Int, ::Type{Val{true}}) =
    sink.columns[col][row] = val
@inline Data.streamto!(sink::DataTableStream, ::Type{Data.Column}, column,
                       row, col::Int, knownrows) =
    append!(sink.columns[col], column)

Data.close!(df::DataTableStream) = DataTable(collect(Any, df.columns), Symbol.(df.header))