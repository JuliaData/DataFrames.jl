##############################################################################
#
# Text output
#
##############################################################################

function escapedprint(io::IO, x::Any, escapes::AbstractString)
    print(io, x)
end

if VERSION < v"0.5.0-dev+4354"
    function escapedprint(io::IO, x::AbstractString, escapes::AbstractString)
        print_escaped(io, x, escapes)
    end
else
    function escapedprint(io::IO, x::AbstractString, escapes::AbstractString)
        escape_string(io, x, escapes)
    end
end

function printtable(io::IO,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    nastring::AbstractString = "NA")
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
            if ! (isna(df[j],i))
                if ! (etypes[j] <: Real)
                    print(io, quotemark)
                    escapedprint(io, df[i, j], quotestr)
                    print(io, quotemark)
                else
                    print(io, df[i, j])
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

function printtable(df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = ',',
                    quotemark::Char = '"',
                    nastring::AbstractString = "NA")
    printtable(STDOUT,
               df,
               header = header,
               separator = separator,
               quotemark = quotemark,
               nastring = nastring)
    return
end

"""
Write data to a tabular-file format (CSV, TSV, ...)

```julia
writetable(filename, df, [keyword options])
```

### Arguments

* `filename::AbstractString` : the filename to be created
* `df::AbstractDataFrame` : the AbstractDataFrame to be written

### Keyword Arguments

* `separator::Char` -- The separator character that you would like to use. Defaults to the output of `getseparator(filename)`, which uses commas for files that end in `.csv`, tabs for files that end in `.tsv` and a single space for files that end in `.wsv`.
* `quotemark::Char` -- The character used to delimit string fields. Defaults to `'"'`.
* `header::Bool` -- Should the file contain a header that specifies the column names from `df`. Defaults to `true`.
* `nastring::AbstractString` -- What to write in place of missing data. Defaults to `"NA"`.

### Result

* `::DataFrame`

### Examples

```julia
df = DataFrame(A = 1:10)
writetable("output.csv", df)
writetable("output.dat", df, separator = ',', header = false)
writetable("output.dat", df, quotemark = '\', separator = ',')
writetable("output.dat", df, header = false)
```
"""
function writetable(filename::AbstractString,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = getseparator(filename),
                    quotemark::Char = '"',
                    nastring::AbstractString = "NA",
                    append::Bool = false)

    if endswith(filename, ".bz") || endswith(filename, ".bz2")
        throw(ArgumentError("BZip2 compression not yet implemented"))
    end

    if append && isfile(filename) && filesize(filename) > 0
        file_df = readtable(filename, header = false, nrows = 1)

        # Check if number of columns matches
        if size(file_df, 2) != size(df, 2)
            throw(DimensionMismatch("Number of columns differ between file and DataFrame"))
        end

        # When 'append'-ing to a nonempty file,
        # 'header' triggers a check for matching colnames
        if header
            if any(i -> Symbol(file_df[1, i]) != index(df)[i], 1:size(df, 2))
                throw(KeyError("Column names don't match names in file"))
            end

            header = false
        end
    end

    openfunc = endswith(filename, ".gz") ? gzopen : open

    openfunc(filename, append ? "a" : "w") do io
        printtable(io,
                   df,
                   header = header,
                   separator = separator,
                   quotemark = quotemark,
                   nastring = nastring)
    end

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

@compat function Base.show(io::IO, ::MIME"text/html", df::AbstractDataFrame)
    n = size(df, 1)
    cnames = _names(df)
    write(io, "<table class=\"data-frame\">")
    write(io, "<thead>")
    write(io, "<tr>")
    write(io, "<th></th>")
    for column_name in cnames
        write(io, "<th>$column_name</th>")
    end
    write(io, "</tr>")
    write(io, "</thead>")
    write(io, "<tbody>")
    tty_rows, tty_cols = displaysize(io)
    mxrow = min(n,tty_rows)
    for row in 1:mxrow
        write(io, "<tr>")
        write(io, "<th>$row</th>")
        for column_name in cnames
            cell = string(df[row, column_name])
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

##############################################################################
#
# MIME
#
##############################################################################

@compat function Base.show(io::IO, ::MIME"text/csv", df::AbstractDataFrame)
    printtable(io, df, true, ',')
end

@compat function Base.show(io::IO, ::MIME"text/tab-separated-values", df::AbstractDataFrame)
    printtable(io, df, true, '\t')
end

##############################################################################
#
# CSV/DataStreams-based IO
#
##############################################################################

importall DataStreams

# DataFrames DataStreams implementation
function Data.schema(df::DataFrame, ::Type{Data.Column})
    return Data.Schema(map(string, names(df)),
            DataType[typeof(A) for A in df.columns], size(df, 1))
end

# DataFrame as a Data.Source
function Data.isdone(source::DataFrame, row, col)
    rows, cols = size(source)
    return row > rows || col > cols
end

Data.streamtype(::Type{DataFrame}, ::Type{Data.Column}) = true
Data.streamtype(::Type{DataFrame}, ::Type{Data.Field}) = true

Data.streamfrom{T <: AbstractVector}(source::DataFrame, ::Type{Data.Column}, ::Type{T}, col) = (
    @inbounds A = source.columns[col]::T; return A)
Data.streamfrom{T}(source::DataFrame, ::Type{Data.Column}, ::Type{T}, col) = (
    @inbounds A = source.columns[col]; return A)
Data.streamfrom{T}(source::DataFrame, ::Type{Data.Field}, ::Type{T}, row, col) = (
    @inbounds A = Data.streamfrom(source, Data.Column, T, col); return A[row]::T)

# DataFrame as a Data.Sink
allocate{T}(::Type{T}, rows) = Vector{T}(rows)
allocate{T}(::Type{Vector{T}}, rows) = Vector{T}(rows)
allocate{T}(::Type{Nullable{T}}, rows) = DataArray(Vector{T}(rows))
allocate{T}(::Type{DataVector{T}}, rows) = DataArray(Vector{T}(rows))
allocate{S,R}(::Type{PooledDataVector{S,R}}, rows) = PooledDataArray{S,1,R}(rows)

function DataFrame{T <: Data.StreamType}(sch::Data.Schema,
                                         ::Type{T}=Data.Field,
                                         append::Bool=false,
                                         ref::Vector{UInt8}=UInt8[], args...)
    rows, cols = size(sch)
    rows = max(0, T <: Data.Column ? 0 : rows) # don't pre-allocate for Column streaming
    columns = Vector{Any}(cols)
    types = Data.types(sch)
    for i = 1:cols
        columns[i] = allocate(types[i], rows)
    end
    return DataFrame(columns, map(Symbol, Data.header(sch)))
end

# given an existing DataFrame (`sink`), make any necessary changes for streaming source
# with Data.Schema `sch` to it, given we know if we'll be `appending` or not
function DataFrame(sink, sch::Data.Schema, ::Type{Data.Field}, append::Bool, ref::Vector{UInt8})
    rows, cols = size(sch)
    newsize = max(0, rows) + (append ? size(sink, 1) : 0)
    newsize != size(sink, 1) && foreach(x->resize!(x, newsize), sink.columns)
    sch.rows = newsize
    return sink
end
function DataFrame(sink, sch::Data.Schema, ::Type{Data.Column}, append::Bool, ref::Vector{UInt8})
    rows, cols = size(sch)
    append ? (sch.rows += size(sink, 1)) : foreach(empty!, sink.columns)
    return sink
end

Data.streamtypes(::Type{DataFrame}) = [Data.Column, Data.Field]

Data.streamto!{T}(sink::DataFrame,
                  ::Type{Data.Field},
                  val::T,
                  row,
                  col,
                  sch::Data.Schema{false}) = push!(sink.columns[col]::Vector{T}, val)
Data.streamto!{T}(sink::DataFrame,
                  ::Type{Data.Field},
                  val::Nullable{T},
                  row,
                  col,
                  sch::Data.Schema{false}) = push!(sink.columns[col]::DataVector{T}, isnull(val) ? NA : get(val))
Data.streamto!{T}(sink::DataFrame,
                  ::Type{Data.Field},
                  val::T,
                  row,
                  col,
                  sch::Data.Schema{true}) = (sink.columns[col]::Vector{T})[row] = val
Data.streamto!{T}(sink::DataFrame,
                  ::Type{Data.Field},
                  val::Nullable{T},
                  row,
                  col,
                  sch::Data.Schema{true}) = (sink.columns[col]::DataVector{T})[row] = isnull(val) ? NA : get(val)

function Data.streamto!{T}(sink::DataFrame, ::Type{Data.Column}, column::T, row, col, sch::Data.Schema)
    if row == 0
        sink.columns[col] = column
    else
        append!(sink.columns[col]::T, column)
    end
    return length(column)
end
