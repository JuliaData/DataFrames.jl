# TODO: remove when DataAPI.jl version is bumped
metadata(::T, ::AbstractString; style::Bool=false) where {T} =
    throw(ArgumentError("Objects of type $T do not support getting metadata"))
metadatakeys(::Any) = ()
metadata!(::T, ::AbstractString, ::Any; style) where {T} =
    throw(ArgumentError("Objects of type $T do not support setting metadata"))
deletemetadata!(::T, ::AbstractString) where {T} =
    throw(ArgumentError("Objects of type $T do not support metadata deletion"))
emptymetadata!(::T) where {T} =
    throw(ArgumentError("Objects of type $T do not support metadata deletion"))
colmetadata(::T, ::Int, ::AbstractString; style::Bool=false) where {T} =
    throw(ArgumentError("Objects of type $T do not support getting column metadata"))
colmetadata(::T, ::Symbol, ::AbstractString; style::Bool=false) where {T} =
    throw(ArgumentError("Objects of type $T do not support getting column metadata"))
colmetadatakeys(::Any, ::Int) = ()
colmetadatakeys(::Any, ::Symbol) = ()
colmetadatakeys(::Any) = ()
colmetadata!(::T, ::Int, ::AbstractString, ::Any; style) where {T} =
    throw(ArgumentError("Objects of type $T do not support setting metadata"))
colmetadata!(::T, ::Symbol, ::AbstractString, ::Any; style) where {T} =
    throw(ArgumentError("Objects of type $T do not support setting metadata"))
deletecolmetadata!(::T, ::Symbol, ::AbstractString) where {T} =
    throw(ArgumentError("Objects of type $T do not support metadata deletion"))
deletecolmetadata!(::T, ::Int, ::AbstractString) where {T} =
    throw(ArgumentError("Objects of type $T do not support metadata deletion"))
emptycolmetadata!(::T, ::Symbol) where {T} =
    throw(ArgumentError("Objects of type $T do not support metadata deletion"))
emptycolmetadata!(::T, ::Int) where {T} =
    throw(ArgumentError("Objects of type $T do not support metadata deletion"))
emptycolmetadata!(::T) where {T} =
    throw(ArgumentError("Objects of type $T do not support metadata deletion"))

### Metadata API from DataAPI.jl

"""
    metadata(df::AbstractDataFrame, key::AbstractString; style::Bool=false)
    metadata(dfr::DataFrameRow, key::AbstractString; style::Bool=false)
    metadata(dfc::DataFrameColumns, key::AbstractString; style::Bool=false)
    metadata(dfr::DataFrameRows, key::AbstractString; style::Bool=false)

Return table-level metadata value associated with `df` for key `key`.
If `style=true` return a tuple of metadata value and metadata style.

`SubDataFrame` and `DataFrameRow` expose only `:note`-style metadata of their
parent.

See also: [`metadatakeys`](@ref), [`metadata!`](@ref),
[`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> metadatakeys(df)
()

julia> metadata!(df, "name", "example", style=:note);

julia> metadatakeys(df)
KeySet for a Dict{String, Tuple{Any, Any}} with 1 entry. Keys:
  "name"

julia> metadata(df, "name")
"example"
```
"""
function metadata(df::DataFrame, key::AbstractString; style::Bool=false)
    meta = getfield(df, :metadata)
    meta === nothing && throw(KeyError(key))
    return style ? meta[key] : meta[key][1]
end

metadata(x::Union{DataFrameRows, DataFrameColumns},
         key::AbstractString; style::Bool=false) =
    metadata(parent(x), key, style=style)

function metadata(x::Union{DataFrameRow, SubDataFrame},
                  key::AbstractString; style::Bool=false)
    meta_value, meta_style = metadata(parent(x), key, style=true)
    meta_style !== :note && throw(KeyError("$key with :note-style"))
    return style ? (meta_value, meta_style) : meta_value
end

"""
    metadatakeys(df::AbstractDataFrame)
    metadatakeys(dfr::DataFrameRow)
    metadatakeys(dfc::DataFrameColumns)
    metadatakeys(dfr::DataFrameRows)

Return an iterator of table-level metadata keys which are set in the object.

Values can be accessed using [`metadata(df, key)`](@ref).

`SubDataFrame` and `DataFrameRow` expose only `:note`-style metadata keys of
their parent.

See also: [`metadata`](@ref), [`metadata!`](@ref),
[`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> metadatakeys(df)
()

julia> metadata!(df, "name", "example", style=:note);

julia> metadatakeys(df)
KeySet for a Dict{String, Tuple{Any, Any}} with 1 entry. Keys:
  "name"

julia> metadata(df, "name")
"example"
```
"""
function metadatakeys(df::DataFrame)
    meta = getfield(df, :metadata)
    meta === nothing && return ()
    metakeys = keys(meta)
    @assert !isempty(metakeys) # by design if isempty(metakeys) then meta === nothing should be met
    return metakeys
end

metadatakeys(x::Union{DataFrameRows, DataFrameColumns}) =
    metadatakeys(parent(x))

function metadatakeys(x::Union{DataFrameRow, SubDataFrame})
    df = parent(x)
    meta = getfield(df, :metadata)
    meta === nothing && return ()
    @assert !isempty(meta)
    return (k for (k, (_, s)) in pairs(meta) if s === :note)
end

"""
    metadata!(df::AbstractDataFrame, key::AbstractString, value; style)
    metadata!(dfr::DataFrameRow, key::AbstractString, value; style)
    metadata!(dfc::DataFrameColumns, key::AbstractString, value; style)
    metadata!(dfr::DataFrameRows, key::AbstractString, value; style)

Set table-level metadata for object `df` for key `key` to have value `value`
and style `style` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note`-style is allowed.
Trying to set a key-value pair for which the key already exists in the parent
data frame with another style throws an error.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> metadatakeys(df)
()

julia> metadata!(df, "name", "example", style=:note);

julia> metadatakeys(df)
KeySet for a Dict{String, Tuple{Any, Any}} with 1 entry. Keys:
  "name"

julia> metadata(df, "name")
"example"
```
"""
function metadata!(df::DataFrame, key::AbstractString, value::Any; style)
    premeta = getfield(df, :metadata)
    if premeta === nothing
        meta = Dict{String, Tuple{Any, Any}}()
        setfield!(df, :metadata, meta)
    else
        meta = premeta
    end
    meta[key] = (value, style)
    if style !== :note
        setfield!(df, :allnotemetadata, false)
    end
    return df
end

function metadata!(x::Union{DataFrameRows, DataFrameColumns},
                   key::AbstractString, value::Any; style)
    metadata!(parent(x), key, value, style=style)
    return x
end

function metadata!(x::Union{DataFrameRow, SubDataFrame},
                   key::AbstractString, value::Any; style)
    style === :note || throw(ArgumentError("only :note-style is supported for " *
                                           "DataFrameRow and SubDataFrame"))
    df = parent(x)
    meta = getfield(df, :metadata)
    if meta !== nothing && haskey(meta, key) && meta[key][2] !== :note
        throw(ArgumentError("setting metadata for DataFrameRow and SubDataFrame" *
                            "that is already present in the parent and does not " *
                            "have :note-style is not allowed"))
    end
    metadata!(df, key, value, style=style)
    return x
end

"""
    deletemetadata!(df::AbstractDataFrame, key::AbstractString)
    deletemetadata!(dfr::DataFrameRow, key::AbstractString)
    deletemetadata!(dfc::DataFrameColumns, key::AbstractString)
    deletemetadata!(dfr::DataFrameRows, key::AbstractString)

Delete table-level metadata from object `df` for key `key` and return `df`.
If key does not exist, return `df` without modification.

For `SubDataFrame` and `DataFrameRow` only `:note`-style for metadata is deleted.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> metadatakeys(df)
()

julia> metadata!(df, "name", "example", style=:note);

julia> metadatakeys(df)
KeySet for a Dict{String, Tuple{Any, Any}} with 1 entry. Keys:
  "name"

julia> metadata(df, "name")
"example"

julia> deletemetadata!(df, "name");

julia> metadatakeys(df)
()
```
"""
function deletemetadata!(df::DataFrame, key::AbstractString)
    meta = getfield(df, :metadata)
    # if metadata is nothing or key is missing in metadata this is a no-op
    meta === nothing && return df
    delete!(meta, key)
    isempty(meta) && setfield!(df, :metadata, nothing)
    return df
end

function deletemetadata!(x::Union{DataFrameRows, DataFrameColumns},
                         key::AbstractString)
    deletemetadata!(parent(x), key)
    return x
end

function deletemetadata!(x::Union{DataFrameRow, SubDataFrame},
                         key::AbstractString)
    df = parent(x)
    if key in metadatakeys(df)
        _, s = metadata(df, key, style=true)
        s == :note && deletemetadata!(df, key)
    end
    return x
end

"""
    emptymetadata!(df::AbstractDataFrame)
    emptymetadata!(dfr::DataFrameRow)
    emptymetadata!(dfc::DataFrameColumns)
    emptymetadata!(dfr::DataFrameRows)

Delete all table-level metadata from object `df`.

For `SubDataFrame` and `DataFrameRow` only `:note`-style for metadata is deleted.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`deletemetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> metadatakeys(df)
()

julia> metadata!(df, "name", "example", style=:note);

julia> metadatakeys(df)
KeySet for a Dict{String, Tuple{Any, Any}} with 1 entry. Keys:
  "name"

julia> metadata(df, "name")
"example"

julia> emptymetadata!(df);

julia> metadatakeys(df)
()
```
"""
function emptymetadata!(df::DataFrame)
    setfield!(df, :metadata, nothing)
    return df
end

function emptymetadata!(x::Union{DataFrameRows, DataFrameColumns})
    emptymetadata!(parent(x))
    return x
end

function emptymetadata!(x::Union{DataFrameRow, SubDataFrame})
    df = parent(x)
    for key in metadatakeys(df)
        _, s = metadata(df, key, style=true)
        s == :note && deletemetadata!(df, key)
    end
    return x
end

"""
    colmetadata(df::AbstractDataFrame, col::ColumnIndex, key::AbstractString; style::Bool=false)
    colmetadata(dfr::DataFrameRow, col::ColumnIndex, key::AbstractString; style::Bool=false)
    colmetadata(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString; style::Bool=false)
    colmetadata(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString; style::Bool=false)

Return column-level metadata value associated with `df` for column `col` and key `key`.

`SubDataFrame` and `DataFrameRow` expose only `:note`-style metadata of their parent.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> colmetadatakeys(df)
()

julia> colmetadata!(df, :a, "name", "example", style=:note);

julia> collect(colmetadatakeys(df))
1-element Vector{Pair{Symbol, Base.KeySet{String, Dict{String, Tuple{Any, Any}}}}}:
 :a => ["name"]

julia> colmetadata(df, :a, "name")
"example"
```
"""
function colmetadata(df::DataFrame, col::Int, key::AbstractString; style::Bool=false)
    idx = index(df)[col] # bounds checking
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && throw(KeyError("$key for column $col"))
    col_meta = cols_meta[idx]
    return style ? col_meta[key] : col_meta[key][1]

end

# here and similar definitions below are added to avoid dispatch ambiguity
colmetadata(df::DataFrame, col::Symbol, key::AbstractString; style::Bool=false) =
    colmetadata(df, Int(index(df)[col]), key, style=style)
colmetadata(df::DataFrame, col::ColumnIndex, key::AbstractString; style::Bool=false) =
    colmetadata(df, Int(index(df)[col]), key, style=style)

colmetadata(x::DataFrameRows,
            col::Int, key::AbstractString; style::Bool=false) =
    colmetadata(parent(x), col, key; style=style)
colmetadata(x::DataFrameColumns,
            col::Int, key::AbstractString; style::Bool=false) =
    colmetadata(parent(x), col, key; style=style)
colmetadata(x::DataFrameRows,
            col::Symbol, key::AbstractString; style::Bool=false) =
    colmetadata(parent(x), col, key; style=style)
colmetadata(x::DataFrameColumns,
            col::Symbol, key::AbstractString; style::Bool=false) =
    colmetadata(parent(x), col, key; style=style)
colmetadata(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString; style::Bool=false) =
    colmetadata(parent(dfr), col, key; style=style)
colmetadata(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString; style::Bool=false) =
    colmetadata(parent(dfc), col, key; style=style)

function colmetadata(x::DataFrameRow,
                     col::Int, key::AbstractString; style::Bool=false)
    col_name = _names(x)[col]
    df = parent(x)
    val_meta, style_meta = colmetadata(df, col_name, key, style=true)
    if style_meta !== :note
        throw(KeyError("$key for column $col with :note-style"))
    end
    return style ? (val_meta, style_meta) : val_meta
end

function colmetadata(x::SubDataFrame,
                     col::Int, key::AbstractString; style::Bool=false)
    col_name = _names(x)[col]
    df = parent(x)
    val_meta, style_meta = colmetadata(df, col_name, key, style=true)
    if style_meta !== :note
        throw(KeyError("$key for column $col with :note-style"))
    end
    return style ? (val_meta, style_meta) : val_meta
end

colmetadata(x::DataFrameRow,
            col::Symbol, key::AbstractString; style::Bool=false) =
    colmetadata(x, Int(index(x)[col]), key, style=style)
colmetadata(x::SubDataFrame,
            col::Symbol, key::AbstractString; style::Bool=false) =
    colmetadata(x, Int(index(x)[col]), key, style=style)
colmetadata(x::DataFrameRow,
            col::ColumnIndex, key::AbstractString; style::Bool=false) =
    colmetadata(x, Int(index(x)[col]), key, style=style)
colmetadata(x::SubDataFrame,
            col::ColumnIndex, key::AbstractString; style::Bool=false) =
    colmetadata(x, Int(index(x)[col]), key, style=style)

"""
    colmetadatakeys(df::AbstractDataFrame, [col::ColumnIndex])
    colmetadatakeys(dfr::DataFrameRow, [col::ColumnIndex])
    colmetadatakeys(dfc::DataFrameColumns, [col::ColumnIndex])
    colmetadatakeys(dfr::DataFrameRows, [col::ColumnIndex])

If `col` is passed return an iterator of column-level metadata keys
which are set for column `col`.
If `col` is not passed return an iterator of `col => colmetadatakeys(x, col)`
pairs for all columns that have metadata, where `col` are `Symbol`.

Values can be accessed using [`colmetadata(df, col, key)`](@ref).

`SubDataFrame` and `DataFrameRow` expose only `:note`-style metadata of their parent.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> colmetadatakeys(df)
()

julia> colmetadata!(df, :a, "name", "example", style=:note);

julia> collect(colmetadatakeys(df))
1-element Vector{Pair{Symbol, Base.KeySet{String, Dict{String, Tuple{Any, Any}}}}}:
 :a => ["name"]

julia> colmetadata(df, :a, "name")
"example"
```
"""
function colmetadatakeys(df::DataFrame, col::Int)
    idx = index(df)[col] # bounds checking
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    haskey(cols_meta, idx) || return ()
    metakeys = keys(cols_meta[idx])
    @assert !isempty(metakeys) # by design in such cases meta === nothing should be met
    return metakeys
end

colmetadatakeys(df::DataFrame, col::Symbol) = colmetadatakeys(df, Int(index(df)[col]))
colmetadatakeys(df::DataFrame, col::ColumnIndex) = colmetadatakeys(df, Int(index(df)[col]))

function colmetadatakeys(df::DataFrame)
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    return (_names(df)[idx] => colmetadatakeys(df, idx) for idx in keys(cols_meta))
end

colmetadatakeys(x::DataFrameRows, col::Int) =
    colmetadatakeys(parent(x), col)
colmetadatakeys(x::DataFrameColumns, col::Int) =
    colmetadatakeys(parent(x), col)
colmetadatakeys(x::DataFrameRows, col::Symbol) =
    colmetadatakeys(parent(x), col)
colmetadatakeys(x::DataFrameColumns, col::Symbol) =
    colmetadatakeys(parent(x), col)
colmetadatakeys(dfr::DataFrameRows, col::ColumnIndex) = colmetadatakeys(parent(dfr), col)
colmetadatakeys(dfc::DataFrameColumns, col::ColumnIndex) = colmetadatakeys(parent(dfc), col)
colmetadatakeys(x::Union{DataFrameRows, DataFrameColumns}) =
    colmetadatakeys(parent(x))

function colmetadatakeys(x::DataFrameRow, col::Int)
    col_name = _names(x)[col]
    df = parent(x)
    idx = index(df)[col_name]
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    haskey(cols_meta, idx) || return ()
    col_meta = cols_meta[idx]
    @assert !isempty(col_meta) # by design in such cases meta === nothing should be met
    return (k for (k, (_, s)) in pairs(col_meta) if s === :note)
end

function colmetadatakeys(x::SubDataFrame, col::Int)
    col_name = _names(x)[col]
    df = parent(x)
    idx = index(df)[col_name]
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    haskey(cols_meta, idx) || return ()
    col_meta = cols_meta[idx]
    @assert !isempty(col_meta) # by design in such cases meta === nothing should be met
    return (k for (k, (_, s)) in pairs(col_meta) if s === :note)
end

colmetadatakeys(x::DataFrameRow, col::Symbol) =
    colmetadatakeys(x, Int(index(x)[col]))
colmetadatakeys(x::SubDataFrame, col::Symbol) =
    colmetadatakeys(x, Int(index(x)[col]))
colmetadatakeys(x::DataFrameRow, col::ColumnIndex) =
    colmetadatakeys(x, Int(index(x)[col]))
colmetadatakeys(x::SubDataFrame, col::ColumnIndex) =
    colmetadatakeys(x, Int(index(x)[col]))

function colmetadatakeys(x::Union{DataFrameRow, SubDataFrame})
    df = parent(x)
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    return (col => colmetadatakeys(x, col) for col in _names(x) if !isempty(colmetadatakeys(x, col)))
end

"""
    colmetadata!(df::AbstractDataFrame, col::ColumnIndex, key::AbstractString, value; style)
    colmetadata!(dfr::DataFrameRow, col::ColumnIndex, key::AbstractString, value; style)
    colmetadata!(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString, value; style)
    colmetadata!(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString, value; style)

Set column-level metadata in `df` for column `col` and key `key` to have value `value`
and style `style` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note`-style is allowed.
Trying to set a key-value pair for which the key already exists in the parent
data frame with another style throws an error.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> colmetadatakeys(df)
()

julia> colmetadata!(df, :a, "name", "example", style=:note);

julia> collect(colmetadatakeys(df))
1-element Vector{Pair{Symbol, Base.KeySet{String, Dict{String, Tuple{Any, Any}}}}}:
 :a => ["name"]

julia> colmetadata(df, :a, "name")
"example"
```
"""
function colmetadata!(df::DataFrame, col::Int, key::AbstractString, value::Any; style)
    idx = index(df)[col] # bounds checking
    pre_cols_meta = getfield(df, :colmetadata)
    if pre_cols_meta === nothing
        cols_meta = Dict{Int, Dict{String,Tuple{Any, Any}}}()
        setfield!(df, :colmetadata, cols_meta)
    else
        cols_meta = pre_cols_meta
    end
    col_meta = get!(Dict{String, Tuple{Any, Any}}, cols_meta, idx)
    col_meta[key] = (value, style)
    if style !== :note
        setfield!(df, :allnotemetadata, false)
    end
    return df
end

colmetadata!(df::DataFrame, col::Symbol, key::AbstractString, value::Any; style) =
    colmetadata!(df, Int(index(df)[col]), key, value; style=style)
colmetadata!(df::DataFrame, col::ColumnIndex, key::AbstractString, value::Any; style) =
    colmetadata!(df, Int(index(df)[col]), key, value; style=style)

function colmetadata!(x::DataFrameRows,
                      col::Int, key::AbstractString, value::Any; style)
    colmetadata!(parent(x), col, key, value; style=style)
    return x
end

function colmetadata!(x::DataFrameColumns,
                      col::Int, key::AbstractString, value::Any; style)
    colmetadata!(parent(x), col, key, value; style=style)
    return x
end

function colmetadata!(x::DataFrameRows,
                      col::Symbol, key::AbstractString, value::Any; style)
    colmetadata!(parent(x), col, key, value; style=style)
    return x
end

function colmetadata!(x::DataFrameColumns,
                      col::Symbol, key::AbstractString, value::Any; style)
    colmetadata!(parent(x), col, key, value; style=style)
    return x
end

function colmetadata!(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString, value::Any; style)
    colmetadata!(parent(dfr), col, key, value; style=style)
    return dfr
end

function colmetadata!(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString, value::Any; style)
    colmetadata!(parent(dfc), col, key, value; style=style)
    return dfc
end

function colmetadata!(x::DataFrameRow,
                      col::Int, key::AbstractString, value::Any; style)
    col_name = _names(x)[col]
    style === :note || throw(ArgumentError("only :note-style is supported for " *
                                           "DataFrameRow and SubDataFrame"))
    df = parent(x)
    cols_meta = getfield(df, :colmetadata)
    idx = index(df)[col_name]
    if cols_meta !== nothing && haskey(cols_meta, idx) &&
       haskey(cols_meta[idx], key) && cols_meta[idx][key][2] !== :note
        throw(ArgumentError("setting metadata for DataFrameRow and SubDataFrame" *
                            "that is already present in the parent and does not " *
                            "have :note-style is not allowed"))
    end
    colmetadata!(df, idx, key, value, style=style)
    return x
end

function colmetadata!(x::SubDataFrame,
                      col::Int, key::AbstractString, value::Any; style)
    col_name = _names(x)[col]
    style === :note || throw(ArgumentError("only :note-style is supported for " *
                                           "DataFrameRow and SubDataFrame"))
    df = parent(x)
    cols_meta = getfield(df, :colmetadata)
    idx = index(df)[col_name]
    if cols_meta !== nothing && haskey(cols_meta, idx) &&
       haskey(cols_meta[idx], key) && cols_meta[idx][key][2] !== :note
        throw(ArgumentError("setting metadata for DataFrameRow and SubDataFrame" *
                            "that is already present in the parent and does not " *
                            "have :note-style is not allowed"))
    end
    colmetadata!(df, idx, key, value, style=style)
    return x
end

colmetadata!(x::DataFrameRow,
             col::Symbol, key::AbstractString, value::Any; style) =
    colmetadata!(x, Int(index(x)[col]), key, value; style=style)
colmetadata!(x::SubDataFrame,
             col::Symbol, key::AbstractString, value::Any; style) =
    colmetadata!(x, Int(index(x)[col]), key, value; style=style)
colmetadata!(x::DataFrameRow, col::ColumnIndex, key::AbstractString, value::Any; style) =
    colmetadata!(x, Int(index(x)[col]), key, value; style=style)
colmetadata!(x::SubDataFrame, col::ColumnIndex, key::AbstractString, value::Any; style) =
    colmetadata!(x, Int(index(x)[col]), key, value; style=style)

"""
    deletecolmetadata!(df::AbstractDataFrame, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(dfr::DataFrameRow, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString)

Delete column-level metadata set in `df` for column `col` and key `key` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note`-style for metadata is deleted.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref),
[`colmetadata!`](@ref), [`emptycolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> colmetadata!(df, :a, "name", "example", style=:note);

julia> collect(colmetadatakeys(df))
1-element Vector{Pair{Symbol, Base.KeySet{String, Dict{String, Tuple{Any, Any}}}}}:
 :a => ["name"]

julia> colmetadata(df, :a, "name")
"example"

julia> deletecolmetadata!(df, :a, "name");

julia> colmetadatakeys(df)
()
```
"""
function deletecolmetadata!(df::DataFrame, col::Int, key::AbstractString)
    idx = index(df)[col] # bounds checking
    cols_meta = getfield(df, :colmetadata)
    # if metadata is nothing or key is missing in metadata this is a no-op
    cols_meta === nothing && return df
    haskey(cols_meta, idx) || return df
    col_meta = cols_meta[idx]
    delete!(col_meta, key)
    isempty(col_meta) && delete!(cols_meta, idx)
    isempty(cols_meta) && setfield!(df, :colmetadata, nothing)
    return df
end

deletecolmetadata!(df::DataFrame, col::Symbol, key::AbstractString) =
    deletecolmetadata!(df, Int(index(df)[col]), key)
deletecolmetadata!(df::DataFrame, col::ColumnIndex, key::AbstractString) =
    deletecolmetadata!(df, Int(index(df)[col]), key)

function deletecolmetadata!(x::DataFrameRows,
                            col::Int, key::AbstractString)
    deletecolmetadata!(parent(x), col, key)
    return x
end

function deletecolmetadata!(x::DataFrameColumns,
                            col::Int, key::AbstractString)
    deletecolmetadata!(parent(x), col, key)
    return x
end

function deletecolmetadata!(x::DataFrameRows,
                            col::Symbol, key::AbstractString)
    deletecolmetadata!(parent(x), col, key)
    return x
end

function deletecolmetadata!(x::DataFrameColumns,
                            col::Symbol, key::AbstractString)
    deletecolmetadata!(parent(x), col, key)
    return x
end

function deletecolmetadata!(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(parent(dfr), col, key)
    return dfr
end

function deletecolmetadata!(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(parent(dfc), col, key)
    return dfc
end

function deletecolmetadata!(x::DataFrameRow, col::Int, key::AbstractString)
    col_name = _names(x)[col]
    df = parent(x)
    idx = index(df)[col_name]

    if key in colmetadatakeys(df, idx)
        _, s = colmetadata(df, idx, key, style=true)
        s == :note && deletecolmetadata!(df, idx, key)
    end
    return x
end

function deletecolmetadata!(x::SubDataFrame, col::Int, key::AbstractString)
    col_name = _names(x)[col]
    df = parent(x)
    idx = index(df)[col_name]

    if key in colmetadatakeys(df, idx)
        _, s = colmetadata(df, idx, key, style=true)
        s == :note && deletecolmetadata!(df, idx, key)
    end
    return x
end

deletecolmetadata!(x::DataFrameRow, col::Symbol, key::AbstractString) =
    deletecolmetadata!(x, Int(index(x)[col]), key)
deletecolmetadata!(x::SubDataFrame, col::Symbol, key::AbstractString) =
    deletecolmetadata!(x, Int(index(x)[col]), key)
deletecolmetadata!(x::DataFrameRow, col::ColumnIndex, key::AbstractString) =
    deletecolmetadata!(x, Int(index(x)[col]), key)
deletecolmetadata!(x::SubDataFrame, col::ColumnIndex, key::AbstractString) =
    deletecolmetadata!(x, Int(index(x)[col]), key)

"""
    emptycolmetadata!(df::AbstractDataFrame, [col::ColumnIndex])
    emptycolmetadata!(dfr::DataFrameRow, [col::ColumnIndex])
    emptycolmetadata!(dfc::DataFrameColumns, [col::ColumnIndex])
    emptycolmetadata!(dfr::DataFrameRows, [col::ColumnIndex])

Delete column-level metadata set in `df` for column `col` and key `key` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note`-style for metadata is deleted.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref),
[`colmetadata!`](@ref), [`deletecolmetadata!`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(a=1, b=2);

julia> colmetadata!(df, :a, "name", "example", style=:note);

julia> collect(colmetadatakeys(df))
1-element Vector{Pair{Symbol, Base.KeySet{String, Dict{String, Tuple{Any, Any}}}}}:
 :a => ["name"]

julia> colmetadata(df, :a, "name")
"example"

julia> emptycolmetadata!(df, :a);

julia> colmetadatakeys(df)
()
```
"""
function emptycolmetadata!(df::DataFrame, col::Int)
    idx = index(df)[col] # bounds checking
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return df
    delete!(cols_meta, idx)
    isempty(cols_meta) && setfield!(df, :colmetadata, nothing)
    return df
end

emptycolmetadata!(df::DataFrame, col::Symbol) =
    emptycolmetadata!(df, Int(index(df)[col]))
emptycolmetadata!(df::DataFrame, col::ColumnIndex) =
    emptycolmetadata!(df, Int(index(df)[col]))

function emptycolmetadata!(df::DataFrame)
    setfield!(df, :colmetadata, nothing)
    return df
end

function emptycolmetadata!(x::DataFrameRows, col::Int)
    emptycolmetadata!(parent(x), col)
    return x
end

function emptycolmetadata!(x::DataFrameColumns, col::Int)
    emptycolmetadata!(parent(x), col)
    return x
end

function emptycolmetadata!(x::DataFrameRows, col::Symbol)
    emptycolmetadata!(parent(x), col)
    return x
end

function emptycolmetadata!(x::DataFrameColumns, col::Symbol)
    emptycolmetadata!(parent(x), col)
    return x
end

function emptycolmetadata!(dfr::DataFrameRows, col::ColumnIndex)
    emptycolmetadata!(parent(dfr), col)
    return dfr
end

function emptycolmetadata!(dfc::DataFrameColumns, col::ColumnIndex)
    emptycolmetadata!(parent(dfc), col)
    return dfc
end

function emptycolmetadata!(x::Union{DataFrameRows, DataFrameColumns})
    emptycolmetadata!(parent(x))
    return x
end

function emptycolmetadata!(x::DataFrameRow, col::Int)
    col_name = _names(x)[col]
    df = parent(x)
    idx = index(df)[col_name]

    for key in colmetadatakeys(df, idx)
        _, s = colmetadata(df, idx, key, style=true)
        s == :note && deletecolmetadata!(df, idx, key)
    end
    return x
end

function emptycolmetadata!(x::SubDataFrame, col::Int)
    col_name = _names(x)[col]
    df = parent(x)
    idx = index(df)[col_name]

    for key in colmetadatakeys(df, idx)
        _, s = colmetadata(df, idx, key, style=true)
        s == :note && deletecolmetadata!(df, idx, key)
    end
    return x
end

emptycolmetadata!(x::DataFrameRow, col::Symbol) =
    emptycolmetadata!(x, Int(index(x)[col]))
emptycolmetadata!(x::SubDataFrame, col::Symbol) =
    emptycolmetadata!(x, Int(index(x)[col]))
emptycolmetadata!(x::DataFrameRow, col::ColumnIndex) =
    emptycolmetadata!(x, Int(index(x)[col]))
emptycolmetadata!(x::SubDataFrame, col::ColumnIndex) =
    emptycolmetadata!(x, Int(index(x)[col]))

function emptycolmetadata!(x::DataFrameRow)
    for i in 1:length(x)
        emptycolmetadata!(x, i)
    end
    return x
end

function emptycolmetadata!(x::SubDataFrame)
    for i in 1:ncol(x)
        emptycolmetadata!(x, i)
    end
    return x
end

### Internal utility functions for metadata handling

# copy table-level :note-style metadata from Tables.jl table src to dst
# discarding previous metadata contents of dst
function _copy_df_note_metadata!(dst::DataFrame, src)
    emptymetadata!(dst)
    for key in metadatakeys(src)
        val, style = metadata(src, key, style=true)
        style === :note && metadata!(dst, key, val, style=:note)
    end
    return nothing
end

# copy column-level :note-style metadata from Tables.jl table src to dst
# from column src_col to dst_col
# discarding previous metadata contents of dst
function _copy_col_note_metadata!(dst::DataFrame, dst_col, src, src_col)
    emptycolmetadata!(dst, dst_col)
    for key in colmetadatakeys(src, src_col)
        val, style = colmetadata(src, src_col, key, style=true)
        style === :note && colmetadata!(dst, dst_col, key, val, style=:note)
    end
    return nothing
end

# this is a function used to copy table-level and column-level :note-style metadata
# from Tables.jl table src to dst, discarding previous metadata contents of dst
function _copy_all_note_metadata!(dst::DataFrame, src)
    _copy_df_note_metadata!(dst, src)
    emptycolmetadata!(dst)
    for (col, col_keys) in colmetadatakeys(src)
        if hasproperty(dst, col)
            for key in col_keys
                val, style = colmetadata(src, col, key, style=true)
                style === :note && colmetadata!(dst, col, key, val, style=:note)
            end
        end
    end
    return nothing
end

# this is a function used to copy all table and column-level metadata
# from Tables.jl table src to dst, discarding previous metadata contents of dst
function _copy_all_all_metadata!(dst::DataFrame, src)
    emptymetadata!(dst)
    for key in metadatakeys(src)
        val, style = metadata(src, key, style=true)
        metadata!(dst, key, val, style=style)
    end
    emptycolmetadata!(dst)
    for (col, col_keys) in colmetadatakeys(src)
        if hasproperty(dst, col)
            for key in col_keys
                val, style = colmetadata(src, col, key, style=true)
                colmetadata!(dst, col, key, val, style=style)
            end
        end
    end
    return nothing
end

# this is a function used to drop table-level metadata that is not :note-style
function _drop_df_nonnote_metadata!(df::DataFrame)
    getfield(df, :allnotemetadata) && return nothing
    for key in metadatakeys(df)
        _, style = metadata(df, key, style=true)
        style === :note || deletemetadata!(df, key)
    end
    return nothing
end

# this is a function used to drop table and column-level metadata that is not :note-style
function _drop_all_nonnote_metadata!(df::DataFrame)
    getfield(df, :allnotemetadata) && return nothing
    _drop_df_nonnote_metadata!(df)
    for (col, col_keys) in colmetadatakeys(df)
        for key in col_keys
            _, style = colmetadata(df, col, key, style=true)
            style === :note || deletecolmetadata!(df, col, key)
        end
    end
    setfield!(df, :allnotemetadata, true)
    return nothing
end

# this is a function used to merge matching table-level metadata that has :note-style
# it removes all table-level metadata previously stored in `res`
function _merge_matching_df_note_metadata!(res::DataFrame, dfs)
    emptymetadata!(res)
    @assert firstindex(dfs) == 1
    if !isempty(dfs) && all(x -> !isempty(metadatakeys(x)), dfs)
        df1 = dfs[1]
        for key1 in metadatakeys(df1)
            meta_val1, meta_style1 = metadata(df1, key1, style=true)
            if meta_style1 === :note
                good_key = true
                for i in 2:length(dfs)
                    dfi = dfs[i]
                    if key1 in metadatakeys(dfi)
                        meta_vali, meta_stylei = metadata(dfi, key1, style=true)
                        if !(meta_stylei === :note && isequal(meta_val1, meta_vali))
                            good_key = false
                            break
                        end
                    else
                        good_key = false
                        break
                    end
                end
                good_key && metadata!(res, key1, meta_val1, style=:note)
            end
        end
    end
    return nothing
end

# this is a function used to keep only table-level :note-style metadata matching
# between dst and src all other table-level metadata is dropped
function _keep_matching_df_note_metadata!(dst::DataFrame, src::AbstractDataFrame)
    _drop_df_nonnote_metadata!(dst)
    src_keys = metadatakeys(src)

    # here we know it is only :note-style metadata
    for key in metadatakeys(dst)
        if key in src_keys
            src_val, src_style = metadata(src, key, style=true)
            if src_style == :note
                isequal(metadata(dst, key), src_val) || deletemetadata!(dst, key)
            else
                deletemetadata!(dst, key)
            end
        else
            deletemetadata!(dst, key)
        end
    end
    return nothing
end
