### Metadata API from DataAPI.jl

# private type that is passed as a default value in the metadata and colmetadata
# functions to detect the fact that no default was passed
struct MetadataMissingDefault end

# DataAPI.metadatasupport and DataAPI.colmetadatasupport are not exported
DataAPI.metadatasupport(::Type{<:AbstractDataFrame}) = (read=true, write=true)
DataAPI.metadatasupport(::Type{<:DataFrameRow}) = (read=true, write=true)
DataAPI.metadatasupport(::Type{<:DataFrameRows}) = (read=true, write=true)
DataAPI.metadatasupport(::Type{<:DataFrameColumns}) = (read=true, write=true)
DataAPI.colmetadatasupport(::Type{<:AbstractDataFrame}) = (read=true, write=true)
DataAPI.colmetadatasupport(::Type{<:DataFrameRow}) = (read=true, write=true)
DataAPI.colmetadatasupport(::Type{<:DataFrameRows}) = (read=true, write=true)
DataAPI.colmetadatasupport(::Type{<:DataFrameColumns}) = (read=true, write=true)

const TABLEMETA_EXAMPLE =
    """
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

    julia> metadata(df, "name", style=true)
    ("example", :note)

    julia> deletemetadata!(df, "name");

    julia> metadatakeys(df)
    ()
    ```
    """

const COLMETADATA_EXAMPLE =
    """
    # Examples

    ```jldoctest
    julia> df = DataFrame(a=1, b=2);

    julia> colmetadatakeys(df)
    ()

    julia> colmetadata!(df, :a, "name", "example", style=:note);

    julia> collect(colmetadatakeys(df))
    1-element Vector{Pair{Symbol, Base.KeySet{String, Dict{String, Tuple{Any, Any}}}}}:
     :a => ["name"]

    julia> colmetadatakeys(df, :a)
    KeySet for a Dict{String, Tuple{Any, Any}} with 1 entry. Keys:
      "name"

    julia> colmetadata(df, :a, "name")
    "example"

    julia> colmetadata(df, :a, "name", style=true)
    ("example", :note)

    julia> deletecolmetadata!(df, :a, "name");

    julia> colmetadatakeys(df)
    ()
    ```
    """

"""
    metadata(df::AbstractDataFrame, key::AbstractString, [default]; style::Bool=false)
    metadata(dfr::DataFrameRow, key::AbstractString, [default]; style::Bool=false)
    metadata(dfc::DataFrameColumns, key::AbstractString, [default]; style::Bool=false)
    metadata(dfr::DataFrameRows, key::AbstractString, [default]; style::Bool=false)

Return table-level metadata value associated with `df` for key `key`.
If `style=true` return a tuple of metadata value and metadata style.

`SubDataFrame` and `DataFrameRow` expose only `:note`-style metadata of their
parent.

If `default` is passed then return it if `key` does not exist;
if `style=true` return `(default, :default)`.

See also: [`metadatakeys`](@ref), [`metadata!`](@ref),
[`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

$TABLEMETA_EXAMPLE
```
"""
function metadata(df::DataFrame, key::AbstractString,
                  default=MetadataMissingDefault(); style::Bool=false)
    meta = getfield(df, :metadata)
    if meta === nothing || !haskey(meta, key)
        if default === MetadataMissingDefault()
            throw(ArgumentError("\"$key\" not found in table-level metadata"))
        else
            return style ? (default, :default) : default
        end
    end
    return style ? meta[key] : meta[key][1]
end

metadata(x::Union{DataFrameRows, DataFrameColumns}, key::AbstractString,
         default=MetadataMissingDefault(); style::Bool=false) =
    metadata(parent(x), key, default, style=style)

function metadata(x::Union{DataFrameRow, SubDataFrame}, key::AbstractString,
                  default=MetadataMissingDefault(); style::Bool=false)
    meta_value, meta_style = metadata(parent(x), key, default, style=true)
    # the `key in metadatakeys(parent(x))` check
    # allows returning default value as it has :default style
    if meta_style !== :note && key in metadatakeys(parent(x))
        throw(ArgumentError("\"$key\" was found in table-level metadata of parent " *
                            "data frame, but it does not have :note style"))
    end
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

$TABLEMETA_EXAMPLE
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
    metadata!(df::AbstractDataFrame, key::AbstractString, value; style::Symbol=:default)
    metadata!(dfr::DataFrameRow, key::AbstractString, value; style::Symbol=:default)
    metadata!(dfc::DataFrameColumns, key::AbstractString, value; style::Symbol=:default)
    metadata!(dfr::DataFrameRows, key::AbstractString, value; style::Symbol=:default)

Set table-level metadata for object `df` for key `key` to have value `value`
and style `style` (`:default` by default) and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note`-style is allowed.
Trying to set a key-value pair for which the key already exists in the parent
data frame with another style throws an error.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

$TABLEMETA_EXAMPLE
```
"""
function metadata!(df::DataFrame, key::AbstractString, value::Any;
                   style::Symbol=:default)
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
                   key::AbstractString, value::Any; style::Symbol=:default)
    metadata!(parent(x), key, value, style=style)
    return x
end

function metadata!(x::Union{DataFrameRow, SubDataFrame},
                   key::AbstractString, value::Any; style::Symbol=:default)
    if style !== :note
        throw(ArgumentError("only :note-style metadata is supported for " *
                            "DataFrameRow and SubDataFrame"))
    end
    df = parent(x)
    meta = getfield(df, :metadata)
    if meta !== nothing && haskey(meta, key) && meta[key][2] !== :note
        throw(ArgumentError("setting metadata for DataFrameRow and SubDataFrame" *
                            "that is already present in the parent and does not " *
                            "have :note style is not allowed"))
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

For `SubDataFrame` and `DataFrameRow` only `:note`-style metadata from their
parent can be deleted (as other styles are not propagated to views).

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

$TABLEMETA_EXAMPLE
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
    # key in metadatakeys(df) is more efficient than key in metadatakeys(x)
    # as it is an O(1) operation
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

For `SubDataFrame` and `DataFrameRow` only `:note`-style metadata from their
parent can be deleted (as other styles are not propagated to views).

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

julia> metadata(df, "name", style=true)
("example", :note)

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
    colmetadata(df::AbstractDataFrame, col::ColumnIndex, key::AbstractString, [default]; style::Bool=false)
    colmetadata(dfr::DataFrameRow, col::ColumnIndex, key::AbstractString, [default]; style::Bool=false)
    colmetadata(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString, [default]; style::Bool=false)
    colmetadata(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString, [default]; style::Bool=false)

Return column-level metadata value associated with `df` for column `col` and key `key`.

`SubDataFrame` and `DataFrameRow` expose only `:note`-style metadata of their parent.

If `default` is passed then return it if `key` does not exist for column `col`;
if `style=true` return `(default, :default)`.
If `col` does not exist in `df` always throw an error.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadatakeys`](@ref), [`colmetadata!`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

$COLMETADATA_EXAMPLE
```
"""
function colmetadata(df::DataFrame, col::ColumnIndex, key::AbstractString,
                     default=MetadataMissingDefault(); style::Bool=false)
    idx = index(df)[col] # check if column exists and get its integer number
    cols_meta = getfield(df, :colmetadata)
    if cols_meta === nothing || !haskey(cols_meta, idx)
        if default === MetadataMissingDefault()
            colname = names(df)[idx]
            throw(ArgumentError("no column-level metadata found for column \"$colname\""))
        else
            return style ? (default, :default) : default
        end
    end
    col_meta = cols_meta[idx]
    if !haskey(col_meta, key)
        if default === MetadataMissingDefault()
            colname = names(df)[idx]
            throw(ArgumentError("\"$key\" not found in column-level metadata for column \"$colname\""))
        else
            return style ? (default, :default) : default
        end
    end
    return style ? col_meta[key] : col_meta[key][1]
end

colmetadata(x::Union{DataFrameRows, DataFrameColumns}, col::ColumnIndex,
            key::AbstractString, default=MetadataMissingDefault(); style::Bool=false) =
    colmetadata(parent(x), col, key, default; style=style)


function colmetadata(x::Union{DataFrameRow, SubDataFrame}, col::ColumnIndex,
                     key::AbstractString, default=MetadataMissingDefault(); style::Bool=false)
    col_name = _names(x)[index(x)[col]]
    df = parent(x)
    meta_value, meta_style = colmetadata(df, col_name, key, default, style=true)
    # the `key in colmetadatakeys(parent(x), col)` check
    # allows returning default value as it has :default style
    if meta_style !== :note && key in colmetadatakeys(parent(x), col)
        throw(ArgumentError("\"$key\" for column \"$(string(col_name))\" was found in column-level metadata " *
                            "of parent data frame, but it does not have :note style"))
    end
    return style ? (meta_value, meta_style) : meta_value
end

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

$COLMETADATA_EXAMPLE
```
"""
function colmetadatakeys(df::DataFrame, col::ColumnIndex)
    idx = index(df)[col] # check if column exists and get its integer index
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    haskey(cols_meta, idx) || return ()
    col_meta = keys(cols_meta[idx])
    @assert !isempty(col_meta) # by design if isempty(col_meta) then cols_meta should not have an entry for idx
    return col_meta
end

function colmetadatakeys(df::DataFrame)
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    return (_names(df)[idx] => colmetadatakeys(df, idx) for idx in keys(cols_meta))
end

colmetadatakeys(x::Union{DataFrameRows, DataFrameColumns}, col::ColumnIndex) =
    colmetadatakeys(parent(x), col)

colmetadatakeys(x::Union{DataFrameRows, DataFrameColumns}) =
    colmetadatakeys(parent(x))

function colmetadatakeys(x::Union{DataFrameRow, SubDataFrame}, col::ColumnIndex)
    col_name = _names(x)[index(x)[col]]
    df = parent(x)
    idx = index(df)[col_name]
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    haskey(cols_meta, idx) || return ()
    col_meta = cols_meta[idx]
    @assert !isempty(col_meta) # by design if isempty(col_meta) then cols_meta should not have an entry for idx
    return (k for (k, (_, s)) in pairs(col_meta) if s === :note)
end

function colmetadatakeys(x::Union{DataFrameRow, SubDataFrame})
    df = parent(x)
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    return (col => colmetadatakeys(x, col) for col in _names(x) if !isempty(colmetadatakeys(x, col)))
end

"""
    colmetadata!(df::AbstractDataFrame, col::ColumnIndex, key::AbstractString, value; style::Symbol=:default)
    colmetadata!(dfr::DataFrameRow, col::ColumnIndex, key::AbstractString, value; style::Symbol=:default)
    colmetadata!(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString, value; style::Symbol=:default)
    colmetadata!(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString, value; style::Symbol=:default)

Set column-level metadata in `df` for column `col` and key `key` to have value `value`
and style `style` (`:default` by default) and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note` style is allowed.
Trying to set a key-value pair for which the key already exists in the parent
data frame with another style throws an error.

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref),
[`deletecolmetadata!`](@ref), [`emptycolmetadata!`](@ref).

$COLMETADATA_EXAMPLE
```
"""
function colmetadata!(df::DataFrame, col::ColumnIndex, key::AbstractString, value::Any;
                      style::Symbol=:default)
    idx = index(df)[col] # check if column exists and get its integer index
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

function colmetadata!(x::Union{DataFrameRows, DataFrameColumns},
                      col::ColumnIndex, key::AbstractString, value::Any;
                      style::Symbol=:default)
    colmetadata!(parent(x), col, key, value; style=style)
    return x
end

function colmetadata!(x::Union{DataFrameRow, SubDataFrame},
                      col::ColumnIndex, key::AbstractString, value::Any;
                      style::Symbol=:default)
    col_name = _names(x)[index(x)[col]]
    if style !== :note
        throw(ArgumentError("only :note-style metadata is supported for " *
                            "DataFrameRow and SubDataFrame"))
    end
    df = parent(x)
    cols_meta = getfield(df, :colmetadata)
    idx = index(df)[col_name]
    if cols_meta !== nothing && haskey(cols_meta, idx) &&
        haskey(cols_meta[idx], key) && cols_meta[idx][key][2] !== :note
        throw(ArgumentError("setting metadata for DataFrameRow and SubDataFrame" *
                            "that is already present in the parent and does not " *
                            "have :note style is not allowed"))
    end
    colmetadata!(df, idx, key, value, style=style)
    return x
end

"""
    deletecolmetadata!(df::AbstractDataFrame, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(dfr::DataFrameRow, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString)

Delete column-level metadata set in `df` for column `col` and key `key` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note`-style metadata from their
parent can be deleted (as other styles are not propagated to views).

See also: [`metadata`](@ref), [`metadatakeys`](@ref),
[`metadata!`](@ref), [`deletemetadata!`](@ref), [`emptymetadata!`](@ref),
[`colmetadata`](@ref), [`colmetadatakeys`](@ref),
[`colmetadata!`](@ref), [`emptycolmetadata!`](@ref).

$COLMETADATA_EXAMPLE
"""
function deletecolmetadata!(df::DataFrame, col::ColumnIndex, key::AbstractString)
    idx = index(df)[col] # check if column exists and get its integer index
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

function deletecolmetadata!(x::Union{DataFrameRows, DataFrameColumns},
                                  col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(parent(x), col, key)
    return x
end

function deletecolmetadata!(x::Union{DataFrameRow, SubDataFrame},
                            col::ColumnIndex, key::AbstractString)
    col_name = _names(x)[index(x)[col]]
    df = parent(x)
    idx = index(df)[col_name]

    # key in colmetadatakeys(df, idx) is more efficient than key in colmetadatakeys(x, idx)
    # as it is an O(1) operation
    if key in colmetadatakeys(df, idx)
        _, s = colmetadata(df, idx, key, style=true)
        s == :note && deletecolmetadata!(df, idx, key)
    end
    return x
end

"""
    emptycolmetadata!(df::AbstractDataFrame, [col::ColumnIndex])
    emptycolmetadata!(dfr::DataFrameRow, [col::ColumnIndex])
    emptycolmetadata!(dfc::DataFrameColumns, [col::ColumnIndex])
    emptycolmetadata!(dfr::DataFrameRows, [col::ColumnIndex])

Delete column-level metadata set in `df` for column `col` and key `key` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note`-style metadata from their
parent can be deleted (as other styles are not propagated to views).

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

julia> colmetadatakeys(df, :a)
KeySet for a Dict{String, Tuple{Any, Any}} with 1 entry. Keys:
  "name"

julia> colmetadata(df, :a, "name")
"example"

julia> colmetadata(df, :a, "name", style=true)
("example", :note)

julia> emptycolmetadata!(df, :a);

julia> colmetadatakeys(df)
()
```
"""
function emptycolmetadata!(df::DataFrame, col::ColumnIndex)
    idx = index(df)[col] # check if column exists and get its integer index
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return df
    delete!(cols_meta, idx)
    isempty(cols_meta) && setfield!(df, :colmetadata, nothing)
    return df
end

function emptycolmetadata!(df::DataFrame)
    setfield!(df, :colmetadata, nothing)
    return df
end

function emptycolmetadata!(x::Union{DataFrameRows, DataFrameColumns}, col::ColumnIndex)
    emptycolmetadata!(parent(x), col)
    return x
end

function emptycolmetadata!(x::Union{DataFrameRows, DataFrameColumns})
    emptycolmetadata!(parent(x))
    return x
end

function emptycolmetadata!(x::Union{DataFrameRow, SubDataFrame}, col::ColumnIndex)
    col_name = _names(x)[index(x)[col]]
    df = parent(x)
    idx = index(df)[col_name]

    for key in colmetadatakeys(df, idx)
        _, s = colmetadata(df, idx, key, style=true)
        s == :note && deletecolmetadata!(df, idx, key)
    end
    return x
end

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
function _copy_table_note_metadata!(dst::DataFrame, src)
    emptymetadata!(dst)
    if DataAPI.metadatasupport(typeof(src)).read
        for key in metadatakeys(src)
            val, style = metadata(src, key, style=true)
            style === :note && metadata!(dst, key, val, style=:note)
        end
    end
    return nothing
end

# copy column-level :note-style metadata from Tables.jl table src to dst
# from column src_col to dst_col
# discarding previous metadata contents of dst
function _copy_col_note_metadata!(dst::DataFrame, dst_col, src, src_col)
    emptycolmetadata!(dst, dst_col)
    if DataAPI.colmetadatasupport(typeof(src)).read
        for key in colmetadatakeys(src, src_col)
            val, style = colmetadata(src, src_col, key, style=true)
            style === :note && colmetadata!(dst, dst_col, key, val, style=:note)
        end
    end
    return nothing
end

# this is a function used to copy table-level and column-level :note-style metadata
# from Tables.jl table src to dst, discarding previous metadata contents of dst
function _copy_all_note_metadata!(dst::DataFrame, src)
    _copy_table_note_metadata!(dst, src)
    emptycolmetadata!(dst)
    if DataAPI.colmetadatasupport(typeof(src)).read
        for (col, col_keys) in colmetadatakeys(src)
            if hasproperty(dst, col)
                for key in col_keys
                    val, style = colmetadata(src, col, key, style=true)
                    style === :note && colmetadata!(dst, col, key, val, style=:note)
                end
            end
        end
    end
    return nothing
end

# this is a function used to copy all table and column-level metadata
# from Tables.jl table src to dst, discarding previous metadata contents of dst
function _copy_all_all_metadata!(dst::DataFrame, src)
    emptymetadata!(dst)
    if DataAPI.metadatasupport(typeof(src)).read
        for key in metadatakeys(src)
            val, style = metadata(src, key, style=true)
            metadata!(dst, key, val, style=style)
        end
    end
    emptycolmetadata!(dst)
    if DataAPI.colmetadatasupport(typeof(src)).read
        for (col, col_keys) in colmetadatakeys(src)
            if hasproperty(dst, col)
                for key in col_keys
                    val, style = colmetadata(src, col, key, style=true)
                    colmetadata!(dst, col, key, val, style=style)
                end
            end
        end
    end
    return nothing
end

# this is a function used to drop table-level metadata that is not :note-style
function _drop_table_nonnote_metadata!(df::DataFrame)
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
    _drop_table_nonnote_metadata!(df)
    for (col, col_keys) in colmetadatakeys(df)
        for key in col_keys
            _, style = colmetadata(df, col, key, style=true)
            style === :note || deletecolmetadata!(df, col, key)
        end
    end
    setfield!(df, :allnotemetadata, true)
    return nothing
end

# this is a function used to merge matching table-level metadata that has
# :note style and store it in `res`
# it removes all table-level metadata previously stored in `res`
# key-value metadata pair is matching if it has :note style and is present
# in all tables passed in dfs collection
function _merge_matching_table_note_metadata!(res::DataFrame,
                                              dfs::Union{AbstractVector{<:AbstractDataFrame},
                                                         Tuple{AbstractDataFrame, Vararg{AbstractDataFrame}}})
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

# this is a function used to keep in dst only table-level :note-style metadata
# matching between dst and src all other table-level metadata is dropped
# key-value metadata pair is matching if it has :note style and is present
# both in dst and src
function _keep_matching_table_note_metadata!(dst::DataFrame, src::AbstractDataFrame)
    _drop_table_nonnote_metadata!(dst)
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
