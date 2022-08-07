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
    metadata(gdf::GroupedDataFrame, key::AbstractString; style::Bool=false)

Return table level metadata value associated with `df` for key `key`.
If `style=true` return a tuple of metadata value and metadata style.

`SubDataFrame` and `DataFrameRow` expose only `:note` style metadata of their
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
    meta === nothing && throw(KeyError("Metadata for key $key not found"))
    return style ? meta[key] : meta[key][1]
end

metadata(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame},
         key::AbstractString; style::Bool=false) =
    metadata(parent(x), key, style=style)

function metadata(x::Union{DataFrameRow, SubDataFrame},
                  key::AbstractString; style::Bool=false)
    meta_value, meta_style = metadata(parent(x), key, style=true)
    meta_style !== :note && throw(KeyError("Metadata for key $key not found"))
    return style ? (meta_value, meta_style) : meta_value
end

"""
    metadatakeys(df::AbstractDataFrame)
    metadatakeys(dfr::DataFrameRow)
    metadatakeys(dfc::DataFrameColumns)
    metadatakeys(dfr::DataFrameRows)
    metadatakeys(gdf::GroupedDataFrame)

Return an iterator of table level metadata keys for which `metadata(df, key)`
returns a metadata value.

`SubDataFrame` and `DataFrameRow` expose only `:note` style metadata keys of
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
    @assert !isempty(metakeys) # by design in such cases meta === nothing should be met
    return metakeys
end

metadatakeys(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame}) =
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
    metadata!(gdf::GroupedDataFrame, key::AbstractString, value; style)

Set table level metadata for object `df` for key `key` to have value `value`
and style `style` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note` style for metadata is allowed.
Trying to add key-value pair such that in the parent data frame already
mapping for key exists with `:none` style throws an error.

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
    return df
end

function metadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame},
                   key::AbstractString, value::Any; style)
    metadata!(parent(x), key, value, style=style)
    return x
end

function metadata!(x::Union{DataFrameRow, SubDataFrame},
                   key::AbstractString, value::Any; style)
    style === :note || throw(ArgumentError("only :note style is supported for " *
                                           "DataFrameRow and SubDataFrame"))
    df = parent(x)
    meta = getfield(df, :metadata)
    if meta !== nothing && haskey(meta, key) && meta[key][2] !== :note
        throw(ArgumentError("trying to set metadata for DataFrameRow and SubDataFrame" *
                            "that is already present in the parent and does not " *
                            "have :note style"))
    end
    metadata!(df, key, value, style=style)
    return x
end

"""
    deletemetadata!(df::AbstractDataFrame, key::AbstractString)
    deletemetadata!(dfr::DataFrameRow, key::AbstractString)
    deletemetadata!(dfc::DataFrameColumns, key::AbstractString)
    deletemetadata!(dfr::DataFrameRows, key::AbstractString)
    deletemetadata!(gdf::GroupedDataFrame, key::AbstractString)

Delete table level metadata from object `df` for key `key`.

For `SubDataFrame` and `DataFrameRow` only `:note` style for metadata is deleted.

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

function  deletemetadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame},
                          key::AbstractString)
    deletemetadata!(parent(x), key)
    return x
end

function deletemetadata!(x::Union{DataFrameRow, SubDataFrame}, key::AbstractString)
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
    emptymetadata!(gdf::GroupedDataFrame)

Delete table level metadata from object `df` for key `key`.

For `SubDataFrame` and `DataFrameRow` only `:note` style for metadata is deleted.

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

function emptymetadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame})
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
    colmetadata(gdf::GroupedDataFrame, col::ColumnIndex, key::AbstractString; style::Bool=false)

Return column level metadata value associated with `df` for column `col` and key `key`.

`SubDataFrame` and `DataFrameRow` expose only `:note` style metadata of their parent.

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
    cols_meta === nothing && throw(KeyError("Metadata for column $col for key $key not found"))
    col_meta = cols_meta[idx]
    return style ? col_meta[key] : col_meta[key][1]

end

# here and similar definitions below are added to avoid against dispatch ambiguity
colmetadata(df::DataFrame, col::Symbol, key::AbstractString; style::Bool=false) =
    colmetadata(df, Int(index(df)[col]), key, style=style)
colmetadata(df::DataFrame, col::ColumnIndex, key::AbstractString; style::Bool=false) =
    colmetadata(df, Int(index(df)[col]), key, style=style)

colmetadata(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame},
            col::Int, key::AbstractString; style::Bool=false) =
    colmetadata(parent(x), col, key; style=style)
colmetadata(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame},
            col::Symbol, key::AbstractString; style::Bool=false) =
    colmetadata(parent(x), col, key; style=style)
colmetadata(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString; style::Bool=false) =
    colmetadata(parent(dfr), col, key; style=style)
colmetadata(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString; style::Bool=false) =
    colmetadata(parent(dfc), col, key; style=style)
colmetadata(gdf::GroupedDataFrame, col::ColumnIndex, key::AbstractString; style::Bool=false) =
    colmetadata(parent(gdf), col, key; style=style)

function colmetadata(x::Union{DataFrameRow, SubDataFrame},
                     col::Int, key::AbstractString; style::Bool=false)
    col_name = _names(x)[col]
    df = parent(x)
    val_meta, style_meta = colmetadata(df, col_name, key, style=true)
    if style_meta !== :note
        throw(KeyError("Metadata for column $col for key $key with :note style not found"))
    end
    return style ? (val_meta, style_meta) : val_meta
end

colmetadata(x::Union{DataFrameRow, SubDataFrame},
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
    colmetadatakeys(gdf::GroupedDataFrame, [col::ColumnIndex])

If `col` is passed return an iterator of column level metadata keys for which
`metadata(x, col, key)` returns a metadata value.

`SubDataFrame` and `DataFrameRow` expose only `:note` style metadata of their parent.

If `col` is not passed return an iterator of `col => colmetadatakeys(x, col)`
pairs for all columns that have metadata, where `col` are `Symbol`.

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

colmetadatakeys(df::DataFrame, col::Symbol) = colmetadatakes(df, Int(index(df)[col]))
colmetadatakeys(df::DataFrame, col::ColumnIndex) = colmetadatakes(df, Int(index(df)[col]))

function colmetadatakeys(df::DataFrame)
    cols_meta = getfield(df, :colmetadata)
    cols_meta === nothing && return ()
    return (_names(df)[idx] => colmetadatakeys(df, idx) for idx in keys(cols_meta))
end

colmetadatakeys(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame}, col::Int) =
    colmetadatakeys(parent(x), col)
colmetadatakeys(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame}, col::Symbol) =
    colmetadatakeys(parent(x), col)
colmetadatakeys(dfr::DataFrameRows, col::ColumnIndex) = colmetadatakeys(parent(dfr), col)
colmetadatakeys(dfc::DataFrameColumns, col::ColumnIndex) = colmetadatakeys(parent(dfc), col)
colmetadatakeys(gdf::GroupedDataFrame, col::ColumnIndex) = colmetadatakeys(parent(gdf), col)
colmetadatakeys(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame}) =
    colmetadatakeys(parent(x))

function colmetadatakeys(x::Union{DataFrameRow, SubDataFrame}, col::Int)
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

colmetadatakeys(x::Union{DataFrameRow, SubDataFrame}, col::Symbol) =
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
    colmetadata!(gdf::GroupedDataFrame, col::ColumnIndex, key::AbstractString, value; style)

Set column level metadata for `df` for column `col` for key `key` to have value `value`
and style `style` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note` style for metadata is allowed.
Trying to add key-value pair such that in the parent data frame already
mapping for key exists with `:none` style throws an error.

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
    return df
end

colmetadata!(df::DataFrame, col::Symbol, key::AbstractString, value::Any; style) =
    colmetadata!(df, Int(index(df)[col]), key, value; style=style)
colmetadata!(df::DataFrame, col::ColumnIndex, key::AbstractString, value::Any; style) =
    colmetadata!(df, Int(index(df)[col]), key, value; style=style)

function colmetadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame},
                      col::Int, key::AbstractString, value::Any; style)
    colmetadata!(parent(x), col, key, value; style=style)
    return x
end

function colmetadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame},
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

function colmetadata!(gdf::GroupedDataFrame, col::ColumnIndex, key::AbstractString, value::Any; style)
    colmetadata!(parent(gdf), col, key, value; style=style)
    return gdf
end

function colmetadata!(x::Union{DataFrameRow, SubDataFrame},
                      col::Int, key::AbstractString, value::Any; style)
    col_name = _names(x)[col]
    style === :note || throw(ArgumentError("only :note style is supported for " *
                                           "DataFrameRow and SubDataFrame"))
    df = parent(x)
    cols_meta = getfield(df, :colsmetadata)
    idx = index(df)[col_name]
    if cols_meta !== nothing && haskey(cols_meta, idx) &&
       haskey(cols_meta[idx], key) && cols_meta[idx][key][2] !== :note
        throw(ArgumentError("trying to set metadata for DataFrameRow and SubDataFrame" *
                            "that is already present in the parent and does not " *
                            "have :note style"))
    end
    colmetadata!(df, idx, key, value, style=style)
    return x
end

colmetadata!(x::Union{DataFrameRow, SubDataFrame},
             col::Symbol, key::AbstractString, value::Any; style) =
    colmetadata!(x, Int(index(x)[col]), key, value; style=style)
colmetadata!(x::DataFrameRow, col::ColumnIndex, key::AbstractString, value::Any; style) =
    colmetadata!(x, Int(index(x)[col]), key, value; style=style)
colmetadata!(x::SubDataFrame, col::ColumnIndex), key::AbstractString, value::Any; style =
    colmetadata!(x, Int(index(x)[col]), key, value; style=style)

"""
    deletecolmetadata!(df::AbstractDataFrame, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(dfr::DataFrameRow, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(gdf::GroupedDataFrame, col::ColumnIndex, key::AbstractString)

Delete column level metadata for `df` for column `col` for key `key` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note` style for metadata is deleted.

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

function deletecolmetadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame},
                            col::Int, key::AbstractString)
    deletecolmetadata!(parent(x), col, key)
    return x
end

function deletecolmetadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame},
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

function deletecolmetadata!(gdf::GroupedDataFrame, col::ColumnIndex, key::AbstractString)
    deletecolmetadata!(parent(gdf), col, key)
    return gdf
end

function deletecolmetadata!(x::Union{DataFrameRow, SubDataFrame}, col::Int, key::AbstractString)
    col_name = _names(x)[col]
    df = parent(x)
    idx = index(df)[col_name]

    if key in colmetadatakeys(df, idx)
        _, s = colmetadata(df, idx, key, style=true)
        s == :note && deletecolmetadata!(df, idx, key)
    end
    return x
end

deletecolmetadata!(x::Union{DataFrameRow, SubDataFrame}, col::Symbol, key::AbstractString) =
    deletecolmetadata!(x, Int(index(x)[col]), key)
deletecolmetadata!(x::DataFrameRow, col::ColumnIndex, key::AbstractString) =
    deletecolmetadata!(x, Int(index(x)[col]), key)
deletecolmetadata!(x::SubDataFrame, col::ColumnIndex, key::AbstractString) =
    deletecolmetadata!(x, Int(index(x)[col]), key)

"""
    emptycolmetadata!(df::AbstractDataFrame, col::ColumnIndex, key::AbstractString)
    emptycolmetadata!(dfr::DataFrameRow, col::ColumnIndex, key::AbstractString)
    emptycolmetadata!(dfc::DataFrameColumns, col::ColumnIndex, key::AbstractString)
    emptycolmetadata!(dfr::DataFrameRows, col::ColumnIndex, key::AbstractString)
    emptycolmetadata!(gdf::GroupedDataFrame, col::ColumnIndex, key::AbstractString)

Delete column level metadata for `df` for column `col` for key `key` and return `df`.

For `SubDataFrame` and `DataFrameRow` only `:note` style for metadata is deleted.

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

function emptycolmetadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame}, col::Int)
    emptycolmetadata!(parent(x), col)
    return x
end

function emptycolmetadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame}, col::Symbol)
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

function emptycolmetadata!(gdf::GroupedDataFrame, col::ColumnIndex)
    emptycolmetadata!(parent(gdf), col)
    return gdf
end

function emptycolmetadata!(x::Union{DataFrameRows, DataFrameColumns, GroupedDataFrame})
    emptycolmetadata!(parent(x))
    return x
end


function emptycolmetadata!(x::Union{DataFrameRow, SubDataFrame}, col::Int)
    col_name = _names(x)[col]
    df = parent(x)
    idx = index(df)[col_name]

    for key in colmetadatakeys(df, idx)
        _, s = colmetadata(df, idx, key, style=true)
        s == :note && deletecolmetadata!(df, idx, key)
    end
    return x
end

emptycolmetadata!(x::Union{DataFrameRow, SubDataFrame}, col::Symbol) =
    emptycolmetadata!(x, Int(index(x)[col]), key)
emptycolmetadata!(x::DataFrameRow, col::ColumnIndex) =
    emptycolmetadata!(x, Int(index(x)[col]), key)
emptycolmetadata!(x::SubDataFrame, col::ColumnIndex) =
    emptycolmetadata!(x, Int(index(x)[col]), key)

function emptycolmetadata!(x::Union{DataFrameRow, SubDataFrame})
    for i in 1:ncol(x)
        emptycolmetadata!(x, i)
    end
    return x
end

### Internal utility functions for metadata handling

# copy table level :note metadata from src to dst
# discarding previous metadata contents of dst
function _copy_df_note_metadata!(dst::DataFrame, src)
    emptymetadata!(dst)
    for key in metadatakeys(src)
        val, style = metadata(src, key, style=true)
        style === :note && metadata!(dst, key, val, style=:note)
    end
    return nothing
end

# copy column level :note metadata from src to dst from column src_col to dst_col
# discarding previous metadata contents of dst
function _copy_col_note_metadata!(dst::DataFrame, dst_col, src, src_col)
    emptycolmetadata!(dst, dst_col)
    for key in colmetadatakeys(src, src_col)
        val, style = colmetadata(src, src_col, key, style=true)
        style === :note && colmetadata!(dst, dst_col, key, val, style=:note)
    end
    return nothing
end

# this is a function used to copy table and column level :note metadata
# discarding previous metadata contents of dst
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

# this is a function used to drop table level metadata that is not :note style
function _drop_df_nonnote_metadata!(df::DataFrame)
    for key in metadatakeys(df)
        _, style = metadata(src, key, style=true)
        style === :note || deletemetadata!(df, key)
    end
    return nothing
end

# this is a function used to drop table and column level metadata that is not :note style
function _drop_all_nonnote_metadata!(df::DataFrame)
    _drop_df_nonnote_metadata!(df)
    for (col, col_keys) in colmetadatakeys(df)
        for key in col_keys
            _, style = colmetadata(src, col, key, style=true)
            style === :note || deletecolmetadata!(df, col, key)
        end
    end
    return nothing
end

# this is a function used to merge matching table level metadata that has :note style
function _merge_matching_df_note_metadata!(res::DataFrame, dfs)
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
end











struct _MetadataMergeSentinelType end

function _intersect_dicts(d1::Dict{String, Any}, d2::Dict{String, Any})
    length(d1) > length(d2) && return _intersect_dicts(d2, d1)
    d_out = Dict{String,Any}()
    for (k, v) in pairs(d1)
        if isequal(v, get(d2, k, _MetadataMergeSentinelType()))
            d_out[k] = v
        end
    end
    return d_out
end

function _intersect_dicts!(d1::Dict{String, Any}, d2::Dict{String, Any})
    for (k, v) in pairs(d1)
        if !isequal(v, get(d2, k, _MetadataMergeSentinelType()))
            delete!(d1, k)
        end
    end
    return d1
end


_drop_metadata!(df::DataFrame) = setfield!(df, :metadata, nothing)
_drop_colmetadata!(df::DataFrame) = setfield!(df, :colmetadata, nothing)

function _drop_colmetadata!(df::AbstractDataFrame, col::ColumnIndex)
    colmetadata = getfield(parent(df), :colmetadata)
    if colmetadata !== nothing
        delete!(colmetadata, index(df)[col])
    end
    return nothing
end

function _copy_colmetadata!(dst::AbstractDataFrame, dstcol::ColumnIndex,
                            src, srccol::ColumnIndex)
    if hascolmetadata(src, srccol) === true
        copy!(colmetadata(dst, dstcol), colmetadata(src, srccol))
    else
        _drop_colmetadata!(dst, dstcol)
    end
    return nothing
end
