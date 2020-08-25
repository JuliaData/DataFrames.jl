# Implementation note
# There are two important design features of GroupedDataFrame
# 1. idx, starts, ends and keymap are by default left uninitialized;
#    they get populated only on demand; this means that every GroupedDataFrame
#    has lazy_lock field which is used to make sure that two threads concurrently
#    do not try to create them. The lock should be used in every function that
#    does a direct access to these fields via getfield.
# 2. Except for point 1 above currently fields of GroupedDataFrame are never
#    mutated after it is created. This means that internally when copying
#    a GroupedDataFrame they are not copied for efficiency. If in the future
#    operations that mutate GroupedDataFrame are introduced all non-copying
#    passing of the internal fields to a new GroupedDataFrame should be
#    updated. Currently this applies to `getindex` and `combine_helper` functions

"""
    GroupedDataFrame

The result of a [`groupby`](@ref) operation on an `AbstractDataFrame`; a
view into the `AbstractDataFrame` grouped by rows.

Not meant to be constructed directly, see `groupby`.
"""
mutable struct GroupedDataFrame{T<:AbstractDataFrame}
    parent::T
    cols::Vector{Symbol}                 # column names used for grouping
    groups::Vector{Int}                  # group indices for each row in 0:ngroups, 0 skipped
    idx::Union{Vector{Int},Nothing}      # indexing vector sorting rows into groups
    starts::Union{Vector{Int},Nothing}   # starts of groups after permutation by idx
    ends::Union{Vector{Int},Nothing}     # ends of groups after permutation by idx
    ngroups::Int                         # number of groups
    keymap::Union{Dict{Any,Int},Nothing} # mapping of key tuples to group indices
    lazy_lock::Threads.ReentrantLock     # lock is needed to make lazy operations
                                         # thread safe
end

function genkeymap(gd, cols)
    # currently we use Dict{Any,Int} because then field :keymap in GroupedDataFrame
    # has a concrete type which makes the access to it faster as we do not have a dynamic
    # dispatch when indexing into it. In the future an optimization of this approach
    # can be investigated (also taking compilation time into account).
    d = Dict{Any,Int}()
    gdidx = gd.idx
    sizehint!(d, length(gd.starts))
    for (i, s) in enumerate(gd.starts)
        d[getindex.(cols, gdidx[s])] = i
    end
    d
end

function Base.getproperty(gd::GroupedDataFrame, f::Symbol)
    if f in (:idx, :starts, :ends)
        # Group indices are computed lazily the first time they are accessed
        # Do not lock when field is already initialized
        if getfield(gd, f) === nothing
            Threads.lock(gd.lazy_lock) do
                if getfield(gd, f) === nothing # Do not lock when field is already initialized
                    gd.idx, gd.starts, gd.ends = compute_indices(gd.groups, gd.ngroups)
                end
            end
        end
        return getfield(gd, f)::Vector{Int}
    elseif f === :keymap
        # Keymap is computed lazily the first time it is accessed
        if getfield(gd, f) === nothing # Do not lock when field is already initialized
            Threads.lock(gd.lazy_lock) do
                if getfield(gd, f) === nothing
                    gd.keymap = genkeymap(gd, ntuple(i -> parent(gd)[!, gd.cols[i]], length(gd.cols)))
                end
            end
        end
        return getfield(gd, f)::Dict{Any,Int}
    else
        return getfield(gd, f)
    end
end

Base.broadcastable(::GroupedDataFrame) =
    throw(ArgumentError("broadcasting over `GroupedDataFrame`s is reserved"))

"""
    parent(gd::GroupedDataFrame)

Return the parent data frame of `gd`.
"""
Base.parent(gd::GroupedDataFrame) = getfield(gd, :parent)

function Base.:(==)(gd1::GroupedDataFrame, gd2::GroupedDataFrame)
    gd1.cols == gd2.cols &&
        length(gd1) == length(gd2) &&
        all(x -> ==(x...), zip(gd1, gd2))
end

function Base.isequal(gd1::GroupedDataFrame, gd2::GroupedDataFrame)
    isequal(gd1.cols, gd2.cols) &&
        isequal(length(gd1), length(gd2)) &&
        all(x -> isequal(x...), zip(gd1, gd2))
end

Base.names(gd::GroupedDataFrame) = names(gd.parent)
Base.names(gd::GroupedDataFrame, cols) = names(gd.parent, cols)
_names(gd::GroupedDataFrame) = _names(gd.parent)

function DataFrame(gd::GroupedDataFrame; copycols::Bool=true, keepkeys::Bool=true)
    if !copycols
        throw(ArgumentError("It is not possible to construct a `DataFrame`" *
                            "from GroupedDataFrame with `copycols=false`"))
    end
    length(gd) == 0 && return similar(parent(gd), 0)
    gdidx = gd.idx
    idx = similar(gdidx)
    doff = 1
    for (s,e) in zip(gd.starts, gd.ends)
        n = e - s + 1
        copyto!(idx, doff, gdidx, s, n)
        doff += n
    end
    resize!(idx, doff - 1)
    if keepkeys
        return parent(gd)[idx, :]
    else
        return parent(gd)[idx, Not(gd.cols)]
    end
end


#
# Accessing group indices, columns, and values
#

"""
    groupindices(gd::GroupedDataFrame)

Return a vector of group indices for each row of `parent(gd)`.

Rows appearing in group `gd[i]` are attributed index `i`. Rows not present in
any group are attributed `missing` (this can happen if `skipmissing=true` was
passed when creating `gd`, or if `gd` is a subset from
a larger [`GroupedDataFrame`](@ref)).
"""
groupindices(gd::GroupedDataFrame) = replace(gd.groups, 0=>missing)

"""
    groupcols(gd::GroupedDataFrame)

Return a vector of `Symbol` column names in `parent(gd)` used for grouping.
"""
function groupcols(gd::GroupedDataFrame)
    issubset(gd.cols, _names(parent(gd))) ||
        throw(ErrorException("grouping column names not found in data frame column names"))
    return copy(gd.cols)
end

"""
    valuecols(gd::GroupedDataFrame)

Return a vector of `Symbol` column names in `parent(gd)` not used for grouping.
"""
function valuecols(gd::GroupedDataFrame)
    issubset(gd.cols, _names(parent(gd))) || throw(ErrorException("grouping column " *
        "names not found in data frame column names"))
    return setdiff(_names(gd), gd.cols)
end


# Get grouping variable index by its name
function _groupvar_idx(gd::GroupedDataFrame, name::Symbol, strict::Bool)
    i = findfirst(==(name), gd.cols)
    i === nothing && strict && throw(ArgumentError("$name is not a grouping column"))
    return i
end

# Get values of grouping columns for single group
_groupvalues(gd::GroupedDataFrame, i::Integer) =
    gd.parent[gd.idx[gd.starts[i]], gd.cols]

# Get values of single grouping column for single group
_groupvalues(gd::GroupedDataFrame, i::Integer, col::Integer) =
    gd.parent[gd.idx[gd.starts[i]], gd.cols[col]]
_groupvalues(gd::GroupedDataFrame, i::Integer, col::Symbol) =
    _groupvalues(gd, i, _groupvar_idx(gd, col, true))


#
# Vector interface and integer indexing
#

Base.length(gd::GroupedDataFrame) = gd.ngroups

function Base.iterate(gd::GroupedDataFrame, i=1)
    if i > length(gd)
        nothing
    else
        (view(gd.parent, gd.idx[gd.starts[i]:gd.ends[i]], :), i+1)
    end
end

Compat.lastindex(gd::GroupedDataFrame) = gd.ngroups
Base.first(gd::GroupedDataFrame) = gd[1]
Base.last(gd::GroupedDataFrame) = gd[end]

# These have to be defined for some to_indices() logic to work, as long
# as GroupedDataFrame is not <: AbstractArray
Base.IndexStyle(::Type{<:GroupedDataFrame}) = IndexLinear()
Base.IndexStyle(::GroupedDataFrame) = IndexLinear()
Base.keys(::IndexLinear, gd::GroupedDataFrame) = Base.OneTo(length(gd))

# Single integer indexing
Base.getindex(gd::GroupedDataFrame, idx::Integer) =
    view(gd.parent, gd.idx[gd.starts[idx]:gd.ends[idx]], :)

# Index with array of integers OR bools
function Base.getindex(gd::GroupedDataFrame, idxs::AbstractVector{<:Integer})
    new_starts = gd.starts[idxs]
    new_ends = gd.ends[idxs]
    if !allunique(new_starts)
        throw(ArgumentError("duplicates in idxs argument are not allowed"))
    end
    new_groups = zeros(Int, length(gd.groups))
    idx = gd.idx
    for i in eachindex(new_starts)
        @inbounds for j in new_starts[i]:new_ends[i]
            new_groups[idx[j]] = i
        end
    end
    GroupedDataFrame(gd.parent, copy(gd.cols), new_groups, gd.idx,
                     new_starts, new_ends, length(new_starts), nothing,
                     Threads.ReentrantLock())
end

# Index with colon (creates copy)
Base.getindex(gd::GroupedDataFrame, idxs::Colon) =
    Threads.lock(gd.lazy_lock) do
        return GroupedDataFrame(gd.parent, copy(gd.cols), gd.groups, getfield(gd, :idx),
                                getfield(gd, :starts), getfield(gd, :ends), gd.ngroups,
                                getfield(gd, :keymap), Threads.ReentrantLock())
    end


#
# GroupKey and GroupKeys
#

"""
    GroupKey{T<:GroupedDataFrame}

Key for one of the groups of a [`GroupedDataFrame`](@ref). Contains the values
of the corresponding grouping columns and behaves similarly to a `NamedTuple`,
but using it to index its `GroupedDataFrame` is much more effecient than using the
equivalent `Tuple` or `NamedTuple`.

Instances of this type are returned by `keys(::GroupedDataFrame)` and are not
meant to be constructed directly.

Indexing fields of `GroupKey` is allowed using an integer, a `Symbol`, or a string.
It is also possible to access the data in a `GroupKey` using the `getproperty`
function. A `GroupKey` can be converted to a `Tuple`, `NamedTuple`, or `Vector`.

See [`keys(::GroupedDataFrame)`](@ref) for more information.
"""
struct GroupKey{T<:GroupedDataFrame}
    parent::T
    idx::Int
end

function Base.show(io::IO, k::GroupKey)
    print(io, "GroupKey: ")
    show(io, NamedTuple(k))
end

Base.parent(key::GroupKey) = getfield(key, :parent)
Base.length(key::GroupKey) = length(parent(key).cols)
Base.names(key::GroupKey) = string.(parent(key).cols)
# Private fields are never exposed since they can conflict with column names
Base.propertynames(key::GroupKey, private::Bool=false) = copy(parent(key).cols)
Base.keys(key::GroupKey) = propertynames(key)
Base.haskey(key::GroupKey, idx::Symbol) = idx in parent(key).cols
Base.haskey(key::GroupKey, idx::AbstractString) = haskey(key, Symbol(idx))
Base.haskey(key::GroupKey, idx::Union{Signed,Unsigned}) = 1 <= idx <= length(key)
Base.values(key::GroupKey) = Tuple(_groupvalues(parent(key), getfield(key, :idx)))
Base.IteratorEltype(::Type{<:GroupKey}) = Base.EltypeUnknown()
Base.iterate(key::GroupKey, i::Integer=1) =
    i <= length(key) ? (key[i], i + 1) : nothing
Base.getindex(key::GroupKey, i::Integer) =
    _groupvalues(parent(key), getfield(key, :idx), i)

function Base.getindex(key::GroupKey, n::Symbol)
    try
        return _groupvalues(parent(key), getfield(key, :idx), n)
    catch e
        throw(KeyError(n))
    end
end

Base.getindex(key::GroupKey, n::AbstractString) = key[Symbol(n)]

function Base.getproperty(key::GroupKey, p::Symbol)
    try
        return key[p]
    catch e
        throw(ArgumentError("$(typeof(key)) has no property $p"))
    end
end

Base.getproperty(key::GroupKey, p::AbstractString) = getproperty(key, Symbol(p))

function Base.NamedTuple(key::GroupKey)
    N = NamedTuple{Tuple(parent(key).cols)}
    N(_groupvalues(parent(key), getfield(key, :idx)))
end

"""
    copy(key::GroupKey)

Construct a `NamedTuple` with the same contents as the [`GroupKey`](@ref).
"""
Base.copy(key::GroupKey) = NamedTuple(key)

Base.convert(::Type{NamedTuple}, key::GroupKey) = NamedTuple(key)
Base.convert(::Type{Tuple}, key::GroupKey) = Tuple(key)

Base.convert(::Type{Vector}, key::GroupKey) = [v for v in key]
Base.convert(::Type{Vector{T}}, key::GroupKey) where T = T[v for v in key]
Base.Vector(key::GroupKey) = convert(Vector, key)
Base.Vector{T}(key::GroupKey) where T = convert(Vector{T}, key)

Base.convert(::Type{Array}, key::GroupKey) = Vector(key)
Base.convert(::Type{Array{T}}, key::GroupKey) where {T} = Vector{T}(key)
Base.Array(key::GroupKey) = Vector(key)
Base.Array{T}(key::GroupKey) where {T} = Vector{T}(key)

Base.broadcastable(::GroupKey) =
    throw(ArgumentError("broadcasting over `GroupKey`s is reserved"))

"""
    GroupKeys{T<:GroupedDataFrame} <: AbstractVector{GroupKey{T}}

A vector containing all [`GroupKey`](@ref) objects for a given
[`GroupedDataFrame`](@ref).

See [`keys(::GroupedDataFrame)`](@ref) for more information.
"""
struct GroupKeys{T<:GroupedDataFrame} <: AbstractVector{GroupKey{T}}
    parent::T
end

Base.parent(gk::GroupKeys) = gk.parent

Base.size(gk::GroupKeys) = (length(parent(gk)),)
Base.IndexStyle(::Type{<:GroupKeys}) = IndexLinear()
@Base.propagate_inbounds function Base.getindex(gk::GroupKeys, i::Integer)
    @boundscheck checkbounds(gk, i)
    return GroupKey(parent(gk), i)
end


#
# Non-standard indexing
#

# Non-standard indexing relies on converting to integer indices first
# The full version (to_indices) is required rather than to_index even though
# GroupedDataFrame behaves as a 1D array due to the behavior of Colon and Not.
# Note that this behavior would be the default if it was <:AbstractArray
function Base.getindex(gd::GroupedDataFrame, idx...)
    length(idx) == 1 || throw(ArgumentError("GroupedDataFrame requires a single index"))
    return getindex(gd, Base.to_indices(gd, idx)...)
end

# The allowed key types for dictionary-like indexing
const GroupKeyTypes = Union{GroupKey, Tuple, NamedTuple}
# All allowed scalar index types
const GroupIndexTypes = Union{Integer, GroupKeyTypes}

# Find integer index for dictionary keys
function Base.to_index(gd::GroupedDataFrame, key::GroupKey)
    gd === parent(key) && return getfield(key, :idx)
    throw(ErrorException("Cannot use a GroupKey to index a GroupedDataFrame " *
                         "other than the one it was derived from."))
end

Base.to_index(gd::GroupedDataFrame, key::Tuple) = gd.keymap[key]

function Base.to_index(gd::GroupedDataFrame, key::NamedTuple{N}) where {N}
    if length(key) != length(gd.cols) || any(n != c for (n, c) in zip(N, gd.cols))
        throw(KeyError(key))
    end
    return Base.to_index(gd, Tuple(key))
end

# Array of (possibly non-standard) indices
function Base.to_index(gd::GroupedDataFrame, idxs::AbstractVector{T}) where {T}
    # A concrete eltype which is <: GroupKeyTypes, don't need to check
    if isconcretetype(T) && T <: GroupKeyTypes
        return [Base.to_index(gd, i) for i in idxs]
    end

    # Edge case - array is empty
    isempty(idxs) && return Int[]

    # Infer eltype based on type of first index, expect rest to match
    idx1 = idxs[1]
    E1 = typeof(idx1)

    E = if E1 <: Integer && E1 !== Bool
        Integer
    elseif E1 <: GroupKey
        GroupKey
    elseif E1 <: Tuple
        Tuple
    elseif E1 <: NamedTuple
        NamedTuple
    else
        throw(ArgumentError("Invalid index: $idx1 of type $E1"))
    end

    # Convert each index to integer format
    ints = Vector{Int}(undef, length(idxs))
    for (i, idx) in enumerate(idxs)
        if !(idx isa GroupIndexTypes) || idx isa Bool
            throw(ArgumentError("Invalid index: $idx of type $(typeof(idx))"))
        end
        idx isa E || throw(ArgumentError("Mixed index types in array not allowed"))
        ints[i] = Base.to_index(gd, idx)
    end

    return ints
end


#
# Indexing with Not/InvertedIndex
#

# InvertedIndex wrapping any other valid index type
# to_indices() is needed here rather than to_index() in order to override the
# to_indices(::Any, ::Tuple{Not}) methods defined in InvertedIndices.jl
function Base.to_indices(gd::GroupedDataFrame, (idx,)::Tuple{<:Not})
    (skip_idx,) = Base.to_indices(gd, (idx.skip,))
    idxs = Base.OneTo(length(gd))[Not(skip_idx)]
    return (idxs,)
end

# InvertedIndex wrapping a boolean array
# The definition above works but we need to define specialized methods to avoid
# ambiguity in dispatch
function Base.to_indices(gd::GroupedDataFrame,
                         (idx,)::Tuple{Not{<:Union{BitArray{1}, Vector{Bool}}}})
    (findall(!, idx.skip),)
end
function Base.to_indices(gd::GroupedDataFrame,
                         (idx,)::Tuple{Not{<:AbstractVector{Bool}}})
    (findall(!, idx.skip),)
end


#
# Dictionary interface
#

"""
    keys(gd::GroupedDataFrame)

Get the set of keys for each group of the `GroupedDataFrame` `gd` as a
[`GroupKeys`](@ref) object. Each key is a [`GroupKey`](@ref), which behaves like
a `NamedTuple` holding the values of the grouping columns for a given group.
Unlike the equivalent `Tuple` and `NamedTuple`, these keys can be used to index
into `gd` efficiently. The ordering of the keys is identical to the ordering of
the groups of `gd` under iteration and integer indexing.

# Examples

```jldoctest groupkeys
julia> df = DataFrame(a = repeat([:foo, :bar, :baz], outer=[4]),
                      b = repeat([2, 1], outer=[6]),
                      c = 1:12);

julia> gd = groupby(df, [:a, :b])
GroupedDataFrame with 6 groups based on keys: a, b
First Group (2 rows): a = :foo, b = 2
│ Row │ a      │ b     │ c     │
│     │ Symbol │ Int64 │ Int64 │
├─────┼────────┼───────┼───────┤
│ 1   │ foo    │ 2     │ 1     │
│ 2   │ foo    │ 2     │ 7     │
⋮
Last Group (2 rows): a = :baz, b = 1
│ Row │ a      │ b     │ c     │
│     │ Symbol │ Int64 │ Int64 │
├─────┼────────┼───────┼───────┤
│ 1   │ baz    │ 1     │ 6     │
│ 2   │ baz    │ 1     │ 12    │

julia> keys(gd)
6-element DataFrames.GroupKeys{GroupedDataFrame{DataFrame}}:
 GroupKey: (a = :foo, b = 2)
 GroupKey: (a = :bar, b = 1)
 GroupKey: (a = :baz, b = 2)
 GroupKey: (a = :foo, b = 1)
 GroupKey: (a = :bar, b = 2)
 GroupKey: (a = :baz, b = 1)
```

`GroupKey` objects behave similarly to `NamedTuple`s:

```jldoctest groupkeys
julia> k = keys(gd)[1]
GroupKey: (a = :foo, b = 2)

julia> keys(k)
(:a, :b)

julia> values(k)  # Same as Tuple(k)
(:foo, 2)

julia> NamedTuple(k)
(a = :foo, b = 2)

julia> k.a
:foo

julia> k[:a]
:foo

julia> k[1]
:foo
```

Keys can be used as indices to retrieve the corresponding group from their
`GroupedDataFrame`:

```jldoctest groupkeys
julia> gd[k]
2×3 SubDataFrame
│ Row │ a      │ b     │ c     │
│     │ Symbol │ Int64 │ Int64 │
├─────┼────────┼───────┼───────┤
│ 1   │ foo    │ 2     │ 1     │
│ 2   │ foo    │ 2     │ 7     │

julia> gd[keys(gd)[1]] == gd[1]
true
```
"""
Base.keys(gd::GroupedDataFrame) = GroupKeys(gd)

function Base.haskey(gd::GroupedDataFrame, key::GroupKey)
    if gd === parent(key)
        if 1 <= getfield(key, :idx) <= length(gd)
            return true
        else
            throw(BoundsError(gd, getfield(key, :idx)))
        end
    else
        msg = "The parent of key does not match the passed GroupedDataFrame"
        throw(ArgumentError(msg))
    end
end

function Base.haskey(gd::GroupedDataFrame, key::Tuple)
    if length(key) != length(gd.cols)
        return throw(ArgumentError("The length of key does not match the " *
                                   "number of grouping columns"))
    end
    return haskey(gd.keymap, key)
end

function Base.haskey(gd::GroupedDataFrame, key::NamedTuple{N}) where {N}
    if length(key) != length(gd.cols) || any(((n, c),) -> n != c, zip(N, gd.cols))
        return throw(ArgumentError("The column names of key do not match " *
                                   "the names of grouping columns"))
    end
    return haskey(gd, Tuple(key))
end

Base.haskey(gd::GroupedDataFrame, key::Union{Signed,Unsigned}) =
    1 <= key <= length(gd)

"""
    get(gd::GroupedDataFrame, key, default)

Get a group based on the values of the grouping columns.

`key` may be a `NamedTuple` or `Tuple` of grouping column values (in the same
order as the `cols` argument to `groupby`).

# Examples

```jldoctest
julia> df = DataFrame(a = repeat([:foo, :bar, :baz], outer=[2]),
                      b = repeat([2, 1], outer=[3]),
                      c = 1:6);

julia> gd = groupby(df, :a)
GroupedDataFrame with 3 groups based on key: a
First Group (2 rows): a = :foo
│ Row │ a      │ b     │ c     │
│     │ Symbol │ Int64 │ Int64 │
├─────┼────────┼───────┼───────┤
│ 1   │ foo    │ 2     │ 1     │
│ 2   │ foo    │ 1     │ 4     │
⋮
Last Group (2 rows): a = :baz
│ Row │ a      │ b     │ c     │
│     │ Symbol │ Int64 │ Int64 │
├─────┼────────┼───────┼───────┤
│ 1   │ baz    │ 2     │ 3     │
│ 2   │ baz    │ 1     │ 6     │

julia> get(gd, (a=:bar,), nothing)
2×3 SubDataFrame
│ Row │ a      │ b     │ c     │
│     │ Symbol │ Int64 │ Int64 │
├─────┼────────┼───────┼───────┤
│ 1   │ bar    │ 1     │ 2     │
│ 2   │ bar    │ 2     │ 5     │

julia> get(gd, (:baz,), nothing)
2×3 SubDataFrame
│ Row │ a      │ b     │ c     │
│     │ Symbol │ Int64 │ Int64 │
├─────┼────────┼───────┼───────┤
│ 1   │ baz    │ 2     │ 3     │
│ 2   │ baz    │ 1     │ 6     │

julia> get(gd, (:qux,), nothing)
```
"""
function Base.get(gd::GroupedDataFrame, key::Union{Tuple, NamedTuple}, default)
    try
        return gd[key]
    catch KeyError
        return default
    end
end

"""
    filter(fun, gdf::GroupedDataFrame)
    filter(cols => fun, gdf::GroupedDataFrame)

Return a new `GroupedDataFrame` containing only groups for which `fun`
returns `true`.

If `cols` is not specified then the predicate `fun` is called with a
`SubDataFrame` for each group.

If `cols` is specified then the predicate `fun` is called for each group with
views of the corresponding columns as separate positional arguments, unless
`cols` is an `AsTable` selector, in which case a `NamedTuple` of these arguments
is passed. `cols` can be any column selector ($COLUMNINDEX_STR;
$MULTICOLUMNINDEX_STR), and column duplicates are allowed if a vector of
`Symbol`s, strings, or integers is passed.

# Examples
```
julia> df = DataFrame(g=[1, 2], x=['a', 'b']);

julia> gd = groupby(df, :g)
GroupedDataFrame with 2 groups based on key: g
First Group (1 row): g = 1
│ Row │ g     │ x    │
│     │ Int64 │ Char │
├─────┼───────┼──────┤
│ 1   │ 1     │ 'a'  │
⋮
Last Group (1 row): g = 2
│ Row │ g     │ x    │
│     │ Int64 │ Char │
├─────┼───────┼──────┤
│ 1   │ 2     │ 'b'  │

julia> filter(x -> x.x[1] == 'a', gd)
GroupedDataFrame with 1 group based on key: g
First Group (1 row): g = 1
│ Row │ g     │ x    │
│     │ Int64 │ Char │
├─────┼───────┼──────┤
│ 1   │ 1     │ 'a'  │

julia> filter(:x => x -> x[1] == 'a', gd)
GroupedDataFrame with 1 group based on key: g
First Group (1 row): g = 1
│ Row │ g     │ x    │
│     │ Int64 │ Char │
├─────┼───────┼──────┤
│ 1   │ 1     │ 'a'  │

```
"""
Base.filter(f, gdf::GroupedDataFrame) =
    gdf[[f(sdf)::Bool for sdf in gdf]]
Base.filter((col, f)::Pair{<:ColumnIndex}, gdf::GroupedDataFrame) =
    _filter_helper(gdf, f, gdf.idx, gdf.starts, gdf.ends, parent(gdf)[!, col])
Base.filter((cols, f)::Pair{<:AbstractVector{Symbol}}, gdf::GroupedDataFrame) =
    filter([index(parent(gdf))[col] for col in cols] => f, gdf)
Base.filter((cols, f)::Pair{<:AbstractVector{<:AbstractString}}, gdf::GroupedDataFrame) =
    filter([index(parent(gdf))[col] for col in cols] => f, gdf)
Base.filter((cols, f)::Pair, gdf::GroupedDataFrame) =
    filter(index(parent(gdf))[cols] => f, gdf)
Base.filter((cols, f)::Pair{<:AbstractVector{Int}}, gdf::GroupedDataFrame) =
    _filter_helper(gdf, f, gdf.idx, gdf.starts, gdf.ends, (parent(gdf)[!, i] for i in cols)...)

function _filter_helper(gdf::GroupedDataFrame, f, idx::Vector{Int},
                        starts::Vector{Int}, ends::Vector{Int}, cols...)
    function mapper(i::Integer)
        idxs = idx[starts[i]:ends[i]]
        return map(x -> view(x, idxs), cols)
    end

    if length(cols) == 0
        throw(ArgumentError("At least one column must be passed to filter on"))
    end
    sel = [f(mapper(i)...)::Bool for i in 1:length(gdf)]
    return gdf[sel]
end

function Base.filter((cols, f)::Pair{<:AsTable}, gdf::GroupedDataFrame)
    df_tmp = select(parent(gdf), cols.cols, copycols=false)
    if ncol(df_tmp) == 0
        throw(ArgumentError("At least one column must be passed to filter on"))
    end
    return _filter_helper_astable(gdf, Tables.columntable(df_tmp), f,
                                      gdf.idx, gdf.starts, gdf.ends)
end

function _filter_helper_astable(gdf::GroupedDataFrame, nt::NamedTuple, f,
                                idx::Vector{Int}, starts::Vector{Int}, ends::Vector{Int})
    function mapper(i::Integer)
        idxs = idx[starts[i]:ends[i]]
        return map(x -> view(x, idxs), nt)
    end

    return gdf[[f(mapper(i))::Bool for i in 1:length(gdf)]]
end
