#
# Type definition and basic methods
#

"""
    GroupedDataFrame

The result of a [`groupby`](@ref) operation on an `AbstractDataFrame`; a
view into the `AbstractDataFrame` grouped by rows.

Not meant to be constructed directly, see `groupby`.
"""
mutable struct GroupedDataFrame{T<:AbstractDataFrame}
    parent::T
    cols::Vector{Int}                  # columns used for grouping
    groups::Vector{Int}                # group indices for each row in 0:ngroups, 0 skipped
    idx::Union{Vector{Int},Nothing}    # indexing vector sorting rows into groups
    starts::Union{Vector{Int},Nothing} # starts of groups after permutation by idx
    ends::Union{Vector{Int},Nothing}   # ends of groups after permutation by idx
    ngroups::Int                       # number of groups
end

function Base.getproperty(gd::GroupedDataFrame, f::Symbol)
    if f in (:idx, :starts, :ends)
        # Group indices are computed lazily the first time they are accessed
        if getfield(gd, f) === nothing
            gd.idx, gd.starts, gd.ends = compute_indices(gd.groups, gd.ngroups)
        end
        return getfield(gd, f)::Vector{Int}
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
_names(gd::GroupedDataFrame) = _names(gd.parent)

function DataFrame(gd::GroupedDataFrame; copycols::Bool=true)
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
    parent(gd)[idx, :]
end

"""
    groupindices(gd::GroupedDataFrame)

Return a vector of group indices for each row of `parent(gd)`.

Rows appearing in group `gd[i]` are attributed index `i`. Rows not present in
any group are attributed `missing` (this can happen if `skipmissing=true` was
passed when creating `gd`, or if `gd` is a subset from a larger [`GroupedDataFrame`](@ref)).
"""
groupindices(gd::GroupedDataFrame) = replace(gd.groups, 0=>missing)

"""
    groupvars(gd::GroupedDataFrame)

Return a vector of column names in `parent(gd)` used for grouping.
"""
groupvars(gd::GroupedDataFrame) = _names(gd)[gd.cols]

# Get grouping variable index by its name
function _groupvar_idx(gd::GroupedDataFrame, name::Symbol, strict::Bool)
    i = findfirst(==(name), groupvars(gd))
    i === nothing && strict && throw(ArgumentError("$name is not a grouping column"))
    return i
end

# Get values of grouping columns for single group
_groupvalues(gd::GroupedDataFrame, i::Integer) = gd.parent[gd.idx[gd.starts[i]], gd.cols]

# Get values of single grouping column for single group
_groupvalues(gd::GroupedDataFrame, i::Integer, col::Integer) =
    gd.parent[gd.idx[gd.starts[i]], gd.cols[col]]
_groupvalues(gd::GroupedDataFrame, i::Integer, col::Symbol) =
    _groupvalues(gd, i, _groupvar_idx(gd, col, true))


#
# Length and iteration
#

Base.length(gd::GroupedDataFrame) = gd.ngroups

function Base.iterate(gd::GroupedDataFrame, i=1)
    if i > length(gd)
        nothing
    else
        (view(gd.parent, gd.idx[gd.starts[i]:gd.ends[i]], :), i+1)
    end
end


#
# Vector interface and integer indexing
#

Compat.lastindex(gd::GroupedDataFrame) = gd.ngroups
Base.first(gd::GroupedDataFrame) = gd[1]
Base.last(gd::GroupedDataFrame) = gd[end]

# Single integer indexing
Base.getindex(gd::GroupedDataFrame, idx::Integer) =
    view(gd.parent, gd.idx[gd.starts[idx]:gd.ends[idx]], :)

# Array of integers
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
    GroupedDataFrame(gd.parent, gd.cols, new_groups, gd.idx,
                     new_starts, new_ends, length(new_starts))
end

# Colon (creates copy)
Base.getindex(gd::GroupedDataFrame, idxs::Colon) =
    GroupedDataFrame(gd.parent, gd.cols, gd.groups, gd.idx,
                     gd.starts, gd.ends, gd.ngroups)


#
# Dictionary interface and indexing
#

"""
    GroupKey{T<:GroupedDataFrame}

Key for one of the groups of a [`GroupedDataFrame`](@ref). Contains the values
of the corresponding grouping columns and behaves similarly to a `NamedTuple`,
but using it to index its `GroupedDataFrame` is much more effecient than using the
equivalent `Tuple` or `NamedTuple`.

Instances of this type are returned by `keys(::GroupedDataFrame)` and are not
meant to be constructed directly.

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
Base.keys(key::GroupKey) = Tuple(groupvars(parent(key)))
Base.names(key::GroupKey) = groupvars(parent(key))
# Private fields are never exposed since they can conflict with column names
Base.propertynames(key::GroupKey, private::Bool=false) = keys(key)
Base.values(key::GroupKey) = Tuple(_groupvalues(parent(key), getfield(key, :idx)))

Base.iterate(key::GroupKey, i::Integer=1) = i <= length(key) ? (key[i], i + 1) : nothing

Base.getindex(key::GroupKey, i::Integer) = _groupvalues(parent(key), getfield(key, :idx), i)

function Base.getindex(key::GroupKey, n::Symbol)
    try
        return _groupvalues(parent(key), getfield(key, :idx), n)
    catch e
        throw(KeyError(n))
    end
end

function Base.getproperty(key::GroupKey, p::Symbol)
    try
        return key[p]
    catch e
        throw(ArgumentError("$(typeof(key)) has no property $p"))
    end
end

function Base.NamedTuple(key::GroupKey)
    N = NamedTuple{Tuple(groupvars(parent(key)))}
    N(_groupvalues(parent(key), getfield(key, :idx)))
end
Base.Tuple(key::GroupKey) = values(key)


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

# Index with GroupKey
function Base.getindex(gd::GroupedDataFrame, key::GroupKey)
    gd === parent(key) && return gd[getfield(key, :idx)]
    throw(ErrorException("Cannot use a GroupKey to index a GroupedDataFrame other than the one it was derived from."))
end

# Index with tuple
function Base.getindex(gd::GroupedDataFrame, key::Tuple)
    for i in 1:length(gd)
        isequal(Tuple(_groupvalues(gd, i)), key) && return gd[i]
    end
    throw(KeyError(key))
end

# Index with named tuple
function Base.getindex(gd::GroupedDataFrame, key::NamedTuple{N}) where {N}
    if length(key) != length(gd.cols) || any(n != _names(gd)[c] for (n, c) in zip(N, gd.cols))
        throw(KeyError(key))
    end
    return gd[Tuple(key)]
end

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
