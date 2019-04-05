# an AbstractIndex is a thing that can be used to look up ordered things by name, but that
# will also accept a position or set of positions or range or other things and pass them
# through cleanly.
abstract type AbstractIndex end

const ColumnIndex = Union{Signed, Unsigned, Symbol}

struct Index <: AbstractIndex   # an OrderedDict would be nice here...
    lookup::Dict{Symbol, Int}      # name => names array position
    names::Vector{Symbol}
end

function Index(names::AbstractVector{Symbol}; makeunique::Bool=false)
    u = make_unique(names, makeunique=makeunique)
    lookup = Dict{Symbol, Int}(zip(u, 1:length(u)))
    Index(lookup, u)
end
Index() = Index(Dict{Symbol, Int}(), Symbol[])
Base.length(x::Index) = length(x.names)
Base.names(x::Index) = copy(x.names)
_names(x::Index) = x.names
Base.copy(x::Index) = Index(copy(x.lookup), copy(x.names))
Base.isequal(x::AbstractIndex, y::AbstractIndex) = _names(x) == _names(y) # it is enough to check names
Base.:(==)(x::AbstractIndex, y::AbstractIndex) = isequal(x, y)


function names!(x::Index, nms::Vector{Symbol}; makeunique::Bool=false)
    if !makeunique
        if length(unique(nms)) != length(nms)
            dup = unique(nms[nonunique(DataFrame(nms=nms))])
            dupstr = join(string.(':', dup), ", ", " and ")
            msg = "Duplicate variable names: $dupstr. Pass makeunique=true" *
                  " to make them unique using a suffix automatically."
            throw(ArgumentError(msg))
        end
    end
    if length(nms) != length(x)
        throw(ArgumentError("Length of nms doesn't match length of x."))
    end
    make_unique!(x.names, nms, makeunique=makeunique)
    empty!(x.lookup)
    for (i, n) in enumerate(x.names)
        x.lookup[n] = i
    end
    return x
end

function rename!(x::Index, nms)
    for (from, to) in nms
        from == to && continue # No change, nothing to do
        if haskey(x, to)
            error("Tried renaming $from to $to, when $to already exists in the Index.")
        end
        x.lookup[to] = col = pop!(x.lookup, from)
        x.names[col] = to
    end
    return x
end

rename!(x::Index, nms::Pair{Symbol,Symbol}...) = rename!(x::Index, collect(nms))
rename!(f::Function, x::Index) = rename!(x, [(x=>f(x)) for x in x.names])

rename(x::Index, args...) = rename!(copy(x), args...)
rename(f::Function, x::Index) = rename!(f, copy(x))

@inline function Base.permute!(x::Index, p::AbstractVector)
    @boundscheck if !(length(p) == length(x) && isperm(p))
        throw(ArgumentError("$p is not a valid column permutation for this Index"))
    end
    oldnames = copy(_names(x))
    for (i, j) in enumerate(p)
        n = oldnames[j]
        x.names[i] = n
        x.lookup[n] = i
    end
    x
end

Base.haskey(x::Index, key::Symbol) = haskey(x.lookup, key)
Base.haskey(x::Index, key::Integer) = 1 <= key <= length(x.names)
Base.haskey(x::Index, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))
Base.keys(x::Index) = names(x)

# TODO: If this should stay 'unsafe', perhaps make unexported
function Base.push!(x::Index, nm::Symbol)
    x.lookup[nm] = length(x) + 1
    push!(x.names, nm)
    return x
end

function Base.merge!(x::Index, y::AbstractIndex; makeunique::Bool=false)
    adds = add_names(x, y, makeunique=makeunique)
    i = length(x)
    for add in adds
        i += 1
        x.lookup[add] = i
    end
    append!(x.names, adds)
    return x
end

Base.merge(x::Index, y::AbstractIndex; makeunique::Bool=false) =
    merge!(copy(x), y, makeunique=makeunique)

function Base.delete!(x::Index, idx::Integer)
    # reset the lookup's beyond the deleted item
    for i in (idx + 1):length(x.names)
        x.lookup[x.names[i]] = i - 1
    end
    delete!(x.lookup, x.names[idx])
    deleteat!(x.names, idx)
    return x
end

function Base.delete!(x::Index, nm::Symbol)
    if !haskey(x.lookup, nm)
        return x
    end
    idx = x.lookup[nm]
    return delete!(x, idx)
end

function Base.empty!(x::Index)
    empty!(x.lookup)
    empty!(x.names)
    x
end

function Base.insert!(x::Index, idx::Integer, nm::Symbol)
    1 <= idx <= length(x.names)+1 || error(BoundsError())
    for i = idx:length(x.names)
        x.lookup[x.names[i]] = i + 1
    end
    x.lookup[nm] = idx
    insert!(x.names, idx, nm)
    x
end

@inline Base.getindex(x::AbstractIndex, idx::Bool) = throw(ArgumentError("invalid index: $idx of type Bool"))
@inline Base.getindex(x::AbstractIndex, idx::Integer) = Int(idx)
@inline Base.getindex(x::AbstractIndex, idx::AbstractVector{Int}) = idx
@inline Base.getindex(x::AbstractIndex, idx::AbstractRange{Int}) = idx
@inline Base.getindex(x::AbstractIndex, idx::AbstractRange{<:Integer}) = collect(Int, idx)
@inline Base.getindex(x::AbstractIndex, ::Colon) = Base.OneTo(length(x))

# Fuzzy matching rules:
# 1. ignore case
# 2. maximum Levenshtein distance is 2
# 3. always show matches with 0 difference (wrong case)
# 4. on top of 3. do not show more than 8 matches in total
# Returns candidates ordered by (distance, name) pair
function fuzzymatch(l::Dict{Symbol, Int}, idx::Symbol)
        idxs = uppercase(string(idx))
        dist = [(REPL.levenshtein(uppercase(string(x)), idxs), x) for x in keys(l)]
        sort!(dist)
        c = [count(x -> x[1] <= i, dist) for i in 0:2]
        maxd = max(0, searchsortedlast(c, 8) - 1)
        [s for (d, s) in dist if d <= maxd]
end

@inline function lookupname(l::Dict{Symbol, Int}, idx::Symbol)
    i = get(l, idx, nothing)
    if i === nothing
        candidates = fuzzymatch(l, idx)
        if isempty(candidates)
            throw(ArgumentError("column name :$idx not found in the data frame"))
        end
        candidatesstr = join(string.(':', candidates), ", ", " and ")
        throw(ArgumentError("column name :$idx not found in the data frame; " *
                            "existing most similar names are: $candidatesstr"))
    end
    i
end

@inline Base.getindex(x::Index, idx::Symbol) = lookupname(x.lookup, idx)
@inline Base.getindex(x::Index, idx::AbstractVector{Symbol}) =
    [lookupname(x.lookup, i) for i in idx]

@inline function Base.getindex(x::AbstractIndex, idx::AbstractVector{<:Integer})
    if any(v -> v isa Bool, idx)
        throw(ArgumentError("Bool values except for AbstractVector{Bool} are not allowed for column indexing"))
    end
    Vector{Int}(idx)
end

@inline Base.getindex(x::AbstractIndex, idx::AbstractRange{Bool}) = getindex(x, collect(idx))
@inline function Base.getindex(x::AbstractIndex, idx::AbstractVector{Bool})
    length(x) == length(idx) || throw(BoundsError(x, idx))
    findall(idx)
end

# catch all method handling cases when type of idx is not narrowest possible, Any in particular
@inline function Base.getindex(x::AbstractIndex, idxs::AbstractVector)
    length(idxs) == 0 && return Int[] # special case of empty idxs
    if idxs[1] isa Real
        if !all(v -> v isa Integer && !(v isa Bool), idxs)
            throw(ArgumentError("Only Integer values allowed when indexing by vector of numbers"))
        end
        return convert(Vector{Int}, idxs)
    end
    idxs[1] isa Symbol && return getindex(x, convert(Vector{Symbol}, idxs))
    throw(ArgumentError("idxs[1] has type $(typeof(idxs[1])); "*
                        "DataFrame only supports indexing columns with integers, symbols or boolean vectors"))
end

# Helpers

function add_names(ind::Index, add_ind::AbstractIndex; makeunique::Bool=false)
    u = names(add_ind)

    seen = Set(_names(ind))
    dups = Int[]

    for i in 1:length(u)
        name = u[i]
        in(name, seen) ? push!(dups, i) : push!(seen, name)
    end
    if length(dups) > 0
        if !makeunique
            dupstr = join(string.(':', unique(u[dups])), ", ", " and ")
            msg = "Duplicate variable names: $dupstr. Pass makeunique=true" *
                  " to make them unique using a suffix automatically."
            throw(ArgumentError(msg))
        end
    end
    for i in dups
        nm = u[i]
        k = 1
        while true
            newnm = Symbol("$(nm)_$k")
            if !in(newnm, seen)
                u[i] = newnm
                push!(seen, newnm)
                break
            end
            k += 1
        end
    end

    return u
end

@inline parentcols(ind::Index) = Base.OneTo(length(ind))
@inline parentcols(ind::Index, cols) = cols

### SubIndex of Index. Used by SubDataFrame, DataFrameRow, and DataFrameRows

struct SubIndex{I<:AbstractIndex,S<:AbstractVector{Int},T<:AbstractVector{Int}} <: AbstractIndex
    parent::I
    cols::S # columns from idx selected in SubIndex
    remap::T # reverse mapping from cols to their position in the SubIndex
end

SubIndex(parent::AbstractIndex, ::Colon) = parent

Base.copy(x::SubIndex) = Index(_names(x))

@inline parentcols(ind::SubIndex) = ind.cols

Base.@propagate_inbounds parentcols(ind::SubIndex, idx::Union{Integer,AbstractVector{<:Integer}}) =
    ind.cols[idx]

Base.@propagate_inbounds function parentcols(ind::SubIndex, idx::Symbol)
    parentcol = ind.parent[idx]
    @boundscheck begin
        remap = ind.remap
        length(remap) == 0 && lazyremap!(ind)
        remap[parentcol] == 0 && throw(ArgumentError("$idx not found"))
    end
    return parentcol
end

Base.@propagate_inbounds parentcols(ind::SubIndex, idx::AbstractVector{Symbol}) =
    [parentcols(ind, i) for i in idx]

Base.@propagate_inbounds parentcols(ind::SubIndex, ::Colon) = ind.cols

Base.@propagate_inbounds function SubIndex(parent::AbstractIndex, cols::AbstractUnitRange{Int})
    l = last(cols)
    f = first(cols)
    @boundscheck if !checkindex(Bool, Base.OneTo(length(parent)), cols)
        throw(BoundsError("invalid columns $cols selected"))
    end
    remap = (1:l) .- f .+ 1
    SubIndex(parent, cols, remap)
end

Base.@propagate_inbounds function SubIndex(parent::AbstractIndex, cols::AbstractVector{Int})
    ncols = length(parent)
    @boundscheck if !all(x -> 0 < x ≤ ncols, cols)
        throw(BoundsError("invalid columns $cols selected"))
    end
    remap = Int[]
    SubIndex(parent, cols, remap)
end

@inline SubIndex(parent::AbstractIndex, cols::ColumnIndex) =
    throw(ArgumentError("cols argument must be a vector (got $cols)"))

Base.@propagate_inbounds SubIndex(parent::AbstractIndex, cols) =
    SubIndex(parent, parent[cols])

# a helper function that lazily creates remap when needed
function lazyremap!(x::SubIndex)
    remap = x.remap
    remap isa AbstractUnitRange{Int} && return remap
    if length(remap) == 0
        resize!(remap, length(x.parent))
        # we set non-existing mappings to 0
        fill!(remap, 0)
        for (i, col) in enumerate(x.cols)
            remap[col] = i
        end
    end
    remap
end

Base.length(x::SubIndex) = length(x.cols)
Base.names(x::SubIndex) = copy(_names(x))
_names(x::SubIndex) = view(_names(x.parent), x.cols)

function Base.haskey(x::SubIndex, key::Symbol)
    haskey(x.parent, key) || return false
    pos = x.parent[key]
    remap = x.remap
    length(remap) == 0 && lazyremap!(x)
    checkbounds(Bool, remap, pos) || return false
    remap[pos] > 0
end

Base.haskey(x::SubIndex, key::Integer) = 1 <= key <= length(x)
Base.haskey(x::SubIndex, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))
Base.keys(x::SubIndex) = names(x)

function Base.getindex(x::SubIndex, idx::Symbol)
    remap = x.remap
    length(remap) == 0 && lazyremap!(x)
    remap[x.parent[idx]]
end
Base.getindex(x::SubIndex, idx::AbstractVector{Symbol}) = [x[i] for i in idx]
