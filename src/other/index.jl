# an AbstractIndex is a thing that can be used to look up ordered things by name, but that
# will also accept a position or set of positions or range or other things and pass them
# through cleanly.
abstract type AbstractIndex end

function Base.summary(idx::AbstractIndex)
    l = length(idx)
    return "data frame with $l column$(l == 1 ? "" : "s")"
end
Base.summary(io::IO, idx::AbstractIndex) = print(io, summary(idx))

const SymbolOrString = Union{Symbol, AbstractString}
const ColumnIndex = Union{Signed, Unsigned, SymbolOrString}
const MultiColumnIndex = Union{AbstractVector, Regex, Not, Between, All, Colon}
const MULTICOLUMNINDEX_TUPLE = (:AbstractVector, :Regex, :Not, :Between, :All, :Colon)

const COLUMNINDEX_STR = "`Symbol`, string or integer"
const MULTICOLUMNINDEX_STR = "`:`, `All`, `Between`, `Not`, a regular expression," *
                          " or a vector of `Symbol`s, strings or integers"

struct Index <: AbstractIndex   # an OrderedDict would be nice here...
    lookup::Dict{Symbol, Int}      # name => names array position
    names::Vector{Symbol}
end

function Index(names::AbstractVector{Symbol}; makeunique::Bool=false)
    u = make_unique(names, makeunique=makeunique)
    lookup = Dict{Symbol, Int}(zip(u, 1:length(u)))
    return Index(lookup, u)
end

Index() = Index(Dict{Symbol, Int}(), Symbol[])
Base.length(x::Index) = length(x.names)
Base.names(x::Index) = string.(x.names)

# _names returns Vector{Symbol}
_names(x::Index) = x.names

Base.copy(x::Index) = Index(copy(x.lookup), copy(x.names))
Base.isequal(x::AbstractIndex, y::AbstractIndex) = _names(x) == _names(y) # it is enough to check names
Base.:(==)(x::AbstractIndex, y::AbstractIndex) = isequal(x, y)


function rename!(x::Index, nms::AbstractVector{Symbol}; makeunique::Bool=false)
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
        throw(DimensionMismatch("Length of nms doesn't match length of x."))
    end
    make_unique!(x.names, nms, makeunique=makeunique)
    empty!(x.lookup)
    for (i, n) in enumerate(x.names)
        x.lookup[n] = i
    end
    return x
end

function rename!(x::Index, nms::AbstractVector{Pair{Symbol, Symbol}})
    xbackup = copy(x)
    processedfrom = Set{Symbol}()
    processedto = Set{Symbol}()
    toholder = Dict{Symbol,Int}()
    for (from, to) in nms
        if from ∈ processedfrom
            copy!(x.lookup, xbackup.lookup)
            x.names .= xbackup.names
            throw(ArgumentError("Tried renaming :$from multiple times."))
        end
        if to ∈ processedto
            copy!(x.lookup, xbackup.lookup)
            x.names .= xbackup.names
            throw(ArgumentError("Tried renaming to :$to multiple times."))
        end
        push!(processedfrom, from)
        push!(processedto, to)
        from == to && continue # No change, nothing to do
        if !haskey(xbackup, from)
            copy!(x.lookup, xbackup.lookup)
            x.names .= xbackup.names
            throw(ArgumentError("Tried renaming :$from to :$to, when :$from " *
                                "does not exist in the Index."))
        end
        if haskey(x, to)
            toholder[to] = x.lookup[to]
        end
        col = haskey(toholder, from) ? pop!(toholder, from) : pop!(x.lookup, from)
        x.lookup[to] = col
        x.names[col] = to
    end
    if !isempty(toholder)
        copy!(x.lookup, xbackup.lookup)
        x.names .= xbackup.names
        throw(ArgumentError("Tried renaming to :$(first(keys(toholder))), " *
                            "when it already exists in the Index."))
    end
    return x
end

rename!(f::Function, x::Index) = rename!(x, [(n=>Symbol(f(string(n)))) for n in x.names])

# we do not define keys on purpose;
# use names to get keys as strings with copying
# or _names to get keys as Symbols without copying
Base.haskey(x::Index, key::Symbol) = haskey(x.lookup, key)
Base.haskey(x::Index, key::AbstractString) = haskey(x.lookup, Symbol(key))
Base.haskey(x::Index, key::Integer) = 1 <= key <= length(x.names)
Base.haskey(x::Index, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))

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

Base.delete!(x::Index, nm::AbstractString) = delete!(x, Symbol(nm))

function Base.empty!(x::Index)
    empty!(x.lookup)
    empty!(x.names)
    return x
end

function Base.insert!(x::Index, idx::Integer, nm::Symbol)
    if !(1 <= idx <= length(x.names)+1)
        throw(BoundsError(x, idx))
     end
    for i = idx:length(x.names)
        x.lookup[x.names[i]] = i + 1
    end
    x.lookup[nm] = idx
    insert!(x.names, idx, nm)
    return x
end

Base.insert!(x::Index, idx::Integer, nm::AbstractString) = insert!(x, idx, Symbol(nm))

@inline Base.getindex(x::AbstractIndex, idx::Bool) =
    throw(ArgumentError("invalid index: $idx of type Bool"))

@inline function Base.getindex(x::AbstractIndex, idx::Integer)
    if !(1 <= idx <= length(x))
        throw(BoundsError(x, idx))
    end
    return Int(idx)
end

@inline function Base.getindex(x::AbstractIndex, idx::AbstractVector{Int})
    isempty(idx) && return idx
    minidx, maxidx = extrema(idx)
    if minidx < 1
        throw(BoundsError(x, idx))
    end
    if maxidx > length(x)
        throw(BoundsError(x, idx))
    end
    allunique(idx) || throw(ArgumentError("Elements of $idx must be unique"))
    return idx
end

@inline function Base.getindex(x::AbstractIndex, idx::AbstractRange{Int})
    isempty(idx) && return idx
    minidx, maxidx = extrema(idx)
    if minidx < 1
        throw(BoundsError(x, idx))
    end
    if maxidx > length(x)
        throw(BoundsError(x, idx))
    end
    allunique(idx) || throw(ArgumentError("Elements of $idx must be unique"))
    return idx
end

@inline Base.getindex(x::AbstractIndex, idx::AbstractRange{<:Integer}) =
    getindex(x, collect(Int, idx))
@inline Base.getindex(x::AbstractIndex, ::Colon) = Base.OneTo(length(x))
@inline Base.getindex(x::AbstractIndex, notidx::Not) =
    setdiff(1:length(x), getindex(x, notidx.skip))
@inline Base.getindex(x::AbstractIndex, idx::Between) = x[idx.first]:x[idx.last]
@inline Base.getindex(x::AbstractIndex, idx::All) =
    isempty(idx.cols) ? (1:length(x)) : union(getindex.(Ref(x), idx.cols)...)

@inline function Base.getindex(x::AbstractIndex, idx::AbstractVector{<:Integer})
    if any(v -> v isa Bool, idx)
        throw(ArgumentError("Bool values except for AbstractVector{Bool} are not" *
                            " allowed for column indexing"))
    end
    return getindex(x, Vector{Int}(idx))
end

@inline Base.getindex(x::AbstractIndex, idx::AbstractRange{Bool}) = getindex(x, collect(idx))

@inline function Base.getindex(x::AbstractIndex, idx::AbstractVector{Bool})
    length(x) == length(idx) || throw(BoundsError(x, idx))
    return findall(idx)
end

# catch all method handling cases when type of idx is not narrowest possible, Any in particular
@inline function Base.getindex(x::AbstractIndex, idxs::AbstractVector)
    isempty(idxs) && return Int[] # special case of empty idxs
    if idxs[1] isa Real
        if !all(v -> v isa Integer && !(v isa Bool), idxs)
            throw(ArgumentError("Only `Integer` values allowed when indexing by vector of numbers"))
        end
        return getindex(x, convert(Vector{Int}, idxs))
    elseif idxs[1] isa Symbol
        if all(x -> x isa Symbol, idxs)
            return getindex(x, convert(Vector{Symbol}, idxs))
        else
            throw(ArgumentError("mixing `Symbol`s with other selectors is not allowed"))
        end
    elseif idxs[1] isa AbstractString
        if all(x -> x isa AbstractString, idxs)
            return getindex(x, Symbol.(idxs))
        else
            throw(ArgumentError("mixing strings with other selectors is not allowed"))
        end
    end
    throw(ArgumentError("idxs[1] has type $(typeof(idxs[1])); only Integer, Symbol, "*
                        "or string values allowed when indexing by vector"))
end

@inline Base.getindex(x::AbstractIndex, rx::Regex) =
    getindex(x, filter(name -> occursin(rx, String(name)), _names(x)))

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
        return [s for (d, s) in dist if d <= maxd]
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
    return i
end

@inline Base.getindex(x::Index, idx::Symbol) = lookupname(x.lookup, idx)
@inline Base.getindex(x::Index, idx::AbstractString) = x[Symbol(idx)]

@inline function Base.getindex(x::Index, idx::Union{AbstractVector{Symbol},
                                              AbstractVector{<:AbstractString}})
    allunique(idx) || throw(ArgumentError("Elements of $idx must be unique"))
    return [x[i] for i in idx]
end

# Helpers

function add_names(ind::Index, add_ind::AbstractIndex; makeunique::Bool=false)
    u = copy(_names(add_ind))

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
@inline parentcols(ind::Index, cols) = ind[cols]

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
Base.@propagate_inbounds parentcols(ind::SubIndex, idx::Bool) =
    throw(ArgumentError("column indexing with Bool is not allowed"))

Base.@propagate_inbounds function parentcols(ind::SubIndex, idx::Symbol)
    parentcol = ind.parent[idx]
    @boundscheck begin
        remap = ind.remap
        remap[parentcol] == 0 && throw(ArgumentError("$idx not found"))
    end
    return parentcol
end

Base.@propagate_inbounds parentcols(ind::SubIndex, idx::AbstractString) =
    parentcols(ind, Symbol(idx))

Base.@propagate_inbounds parentcols(ind::SubIndex, idx::AbstractVector{Symbol}) =
    [parentcols(ind, i) for i in idx]

Base.@propagate_inbounds parentcols(ind::SubIndex, idx::AbstractVector{<:AbstractString}) =
    [parentcols(ind, i) for i in idx]

Base.@propagate_inbounds parentcols(ind::SubIndex, idx::Regex) =
    [parentcols(ind, i) for i in _names(ind) if occursin(idx, String(i))]

Base.@propagate_inbounds parentcols(ind::SubIndex, idx) =
    parentcols(ind, ind[idx])

Base.@propagate_inbounds parentcols(ind::SubIndex, ::Colon) = ind.cols

function SubIndex(parent::AbstractIndex, cols::AbstractUnitRange{Int})
    l = last(cols)
    f = first(cols)
    if !checkindex(Bool, Base.OneTo(length(parent)), cols)
        throw(BoundsError(parent, cols))
    end
    remap = (1:l) .- f .+ 1
    return SubIndex(parent, cols, remap)
end

function SubIndex(parent::AbstractIndex, cols::AbstractVector{Int})
    ncols = length(parent)
    remap = zeros(Int, ncols)
    for (i, col) in enumerate(cols)
        if !(1 <= col <= ncols)
            throw(BoundsError(parent, cols))
        end
        if remap[col] != 0
            throw(ArgumentError("duplicate selected column detected"))
        end
        remap[col] = i
    end
    return SubIndex(parent, cols, remap)
end

@inline SubIndex(parent::AbstractIndex, cols::ColumnIndex) =
    throw(ArgumentError("cols argument must be a vector (got $cols)"))

Base.@propagate_inbounds SubIndex(parent::AbstractIndex, cols) =
    SubIndex(parent, parent[cols])

Base.length(x::SubIndex) = length(x.cols)
Base.names(x::SubIndex) = string.(_names(x))
_names(x::SubIndex) = view(_names(x.parent), x.cols)

function Base.haskey(x::SubIndex, key::Symbol)
    haskey(x.parent, key) || return false
    pos = x.parent[key]
    remap = x.remap
    checkbounds(Bool, remap, pos) || return false
    return remap[pos] > 0
end

Base.haskey(x::SubIndex, key::AbstractString) = haskey(x, Symbol(key))
Base.haskey(x::SubIndex, key::Integer) = 1 <= key <= length(x)
Base.haskey(x::SubIndex, key::Bool) =
    throw(ArgumentError("invalid key: $key of type Bool"))

Base.getindex(x::SubIndex, idx::Symbol) = x.remap[x.parent[idx]]
Base.getindex(x::SubIndex, idx::AbstractString) = x[Symbol(idx)]

function Base.getindex(x::SubIndex, idx::Union{AbstractVector{Symbol},
                                               AbstractVector{AbstractString}})
    allunique(idx) || throw(ArgumentError("Elements of $idx must be unique"))
    return [x[i] for i in idx]
end
