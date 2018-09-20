# an AbstractIndex is a thing that can be used to look up ordered things by name, but that
# will also accept a position or set of positions or range or other things and pass them
# through cleanly.
abstract type AbstractIndex end

mutable struct Index <: AbstractIndex   # an OrderedDict would be nice here...
    lookup::Dict{Symbol, Int}      # name => names array position
    names::Vector{Symbol}
end

function Index(names::Vector{Symbol}; makeunique::Bool=false)
    u = make_unique(names, makeunique=makeunique)
    lookup = Dict{Symbol, Int}(zip(u, 1:length(u)))
    Index(lookup, u)
end
Index() = Index(Dict{Symbol, Int}(), Symbol[])
Base.length(x::Index) = length(x.names)
Base.names(x::Index) = copy(x.names)
_names(x::Index) = x.names
Base.copy(x::Index) = Index(copy(x.lookup), copy(x.names))
Base.deepcopy(x::Index) = copy(x) # all eltypes immutable
Base.isequal(x::Index, y::Index) = isequal(x.lookup, y.lookup) && isequal(x.names, y.names)
# Imported in DataFrames.jl for compatibility across Julia 0.4 and 0.5
Base.:(==)(x::Index, y::Index) = isequal(x, y)

# TODO: after deprecation period remove allow_duplicates part of code
function names!(x::Index, nms::Vector{Symbol}; allow_duplicates=false, makeunique::Bool=false)
    if allow_duplicates
        Base.depwarn("Keyword argument allow_duplicates is deprecated. Use makeunique.", :names!)
    elseif !makeunique
        if length(unique(nms)) != length(nms)
            msg = """Duplicate variable names: $nms.
                     Pass makeunique=true to make them unique using a suffix automatically."""
            throw(ArgumentError(msg))
        end
    end
    if length(nms) != length(x)
        throw(ArgumentError("Length of nms doesn't match length of x."))
    end
    newindex = Index(nms, makeunique=makeunique)
    x.names = newindex.names
    x.lookup = newindex.lookup
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

Base.haskey(x::Index, key::Symbol) = haskey(x.lookup, key)
Base.haskey(x::Index, key::Real) = 1 <= key <= length(x.names)
Base.keys(x::Index) = names(x)

# TODO: If this should stay 'unsafe', perhaps make unexported
function Base.push!(x::Index, nm::Symbol)
    x.lookup[nm] = length(x) + 1
    push!(x.names, nm)
    return x
end

function Base.merge!(x::Index, y::Index; makeunique::Bool=false)
    adds = add_names(x, y, makeunique=makeunique)
    i = length(x)
    for add in adds
        i += 1
        x.lookup[add] = i
    end
    append!(x.names, adds)
    return x
end

Base.merge(x::Index, y::Index; makeunique::Bool=false) =
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

Base.getindex(x::AbstractIndex, idx::Symbol) = x.lookup[idx]
Base.getindex(x::AbstractIndex, idx::AbstractVector{Symbol}) = [x.lookup[i] for i in idx]
Base.getindex(x::AbstractIndex, idx::Bool) = throw(ArgumentError("invalid index: $idx of type Bool"))
Base.getindex(x::AbstractIndex, idx::Integer) = Int(idx)
Base.getindex(x::AbstractIndex, idx::AbstractVector{Int}) = idx
Base.getindex(x::AbstractIndex, idx::AbstractRange{Int}) = idx
Base.getindex(x::AbstractIndex, idx::AbstractRange{<:Integer}) = collect(Int, idx)

function Base.getindex(x::AbstractIndex, idx::AbstractVector{Bool})
    length(x) == length(idx) || throw(BoundsError(x, idx))
    findall(idx)
end

function Base.getindex(x::AbstractIndex, idx::AbstractVector{Union{Bool, Missing}})
    if any(ismissing, idx)
        throw(ArgumentError("missing values are not allowed for column indexing"))
    end
    getindex(x, collect(Missings.replace(idx, false)))
end

function Base.getindex(x::AbstractIndex, idx::AbstractVector{<:Integer})
    if any(v -> v isa Bool, idx)
        throw(ArgumentError("Bool values except for AbstractVector{Bool} are not allowed for column indexing"))
    end
    Vector{Int}(idx)
end

# catch all method handling cases when type of idx is not narrowest possible, Any in particular
# also it handles passing missing values in idx
function Base.getindex(x::AbstractIndex, idx::AbstractVector)
    idxs = filter(!ismissing, idx)
    if length(idxs) != length(idx)
        throw(ArgumentError("missing values are not allowed for column indexing"))
    end
    length(idxs) == 0 && return Int[] # special case of empty idxs
    if idxs[1] isa Real
        if !all(v -> v isa Integer && !(v isa Bool), idxs)
            throw(ArgumentError("Only Integer values allowed when indexing by vector of numbers"))
        end
        return Vector{Int}(idxs)
    end
    idxs[1] isa Symbol && return getindex(x, Vector{Symbol}(idxs))
    throw(ArgumentError("idx[1] has type $(typeof(idx[1])); "*
                        "DataFrame only supports indexing columns with integers, symbols or boolean vectors"))
end

# Helpers

function add_names(ind::Index, add_ind::Index; makeunique::Bool=false)
    u = names(add_ind)

    seen = Set(_names(ind))
    dups = Int[]

    for i in 1:length(u)
        name = u[i]
        in(name, seen) ? push!(dups, i) : push!(seen, name)
    end
    if length(dups) > 0
        if !makeunique
            Base.depwarn("Duplicate variable names are deprecated: pass makeunique=true to add a suffix automatically.", :add_names)
            # TODO: uncomment the lines below after deprecation period
            # msg = """Duplicate variable names: $(u[dups]).
            #          Pass makeunique=true to make them unique using a suffix automatically."""
            # throw(ArgumentError(msg))
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
