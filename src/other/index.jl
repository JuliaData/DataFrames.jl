# an AbstractIndex is a thing that can be used to look up ordered things by name, but that
# will also accept a position or set of positions or range or other things and pass them
# through cleanly.
# an Index is the usual implementation.
# a SimpleIndex only works if the things are integer indexes, which is weird.
abstract AbstractIndex

type Index <: AbstractIndex   # an OrderedDict would be nice here...
    lookup::Dict{Symbol, Int}      # name => names array position
    names::Vector{Symbol}
end
function Index(names::Vector{Symbol}; allow_duplicates=true)
    u = make_unique(names, allow_duplicates=allow_duplicates)
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
(==)(x::Index, y::Index) = isequal(x, y)

function names!(x::Index, nms::Vector{Symbol}; allow_duplicates=false)
    if length(nms) != length(x)
        throw(ArgumentError("Length of nms doesn't match length of x."))
    end
    newindex = Index(nms, allow_duplicates=allow_duplicates)
    x.names = newindex.names
    x.lookup = newindex.lookup
    return x
end

function rename!(x::Index, nms)
    for (from, to) in nms
        if haskey(x, to)
            error("Tried renaming $from to $to, when $to already exists in the Index.")
        end
        x.lookup[to] = col = pop!(x.lookup, from)
        x.names[col] = to
    end
    return x
end

rename!(x::Index, from, to) = rename!(x, zip(from, to))
rename!(x::Index, from::Symbol, to::Symbol) = rename!(x, ((from, to),))
rename!(x::Index, f::Function) = rename!(x, [(x,f(x)) for x in x.names])
rename!(f::Function, x::Index) = rename!(x, f)

rename(x::Index, args...) = rename!(copy(x), args...)
rename(f::Function, x::Index) = rename(x, f)

Base.haskey(x::Index, key::Symbol) = haskey(x.lookup, key)
Base.haskey(x::Index, key::Real) = 1 <= key <= length(x.names)
Base.keys(x::Index) = names(x)

# TODO: If this should stay 'unsafe', perhaps make unexported
function Base.push!(x::Index, nm::Symbol)
    x.lookup[nm] = length(x) + 1
    push!(x.names, nm)
    return x
end

function Base.merge!(x::Index, y::Index)
    adds = add_names(x, y)
    i = length(x)
    for add in adds
        i += 1
        x.lookup[add] = i
    end
    append!(x.names, adds)
    return x
end

Base.merge(x::Index, y::Index) = merge!(copy(x), y)

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

Base.getindex(x::Index, idx::Symbol) = x.lookup[idx]
Base.getindex(x::AbstractIndex, idx::Real) = @compat Int(idx)
Base.getindex(x::AbstractIndex, idx::AbstractDataVector{Bool}) = getindex(x, convert(Array, idx, false))
Base.getindex{T}(x::AbstractIndex, idx::AbstractDataVector{T}) = getindex(x, dropna(idx))
Base.getindex(x::AbstractIndex, idx::AbstractVector{Bool}) = find(idx)
Base.getindex(x::AbstractIndex, idx::Range) = [idx;]
Base.getindex{T <: Real}(x::AbstractIndex, idx::AbstractVector{T}) = convert(Vector{Int}, idx)
Base.getindex(x::AbstractIndex, idx::AbstractVector{Symbol}) = [x.lookup[i] for i in idx]

type SimpleIndex <: AbstractIndex
    length::Integer
end
SimpleIndex() = SimpleIndex(0)
Base.length(x::SimpleIndex) = x.length
Base.names(x::SimpleIndex) = nothing
_names(x::SimpleIndex) = nothing

# Helpers

function add_names(ind::Index, add_ind::Index)
    u = names(add_ind)

    seen = Set(_names(ind))
    dups = Int[]

    for i in 1:length(u)
        name = u[i]
        in(name, seen) ? push!(dups, i) : push!(seen, name)
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
