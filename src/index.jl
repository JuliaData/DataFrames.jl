# an AbstractIndex is a thing that can be used to look up ordered things by name, but that
# will also accept a position or set of positions or range or other things and pass them
# through cleanly.
# an Index is the usual implementation.
# a SimpleIndex only works if the things are integer indexes, which is weird.
typealias Indices Union(Real, AbstractVector{Real})

abstract AbstractIndex

type Index <: AbstractIndex   # an OrderedDict would be nice here...
    lookup::Dict{ByteString, Indices}      # name => names array position
    names::Vector{ByteString}
end
function Index{T <: ByteString}(x::Vector{T})
    x = make_unique(convert(Vector{ByteString}, x))
    Index(Dict{ByteString, Indices}(tuple(x...), tuple([1:length(x)]...)), x)
end
Index() = Index(Dict{ByteString, Indices}(), ByteString[])
Base.length(x::Index) = length(x.names)
Base.names(x::Index) = copy(x.names)
Base.copy(x::Index) = Index(copy(x.lookup), copy(x.names))
Base.deepcopy(x::Index) = Index(deepcopy(x.lookup), deepcopy(x.names))
Base.isequal(x::Index, y::Index) = isequal(x.lookup, y.lookup) && isequal(x.names, y.names)

# I think this should be Vector{T <: String}
function names!(x::Index, nm::Vector)
    if length(nm) != length(x)
        error("lengths don't match.")
    end
    for i in 1:length(nm)
        delete!(x.lookup, x.names[i])
        x.lookup[nm[i]] = i
    end
    x.names = nm
    return x
end

function rename!(x::Index, nms)
    for (from, to) in nms
        if haskey(x, from)
            if haskey(x, to)
                error("Tried renaming $from to $to, when $to already exists in the Index.")
            end
            x.lookup[to] = col = pop!(x.lookup, from)
            if !isa(col, Array)
                x.names[col] = to
            end
        end
    end
    return x
end

rename!(x::Index, from, to) = rename!(x, zip(from, to))
rename!(x::Index, from::String, to::String) = rename!(x, ((from, to),))
rename!(x::Index, f::Function) = rename!(x, [(x,f(x)) for x in x.names])

rename(x::Index, args...) = rename!(copy(x), args...)

Base.haskey(x::Index, key::String) = haskey(x.lookup, key)
Base.haskey(x::Index, key::Symbol) = haskey(x.lookup, string(key))
Base.haskey(x::Index, key::Real) = 1 <= key <= length(x.names)
Base.keys(x::Index) = names(x)

function Base.push!(x::Index, nm::String)
    x.lookup[nm] = length(x) + 1
    push!(x.names, nm)
    return x
end

function Base.delete!(x::Index, idx::Integer)
    # reset the lookup's beyond the deleted item
    for i in (idx + 1):length(x.names)
        x.lookup[x.names[i]] = i - 1
    end
    delete!(x.lookup, x.names[idx])
    splice!(x.names, idx)
    return x
end

function Base.delete!(x::Index, nm::String)
    if !haskey(x.lookup, nm)
        return x
    end
    idx = x.lookup[nm]
    return delete!(x, idx)
end

Base.getindex(x::Index, idx::String) = x.lookup[idx]
Base.getindex(x::Index, idx::Symbol) = x.lookup[string(idx)]
Base.getindex(x::AbstractIndex, idx::Real) = int(idx)
Base.getindex(x::AbstractIndex, idx::AbstractDataVector{Bool}) = getindex(x, array(idx, false))
Base.getindex{T}(x::AbstractIndex, idx::AbstractDataVector{T}) = getindex(x, dropna(idx))
Base.getindex(x::AbstractIndex, idx::AbstractVector{Bool}) = find(idx)
Base.getindex(x::AbstractIndex, idx::Ranges) = [idx]
Base.getindex{T <: Real}(x::AbstractIndex, idx::AbstractVector{T}) = convert(Vector{Int}, idx)
Base.getindex{T <: String}(x::AbstractIndex, idx::AbstractVector{T}) = [[x.lookup[i] for i in idx]...]
Base.getindex{T <: Symbol}(x::AbstractIndex, idx::AbstractVector{T}) = [[x.lookup[string(i)] for i in idx]...]

type SimpleIndex <: AbstractIndex
    length::Integer
end
SimpleIndex() = SimpleIndex(0)
Base.length(x::SimpleIndex) = x.length
Base.names(x::SimpleIndex) = nothing

