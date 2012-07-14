
# an AbstractIndex is a thing that can be used to look up ordered things by name, but that
# will also accept a position or set of positions or range or other things and pass them
# through cleanly.
# an Index is the usual implementation.
# a SimpleIndex only works if the things are integer indexes, which is weird.
abstract AbstractIndex

type Index <: AbstractIndex   # an OrderedDict would be nice here...
    lookup::Dict{ByteString,Int}      # name => names array position
    names::Vector{ByteString}
end
Index{T<:ByteString}(x::Vector{T}) = Index(Dict{ByteString, Int}(tuple(x...), tuple([1:length(x)]...)),
                                           convert(Vector{ByteString}, x))
Index() = Index(Dict{ByteString,Int}(), ByteString[])
length(x::Index) = length(x.names)
names(x::Index) = copy(x.names)
copy(x::Index) = Index(copy(x.lookup), copy(x.names))

function names!(x::Index, nm::Vector)
    if length(nm) != length(x)
        error("lengths don't match.")
    end
    x.names = nm
end

function replace_names!(x::Index, from::Vector, to::Vector)
    if length(from) != length(to)
        error("lengths of from and to don't match.")
    end
    for idx in 1:length(from)
        if has(x, from[idx]) && !has(x, to[idx])
            x.lookup[to[idx]] = x.lookup[from[idx]]
            x.names[x.lookup[from[idx]]] = to[idx]
            del(x.lookup, from[idx])
        end
    end
    x.names
end
replace_names!(x::Index, from, to) = replace_names!(x, [from], [to])
replace_names(x::Index, from, to) = replace_names!(copy(x), from, to)

has(x::Index, key) = has(x.lookup, key)
keys(x::Index) = names(x)
function push(x::Index, nm)
    x.lookup[nm] = length(x) + 1
    push(x.names, nm)
end
function del(x::Index, idx::Integer)
    # reset the lookup's beyond the deleted item
    for i in idx+1:length(x.names)
        x.lookup[x.names[i]] = i - 1
    end
    del(x.lookup, x.names[idx])
    del(x.names, idx)
end
function del(x::Index, nm)
    if !has(x.lookup, nm)
        return
    end
    idx = x.lookup[nm]
    del(x, idx)
end

ref{T<:ByteString}(x::Index, idx::Vector{T}) = convert(Vector{Int}, [x.lookup[i] for i in idx])
ref{T<:ByteString}(x::Index, idx::T) = x.lookup[idx]

# fall-throughs, when something other than the index type is passed
ref(x::AbstractIndex, idx::Int) = idx
ref(x::AbstractIndex, idx::Vector{Int}) = idx
ref(x::AbstractIndex, idx::Range1) = [idx]
ref(x::AbstractIndex, idx::Vector{Bool}) = [1:length(x)][idx]
ref(x::AbstractIndex, idx::AbstractDataVec{Bool}) = x[nareplace(idx, false)]
ref(x::AbstractIndex, idx::AbstractDataVec{Int}) = x[nafilter(idx)]

type SimpleIndex <: AbstractIndex
    length::Integer
end
SimpleIndex() = SimpleIndex(0)
length(x::SimpleIndex) = x.length
names(x::SimpleIndex) = nothing
