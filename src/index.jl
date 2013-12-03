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
names(x::Index) = copy(x.names)
Base.copy(x::Index) = Index(copy(x.lookup), copy(x.names))
Base.deepcopy(x::Index) = Index(deepcopy(x.lookup), deepcopy(x.names))

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
end

function rename!(x::Index, from::Vector, to::Vector)
    if length(from) != length(to)
        error("lengths of from and to don't match.")
    end
    for idx in 1:length(from)
        if haskey(x, from[idx]) && !haskey(x, to[idx])
            x.lookup[to[idx]] = x.lookup[from[idx]]
            if !isa(x.lookup[from[idx]], Array)
                x.names[x.lookup[from[idx]]] = to[idx]
            end
            delete!(x.lookup, from[idx])
        end
    end
    x.names
end
rename!(x::Index, from, to) = rename!(x, [from], [to])
rename!(x::Index, nd::Associative) = rename!(x, keys(nd), values(nd))
rename!(x::Index, f::Function) = (nms = names(x); rename!(x, nms, [f(x)::ByteString for x in nms]))

rename(x::Index, args...) = rename!(copy(x), args...)

Base.haskey(x::Index, key::String) = haskey(x.lookup, key)
Base.haskey(x::Index, key::Symbol) = haskey(x.lookup, string(key))
Base.haskey(x::Index, key::Real) = 1 <= key <= length(x.names)
Base.keys(x::Index) = names(x)
function Base.push!(x::Index, nm::String)
    x.lookup[nm] = length(x) + 1
    push!(x.names, nm)
end
function Base.delete!(x::Index, idx::Integer)
    # reset the lookup's beyond the deleted item
    for i in (idx + 1):length(x.names)
        x.lookup[x.names[i]] = i - 1
    end
    gr = get_groups(x)
    delete!(x.lookup, x.names[idx])
    splice!(x.names, idx)
    # fix groups:
    for (k,v) in gr
        newv = [[haskey(x, vv) ? vv : ASCIIString[] for vv in v]...]
        set_group(x, k, newv)
    end
end
function Base.delete!(x::Index, nm::String)
    if !haskey(x.lookup, nm)
        return
    end
    idx = x.lookup[nm]
    delete!(x, idx)
end

Base.getindex(x::Index, idx::String) = x.lookup[idx]
Base.getindex(x::Index, idx::Symbol) = x.lookup[string(idx)]
Base.getindex(x::AbstractIndex, idx::Real) = int(idx)
Base.getindex(x::AbstractIndex, idx::AbstractDataVector{Bool}) = getindex(x, array(idx, false))
Base.getindex{T}(x::AbstractIndex, idx::AbstractDataVector{T}) = getindex(x, removeNA(idx))
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
names(x::SimpleIndex) = nothing

# Chris's idea of namespaces adapted by Harlan for column groups
function set_group(idx::Index, newgroup, names)
    if !haskey(idx, newgroup) || isa(idx.lookup[newgroup], Array)
        idx.lookup[newgroup] = [[idx.lookup[nm] for nm in names]...]
    end
end
function set_groups(idx::Index, gr::Dict{ByteString,Vector{ByteString}})
    for (k,v) in gr
        if !haskey(idx, k) 
            idx.lookup[k] = [[idx.lookup[nm] for nm in v]...]
        end
    end
end
function get_groups(idx::Index)
    gr = Dict{ByteString,Vector{ByteString}}()
    for (k,v) in idx.lookup
        if isa(v,Array)
            gr[k] = idx.names[v]
        end
    end
    gr
end
function is_group(idx::Index, name::ByteString)
  if haskey(idx, name)
    return isa(idx.lookup[name], Array)
  else
    return false
  end
end

# special pretty-printer for groups, which are just Dicts.
function pretty_show(io::IO, gr::Dict{ByteString,Vector{ByteString}})
    allkeys = keys(gr)
    for k = allkeys
        print(io, "$(k): ")
        print(io, join(gr[k], ", "))
        if k != last(allkeys)
            print(io, "; ")
        end
    end
end
