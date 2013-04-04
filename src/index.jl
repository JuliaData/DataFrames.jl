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
Index{T <: ByteString}(x::Vector{T}) =
    Index(Dict{ByteString, Indices}(tuple(x...), tuple([1:length(x)]...)),
          make_unique(convert(Vector{ByteString}, x)))
Index() = Index(Dict{ByteString, Indices}(), ByteString[])
length(x::Index) = length(x.names)
names(x::Index) = copy(x.names)
copy(x::Index) = Index(copy(x.lookup), copy(x.names))
deepcopy(x::Index) = Index(deepcopy(x.lookup), deepcopy(x.names))

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
        if has(x, from[idx]) && !has(x, to[idx])
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
rename(x::Index, from, to) = rename!(copy(x), from, to)

has(x::Index, key::String) = has(x.lookup, key)
has(x::Index, key::Symbol) = has(x.lookup, string(key))
has(x::Index, key::Real) = 1 <= key <= length(x.names)
keys(x::Index) = names(x)
function push!(x::Index, nm::String)
    x.lookup[nm] = length(x) + 1
    push!(x.names, nm)
end
function delete!(x::Index, idx::Integer)
    # reset the lookup's beyond the deleted item
    for i in (idx + 1):length(x.names)
        x.lookup[x.names[i]] = i - 1
    end
    gr = get_groups(x)
    delete!(x.lookup, x.names[idx])
    delete!(x.names, idx)
    # fix groups:
    for (k,v) in gr
        newv = [[has(x, vv) ? vv : ASCIIString[] for vv in v]...]
        set_group(x, k, newv)
    end
end
function delete!(x::Index, nm::String)
    if !has(x.lookup, nm)
        return
    end
    idx = x.lookup[nm]
    delete!(x, idx)
end

getindex(x::Index, idx::String) = x.lookup[idx]
getindex(x::Index, idx::Symbol) = x.lookup[string(idx)]
getindex(x::AbstractIndex, idx::Real) = int(idx)
getindex(x::AbstractIndex, idx::AbstractDataVector{Bool}) = getindex(x, replaceNA(idx, false))
getindex{T}(x::AbstractIndex, idx::AbstractDataVector{T}) = getindex(x, removeNA(idx))
getindex(x::AbstractIndex, idx::AbstractVector{Bool}) = find(idx)
getindex(x::AbstractIndex, idx::Ranges) = [idx]
getindex{T <: Real}(x::AbstractIndex, idx::AbstractVector{T}) = convert(Vector{Int}, idx)
getindex{T <: String}(x::AbstractIndex, idx::AbstractVector{T}) = [[x.lookup[i] for i in idx]...]
getindex{T <: Symbol}(x::AbstractIndex, idx::AbstractVector{T}) = [[x.lookup[string(i)] for i in idx]...]

type SimpleIndex <: AbstractIndex
    length::Integer
end
SimpleIndex() = SimpleIndex(0)
length(x::SimpleIndex) = x.length
names(x::SimpleIndex) = nothing

# Chris's idea of namespaces adapted by Harlan for column groups
function set_group(idx::Index, newgroup, names)
    if !has(idx, newgroup) || isa(idx.lookup[newgroup], Array)
        idx.lookup[newgroup] = [[idx.lookup[nm] for nm in names]...]
    end
end
function set_groups(idx::Index, gr::Dict{ByteString,Vector{ByteString}})
    for (k,v) in gr
        if !has(idx, k) 
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
  if has(idx, name)
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
