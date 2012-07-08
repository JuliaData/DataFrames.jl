## definitions for "Data" types which can contain NAs
## Inspirations:
## R's NAs
## Panda's discussion of NAs: http://pandas.pydata.org/pandas-docs/stable/missing_data.html
## NumPy's analysis of the issue: https://github.com/numpy/numpy/blob/master/doc/neps/missing-data.rst

## Abstract type supports NAs and indexing

## Primary actual type is DataVec, which is a parameterized type that wraps an vector of a type and a (bit) array
## for the mask. 

## Secondary type is a PooledDataVec, which is a parameterized type that wraps a vector of UInts and a vector of
## the type, indexed by the main vector. NAs are 0s in the UInt vector. 

load("options.jl")

abstract AbstractDataVec{T}

type DataVec{T} <: AbstractDataVec{T}
    data::Vector{T}
    na::AbstractVector{Bool} # TODO use a bit array
    
    # TODO: these three should probably be a single type structure
    filter::Bool
    replace::Bool # replace supercedes filter, if both true
    replaceVal::T
    
    # sanity checks
    function DataVec{T}(d::Vector{T}, m::AbstractVector{Bool}, f::Bool, r::Bool, v::T) 
        if (length(d) != length(m))
            error("data and mask vectors not the same length!")
        elseif (f && r)
            error("please don't set both the filter and replace flags in a DataVec")
        end   
        new(d,m,f,r,v)
    end
end
# the usual partial constructor
DataVec{T}(d::Vector{T}, m::Vector{Bool}) = DataVec{T}(d, m, false, false, zero(T))
# a full constructor (why is this necessary?)
DataVec{T}(d::Vector{T}, m::AbstractVector{Bool}, f::Bool, r::Bool, v::T) = DataVec{T}(d, m, f, r, v)
# a no-op constructor
DataVec(d::DataVec) = d

type PooledDataVec{T} <: AbstractDataVec{T}
    refs::Vector{Uint16} # TODO: make sure we don't overflow
    pool::Vector{T}
    # TODO: ordering
    # TODO: meta-data for dummy conversion
    
    filter::Bool
    replace::Bool
    replaceVal::T
    
    function PooledDataVec{T}(refs::Vector{Uint16}, pool::Vector{T}, f::Bool, r::Bool, v::T)
        # refs mustn't overflow pool
        if (max(refs) > length(pool))
            error("reference vector points beyond the end of the pool!")
        elseif (f && r)
            error("please don't set both the filter and replace flags in a PooledDataVec")
        end   
        new(refs,pool,f,r,v)
    end
end
# a full constructor (why is this necessary?)
PooledDataVec{T}(re::Vector{Uint16}, p::Vector{T}, f::Bool, r::Bool, v::T) = PooledDataVec{T}(re, p, f, r, v)

# how do you construct one? well, from the same sigs as a DataVec!
PooledDataVec{T}(d::Vector{T}, m::Vector{Bool}) = PooledDataVec(d, m, false, false, zero(T))
function PooledDataVec{T}(d::Vector{T}, m::Vector{Bool}, f::Bool, r::Bool, v::T)  
    # algorithm... start with a null pool and a pre-allocated refs, plus hash from T to Int.
    # iterate over d. If in pool already, set the refs accordingly. If new, add to pool then set refs.
    newrefs = Array(Uint16, length(d))
    newpool = Array(T, 0)
    poolref = Dict{T,Uint16}(0)
    maxref = 0

    # loop through once to fill the poolref dict
    for i = 1:length(d)
        if !m[i]
            poolref[d[i]] = 0
        end
    end

    # fill positions in poolref
    newpool = sort(keys(poolref))
    i = 1
    for p in newpool 
        poolref[p] = i
        i += 1
    end

    # fill in newrefs
    for i = 1:length(d)
        if m[i]
            newrefs[i] = 0
        else
            newrefs[i] = poolref[d[i]]
        end
    end
    PooledDataVec(newrefs, newpool, f, r, v)
end
PooledDataVec(dv::DataVec) = PooledDataVec(dv.data, dv.na, dv.filter, dv.replace, dv.replaceVal)
PooledDataVec(d::PooledDataVec) = d

type NAtype; end
const NA = NAtype()
show(io, x::NAtype) = print(io, "NA")

type NAException <: Exception
    msg::String
end

length(x::NAtype) = 1
size(x::NAtype) = ()
isna(x::NAtype) = true
isna(x) = false

==(na::NAtype, na2::NAtype) = NA
==(na::NAtype, b) = NA
==(a, na::NAtype) = NA

# TODO: Move me to a more appropriate spot
# this allows zero(String) to work
oftype(::Type{ASCIIString},c) = repeat(" ",c)


# constructor from type
function _dv_most_generic_type(vals)
    # iterate over vals tuple to find the most generic non-NA type
    toptype = None
    for i = 1:length(vals)
        if !isna(vals[i])
            toptype = promote_type(toptype, typeof(vals[i]))
        end
    end
    # TODO: confirm that this type has a zero() 
    toptype
end
function ref(::Type{DataVec}, vals...)
    # first, get the most generic non-NA type
    toptype = _dv_most_generic_type(vals)
    
    # then, allocate vectors
    lenvals = length(vals)
    ret = DataVec(Array(toptype, lenvals), falses(lenvals))
    # copy from vals into data and mask
    for i = 1:lenvals
        if isna(vals[i])
            ret.data[i] = zero(toptype)
            ret.na[i] = true
        else
            ret.data[i] = vals[i]
            # ret.na[i] = false (default)
        end
    end
    
    return ret
end
function ref(::Type{PooledDataVec}, vals...)
    # for now, just create a DataVec and then convert it
    # TODO: rewrite for speed
    
    PooledDataVec(DataVec[vals...])
end

# constructor from base type object
DataVec(x::Vector) = DataVec(x, falses(length(x)))
PooledDataVec(x::Vector) = PooledDataVec(DataVec(x, falses(length(x))))

# copy does a deep copy
copy{T}(dv::DataVec{T}) = DataVec{T}(copy(dv.data), copy(dv.na), dv.filter, dv.replace, dv.replaceVal)
copy{T}(dv::PooledDataVec{T}) = PooledDataVec{T}(copy(dv.refs), copy(dv.pool), dv.filter, dv.replace, dv.replaceVal)

# TODO: copy_to


# properties
size(v::DataVec) = size(v.data)
size(v::PooledDataVec) = size(v.refs)
length(v::DataVec) = length(v.data)
length(v::PooledDataVec) = length(v.refs)
isna(v::DataVec) = v.na
isna(v::PooledDataVec) = v.refs .== 0
ndims(v::AbstractDataVec) = 1
numel(v::AbstractDataVec) = length(v)
eltype{T}(v::AbstractDataVec{T}) = T

# equality, respecting NAs
function =={T}(a::DataVec{T}, b::DataVec{T})
    if (length(a) != length(b))
        return false
    else
        for i = 1:length(a)
            if (a.na[i] != b.na[i])
                return false
            elseif (!a.na[i] && !b.na[i] && a.data[i] != b.data[i])
                return false
            end
        end
    end
    return true
end
function =={T}(a::AbstractDataVec{T}, b::AbstractDataVec{T})
    if (length(a) != length(b))
        return false
    else
        for i = 1:length(a)
            if isna(a[i]) != isna(b[i])
                return false
            elseif !isna(a[i]) && a[i] != b[i]
                return false
            end
        end
    end
    return true
end

# element-wise equality
function .=={T}(a::AbstractDataVec{T}, v::T)
    # allocate a DataVec for the return value, then assign into it
    ret = DataVec(Array(Bool,length(a)), Array(Bool,length(a)), false, false, false)
    for i = 1:length(a)
        ret[i] = isna(a[i]) ? NA : (a[i] == v)
    end
    ret
end
# TODO: fast version for PooledDataVec
# TODO: a::AbstractDataVec{T}, b::AbstractArray{T}
# TODO: a::AbstractDataVec{T}, b::AbstractDataVec{T}
# TODO: a::AbstractDataVec{T}, NA

# for arithmatic, NAs propogate
for f in (:+, :-, :.*, :div, :mod, :&, :|, :$)
    @eval begin
        function ($f){S,T}(A::DataVec{S}, B::DataVec{T})
            if (length(A) != length(B)) error("DataVec lengths must match"); end
            F = DataVec(Array(promote_type(S,T), length(A)), Array(Bool, length(A)))
            for i=1:length(A)
                F.na[i] = (A.na[i] || B.na[i])
                F.data[i] = ($f)(A.data[i], B.data[i])
            end
            return F
        end
        function ($f){T}(A::Number, B::DataVec{T})
            F = DataVec(Array(promote_type(typeof(A),T), length(B)), B.na)
            for i=1:length(B)
                F.data[i] = ($f)(A, B.data[i])
            end
            return F
        end
        function ($f){T}(A::DataVec{T}, B::Number)
            F = DataVec(Array(promote_type(typeof(B),T), length(A)), A.na)
            for i=1:length(A)
                F.data[i] = ($f)(A.data[i], B)
            end
            return F
        end
    end
end

# single-element access
ref(x::DataVec, i::Number) = x.na[i] ? NA : x.data[i]
ref(x::PooledDataVec, i::Number) = x.refs[i] == 0 ? NA : x.pool[x.refs[i]]

# range access
function ref(x::DataVec, r::Range1)
    DataVec(x.data[r], x.na[r], x.filter, x.replace, x.replaceVal)
end
# PooledDataVec -- be sure copy the pool!
function ref(x::PooledDataVec, r::Range1)
    # TODO: copy the whole pool or just the items in the range?
    # for now, the whole pool
    PooledDataVec(x.refs[r], copy(x.pool), x.filter, x.replace, x.replaceVal)
end

# logical access -- note that unlike Array logical access, this throws an error if
# the index vector is not the same size as the data vector
function ref(x::DataVec, ind::Vector{Bool})
    if length(x) != length(ind)
        throw(ArgumentError("boolean index is not the same size as the DataVec"))
    end
    DataVec(x.data[ind], x.na[ind], x.filter, x.replace, x.replaceVal)
end
# PooledDataVec
function ref(x::PooledDataVec, ind::Vector{Bool})
    if length(x) != length(ind)
        throw(ArgumentError("boolean index is not the same size as the PooledDataVec"))
    end
    PooledDataVec(x.refs[ind], copy(x.pool), x.filter, x.replace, x.replaceVal)
end

# array index access
function ref(x::DataVec, ind::Vector{Int})
    DataVec(x.data[ind], x.na[ind], x.filter, x.replace, x.replaceVal)
end
# PooledDataVec
function ref(x::PooledDataVec, ind::Vector{Int})
    PooledDataVec(x.refs[ind], copy(x.pool), x.filter, x.replace, x.replaceVal)
end

ref(x::AbstractDataVec, ind::AbstractDataVec{Bool}) = x[nareplace(ind, false)]
ref(x::AbstractDataVec, ind::AbstractDataVec{Integer}) = x[nafilter(ind)]

# assign variants
# x[3] = "cat"
function assign{T}(x::DataVec{T}, v::T, i::Int)
    x.data[i] = v
    x.na[i] = false
    return x[i]
end
function assign{T}(x::PooledDataVec{T}, v::T, i::Int)
    # TODO handle pool ordering
    # note: NA replacement comes for free here
    
    # find the index of v in the pool
    pool_idx = findfirst(x.pool, v)
    if pool_idx > 0
        # new item is in the pool
        x.refs[i] = pool_idx
    else
        # new item is not in the pool; add it
        push(x.pool, v)
        x.refs[i] = length(x.pool)
    end
    return x[i]
end

# x[[3, 4]] = "cat"
function assign{T}(x::DataVec{T}, v::T, is::Vector{Int})
    x.data[is] = v
    x.na[is] = false
    return x[is] # this could get slow -- maybe not...
end
# PooledDataVec can use a possibly-slower generic approach
function assign{T}(x::AbstractDataVec{T}, v::T, is::Vector{Int})
    for i in is
        x[i] = v
    end
    return x[is]
end

# x[[3, 4]] = ["cat", "dog"]
function assign{T}(x::DataVec{T}, vs::Vector{T}, is::Vector{Int})
    if length(is) != length(vs)
        throw(ArgumentError("can't assign when index and data vectors are different length"))
    end
    x.data[is] = vs
    x.na[is] = false
    return x[is]
end
# PooledDataVec can use a possibly-slower generic approach
function assign{T}(x::AbstractDataVec{T}, vs::Vector{T}, is::Vector{Int})
    if length(is) != length(vs)
        throw(ArgumentError("can't assign when index and data vectors are different length"))
    end
    for vi in zip(vs, is)
        x[vi[2]] = vi[1]
    end
    return x[is]
end

# x[[true, false, true]] = "cat"
function assign{T}(x::DataVec{T}, v::T, mask::Vector{Bool})
    x.data[mask] = v
    x.na[mask] = false
    return x[mask]
end
# PooledDataVec can use a possibly-slower generic approach
function assign{T}(x::AbstractDataVec{T}, v::T, mask::Vector{Bool})
    for i = 1:length(x)
        if mask[i] == true
            x[i] = v
        end
    end
    return x[mask]
end

# x[[true, false, true]] = ["cat", "dog"]
function assign{T}(x::DataVec{T}, vs::Vector{T}, mask::Vector{Bool})
    if sum(mask) != length(vs)
        throw(ArgumentError("can't assign when boolean trues and data vectors are different length"))
    end
    x.data[mask] = vs
    x.na[mask] = false
    return x[mask]
end
# PooledDataVec can use a possibly-slower generic approach
function assign{T}(x::AbstractDataVec{T}, vs::Vector{T}, mask::Vector{Bool})
    if sum(mask) != length(vs)
        throw(ArgumentError("can't assign when boolean trues and data vectors are different length"))
    end
    ivs = 1
    # walk through mask. whenever true, assign and increment vs index
    for i = 1:length(mask)
        if mask[i] == true
            x[i] = vs[ivs]
            ivs += 1
        end
    end
    return x[mask]
end

# x[2:3] = "cat"
function assign{T}(x::DataVec{T}, v::T, rng::Range1)
    x.data[rng] = v
    x.na[rng] = false
    return x[rng]
end
# PooledDataVec can use a possibly-slower generic approach
function assign{T}(x::AbstractDataVec{T}, v::T, rng::Range1)
    for i in rng
        x[i] = v
    end
end


# x[2:3] = ["cat", "dog"]
function assign{T}(x::DataVec{T}, vs::Vector{T}, rng::Range1)
    if length(rng) != length(vs)
        throw(ArgumentError("can't assign when index and data vectors are different length"))
    end
    x.data[rng] = vs
    x.na[rng] = false
    return x[rng]
end
# PooledDataVec can use a possibly-slower generic approach
function assign{T}(x::AbstractDataVec{T}, vs::Vector{T}, rng::Range1)
    if length(rng) != length(vs)
        throw(ArgumentError("can't assign when index and data vectors are different length"))
    end
    ivs = 1
    # walk through rng. assign and increment vs index
    for i in rng
        x[i] = vs[ivs]
        ivs += 1
    end
    return x[rng]
end

# x[3] = NA
assign{T}(x::DataVec{T}, n::NAtype, i::Int) = begin (x.na[i] = true); return NA; end
assign{T}(x::PooledDataVec{T}, n::NAtype, i::Int) = begin (x.refs[i] = 0); return NA; end

# x[[3,5]] = NA
assign{T}(x::DataVec{T}, n::NAtype, is::Vector{Int}) = begin (x.na[is] = true); return x[is]; end
assign{T}(x::PooledDataVec{T}, n::NAtype, is::Vector{Int}) = begin (x.refs[is] = 0); return x[is]; end

# x[[true, false, true]] = NA
assign{T}(x::DataVec{T}, n::NAtype, mask::Vector{Bool}) = begin (x.na[mask] = true); return x[mask]; end
assign{T}(x::PooledDataVec{T}, n::NAtype, mask::Vector{Bool}) = begin (x.refs[mask] = 0); return x[mask]; end

# x[2:3] = NA
assign{T}(x::DataVec{T}, n::NAtype, rng::Range1) = begin (x.na[rng] = true); return x[rng]; end
assign{T}(x::PooledDataVec{T}, n::NAtype, rng::Range1) = begin (x.refs[rng] = 0); return x[rng]; end

# TODO: Abstract assignment of a union of T's and NAs
# x[3:5] = {"cat", NA, "dog"}
# x[3:5] = DataVec["cat", NA, "dog"]

# TODO: replace!(x::PooledDataVec{T}, from::T, to::T)
# and similar to and from NA
function replace!{T}(x::PooledDataVec{T}, fromval::T, toval::T)
    # throw error if fromval isn't in the pool
    fromidx = findfirst(x.pool, fromval)
    if fromidx == 0
        error("can't replace a value not in the pool in a PooledDataVec!")
    end
        
    # if toval is in the pool too, use that and remove fromval from the pool
    toidx = findfirst(x.pool, toval)
    if toidx != 0
        x.refs[x.refs .== fromidx] = toidx
        #x.pool[fromidx] = None    TODO: what to do here??
    else
        # otherwise, toval is new, swap it in
        x.pool[fromidx] = toval
    end
    
    return toval
end
replace!(x::PooledDataVec{NAtype}, fromval::NAtype, toval::NAtype) = NA # no-op to deal with warning
function replace!{T}(x::PooledDataVec{T}, fromval::T, toval::NAtype)
    fromidx = findfirst(x.pool, fromval)
    if fromidx == 0
        error("can't replace a value not in the pool in a PooledDataVec!")
    end
    
    x.refs[x.refs .== fromidx] = 0
    
    return NA
end
function replace!{T}(x::PooledDataVec{T}, fromval::NAtype, toval::T)
    toidx = findfirst(x.pool, toval)
    # if toval is in the pool, just do the assignment
    if toidx != 0
        x.refs[x.refs .== 0] = toidx
    else
        # otherwise, toval is new, add it to the pool
        push(x.pool, toval)
        x.refs[x.refs .== 0] = length(x.pool)
    end
    
    return toval
end

# things to deal with unwanted NAs -- lower case returns the base type, with overhead,
# mixed case returns an iterator
nafilter{T}(v::DataVec{T}) = v.data[!v.na]
nareplace{T}(v::DataVec{T}, r::T) = [v.na[i] ? r : v.data[i] for i = 1:length(v.data)]
nafilter{T}(v::DataVec{T}) = v.data[!v.na]
nafilter(v::DataVec) = PooledDataVec(v.refs[map(isna, v)], v.pool, v.filter, v.replace, v.replaceVal)
# TODO PooledDataVec
# TODO nareplace! does in-place change; nafilter! shouldn't exist, as it doesn't apply with DataFrames

# naFilter redefines a new DataVec with a flipped bit that determines how start/next/done operate
naFilter{T}(v::DataVec{T}) = DataVec(v.data, v.na, true, false, v.replaceVal)
naFilter{T}(v::PooledDataVec{T}) = PooledDataVec(v.refs, v.pool, true, false, v.replaceVal)

# naReplace is similar to naFilter, but with a replacement value
naReplace{T}(v::DataVec{T}, rv::T) = DataVec(v.data, v.na, false, true, rv)
naReplace{T}(v::PooledDataVec{T}, rv::T) = PooledDataVec(v.refs, v.pool, false, true, rv)

# If neither the filter or replace flags are set, the iterator will return an NA
# when it hits an NA. If one or the other are set, it'll skip/replace the NA.
# Pooled and not share an implementation.
start(x::AbstractDataVec) = 1
function next(x::AbstractDataVec, state::Int)
    # if filter is set, iterate til we find a non-NA value
    if x.filter
        for i = state:length(x)
            if !isna(x[i])
                return (x[i], i+1)
            end
        end
        error("called next(AbstractDataVec) without calling done() first")
    elseif x.replace
        return (isna(x[state]) ? x.replaceVal : x[state], state + 1)
    else
        return (x[state], state+1)
    end
end
function done(x::AbstractDataVec, state::Int)
    # if filter is set, iterate til we find a non-NA value
    if x.filter
        for i = state:length(x)
            if !isna(x[i])
                return false
            end
        end
        return true
    else # just check lengths
        return state > length(x)
    end
end

# can promote in theory based on data type
promote_rule{S,T}(::Type{AbstractDataVec{S}}, ::Type{T}) = promote_rule(S, T)
promote_rule{T}(::Type{AbstractDataVec{T}}, ::Type{T}) = T
# TODO: Abstract? Pooled?

# convert to the base type, but only if there are no NAs
function convert{T}(::Type{T}, x::DataVec{T})
    if (any(x.na))
        throw(NAException("Cannot convert DataVec with NAs to base type -- naFilter or naReplace them first"))
    else
        return x.data
    end
end
function convert{T}(::Type{T}, x::AbstractDataVec{T})
    try
        [i::T for i in x]
    catch ee
        if isa(ee, TypeError)
            throw(NAException("Cannot convert AbstractDataVec with NAs to base type -- naFilter or naReplace them first"))
        else
            throw(ee)
        end
    end
end

# a DataVec is not a StridedArray, so sum() and similar picks up the itr version, which will work without 
# additional code

# print
show(io, x::AbstractDataVec) = show_comma_array(io, x, '[', ']') 

# TODO: vectorizable math functions like sqrt, sin, trunc, etc., which should return a DataVec{T}
# not sure if this is the best approach, but works for a demo
function log{T}(x::DataVec{T})
    newx = log(x.data)
    DataVec(newx, x.na, x.filter, x.replace, convert(eltype(newx), x.replaceVal))
end

# TODO: vectorizable comparison operators like > which should return a DataVec{Bool}

# TODO: div(dat, 2) works, but zz ./ 2 doesn't

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

# A NamedArray is like a list in R or a DataFrame in Julia without the
# requirement that columns be of equal length. The main reason for its
# existence is to allow creation of a DataFrame from unequal column
# lengths like the following:
#   DataFrame(quote
#       a = 1
#       b = [1:5]
#       c = [1:10]
#   end)
type NamedArray <: Associative{Any,Any}
    data::Vector{Any} 
    idx::AbstractIndex
    function NamedArray(data::Vector, idx::AbstractIndex)
        if length(idx) != length(data)
            error("index/names must be the same length as the data")
        end
        new(data, idx)
    end
end
NamedArray() = NamedArray({}, Index())

length(x::NamedArray) = length(x.idx)
names(x::NamedArray) = names(x.idx)

ref(x::NamedArray, c) = x[x.idx[c]]
ref(x::NamedArray, c::Integer) = x.data[c]
ref(x::NamedArray, c::Vector{Int}) = NamedArray(x.data[c], names(x)[c])

function assign(x::NamedArray, newdata, ipos::Integer)
    if ipos > 0 && ipos <= length(x)
        x.data[ipos] = newdata
    else
        throw(ArgumentError("Can't replace a non-existent array position"))
    end
    x
end
function assign(x::NamedArray, newdata, name)
    ipos = get(x.idx.lookup, name, 0)
    if ipos > 0
        # existing
        assign(x, newdata, ipos)
    else
        # new
        push(x.idx, name)
        push(x.data, newdata)
    end
    x
end


# Associative methods:
has(x::NamedArray, key) = has(x.idx, key)
get(x::NamedArray, key, default) = has(x, key) ? x[key] : default
keys(x::NamedArray) = keys(x.idx)
values(x::NamedArray) = x.data
# Collection methods:
start(x::NamedArray) = 1
done(x::NamedArray, i) = i > length(x.data)
next(x::NamedArray, i) = ((x.idx.names[i], x[i]), i + 1)
numel(x::NamedArray) = length(x.data)
isempty(x::NamedArray) = length(x.data) == 0

# Abstract DF includes DataFrame and SubDataFrame
abstract AbstractDataFrame <: Associative{Any,Any}

# ## DataFrame - a list of heterogeneous Data vectors with col and row indexs.
# Columns are a vector, which means that operations that insert/delete columns
# are O(n).
type DataFrame <: AbstractDataFrame
    columns::Vector{Any} 
    colindex::Index
    function DataFrame(cols::Vector, colindex::Index)
        # all columns have to be the same length
        if length(cols) > 1 && !all(map(length, cols) .== length(cols[1]))
            error("all columns in a DataFrame have to be the same length")
        end
        # colindex has to be the same length as columns vector
        if length(colindex) != length(cols)
            error("column names/index must be the same length as the number of columns")
        end
        new(cols, colindex)
    end
end

# constructors
DataFrame(cs::Vector) = DataFrame(cs, paste("x", map(string,[1:length(cs)])))
DataFrame(cs::Vector, cn::Vector) = DataFrame(cs, Index(cn))
# TODO expand the following to allow unequal lengths that are rep'd to the longest length.
DataFrame(ex::Expr) = based_on(DataFrame(), ex)
DataFrame{T}(x::Array{T,2}, cn::Vector) = DataFrame({x[:,i] for i in 1:length(cn)}, cn)
DataFrame{T}(x::Array{T,2}) = DataFrame(x, [strcat("x", i) for i in 1:size(x,2)])


colnames(df::DataFrame) = names(df.colindex)
names!(df::DataFrame, vals) = names!(df.colindex, vals)
colnames!(df::DataFrame, vals) = names!(df.colindex, vals)
replace_names!(df::DataFrame, from, to) = replace_names!(df.colindex, from, to)
replace_names(df::DataFrame, from, to) = replace_names(df.colindex, from, to)
ncol(df::DataFrame) = length(df.colindex)
nrow(df::DataFrame) = ncol(df) > 0 ? length(df.columns[1]) : 0
names(df::AbstractDataFrame) = colnames(df)
size(df::AbstractDataFrame) = (nrow(df), ncol(df))
size(df::AbstractDataFrame, i::Integer) = i==1 ? nrow(df) : (i==2 ? ncol(df) : error("DataFrames have two dimensions only"))
length(df::AbstractDataFrame) = ncol(df)

ref(df::DataFrame, c) = df[df.colindex[c]]
ref(df::DataFrame, c::Integer) = df.columns[c]
ref(df::DataFrame, c::Vector{Int}) = DataFrame(df.columns[c], convert(Vector{ByteString}, colnames(df)[c]))

ref(df::DataFrame, r, c) = df[r, df.colindex[c]]
ref(df::DataFrame, r, c::Int) = df[c][r]
ref(df::DataFrame, r, c::Vector{Int}) =
    DataFrame({x[r] for x in df.columns[c]}, 
              convert(Vector{ByteString}, colnames(df)[c]))

# special cases
ref(df::DataFrame, r::Int, c::Int) = df[c][r]
ref(df::DataFrame, r::Int, c::Vector{Int}) = df[[r], c]
ref(df::DataFrame, r::Int, c) = df[r, df.colindex[c]]
ref(df::DataFrame, dv::AbstractDataVec) = df[with(df, ex), c]
ref(df::DataFrame, ex::Expr) = df[with(df, ex), :]  
ref(df::DataFrame, ex::Expr, c::Int) = df[with(df, ex), c]
ref(df::DataFrame, ex::Expr, c::Vector{Int}) = df[with(df, ex), c]
ref(df::DataFrame, ex::Expr, c) = df[with(df, ex), c]



# Associative methods:
has(df::DataFrame, key) = has(df.colindex, key)
get(df::DataFrame, key, default) = has(df, key) ? df[key] : default
keys(df::DataFrame) = keys(df.colindex)
values(df::DataFrame) = df.columns
del_all(df::DataFrame) = DataFrame()
# Collection methods:
start(df::AbstractDataFrame) = 1
done(df::AbstractDataFrame, i) = i > ncol(df)
next(df::AbstractDataFrame, i) = (df[i], i + 1)
numel(df::AbstractDataFrame) = ncol(df)
isempty(df::AbstractDataFrame) = ncol(df) == 0

function insert(df::DataFrame, index::Integer, item, name)
    @assert 0 < index <= ncol(df) + 1
    df = shallowcopy(df)
    df[name] = item
    # rearrange:
    df[[1:index-1, end, index:end-1]]
end

# if we have something else, convert each value in this tuple to a DataVec and pass it in, hoping for the best
DataFrame(vals...) = DataFrame([DataVec(x) for x = vals])
# if we have a matrix, create a tuple of columns and pass that in
DataFrame{T}(m::Array{T,2}) = DataFrame([DataVec(squeeze(m[:,i])) for i = 1:size(m)[2]])
# 

function DataFrame{K,V}(d::Associative{K,V})
    # Find the first position with maximum length in the Dict.
    # I couldn't get findmax to work here.
    ## (Nrow,maxpos) = findmax(map(length, values(d)))
    lengths = map(length, values(d))
    maxpos = find(lengths .== max(lengths))[1]
    keymaxlen = keys(d)[maxpos]
    Nrow = length(d[keymaxlen])
    # Start with a blank DataFrame
    df = DataFrame() 
    for (k,v) in d
        if length(v) == Nrow
            df[k] = v  
        elseif rem(Nrow, length(v)) == 0    # Nrow is a multiple of length(v)
            df[k] = vcat(fill(v, div(Nrow, length(v)))...)
        else
            println("Warning: Column $(string(k)) ignored: mismatched column lengths")
        end
    end
    df
end

# Blank DataFrame
DataFrame() = DataFrame({}, Index())

# copy of a data frame does a deep copy
copy(df::DataFrame) = DataFrame([copy(x) for x in df.columns], colnames(df))
shallowcopy(df::DataFrame) = DataFrame(df.columns, colnames(df))

# dimilar of a data frame creates new vectors, but with the same columns. Dangerous, as 
# changing the in one df can break the other.

# # TODO: move
# function idxFirstEqual{T}(x::Vector{T}, y::T)
#     for i = 1:length(x)
#         if x[i] == y
#             return i
#         end
#     end
#     return nothing
# end

# Equality
function ==(df1::AbstractDataFrame, df2::AbstractDataFrame)
    if ncol(df1) != ncol(df2)
        return false
    end
    for idx in 1:ncol(df1)
        if !(df1[idx] == df2[idx])
            return false
        end
    end
    return true
end

head(df::DataFrame, r::Int) = df[1:r, :]
head(df::DataFrame) = head(df, 6)
tail(df::DataFrame, r::Int) = df[(nrow(df)-r+1):nrow(df), :]
tail(df::DataFrame) = tail(df, 6)



# to print a DataFrame, find the max string length of each column
# then print the column names with an appropriate buffer
# then row-by-row print with an appropriate buffer
_string(x) = sprint(showcompact, x)
maxShowLength(v::Vector) = length(v) > 0 ? max([length(_string(x)) for x = v]) : 0
maxShowLength(dv::AbstractDataVec) = max([length(_string(x)) for x = dv])
function show(io, df::AbstractDataFrame)
    ## TODO use alignment() like print_matrix in show.jl.
    println(io, "$(typeof(df))  $(size(df))")
    N = nrow(df)
    Nmx = 20   # maximum head and tail lengths
    if N <= 2Nmx
        rowrng = 1:min(2Nmx,N)
    else
        rowrng = [1:Nmx, N-Nmx+1:N]
    end
    # we don't have row names -- use indexes
    rowNames = [sprintf("[%d,]", r) for r = rowrng]
    
    rownameWidth = maxShowLength(rowNames)
    
    # if we don't have columns names, use indexes
    # note that column names in R are obligatory
    if eltype(colnames(df)) == Nothing
        colNames = [sprintf("[,%d]", c) for c = 1:ncol(df)]
    else
        colNames = colnames(df)
    end
    
    colWidths = [max(length(string(colNames[c])), maxShowLength(df[rowrng,c])) for c = 1:ncol(df)]

    header = strcat(" " ^ (rownameWidth+1),
                    join([lpad(string(colNames[i]), colWidths[i]+1, " ") for i = 1:ncol(df)], ""))
    println(io, header)

    for i = 1:length(rowrng)
        rowname = rpad(string(rowNames[i]), rownameWidth+1, " ")
        line = strcat(rowname,
                      join([lpad(_string(df[rowrng[i],c]), colWidths[c]+1, " ") for c = 1:ncol(df)], ""))
        println(io, line)
        if i == Nmx && N > 2Nmx
            println(io, "  :")
        end
    end
end

# get the structure of a DF
# TODO: return a string or something instead of printing?
# TODO: AbstractDataFrame
str(df::DataFrame) = str(OUTPUT_STREAM::IOStream, df)
function str(io, df::DataFrame)
    println(io, sprintf("%d observations of %d variables", nrow(df), ncol(df)))

    if eltype(colnames(df)) == Nothing
        colNames = [sprintf("[,%d]", c) for c = 1:ncol(df)]
    else
        colNames = colnames(df)
    end
    
    # foreach column, print the column name or index, the type, and then print the first elements of 
    # the column until the total column width would exceed a constant
    maxPrintedWidth = 60
    for c in 1:ncol(df)
        printedWidth = 0
        ispooled = isa(df[c], PooledDataVec) ? "Pooled " : ""
        colstr = strcat(string(colNames[c]), " [", ispooled, string(eltype(df[c])), "] ")
        print(io, colstr)
        printedWidth += length(colstr)
        
        for r in 1:nrow(df)
            elemstr = strcat(string(df[r,c]), " ")
            if printedWidth + length(elemstr) > maxPrintedWidth
                print(io, "...")
                break
            end
            print(io, elemstr)
            printedWidth += length(elemstr)
        end
        println(io)
    end
end

function dump(io::IOStream, x::AbstractDataFrame, n::Int, indent)
    println(io, typeof(x), "  $(nrow(x)) observations of $(ncol(x)) variables")
    if n > 0
        for col in names(x)[1:min(10,end)]
            print(io, indent, "  ", col, ": ")
            dump(io, x[col], n - 1, strcat(indent, "  "))
        end
    end
end
dump(io::IOStream, x::AbstractDataVec, n::Int, indent) =
    println(io, typeof(x), "(", length(x), ") ", x[1:min(4, end)])

# summarize the columns of a DF
# if the column's base type derives from Number, 
# compute min, 1st quantile, median, mean, 3rd quantile, and max
# filtering NAs, which are reported separately
# if boolean, report trues, falses, and NAs
# if anything else, punt.
# Note that R creates a summary object, which has a print method. That's
# a reasonable alternative to this. The summary() functions in show.jl
# return a string.
summary(dv::AbstractDataVec) = summary(OUTPUT_STREAM::IOStream, dv)
summary(df::DataFrame) = summary(OUTPUT_STREAM::IOStream, df)
function summary{T<:Number}(io, dv::AbstractDataVec{T})
    filtered = nafilter(dv)
    qs = quantile(filtered, [0, .25, .5, .75, 1])
    statNames = ["Min", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max"]
    statVals = [qs[1:3], mean(filtered), qs[4:5]]
    for i = 1:6
        println(io, strcat(rpad(statNames[i], 8, " "), " ", string(statVals[i])))
    end
    nas = sum(isna(dv))
    if nas > 0
        println(io, "NAs      $nas")
    end
end
# function summary{T<:Bool}(dv::DataVec{T})
#     error("TODO")
# end
function summary{T}(io, dv::AbstractDataVec{T})
    ispooled = isa(dv, PooledDataVec) ? "Pooled " : ""
    # if nothing else, just give the length and element type and NA count
    println(io, "Length: $(length(dv))")
    println(io, "Type  : $(ispooled)$(string(eltype(dv)))")
    println(io, "NAs   : $(sum(isna(dv)))")
end

# TODO: clever layout in rows
# TODO: AbstractDataFrame
function summary(io, df::AbstractDataFrame)
    for c in 1:ncol(df)
        col = df[c]
        println(io, colnames(df)[c])
        summary(io, col)
        println(io, )
    end
end





# for now, use csvread to pull CSV data from disk
function _remove_quotes(s)
    m = match(r"^\"(.*)\"$", s)
    if m != nothing
        return m.captures[1]::ASCIIString
    else
        return s::ASCIIString
    end
end

function _same_set(a, b)
    # there are definitely MUCH faster ways of doing this
    length(a) == length(b) && all(sort(a) == sort(b))
end

# colnames = "true", "false", "check" (default)
# poolstrings = "check" (default), "never" 
function csvDataFrame(filename, o::Options)
    @defaults o colnames="check" poolstrings="check"
    # TODO
    # for now, use the built-in csvread that creates a matrix of Anys, functionally numbers and strings. 
    # Ideally, we'd probably save RAM by doing a two-pass read over the file, once to determine types and
    # once to build the data structures.
    dat = csvread(filename)
    
    # if the first row looks like strings, chop it off and process it as the 
    # column names
    if colnames == "check"
        colnames = all([typeof(x)==ASCIIString for x = dat[1,:]]) ? "true" : "false"
    end
    if colnames == "true"
        columnNames = [_remove_quotes(x) for x = dat[1,:]]
        dat = dat[2:,:]
    else
        # null column names
        columnNames = []
    end
    
    # foreach column, if everything is either numeric or empty string, then build a numeric DataVec
    # otherwise build a string DataVec
    cols = Array(AbstractDataVec, size(dat,2)) # elements will be #undef initially
    for c = 1:size(dat,2)
        nas = [(x == "")::Bool for x = dat[:,c]]
        # iterate over the column, ignoring null strings, promoting through numeric types as we go, until we're done
        # simultaneously, collect a hash of up to 64K defined elements (which might look like
        # numbers, in case we have a heterogeneous column)
        # never short-circuit, as we need the full list of keys to test for booleans
        colType = None
        colIsNum = true
        colStrings = Dict{String,Bool}(0)
        colPoolStrings = poolstrings == "check" 
        for r = 1:size(dat,1)
            v = dat[r,c]
            if v != ""
                if isa(v,Number) && colIsNum
                    colType = promote_type(colType, typeof(v))
                else
                    colIsNum = false
                end
                # store that we saw this string
                colStrings[string(v)] = true # do we need a count here?
            end
        end
        if colPoolStrings && length(keys(colStrings)) > typemax(Uint16)
            # we've ran past the limit of pooled strings!
            colPoolStrings = false
        end
        
        # build DataVecs
        if _same_set(keys(colStrings), ["0", "1"])
            # boolean
            cols[c] = DataVec(dat[:,c] == "1", nas)
        elseif (colIsNum && colType != None)
            # this is annoying to have to pre-allocate the array, but comprehensions don't
            # seem to get the type right
            tmpcol = Array(colType, size(dat,1))
            for r = 1:length(tmpcol)
                tmpcol[r] = dat[r,c] == "" ? false : dat[r,c] # false is the smallest numeric 0
            end
            cols[c] = DataVec(tmpcol, nas)
        elseif _same_set(keys(colStrings), ["TRUE", "FALSE"])
            # boolean 
            cols[c] = DataVec(dat[:,c] == "TRUE", nas)
        elseif colPoolStrings
            # TODO: if we're trying to pool, build the underlying refs and pool as we check, rather
            # than throwing away eveything and starting over! we've got a perfectly nice constructor...
            cols[c] = PooledDataVec([string(_remove_quotes(x))::ASCIIString for x = dat[:,c]], nas)
        else
            cols[c] = DataVec([string(_remove_quotes(x))::ASCIIString for x = dat[:,c]], nas)
        end
    end
    
    @check_used o
    
    # combine the columns into a DataFrame and return
    DataFrame(cols, columnNames)
end
csvDataFrame(filename) = csvDataFrame(filename, Options())


# a SubDataFrame is a lightweight wrapper around a DataFrame used most frequently in
# split/apply sorts of operations.
type SubDataFrame <: AbstractDataFrame
    parent::DataFrame
    rows::Vector{Int} # maps from subdf row indexes to parent row indexes
    
    # TODO: constructor to check params
end

sub(D::DataFrame, r, c) = sub(D[[c]], r)    # If columns are given, pass in a subsetted parent D.
                                            # Columns are not copies, so it's not expensive.
sub(D::DataFrame, r::Int) = sub(D, [r])
sub(D::DataFrame, rs::Vector{Int}) = SubDataFrame(D, rs)
sub(D::DataFrame, r) = sub(D, ref(SimpleIndex(nrow(D)), r)) # this is a wacky fall-through that uses light-weight fake indexes!
sub(D::DataFrame, ex::Expr) = sub(D, with(D, ex))

sub(D::SubDataFrame, r, c) = sub(D[[c]], r)
sub(D::SubDataFrame, r::Int) = sub(D, [r])
sub(D::SubDataFrame, rs::Vector{Int}) = SubDataFrame(D.parent, D.rows[rs])
sub(D::SubDataFrame, r) = sub(D, ref(SimpleIndex(nrow(D)), r)) # another wacky fall-through
sub(D::SubDataFrame, ex::Expr) = sub(D, with(D, ex))

ref(df::SubDataFrame, c) = df.parent[df.rows, c]
ref(df::SubDataFrame, r, c) = df.parent[df.rows[r], c]

nrow(df::SubDataFrame) = length(df.rows)
ncol(df::SubDataFrame) = ncol(df.parent)
colnames(df::SubDataFrame) = colnames(df.parent) 

head(df::AbstractDataFrame, r::Int) = df[1:min(r,nrow(df)), :]
head(df::AbstractDataFrame) = head(df, 6)
tail(df::AbstractDataFrame, r::Int) = df[max(1,nrow(df)-r+1):nrow(df), :]
tail(df::AbstractDataFrame) = tail(df, 6)

# Associative methods:
has(df::SubDataFrame, key) = has(df.colindex, key)
get(df::SubDataFrame, key, default) = has(df, key) ? df[key] : default
keys(df::SubDataFrame) = keys(df.colindex)
values(df::SubDataFrame, key) = keys(df.colindex)
del_all(df::SubDataFrame) = DataFrame()



# DF column operations
######################

# assignments return the complete object...

# df[1] = replace column
function assign(df::DataFrame, newcol::AbstractDataVec, icol::Integer)
    if icol > 0 && icol <= ncol(df)
        df.columns[icol] = newcol
    else
        throw(ArgumentError("Can't replace a non-existent DataFrame column"))
    end
    df
end
assign{T}(df::DataFrame, newcol::Vector{T}, icol::Integer) = assign(df, DataVec(newcol), icol)

# df["old"] = replace old columns
# df["new"] = append new column
function assign(df::DataFrame, newcol::AbstractDataVec, colname)
    icol = get(df.colindex.lookup, colname, 0)
    if length(newcol) != nrow(df) && nrow(df) != 0
        error("length of data doesn't match the number of rows.")
    end
    if icol > 0
        # existing
        assign(df, newcol, icol)
    else
        # new
        push(df.colindex, colname)
        push(df.columns, newcol)
    end
    df
end
assign{T}(df::DataFrame, newcol::Vector{T}, colname) = assign(df, DataVec(newcol), colname)

assign(df::DataFrame, newcol, colname) =
    nrow(df) > 0 ? assign(df, DataVec(fill(newcol, nrow(df))), colname) : assign(df, DataVec([newcol]), colname)

# do I care about vectorized assignment? maybe not...
# df[1:3] = (replace columns) eh...
# df[["new", "newer"]] = (new columns)

# df[1] = nothing
assign(df::DataFrame, x::Nothing, icol::Integer) = del!(df, icol)

# del!(df, 1)
# del!(df, "old")
function del!(df::DataFrame, icols::Vector{Int})
    for icol in icols 
        if icol > 0 && icol <= ncol(df)
            del(df.columns, icol)
            del(df.colindex, icol)
        else
            throw(ArgumentError("Can't delete a non-existent DataFrame column"))
        end
    end
    df
end
del!(df::DataFrame, c::Int) = del!(df, [c])
del!(df::DataFrame, c) = del!(df, df.colindex[c])

# df2 = del(df, 1) new DF, minus vectors
function del(df::DataFrame, icols::Vector{Int})
    newcols = _setdiff([1:ncol(df)], icols) 
    if length(newcols) == 0
        throw(ArgumentError("Can't delete a non-existent DataFrame column"))
    end
    # Note: this does not copy columns.
    df[newcols]
end
del(df::DataFrame, i::Int) = del(df, [i])
del(df::DataFrame, c) = del(df, df.colindex[c])
del(df::SubDataFrame, c) = SubDataFrame(del(df.parent, c), df.rows)


#### cbind, rbind, hcat, vcat
# cbind!(df, ...) will append to an existing df
# cbind!(not df, ...) will maintain the data, creating a new df
# cbind(...) always makes copies
# hcat() is just cbind()
# rbind(df, ...) only accepts data frames. Finds union of columns, maintaining order
# of first df. Missing data becomes NAs.
# vcat() is just rbind()
 

# df2 = cbind!(things...) any combination of dfs and dvs and vectors
# arguments can be either dfs, or {"colname", vector} or {"colname", DataVec} cell arrays,
# or {"colname", scalar} cell arrays
# item lengths have to either be the same
# colname types have to promotable

# two-argument form, one df, clobbering the argument
function cbind!(df::DataFrame, pair::Vector{Any})
    newcolname = pair[1]
    newcol = pair[2]
    if isa(newcol, Vector)
        if length(newcol) == nrow(df)
            newcol = DataVec(newcol)
        else
            error("Can't cbind vector to DataFrame of different length!")
        end
    elseif isa(newcol, AbstractDataVec)
        if length(newcol) != nrow(df)
            error("Can't cbind vector to DataFrame of different length!")
        end            
    else # try to build a DataVec out of this presumably singleton
        if isa(newcol, String)
            newcol_bytes = strlen(newcol)
        else
            newcol_bytes = sizeof(newcol)
        end
        if newcol_bytes > sizeof(Uint16)
            # cheaper to make this a pooled DV
            newcol = PooledDataVec(fill(newcol, nrow(df)))
        else
            newcol = DataVec(fill(newcol, nrow(df)))
        end
    end
    push(df.columns, newcol)
    
    push(df.colindex, convert(CT, newcolname))
    
    df
end

# TODO: move to set.jl? call nointer or nodupes?
# reasonably fast approach: foreach argument, iterate over
# the (presumed) set, checking for duplicates, adding to a hash table as we go
function nointer(ss...)
    d = Dict{Any,Int}(0)
    for s in ss
        for item in s
            ct = get(d, item, 0)
            if ct == 0 # we're good, add it
                d[item] = 1
            else
                return false
            end
        end
    end
    return true
end

function concat{T1,T2}(v1::Vector{T1}, v2::Vector{T2})
    # concatenate vectors, converting to type Any if needed.
    if T1 == T2 && T1 != Any
        [v1, v2]
    else
        res = Array(Any, length(v1) + length(v2))
        res[1:length(v1)] = v1
        res[length(v1)+1 : length(v1)+length(v2)] = v2
        res
    end
end


# two-argument form, two dfs, references only
function cbind!(df1::DataFrame, df2::DataFrame)
    # this only works if the column names can be promoted
    # TODO fix this
    ## newcolnames = convert(Vector{CT1}, df2.colnames)
    newcolnames = colnames(df2)
    # and if there are no duplicate column names
    if !nointer(colnames(df1), newcolnames)
        error("can't cbind dataframes with overlapping column names!")
    end
    df1.colindex = Index(concat(colnames(df1), colnames(df2)))
    df1.columns = [df1.columns, df2.columns]
    df1
end
    
    
# three-plus-argument form recurses
cbind!(a, b, c...) = cbind!(cbind(a, b), c...)

# without a bang, just copy then bind
cbind(a, b) = cbind!(copy(a), copy(b))
cbind(a, b, c...) = cbind(cbind(a,b), c...)

similar{T}(dv::DataVec{T}, dims) =
    DataVec(similar(dv.data, dims), fill(true, dims), dv.filter, dv.replace, dv.replaceVal)  

similar{T}(dv::PooledDataVec{T}, dims) =
    PooledDataVec(fill(uint16(1), dims), dv.pool, dv.filter, dv.replace, dv.replaceVal)  

similar(df::DataFrame, dims) = 
    DataFrame([similar(x, dims) for x in df.columns], colnames(df)) 

similar(df::SubDataFrame, dims) = 
    DataFrame([similar(df[x], dims) for x in colnames(df)], colnames(df)) 

function rbind(dfs::DataFrame...)
    Nrow = sum(nrow, dfs)
    Ncol = ncol(dfs[1])
    res = similar(dfs[1], Nrow)
    # TODO fix PooledDataVec columns with different pools.
    for idx in 2:length(dfs)
        if colnames(dfs[1]) != colnames(dfs[idx])
            error("DataFrame column names must match.")
        end
    end
    idx = 1
    for df in dfs
        for kdx in 1:nrow(df)
            for jdx in 1:Ncol
                res[jdx][idx] = df[kdx, jdx]
            end
            idx += 1
        end
    end
    res
end

function rbind(dfs::Vector)   # for a Vector of DataFrame's
    Nrow = sum(nrow, dfs)
    Ncol = ncol(dfs[1])
    res = similar(dfs[1], Nrow)
    # TODO fix PooledDataVec columns with different pools.
    for idx in 2:length(dfs)
        if colnames(dfs[1]) != colnames(dfs[idx])
            error("DataFrame column names must match.")
        end
    end
    idx = 1
    for df in dfs
        for kdx in 1:nrow(df)
            for jdx in 1:Ncol
                res[jdx][idx] = df[kdx, jdx]
            end
            idx += 1
        end
    end
    res
end


# DF row operations -- delete and append
# df[1] = nothing
# df[1:3] = nothing
# df3 = rbind(df1, df2...)
# rbind!(df1, df2...)


# split-apply-combine
# co(ap(myfun,
#    sp(df, ["region", "product"])))
# (|)(x, f::Function) = f(x)
# split(df, ["region", "product"]) | (apply(nrow)) | mean
# apply(f::function) = (x -> map(f, x))
# split(df, ["region", "product"]) | @@@)) | mean
# how do we add col names to the name space?
# transform(df, :(cat=dog*2, clean=proc(dirty)))
# summarise(df, :(cat=sum(dog), all=strcat(strs)))


function with(d::Associative, ex::Expr)
    # Note: keys must by symbols
    replace_symbols(x, d::Dict) = x
    replace_symbols(e::Expr, d::Dict) = Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, d), e.args), e.typ)
    function replace_symbols{K,V}(s::Symbol, d::Dict{K,V})
        if (K == Any || K == Symbol) && has(d, s)
            :(_D[$expr(:quote,s)])
        elseif (K == Any || K <: String) && has(d, string(s))
            :(_D[$string(s)])
        else
            s
        end
    end
    ex = replace_symbols(ex, d)
    global _ex = ex
    f = @eval (_D) -> $ex
    f(d)
end

function within!(d::Associative, ex::Expr)
    # Note: keys must by symbols
    replace_symbols(x, d::Associative) = x
    function replace_symbols{K,V}(e::Expr, d::Associative{K,V})
        if e.head == :(=) # replace left-hand side of assignments:
            if (K == Symbol || (K == Any && isa(keys(d)[1], Symbol)))
                exref = expr(:quote, e.args[1])
                if !has(d, e.args[1]) # Dummy assignment to reserve a slot.
                                      # I'm not sure how expensive this is.
                    d[e.args[1]] = values(d)[1]
                end
            else
                exref = string(e.args[1])
                if !has(d, exref) # dummy assignment to reserve a slot
                    d[exref] = values(d)[1]
                end
            end
            Expr(e.head,
                 vcat({:(_D[$exref])}, map(x -> replace_symbols(x, d), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, d), e.args), e.typ)
        end
    end
    function replace_symbols{K,V}(s::Symbol, d::Associative{K,V})
        if (K == Any || K == Symbol) && has(d, s)
            :(_D[$expr(:quote,s)])
        elseif (K == Any || K <: String) && has(d, string(s))
            :(_D[$string(s)])
        else
            s
        end
    end
    ex = replace_symbols(ex, d)
    f = @eval (_D) -> begin
        $ex
        _D
    end
    f(d)
end

function based_on(d::Associative, ex::Expr)
    # Note: keys must by symbols
    replace_symbols(x, d::Associative) = x
    function replace_symbols{K,V}(e::Expr, d::Associative{K,V})
        if e.head == :(=) # replace left-hand side of assignments:
            if (K == Symbol || (K == Any && isa(keys(d)[1], Symbol)))
                exref = expr(:quote, e.args[1])
                if !has(d, e.args[1]) # Dummy assignment to reserve a slot.
                                      # I'm not sure how expensive this is.
                    d[e.args[1]] = values(d)[1]
                end
            else
                exref = string(e.args[1])
                if !has(d, exref) # dummy assignment to reserve a slot
                    d[exref] = values(d)[1]
                end
            end
            Expr(e.head,
                 vcat({:(_ND[$exref])}, map(x -> replace_symbols(x, d), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, d), e.args), e.typ)
        end
    end
    function replace_symbols{K,V}(s::Symbol, d::Associative{K,V})
        if (K == Any || K == Symbol) && has(d, s)
            :(_D[$expr(:quote,s)])
        elseif (K == Any || K <: String) && has(d, string(s))
            :(_D[$string(s)])
        else
            s
        end
    end
    ex = replace_symbols(ex, d)
    f = @eval (_D) -> begin
        _ND = similar(_D)
        $ex
        _ND
    end
    f(d)
end

similar{K,V}(d::Dict{K,V}) = Dict{K,V}(length(d.keys))

function within!(df::AbstractDataFrame, ex::Expr)
    # By-column operation within a DataFrame that allows replacing or adding columns.
    # Returns the transformed DataFrame.
    #   
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in df
    replace_symbols(x, syms::Dict) = x
    function replace_symbols(e::Expr, syms::Dict)
        if e.head == :(=) # replace left-hand side of assignments:
            if !has(syms, string(e.args[1]))
                syms[string(e.args[1])] = length(syms) + 1
            end
            Expr(e.head,
                 vcat({:(_DF[$(string(e.args[1]))])}, map(x -> replace_symbols(x, syms), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, syms), e.args), e.typ)
        end
    end
    function replace_symbols(s::Symbol, syms::Dict)
        if contains(keys(syms), string(s))
            :(_DF[$(syms[string(s)])])
        else
            s
        end
    end
    # Make a dict of colnames and column positions
    cn_dict = dict(tuple(colnames(df)...), tuple([1:ncol(df)]...))
    ex = replace_symbols(ex, cn_dict)
    f = @eval (_DF) -> begin
        $ex
        _DF
    end
    f(df)
end

within(x, args...) = within!(copy(x), args...)

function based_on_f(df::AbstractDataFrame, ex::Expr)
    # Returns a function for use on an AbstractDataFrame
    
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in a new df
    replace_symbols(x, syms::Dict) = x
    function replace_symbols(e::Expr, syms::Dict)
        if e.head == :(=) # replace left-hand side of assignments:
            if !has(syms, string(e.args[1]))
                syms[string(e.args[1])] = length(syms) + 1
            end
            Expr(e.head,
                 vcat({:(_col_dict[$(string(e.args[1]))])}, map(x -> replace_symbols(x, syms), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, syms), e.args), e.typ)
        end
    end
    function replace_symbols(s::Symbol, syms::Dict)
        if contains(keys(syms), string(s))
            :(_DF[$(syms[string(s)])])
        else
            s
        end
    end
    # Make a dict of colnames and column positions
    cn_dict = dict(tuple(colnames(df)...), tuple([1:ncol(df)]...))
    ex = replace_symbols(ex, cn_dict)
    @eval (_DF) -> begin
        _col_dict = NamedArray()
        $ex
        DataFrame(_col_dict)
    end
end
function based_on(df::AbstractDataFrame, ex::Expr)
    # By-column operation within a DataFrame.
    # Returns a new DataFrame.
    f = based_on_f(df, ex)
    f(df)
end

function with(df::AbstractDataFrame, ex::Expr)
    # By-column operation with the columns of a DataFrame.
    # Returns the result of evaluating ex.
    
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in df
    replace_symbols(x, syms::Dict) = x
    replace_symbols(e::Expr, syms::Dict) = Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, syms), e.args), e.typ)
    function replace_symbols(s::Symbol, syms::Dict)
        if contains(keys(syms), string(s))
            :(_DF[$(syms[string(s)])])
        else
            s
        end
    end
    # Make a dict of colnames and column positions
    cn_dict = dict(tuple(colnames(df)...), tuple([1:ncol(df)]...))
    ex = replace_symbols(ex, cn_dict)
    f = @eval (_DF) -> $ex
    f(df)
end

with(df::AbstractDataFrame, s::Symbol) = df[string(s)]

# add function curries to ease pipelining:
with(e::Expr) = x -> with(x, e)
within(e::Expr) = x -> within(x, e)
within!(e::Expr) = x -> within!(x, e)
based_on(e::Expr) = x -> based_on(x, e)


# allow pipelining straight to an expression using within!:
(|)(x::AbstractDataFrame, e::Expr) = within!(x, e)


#
#  Split - Apply - Combine operations
#


function groupsort_indexer(x::Vector, ngroups::Integer)
    ## translated from Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).

    ## count group sizes, location 0 for NA
    n = length(x)
    ## counts = x.pool
    counts = fill(0, ngroups + 1)
    for i = 1:n
        counts[x[i] + 1] += 1
    end

    ## mark the start of each contiguous group of like-indexed data
    where = fill(1, ngroups + 1)
    for i = 2:ngroups+1
        where[i] = where[i - 1] + counts[i - 1]
    end
    
    ## this is our indexer
    result = fill(0, n)
    for i = 1:n
        label = x[i] + 1
        result[where[label]] = i
        where[label] += 1
    end
    result, where, counts
end
groupsort_indexer(pv::PooledDataVec) = groupsort_indexer(pv.refs, length(pv.pool))

type GroupedDataFrame
    parent::DataFrame
    cols::Vector         # columns used for sorting
    idx::Vector{Int}     # indexing vector when sorted by the given columns
    starts::Vector{Int}  # starts of groups
    ends::Vector{Int}    # ends of groups 
end

#
# Split
#
function groupby{T}(df::DataFrame, cols::Vector{T})
    ## a subset of Wes McKinney's algorithm here:
    ##     http://wesmckinney.com/blog/?p=489
    
    # use the pool trick to get a set of integer references for each unique item
    dv = PooledDataVec(df[cols[1]])
    # if there are NAs, add 1 to the refs to avoid underflows in x later
    dv_has_nas = (findfirst(dv.refs, 0) > 0 ? 1 : 0)
    x = copy(dv.refs) + dv_has_nas
    # also compute the number of groups, which is the product of the set lengths
    ngroups = length(dv.pool) + dv_has_nas
    # if there's more than 1 column, do roughly the same thing repeatedly
    for j = 2:length(cols)
        dv = PooledDataVec(df[cols[j]])
        dv_has_nas = (findfirst(dv.refs, 0) > 0 ? 1 : 0)
        for i = 1:nrow(df)
            x[i] += (dv.refs[i] + dv_has_nas- 1) * ngroups
        end
        ngroups = ngroups * (length(dv.pool) + dv_has_nas)
        # TODO if ngroups is really big, shrink it
    end
    (idx, starts) = groupsort_indexer(x, ngroups)
    # Remove zero-length groupings
    starts = _uniqueofsorted(starts) 
    ends = [starts[2:end] - 1]
    GroupedDataFrame(df, cols, idx, starts[1:end-1], ends)
end
groupby(d::DataFrame, cols) = groupby(d, [cols])

# add a function curry
groupby{T}(cols::Vector{T}) = x -> groupby(x, cols)
groupby(cols) = x -> groupby(x, cols)

function unique(x::Vector)
    idx = fill(true, length(x))
    d = Dict()
    d[x[1]] = true
    for i = 2:length(x)
        if has(d, x[i])
            idx[i] = false
        else
            d[x[i]] = true
        end
    end
    x[idx]
end

function _uniqueofsorted(x::Vector)
    idx = fill(true, length(x))
    lastx = x[1]
    for i = 2:length(x)
        if lastx == x[i]
            idx[i] = false
        else
            lastx = x[i]
        end
    end
    x[idx]
end

unique(pd::PooledDataVec) = pd.pool
sort(pd::PooledDataVec) = pd[order(pd)]
order(pd::PooledDataVec) = groupsort_indexer(pd)[1]

start(gd::GroupedDataFrame) = 1
next(gd::GroupedDataFrame, state::Int) = 
    (sub(gd.parent, gd.idx[gd.starts[state]:gd.ends[state]]),
     state + 1)
done(gd::GroupedDataFrame, state::Int) = state > length(gd.starts)
length(gd::GroupedDataFrame) = length(gd.starts)
ref(gd::GroupedDataFrame, idx::Int) = sub(gd.parent, gd.idx[gd.starts[idx]:gd.ends[idx]]) 

function show(io, gd::GroupedDataFrame)
    N = length(gd)
    println(io, "$(typeof(gd))  $N groups with keys: $(gd.cols)")
    println(io, "First Group:")
    show(io, gd[1])
    if N > 1
        println(io, "       :")
        println(io, "       :")
        println(io, "Last Group:")
        show(io, gd[N])
    end
end

#
# Apply / map
#

# map() sweeps along groups
function map(f::Function, gd::GroupedDataFrame)
    [f(d) for d in gd]
end
## function map(f::Function, gd::GroupedDataFrame)
##     # preallocate based on the results on the first one
##     x = f(gd[1])
##     res = Array(typeof(x), length(gd))
##     res[1] = x
##     for idx in 2:length(gd)
##         res[idx] = f(gd[idx])
##     end
##     res
## end

# with() sweeps along groups and applies with to each group
function with(gd::GroupedDataFrame, e::Expr)
    [with(d, e) for d in gd]
end

# within() sweeps along groups and applies within to each group
function within!(gd::GroupedDataFrame, e::Expr)   
    x = [within!(d[:,:], e) for d in gd]
    rbind(x...)
end

within!(x::SubDataFrame, e::Expr) = within!(x[:,:], e)

function within(gd::GroupedDataFrame, e::Expr)  
    x = [within(d, e) for d in gd]
    rbind(x...)
end

within(x::SubDataFrame, e::Expr) = within(x[:,:], e)

# MAYBE try to get in base?
function fill(x::Vector, lengths::Vector{Int})
    if length(x) != length(lengths)
        error("vector lengths must match")
    end
    res = similar(x, sum(lengths))
    i = 1
    for idx in 1:length(x)
        tmp = x[idx]
        for kdx in 1:lengths[idx]
            res[i] = tmp
            i += 1
        end
    end
    res
end

# based_on() sweeps along groups and applies based_on to each group
function based_on(gd::GroupedDataFrame, ex::Expr)  
    f = based_on_f(gd.parent, ex)
    x = [f(d) for d in gd]
    idx = fill([1:length(x)], convert(Vector{Int}, map(nrow, x)))
    keydf = gd.parent[gd.idx[gd.starts[idx]], gd.cols]
    resdf = rbind(x)
    cbind!(keydf, resdf)
end

# default pipelines:
map(f::Function, x::SubDataFrame) = f(x)
(|)(x::GroupedDataFrame, e::Expr) = based_on(x, e)   
## (|)(x::GroupedDataFrame, f::Function) = map(f, x)

# apply a function to each column in a DataFrame
colwise(f::Function, d::AbstractDataFrame) = [f(d[idx]) for idx in 1:ncol(d)]
colwise(f::Function, d::GroupedDataFrame) = map(colwise(f), d)
colwise(f::Function) = x -> colwise(f, x)
colwise(f) = x -> colwise(f, x)
# apply several functions to each column in a DataFrame
colwise(fns::Vector{Function}, d::AbstractDataFrame) = [f(d[idx]) for f in fns, idx in 1:ncol(d)][:]
colwise(fns::Vector{Function}, d::GroupedDataFrame) = map(colwise(fns), d)
colwise(fns::Vector{Function}, d::GroupedDataFrame, cn::Vector{String}) = map(colwise(fns), d)
colwise(fns::Vector{Function}) = x -> colwise(fns, x)

function colwise(d::AbstractDataFrame, s::Vector{Symbol}, cn::Vector)
    header = [s2 * "_" * string(s1) for s1 in s, s2 in cn][:]
    payload = colwise(map(eval, s), d)
    df = DataFrame()
    # TODO fix this to assign the longest column first or preallocate
    # based on the maximum length.
    for i in 1:length(header)
        df[header[i]] = payload[i]
    end
    df
end
## function colwise(d::AbstractDataFrame, s::Vector{Symbol}, cn::Vector)
##     header = [s2 * "_" * string(s1) for s1 in s, s2 in cn][:]
##     payload = colwise(map(eval, s), d)
##     DataFrame(payload, header)
## end
colwise(d::AbstractDataFrame, s::Symbol, x) = colwise(d, [s], x)
colwise(d::AbstractDataFrame, s::Vector{Symbol}, x::String) = colwise(d, s, [x])
colwise(d::AbstractDataFrame, s::Symbol) = colwise(d, [s], colnames(d))
colwise(d::AbstractDataFrame, s::Vector{Symbol}) = colwise(d, s, colnames(d))

# TODO make this faster by applying the header just once.
# BUG zero-rowed groupings cause problems here, because a sum of a zero-length
# DataVec is 0 (not 0.0).
colwise(d::GroupedDataFrame, s::Vector{Symbol}) = rbind(map(x -> colwise(del(x, d.cols),s), d)...)
colwise(d::GroupedDataFrame, s::Symbol, x) = colwise(d, [s], x)
colwise(d::GroupedDataFrame, s::Vector{Symbol}, x::String) = colwise(d, s, [x])
colwise(d::GroupedDataFrame, s::Symbol) = colwise(d, [s])
(|)(d::GroupedDataFrame, s::Vector{Symbol}) = colwise(d, s)
(|)(d::GroupedDataFrame, s::Symbol) = colwise(d, [s])
colnames(d::GroupedDataFrame) = colnames(d.parent)


# by() convenience function
by(d::AbstractDataFrame, cols, f::Function) = map(f, groupby(d, cols))
by(d::AbstractDataFrame, cols, e::Expr) = based_on(groupby(d, cols), e)
by(d::AbstractDataFrame, cols, s::Vector{Symbol}) = colwise(groupby(d, cols), s)
by(d::AbstractDataFrame, cols, s::Symbol) = colwise(groupby(d, cols), s)


##
## Reshaping
##


# slow, but maintains order and seems to work:
function _setdiff(a::Vector, b::Vector)
    idx = Int[]
    for i in 1:length(a)
        if !contains(b, a[i])
            push(idx, i)
        end
    end
    a[idx]
end

## Issue: this doesn't maintain the order in a:
## setdiff(a::Vector, b::Vector) = elements(Set(a...) - Set(b...))


function stack(df::AbstractDataFrame, icols::Vector{Int})
    remainingcols = _setdiff([1:ncol(df)], icols)
    res = rbind([insert(df[[i, remainingcols]], 1, colnames(df)[i], "key") for i in icols]...)
    replace_names!(res, colnames(res)[2], "value")
    res 
end

function unstack(df::AbstractDataFrame, ikey::Int, ivalue::Int, irefkey::Int)
    keycol = PooledDataVec(df[ikey])
    valuecol = df[ivalue]
    refkeycol = PooledDataVec(df[irefkey])
    remainingcols = _setdiff([1:ncol(df)], [ikey, ivalue])
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    # TODO make fillNA(type, length) 
    payload = DataFrame({DataVec([fill(valuecol[1],Nrow)], fill(true, Nrow))  for i in 1:Ncol}, map(string, keycol.pool))
    nowarning = true 
    for k in 1:nrow(df)
        j = int(keycol.refs[k])
        i = int(refkeycol.refs[k])
        if i > 0 && j > 0
            if nowarning && !isna(payload[j][i]) 
                println("Warning: duplicate entries in unstack.")
                nowarning = false
            end
            payload[j][i]  = valuecol[k]
        end
    end
    insert(payload, 1, refkeycol.pool, colnames(df)[irefkey])
end


##
## Join / merge
##


function join_idx(left, right, max_groups)
    ## adapted from Wes McKinney's full_outer_join in pandas (file: src/join.pyx).

    # NA group in location 0

    left_sorter, where, left_count = groupsort_indexer(left, max_groups)
    right_sorter, where, right_count = groupsort_indexer(right, max_groups)

    # First pass, determine size of result set, do not use the NA group
    count = 0
    rcount = 0
    lcount = 0
    for i in 2 : max_groups + 1
        lc = left_count[i]
        rc = right_count[i]

        if rc > 0 && lc > 0
            count += lc * rc
        elseif rc > 0
            rcount += rc
        else
            lcount += lc
        end
    end
    
    # group 0 is the NA group
    position = 0
    lposition = 0
    rposition = 0

    # exclude the NA group
    left_pos = left_count[1]
    right_pos = right_count[1]

    left_indexer = Array(Int, count)
    right_indexer = Array(Int, count)
    leftonly_indexer = Array(Int, lcount)
    rightonly_indexer = Array(Int, rcount)
    for i in 1 : max_groups + 1
        lc = left_count[i]
        rc = right_count[i]

        if rc == 0
            for j in 1:lc
                leftonly_indexer[lposition + j] = left_pos + j
            end
            lposition += lc
        elseif lc == 0
            for j in 1:rc
                rightonly_indexer[rposition + j] = right_pos + j
            end
            rposition += rc
        else
            for j in 1:lc
                offset = position + (j-1) * rc
                for k in 1:rc
                    left_indexer[offset + k] = left_pos + j
                    right_indexer[offset + k] = right_pos + k
                end
            end
            position += lc * rc
        end
        left_pos += lc
        right_pos += rc
    end

    ## (left_sorter, left_indexer, leftonly_indexer,
    ##  right_sorter, right_indexer, rightonly_indexer)
    (left_sorter[left_indexer], left_sorter[leftonly_indexer],
     right_sorter[right_indexer], right_sorter[rightonly_indexer])
end

function PooledDataVecs{T}(v1::AbstractDataVec{T}, v2::AbstractDataVec{T})
    ## Return two PooledDataVecs that share the same pool.
    
    refs1 = Array(Uint16, length(v1))
    refs2 = Array(Uint16, length(v2))
    poolref = Dict{T,Uint16}(length(v1))
    maxref = 0

    # loop through once to fill the poolref dict
    for i = 1:length(v1)
        ## TODO see if we really need the NA checking here.
        ## if !isna(v1[i])
            poolref[v1[i]] = 0
        ## end
    end
    for i = 1:length(v2)
        ## if !isna(v2[i])
            poolref[v2[i]] = 0
        ## end
    end

    # fill positions in poolref
    pool = sort(keys(poolref))
    i = 1
    for p in pool 
        poolref[p] = i
        i += 1
    end

    # fill in newrefs
    for i = 1:length(v1)
        ## if isna(v1[i])
        ##     refs1[i] = 0
        ## else
            refs1[i] = poolref[v1[i]]
        ## end
    end
    for i = 1:length(v2)
        ## if isna(v2[i])
        ##     refs2[i] = 0
        ## else
            refs2[i] = poolref[v2[i]]
        ## end
    end
    (PooledDataVec(refs1, pool, false, false, zero(T)),
     PooledDataVec(refs2, pool, false, false, zero(T)))
end

function merge(df1::AbstractDataFrame, df2::AbstractDataFrame, bycol)

    dv1, dv2 = PooledDataVecs(df1[bycol], df2[bycol])
    left_indexer, leftonly_indexer,
    right_indexer, rightonly_indexer =
        join_idx(dv1.refs, dv2.refs, length(dv1.pool))

    # inner join:
    cbind!(df1[left_indexer,:], del(df2, bycol)[right_indexer,:])
    # TODO left/right join, outer join - needs better
    #      NA indexing or a way to create NA DataFrames.
    # TODO add support for multiple columns
end



##
## Extras
##


const letters = split("abcdefghijklmnopqrstuvwxyz", "")
const LETTERS = split("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "")

function paste{T<:String}(s::Union(Vector{T},T)...)
    sa = {s...}
    N = max(length, sa)
    res = fill("", N)
    for i in 1:length(sa)
        if length(sa[i]) == N
            for j = 1:N
                res[j] = strcat(res[j], sa[i][j])
            end
        elseif length(sa[i]) == 1
            for j = 1:N
                res[j] = strcat(res[j], sa[i][1])
            end
        end
    end
    res
end

function cut{T}(x::Vector{T}, breaks::Vector{T})
    refs = fill(uint16(0), length(x))
    for i in 1:length(x)
        refs[i] = searchsorted(breaks, x[i])
    end
    from = map(x -> sprint(showcompact, x), [min(x), breaks])
    to = map(x -> sprint(showcompact, x), [breaks, max(x)])
    pool = paste(["[", fill("(", length(breaks))], from, ",", to, "]")
    PooledDataVec(refs, pool, false, false, "")
end
cut(x::Vector, ngroups::Integer) = cut(x, quantile(x, [1 : ngroups - 1] / ngroups))
