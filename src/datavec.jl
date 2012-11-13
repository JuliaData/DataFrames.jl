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

abstract AbstractDataVec{T}

bitstype 8 NARule
@enum NARule KEEP FILTER REPLACE

type DataVec{T} <: AbstractDataVec{T}
    data::Vector{T}
    na::BitVector
    
    naRule::NARule
    replaceVal::T
    
    # sanity checks
    function DataVec{T}(d::Vector{T}, m::BitVector, nar::NARule, v::T) 
        if (length(d) != length(m))
            error("data and mask vectors not the same length!")
        end   
        new(d,m,nar,v)
    end
end
# the usual partial constructor
DataVec{T}(d::Vector{T}, m::Vector{Bool}) = DataVec(d, bitpack(m))
DataVec{T}(d::Vector{T}, m::BitVector) = DataVec{T}(d, m, KEEP, baseval(T))
# a full constructor (why is this necessary?)
DataVec{T}(d::Vector{T}, m::Vector{Bool}, nar::NARule, v::T) = DataVec(d, bitpack(m), nar, v)
DataVec{T}(d::Vector{T}, m::BitVector, nar::NARule, v::T) = DataVec{T}(d, m, nar, v)
# a no-op constructor
DataVec(d::DataVec) = d

baseval(x) = zero(x)
baseval{T <: String}(s::Type{T}) = ""

type PooledDataVec{T} <: AbstractDataVec{T}
    refs::Vector{Uint16} # TODO: make sure we don't overflow
    pool::Vector{T}
    # TODO: ordering
    # TODO: meta-data for dummy conversion
    
    naRule::NARule
    replaceVal::T
    
    function PooledDataVec{T}(refs::Vector{Uint16}, pool::Vector{T}, nar::NARule, v::T)
        # refs mustn't overflow pool
        if (max(refs) > length(pool))
            error("reference vector points beyond the end of the pool!")
        end 
        new(refs,pool,nar,v)
    end
end
# a full constructor (why is this necessary?)
PooledDataVec{T}(re::Vector{Uint16}, p::Vector{T}, nar::NARule, v::T) = PooledDataVec{T}(re, p, nar, v)
# allow 0 for default number, "" for default string
PooledDataVec{T <: Number}(re::Vector{Uint16}, p::Vector{T}, nar::NARule) = PooledDataVec{T}(re, p, nar, convert(T,0))
PooledDataVec{T <: String}(re::Vector{Uint16}, p::Vector{T}, nar::NARule) = PooledDataVec{T}(re, p, nar, convert(T,""))

# how do you construct one? well, from the same sigs as a DataVec!
function PooledDataVec{T}(d::Vector{T}, m::AbstractArray{Bool,1}, nar::NARule, v::T)  
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
    PooledDataVec(newrefs, newpool, nar, v)
end

# Allow a pool to be provided
function PooledDataVec{T}(d::Vector{T}, pool::Vector{T}, m::Vector{Bool}, nar::NARule, v::T)  

    # TODO: check if pool greater than 2^16
    newrefs = Array(Uint16, length(d))
    poolref = Dict{T,Uint16}(0)
    maxref = 0

    # loop through once to fill the poolref dict
    for i = 1:length(pool)
        poolref[pool[i]] = 0
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
            if has(poolref, d[i])
              newrefs[i] = poolref[d[i]]
            else
              error("vector contains elements not in provided pool")
            end
        end
    end
    PooledDataVec(newrefs, newpool, nar, v)
end
PooledDataVec{T<:String}(d::Vector{T}, m::Vector{Bool}) = PooledDataVec(d, m, KEEP, convert(T,""))
PooledDataVec{T<:Number}(d::Vector{T}, m::Vector{Bool}) = PooledDataVec(d, m, KEEP, convert(T,0))
PooledDataVec{T<:String}(d::Vector{T}, pool::Vector{T}) = PooledDataVec(d, pool, falses(length(d)), KEEP, convert(T,""))
PooledDataVec{T<:Number}(d::Vector{T}, pool::Vector{T}) = PooledDataVec(d, pool, falses(length(d)), KEEP, convert(T,0))

PooledDataVec(dv::DataVec) = PooledDataVec(dv.data, dv.na, dv.naRule, dv.replaceVal)
PooledDataVec(d::PooledDataVec) = d

naRule(dv::DataVec) = dv.naRule
naRule(pdv::PooledDataVec) = pdv.naRule

# Utilities

values{T}(x::PooledDataVec{T}) = [x.pool[r] for r in x.refs]

levels{T}(x::PooledDataVec{T}) = x.pool

indices{T}(x::PooledDataVec{T}) = x.refs

function index_to_level{T}(x::PooledDataVec{T})
    d = Dict{Uint16, T}()
    for i in uint16(1:length(x.pool))
        d[i] = x.pool[i]
    end
    d
end

function level_to_index{T}(x::PooledDataVec{T})
    d = Dict{T, Uint16}()
    for i in uint16(1:length(x.pool))
        d[x.pool[i]] = i
    end
    d
end

function table{T}(d::PooledDataVec{T})
    poolref = Dict{T,Int64}(0)
    for i = 1:length(d)
        if has(poolref, d[i])
            poolref[d[i]] += 1
        else
            poolref[d[i]] = 1
        end
    end
    return poolref
end

type NAtype; end
const NA = NAtype()
show(io, x::NAtype) = print(io, "NA")

type NAException <: Exception
    msg::String
end

length(x::NAtype) = 1
size(x::NAtype) = ()

==(na::NAtype, na2::NAtype) = NA
==(na::NAtype, b) = NA
==(a, na::NAtype) = NA


# constructor from type
function _dv_most_generic_type(vals)
    # iterate over vals tuple to find the most generic non-NA type
    toptype = None
    for i = 1:length(vals)
        if !isna(vals[i])
            toptype = promote_type(toptype, typeof(vals[i]))
        end
    end
    # TODO: confirm that this type has a baseval() 
    toptype
end
function ref(::Type{DataVec}, vals...)
    # first, get the most generic non-NA type
    toptype = _dv_most_generic_type(vals)
    
    # then, allocate vectors
    lenvals = length(vals)
    ret = DataVec(Array(toptype, lenvals), BitArray(lenvals))
    # copy from vals into data and mask
    for i = 1:lenvals
        if isna(vals[i])
            ret.data[i] = baseval(toptype)
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
DataVec(x::Vector) = DataVec(x, BitArray(length(x)))
PooledDataVec(x::Vector) = PooledDataVec(x, falses(length(x)))

# copy does a deep copy
copy{T}(dv::DataVec{T}) = DataVec{T}(copy(dv.data), copy(dv.na), dv.naRule, dv.replaceVal)
copy{T}(dv::PooledDataVec{T}) = PooledDataVec{T}(copy(dv.refs), copy(dv.pool), dv.naRule, dv.replaceVal)

# TODO: copy_to


# properties
size(v::DataVec) = size(v.data)
size(v::PooledDataVec) = size(v.refs)
length(v::DataVec) = length(v.data)
length(v::PooledDataVec) = length(v.refs)
ndims(v::AbstractDataVec) = 1
numel(v::AbstractDataVec) = length(v)
eltype{T}(v::AbstractDataVec{T}) = T

isna(x::NAtype) = true
isna(v::DataVec) = v.na
isna(v::PooledDataVec) = v.refs .== 0
isna(x::AbstractArray) = falses(size(x))
isna(x) = false

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

# element-wise symmetric (in)equality operators
for (f,scalarf) in ((:(.==),:(==)), (:.!=,:!=))
    @eval begin    
        function ($f){T}(a::AbstractDataVec{T}, v::T)
            # allocate a DataVec for the return value, then assign into it
            ret = DataVec(Array(Bool,length(a)), BitArray(length(a)), naRule(a), false)
            for i = 1:length(a)
                ret[i] = isna(a[i]) ? NA : ($scalarf)(a[i], v)
            end
            ret
        end
        ($f){T}(v::T, a::AbstractDataVec{T}) = ($f)(a::AbstractDataVec{T}, v::T)
    end
end

# element-wise antisymmetric (in)equality operators
for (f,scalarf,scalarantif) in ((:.<, :<, :>), (:.>, :>, :<), (:.<=,:<=, :>=), (:.>=, :>=, :<=))
    @eval begin    
        function ($f){T}(a::AbstractDataVec{T}, v::T)
            # allocate a DataVec for the return value, then assign into it
            ret = DataVec(Array(Bool,length(a)), BitArray(length(a)), naRule(a), false)
            for i = 1:length(a)
                ret[i] = isna(a[i]) ? NA : ($scalarf)(a[i], v)
            end
            ret
        end
        function ($f){T}(v::T, a::AbstractDataVec{T})
            # allocate a DataVec for the return value, then assign into it
            ret = DataVec(Array(Bool,length(a)), BitArray(length(a)), naRule(a), false)
            for i = 1:length(a)
                ret[i] = isna(a[i]) ? NA : ($scalarantif)(a[i], v)
            end
            ret
        end
    end
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
            F = DataVec(Array(promote_type(S,T), length(A)), BitArray(length(A)))
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
    DataVec(x.data[r], x.na[r], x.naRule, x.replaceVal)
end
function ref(x::DataVec, r::Range)
    DataVec(x.data[r], x.na[r], x.naRule, x.replaceVal)
end
# PooledDataVec -- be sure copy the pool!
function ref(x::PooledDataVec, r::Range1)
    # TODO: copy the whole pool or just the items in the range?
    # for now, the whole pool
    PooledDataVec(x.refs[r], copy(x.pool), x.naRule, x.replaceVal)
end

# logical access -- note that unlike Array logical access, this throws an error if
# the index vector is not the same size as the data vector
function ref(x::DataVec, ind::Vector{Bool})
    if length(x) != length(ind)
        throw(ArgumentError("boolean index is not the same size as the DataVec"))
    end
    DataVec(x.data[ind], x.na[ind], x.naRule, x.replaceVal)
end
# PooledDataVec
function ref(x::PooledDataVec, ind::Vector{Bool})
    if length(x) != length(ind)
        throw(ArgumentError("boolean index is not the same size as the PooledDataVec"))
    end
    PooledDataVec(x.refs[ind], copy(x.pool), x.naRule, x.replaceVal)
end

# array index access
function ref(x::DataVec, ind::Vector{Int})
    DataVec(x.data[ind], x.na[ind], x.naRule, x.replaceVal)
end
# PooledDataVec
function ref(x::PooledDataVec, ind::Vector{Int})
    PooledDataVec(x.refs[ind], copy(x.pool), x.naRule, x.replaceVal)
end

ref(x::AbstractDataVec, ind::AbstractDataVec{Bool}) = x[nareplace(ind, false)]
ref(x::AbstractDataVec, ind::AbstractDataVec{Integer}) = x[nafilter(ind)]

ref(x::AbstractIndex, idx::AbstractDataVec{Bool}) = x[nareplace(idx, false)]
ref(x::AbstractIndex, idx::AbstractDataVec{Int}) = x[nafilter(idx)]

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
    (PooledDataVec(refs1, pool, naRule(v1)),
     PooledDataVec(refs2, pool, naRule(v2)))
end


# things to deal with unwanted NAs -- lower case returns the base type, with overhead,
# mixed case returns an iterator
nafilter{T}(v::DataVec{T}) = v.data[!v.na]
nareplace{T}(v::DataVec{T}, r::T) = [v.na[i] ? r : v.data[i] for i = 1:length(v.data)]
nafilter{T}(v::DataVec{T}) = v.data[!v.na]
nafilter(v::DataVec) = PooledDataVec(v.refs[map(isna, v)], v.pool, v.naRule, v.replaceVal)
# TODO PooledDataVec
# TODO nareplace! does in-place change; nafilter! shouldn't exist, as it doesn't apply with DataFrames

# naFilter redefines a new DataVec with a flipped bit that determines how start/next/done operate
naFilter{T}(v::DataVec{T}) = DataVec(v.data, v.na, FILTER, v.replaceVal)
naFilter{T}(v::PooledDataVec{T}) = PooledDataVec(v.refs, v.pool, FILTER, v.replaceVal)

# naReplace is similar to naFilter, but with a replacement value
naReplace{T}(v::DataVec{T}, rv::T) = DataVec(v.data, v.na, REPLACE, rv)
naReplace{T}(v::PooledDataVec{T}, rv::T) = PooledDataVec(v.refs, v.pool, REPLACE, rv)

# If neither the filter or replace flags are set, the iterator will return an NA
# when it hits an NA. If one or the other are set, it'll skip/replace the NA.
# Pooled and not share an implementation.
start(x::AbstractDataVec) = 1
function next(x::AbstractDataVec, state::Int)
    # if filter is set, iterate til we find a non-NA value
    if naRule(x) == FILTER
        for i = state:length(x)
            if !isna(x[i])
                return (x[i], i+1)
            end
        end
        error("called next(AbstractDataVec) without calling done() first")
    elseif naRule(x) == REPLACE
        return (isna(x[state]) ? x.replaceVal : x[state], state + 1)
    else
        return (x[state], state+1)
    end
end
function done(x::AbstractDataVec, state::Int)
    # if filter is set, iterate til we find a non-NA value
    if naRule(x) == FILTER
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
show(io, x::AbstractDataVec) = Base.show_comma_array(io, x, '[', ']') 
function string(x::AbstractDataVec)
    tmp = join(x, ",")
    return "[$tmp]"
end

function show(io, x::PooledDataVec)
    print("values: ")
    Base.show_vector(io, values(x), "[","]")
    print("\n")
    print("levels: ")
    Base.show_vector(io, levels(x), "[", "]")
end

# TODO: vectorizable math functions like sqrt, sin, trunc, etc., which should return a DataVec{T}
# not sure if this is the best approach, but works for a demo
function log{T}(x::DataVec{T})
    newx = log(x.data)
    DataVec(newx, x.na, x.naRule, convert(eltype(newx), x.replaceVal))
end

# TODO: vectorizable comparison operators like > which should return a DataVec{Bool}

# TODO: div(dat, 2) works, but zz ./ 2 doesn't



##
## Extras
##


const letters = convert(Vector{ASCIIString}, split("abcdefghijklmnopqrstuvwxyz", ""))
const LETTERS = convert(Vector{ASCIIString}, split("ABCDEFGHIJKLMNOPQRSTUVWXYZ", ""))

# Like string(s), but preserves Vector{String} and converts
# Vector{Any} to Vector{String}.
_vstring(s) = string(s)
_vstring(s::Vector) = map(_vstring, s)
_vstring{T<:String}(s::T) = s
_vstring{T<:String}(s::Vector{T}) = s
    
function paste{T<:String}(s::Vector{T}...)
    sa = {s...}
    N = max(length, sa)
    res = fill("", N)
    for i in 1:length(sa)
        Ni = length(sa[i])
        k = 1
        for j = 1:N
            res[j] = strcat(res[j], sa[i][k])
            if k == Ni   # This recycles array elements.
                k = 1
            else
                k += 1
            end
        end
    end
    res
end
# The following converts all arguments to Vector{<:String} before
# calling paste.
function paste(s...)
    converted = map(vcat * _vstring, {s...})
    paste(converted...)
end

function cut{T}(x::Vector{T}, breaks::Vector{T})
    refs = fill(uint16(0), length(x))
    for i in 1:length(x)
        refs[i] = search_sorted(breaks, x[i])
    end
    from = map(x -> sprint(showcompact, x), [min(x), breaks])
    to = map(x -> sprint(showcompact, x), [breaks, max(x)])
    pool = paste(["[", fill("(", length(breaks))], from, ",", to, "]")
    PooledDataVec(refs, pool, KEEP, "")
end
cut(x::Vector, ngroups::Integer) = cut(x, quantile(x, [1 : ngroups - 1] / ngroups))
