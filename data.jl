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
    
    for i = 1:length(d)
        if m[i]
            newrefs[i] = 0
        else
            existing = get(poolref, d[i], 0)
            if existing == 0
                maxref += 1
                poolref[d[i]] = maxref 
                push(newpool, d[i])
                newrefs[i] = maxref
            else
                newrefs[i] = existing
            end
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
# TODO: similar


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

function _find_first(x, v)
    for i = 1:length(x)
        if (x[i] == v)
            return i
        end
    end
    return 0
end

# assign variants
# x[3] = "cat"
function assign{T}(x::DataVec{T}, v::T, i::Int)
    x.data[i] = v
    x.na[i] = false
    return x[i]
end
function assign{T}(x::PooledDataVec{T}, v::T, i::Int)
    # note: NA replacement comes for free here
    
    # find the index of v in the pool
    pool_idx = _find_first(x.pool, v)
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
    fromidx = _find_first(x.pool, fromval)
    if fromidx == 0
        error("can't replace a value not in the pool in a PooledDataVec!")
    end
        
    # if toval is in the pool too, use that and remove fromval from the pool
    toidx = _find_first(x.pool, toval)
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
    fromidx = _find_first(x.pool, fromval)
    if fromidx == 0
        error("can't replace a value not in the pool in a PooledDataVec!")
    end
    
    x.refs[x.refs .== fromidx] = 0
    
    return NA
end
function replace!{T}(x::PooledDataVec{T}, fromval::NAtype, toval::T)
    toidx = _find_first(x.pool, toval)
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

# TODO: vectorizable comparison operators like > which should return a DataVec{Bool}

# TODO: div(dat, 2) works, but zz ./ 2 doesn't


# Abstract DF includes DataFrame and SubDataFrame
abstract AbstractDataFrame{CT}

# ## DataFrame - a list of heterogeneous Data vectors with col names.
# columns are a vector, which means that operations that insert/delete columns
# are O(n).
# col names must be the right length, but can be "nothing".
type DataFrame{CT} <: AbstractDataFrame{CT}
    columns::Vector{Any} # actually Vector{AbstractDataVec{*}}
    colnames::Vector{CT}
    
    # inner constructor requires everything to be the right types, checks lengths
    function DataFrame(cols::Vector, cn::Vector{CT})  
        # all cols
        ## if !all([isa(c, AbstractDataVec) for c = cols])
        ##     error("DataFrame inner constructor requires all columns be AbstractDataVecs already")
        ## end
          
        # all columns have to be the same length
        if length(cols) > 1 && !all(map(length, cols) .== length(cols[1]))
            error("all columns in a DataFrame have to be the same length")
        end
        
         # colnames has to be the same length as columns vector
        if length(cn) != length(cols)
            error("colnames must be the same length as the number of columns")
        end
        
        new(cols, cn)
    end
end
# constructors 
# if we already have DataVecs, but no names
nothings(n) = fill(nothing, n) # TODO: move elsewhere?
DataFrame(cs::Vector) = DataFrame(cs, nothings(length(cs)))
# if we have DataVecs and names
DataFrame{CT}(cs::Vector, cn::Vector{CT}) = DataFrame{CT}(cs, cn)

# if we have something else, convert each value in this tuple to a DataVec and pass it in, hoping for the best
DataFrame(vals...) = DataFrame([DataVec(x) for x = vals])
# if we have a matrix, create a tuple of columns and pass that in
DataFrame{T}(m::Array{T,2}) = DataFrame([DataVec(squeeze(m[:,i])) for i = 1:size(m)[2]])
# 

# Blank DataFrame
DataFrame() = DataFrame({}, ASCIIString[])

# copy of a data frame does a deep copy
copy(df::DataFrame) = DataFrame([copy(x) for x in df.columns], copy(df.colnames))


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


nrow(df::DataFrame) = ncol(df) > 0 ? length(df.columns[1]) : 0
ncol(df::DataFrame) = length(df.columns)
names(df::DataFrame) = colnames(df)
colnames(df::DataFrame) = copy(df.colnames)
size(df::DataFrame) = (nrow(df), ncol(df))
size(df::DataFrame, i::Integer) = i==1 ? nrow(df) : (i==2 ? ncol(df) : error("DataFrames have two dimensions only"))

# get columns by name, position
# first two return the DataVec
ref(df::DataFrame, i::Int) = df.columns[i]
ref{CT}(df::DataFrame{CT}, name::CT) = df.columns[_find_first(df.colnames, name)] # TODO make faster
# these all return another DF
ref{CT}(df::DataFrame{CT}, names::Vector{CT}) = df[[_find_first(df.colnames, n)::Int for n = names]] # calls the next one
ref(df::DataFrame, is::Vector{Int}) = DataFrame(df.columns[is], df.colnames[is])
ref(df::DataFrame, rng::Range1) = DataFrame(df.columns[rng], df.colnames[rng])
ref(df::DataFrame, pos::Vector{Bool}) = DataFrame(df.columns[pos], df.colnames[pos])

# get slices
# row slices
ref(df::DataFrame, r::Int, rng::Range1) = DataFrame({x[[r]] for x in df.columns[rng]}, 
                                                    df.colnames[rng])
ref(df::DataFrame, r::Int, cs::Vector{Int}) = DataFrame({x[[r]] for x in df.columns[cs]}, 
                                                        df.colnames[cs])
ref{CT}(df::DataFrame{CT}, r::Int, cs::Vector{CT}) = df[r, [_find_first(df.colnames, c)::Int for c = cs]]
ref(df::DataFrame, r::Int, cs::Vector{Bool}) = df[cs][r,:] # possibly slow, but pretty

# 2-D slices
# rows are vector of indexes
ref(df::DataFrame, rs::Vector{Int}, cs::Vector{Int}) = DataFrame({x[rs] for x in df.columns[cs]}, 
                                                                 df.colnames[cs])
ref(df::DataFrame, rs::Vector{Int}, rng::Range1) = DataFrame({x[rs] for x in df.columns[rng]}, 
                                                             df.colnames[rng])
ref(df::DataFrame, rs::Vector{Int}, cs::Vector{Bool}) = df[cs][rs,:] # slow way
ref{CT}(df::DataFrame{CT}, rs::Vector{Int}, cs::Vector{CT}) = df[cs][rs,:] # slow way
# col slices
ref(df::DataFrame, rs::Vector{Int}, c::Int) = df[c][rs]
ref{CT}(df::DataFrame{CT}, rs::Vector{Int}, name::CT) = df[name][rs]

# TODO: other types of row indexing with 2-D slices
# rows are range, vector of booleans
# is there a macro way to define all of these??
ref(df::DataFrame, rr::Range1, cr::Range1) = DataFrame({x[rr] for x in df.columns[cr]},
                                                             df.colnames[cr])


head(df::DataFrame, r::Int) = df[1:r, :]
head(df::DataFrame) = head(df, 6)
tail(df::DataFrame, r::Int) = df[(nrow(df)-r+1):nrow(df), :]
tail(df::DataFrame) = tail(df, 6)


# get singletons. TODO: nicer error handling
# TODO: deal with oddness if row/col types are ints
ref(df::DataFrame, r::Int, c::Int) = df.columns[c][r]
ref{CT}(df::DataFrame{CT}, r::Int, cn::CT) = df.columns[_find_first(df.colnames, cn)][r]


# to print a DataFrame, find the max string length of each column
# then print the column names with an appropriate buffer
# then row-by-row print with an appropriate buffer
maxShowLength(v::Vector) = length(v) > 0 ? max([length(string(x)) for x = v]) : 0
maxShowLength(dv::AbstractDataVec) = max([length(string(x)) for x = dv])
function show(io, df::DataFrame)
    # we don't have row names -- use indexes
    rowNames = [sprintf("[%d,]", r) for r = 1:nrow(df)]
    
    rownameWidth = maxShowLength(rowNames)
    
    # if we don't have columns names, use indexes
    # note that column names in R are obligatory
    if eltype(df.colnames) == Nothing
        colNames = [sprintf("[,%d]", c) for c = 1:ncol(df)]
    else
        colNames = df.colnames
    end
    
    colWidths = [max(length(string(colNames[c])), maxShowLength(df.columns[c])) for c = 1:ncol(df)]

    header = strcat(" " ^ (rownameWidth+1),
                    join([lpad(string(colNames[i]), colWidths[i]+1, " ") for i = 1:ncol(df)], ""))
    println(io, header)
    
    for i = 1:min(100, nrow(df)) # TODO
        rowname = rpad(string(rowNames[i]), rownameWidth+1, " ")
        line = strcat(rowname,
                      join([lpad(string(df[i,c]), colWidths[c]+1, " ") for c = 1:ncol(df)], ""))
        println(io, line)
    end
end

# get the structure of a DF
# TODO: return a string or something instead of printing?
# TODO: AbstractDataFrame
str(df::DataFrame) = str(OUTPUT_STREAM::IOStream, df)
function str(io, df::DataFrame)
    println(io, sprintf("%d observations of %d variables", nrow(df), ncol(df)))

    if eltype(df.colnames) == Nothing
        colNames = [sprintf("[,%d]", c) for c = 1:ncol(df)]
    else
        colNames = df.colnames
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
    println(io, typeof(x), sprintf("  %d observations of %d variables", nrow(x), ncol(x)))
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
function summary(io, df::DataFrame)
    for c in 1:ncol(df)
        col = df[c]
        println(io, df.colnames[c])
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
type SubDataFrame{CT} <: AbstractDataFrame{CT}
    parent::DataFrame{CT}
    rows::Vector{Int} # maps from subdf row indexes to parent row indexes
    cols::Vector{Int} # maps from subdf col indexes to parent col indexes
    allcols::Bool     # if all cols included, ignore cols mapping
    
    # TODO: constructor to check params
end


sub{CT}(D::DataFrame{CT}, rs::Vector{Int}) = SubDataFrame(D, rs, [1:nrow(D)], true)
sub{CT}(D::DataFrame{CT}, rs::Vector{Int}, cs::Vector{Int}) = SubDataFrame(D, rs, cs, false)

# should use metaprogramming to make all of the below constructors!
sub{CT}(D::DataFrame{CT}, r::Int) = sub(D, [r])
sub{CT}(D::DataFrame{CT}, rng::Range1) = sub(D, [rng])
sub{CT}(D::DataFrame{CT}, b::Vector{Bool}) = sub(D, [1:nrow(D)][b])

sub{CT}(D::DataFrame{CT}, r::Int, c::Int) = sub(D, [r], [c])
sub{CT}(D::DataFrame{CT}, rs::Vector{Int}, c::Int) = sub(D, rs, [c])
sub{CT}(D::DataFrame{CT}, rng::Range1, c::Int) = sub(D, [rng], [c])
sub{CT}(D::DataFrame{CT}, b::Vector{Bool}, c::Int) = sub(D, [1:nrow(D)][b], [c])

sub{CT}(D::DataFrame{CT}, r::Int, cs::Vector{Int}) = sub(D, [r], cs)
#sub{CT}(D::DataFrame{CT}, rs::Vector{Int}, cs::Vector{Int}) = sub(D, r, [c])
sub{CT}(D::DataFrame{CT}, rng::Range1, cs::Vector{Int}) = sub(D, [rng], cs)
sub{CT}(D::DataFrame{CT}, b::Vector{Bool}, cs::Vector{Int}) = sub(D, [1:nrow(D)][b], cs)

sub{CT}(D::DataFrame{CT}, r::Int, crng::Range1) = sub(D, [r], [crng])
sub{CT}(D::DataFrame{CT}, rs::Vector{Int}, crng::Range1) = sub(D, rs, [crng])
sub{CT}(D::DataFrame{CT}, rng::Range1, crng::Range1) = sub(D, [rng], [crng])
sub{CT}(D::DataFrame{CT}, b::Vector{Bool}, crng::Range1) = sub(D, [1:nrow(D)][b], [crng])

sub{CT}(D::DataFrame{CT}, r::Int, cb::Vector{Bool}) = sub(D, [r], [1:ncol(D)][cb])
sub{CT}(D::DataFrame{CT}, rs::Vector{Int}, cb::Vector{Bool}) = sub(D, rs, [1:ncol(D)][cb])
sub{CT}(D::DataFrame{CT}, rng::Range1, cb::Vector{Bool}) = sub(D, [rng], [1:ncol(D)][cb])
sub{CT}(D::DataFrame{CT}, b::Vector{Bool}, cb::Vector{Bool}) = sub(D, [1:nrow(D)][b], [1:ncol(D)][cb])

sub{CT}(D::DataFrame{CT}, r::Int, c::CT) = sub(D, [r], [_find_first(D.colnames, c)])
sub{CT}(D::DataFrame{CT}, rs::Vector{Int}, c::CT) = sub(D, rs, [_find_first(D.colnames, c)])
sub{CT}(D::DataFrame{CT}, rng::Range1, c::CT) = sub(D, [rng], [_find_first(D.colnames, c)])
sub{CT}(D::DataFrame{CT}, b::Vector{Bool}, c::CT) = sub(D, [1:nrow(D)][b], [_find_first(D.colnames, c)])

sub{CT}(D::DataFrame{CT}, rs::Vector{Int}, cs::Vector{CT}) =
    sub(D, rs, [_find_first(D.colnames, c)::Int for c = cs])

# TODO: subs of subs


nrow(df::SubDataFrame) = length(df.rows)
ncol(df::SubDataFrame) = df.allcols ? ncol(df.parent) : length(df.cols)
names(df::SubDataFrame) = colnames(df)
colnames(df::SubDataFrame) = df.allcols ? colnames(df.parent) : colnames(df.parent)[df.cols]
size(df::AbstractDataFrame) = (nrow(df), ncol(df))
size(df::AbstractDataFrame, i::Integer) = i==1 ? nrow(df) : (i==2 ? ncol(df) : error("DataFrames have two dimensions only"))

# tons of refs...
# get columns by name, position
ref(df::SubDataFrame, i::Int) = ref(df.parent, df.rows, df.cols[i])
ref{CT}(df::SubDataFrame{CT}, name::CT) = ref(df.parent, df.rows, name)
ref{CT}(df::SubDataFrame{CT}, names::Vector{CT}) = ref(df.parent, df.rows, names)
ref(df::SubDataFrame, ixs::Vector{Int}) = ref(df.parent, df.rows, df.cols[ixs])
ref(df::SubDataFrame, rng::Range1) = ref(df.parent, df.rows, df.cols[rng])
ref(df::SubDataFrame, pos::Vector{Bool}) = ref(df.parent, df.rows, df.cols[pos])

# get slices
# row slices
ref(df::SubDataFrame, r::Int, rng::Range1) = ref(df.parent, df.rows[r], df.allcols ? rng : df.cols[rng])
ref(df::SubDataFrame, r::Int, cs::Vector{Int}) = ref(df.parent, df.rows[r], df.allcols ? cs : df.cols[cs])
ref{CT}(df::SubDataFrame{CT}, r::Int, cs::Vector{CT}) = ref(df.parent, df.rows[r], cs)
ref(df::SubDataFrame, r::Int, cs::Vector{Bool}) = ref(df.parent, df.rows[r], df.allcols ? cs : df.cols[cs])

# 2-D slices
# rows are vector of indexes
ref(df::SubDataFrame, rs::Vector{Int}, cs::Vector{Int}) = ref(df.parent, df.rows[rs], df.allcols ? cs : df.cols[cs])
ref(df::SubDataFrame, rs::Vector{Int}, rng::Range1) = ref(df.parent, df.rows[rs], df.allcols ? rng : df.cols[rng])
ref(df::SubDataFrame, rs::Vector{Int}, cs::Vector{Bool}) = ref(df.parent, df.rows[rs], df.allcols ? cs : df.cols[cs])
ref{CT}(df::SubDataFrame{CT}, rs::Vector{Int}, cs::Vector{CT}) = ref(df.parent, df.rows[rs], cs)
ref(df::SubDataFrame, rs::Vector{Int}, c::Int) = ref(df.parent, df.rows[rs], df.allcols ? c : df.cols[c])
ref{CT}(df::SubDataFrame{CT}, rs::Vector{Int}, name::CT) = ref(df.parent, df.rows[rs], df.allcols ? c : df.cols[c]) 
# TODO: other types of row indexing with 2-D slices
# rows are range, vector of booleans
# is there a macro way to define all of these??
ref(df::SubDataFrame, rr::Range1, cr::Range1) = ref(df.parent, df.rows[rr], df.allcols ? cr : df.cols[cr])


head(df::AbstractDataFrame, r::Int) = df[1:min(r,nrow(df)), :]
head(df::AbstractDataFrame) = head(df, 6)
tail(df::AbstractDataFrame, r::Int) = df[max(1,nrow(df)-r+1):nrow(df), :]
tail(df::AbstractDataFrame) = tail(df, 6)


# get singletons. TODO: nicer error handling
ref(df::SubDataFrame, r::Int, c::Int) = ref(df.parent, df.rows[r], df.allcols ? c : df.cols[c])
ref{CT}(df::SubDataFrame{CT}, r::Int, cn::CT) = ref(df.parent, df.rows[r], cn)


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
function assign{T}(df::DataFrame{T}, newcol::AbstractDataVec, colname::T)
    icol = _find_first(df.colnames, colname)
    if icol > 0
        # existing
        assign(df, newcol, icol)
    else
        # new
        push(df.colnames, colname)
        push(df.columns, newcol)
    end
    df
end
assign{CT, T}(df::DataFrame{CT}, newcol::Vector{T}, colname::CT) = assign(df, DataVec(newcol), colname)

assign{T}(df::DataFrame{T}, newcol, colname::T) =
    nrow(df) > 0 ? assign(df, DataVec(fill(newcol, nrow(df))), colname) : assign(df, DataVec([newcol]), colname)

# do I care about vectorized assignment? maybe not...
# df[1:3] = (replace columns) eh...
# df[["new", "newer"]] = (new columns)

# df[1] = nothing
assign(df::DataFrame, x::Nothing, icol::Integer) = del!(df, icol)

# del!(df, 1)
# del!(df, "old")
function del!(df::DataFrame, icol::Integer)
    if icol > 0 && icol <= ncol(df)
        del(df.columns, icol)
        del(df.colnames, icol)
    else
        throw(ArgumentError("Can't delete a non-existent DataFrame column"))
    end
    df
end
del!{CT}(df::DataFrame{CT}, colname::CT) = del!(df, _find_first(colnames(df), colname))

# df2 = del(df, 1) new DF, minus vectors
function del(df::DataFrame, icol::Integer)
    if icol > 0 && icol <= ncol(df)
        cols = del(copy(df.columns), icol)
        colnames = del(copy(df.colnames), icol)
        ret = DataFrame(cols, colnames)
    else
        throw(ArgumentError("Can't delete a non-existent DataFrame column"))
    end
end
del{CT}(df::DataFrame{CT}, colname::CT) = del(df, _find_first(colnames(df), colname))


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
function cbind!{CT}(df::DataFrame{CT}, pair::Vector{Any})
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
    
    push(df.colnames, convert(CT, newcolname))
    
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


# two-argument form, two dfs, references only
function cbind!{CT1, CT2}(df1::DataFrame{CT1}, df2::DataFrame{CT2})
    # this only works if the column names can be promoted
    ## newcolnames = convert(Vector{CT1}, df2.colnames)
    newcolnames = df2.colnames
    # and if there are no duplicate column names
    if !nointer(df1.colnames, newcolnames)
        error("can't cbind dataframes with overlapping column names!")
    end
    df1.colnames = [df1.colnames, df2.colnames]
    df1.columns = [df1.columns, df2.columns]
    df1
end
    
    
# three-plus-argument form recurses
cbind!(a, b, c...) = cbind!(cbind(a, b), c...)

# without a bang, just copy then bind
cbind(a, b) = cbind!(copy(a), copy(b))
cbind(a, b, c...) = cbind(cbind(a,b), c...)

similar{T}(dv::DataVec{T}, dims) =
    DataVec(similar(dv.data, dims), similar(dv.na, dims), dv.filter, dv.replace, dv.replaceVal)  

similar{T}(dv::PooledDataVec{T}, dims) =
    PooledDataVec(fill(uint16(1), dims), dv.pool, dv.filter, dv.replace, dv.replaceVal)  

similar{CT}(df::DataFrame{CT}, dims) = 
    DataFrame([similar(x, dims) for x in df.columns], colnames(df)) 

similar{CT}(df::SubDataFrame{CT}, dims) = 
    DataFrame([similar(df[x], dims) for x in colnames(df)], colnames(df)) 

function assign{CT,T}(df::DataFrame{CT}, col::T, i::Int, j::Int)
    df.column[i][]
end

function rbind{CT}(dfs::DataFrame{CT}...)
    Nrow = sum(nrow, dfs)
    Ncol = ncol(dfs[1])
    res = similar(dfs[1], Nrow)
    # TODO fix PooledDataVec columns with different pools.
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



## function within!(df::AbstractDataFrame, ex::Expr)
function within!(df, ex::Expr)
    # By-column operation within a DataFrame that allows replacing or adding columns.
    # Returns the transformed DataFrame.
    #   
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in df
    replace_symbols(x, syms::Dict) = x
    function replace_symbols(e::Expr, syms::Dict)
        if e.head == :(=) # replace left-hand side of assignments:
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

function summarise(df::AbstractDataFrame, ex::Expr)
    # By-column operation within a DataFrame.
    # Returns a new DataFrame.
    
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in a new df
    replace_symbols(x, syms::Dict) = x
    function replace_symbols(e::Expr, syms::Dict)
        if e.head == :(=) # replace left-hand side of assignments:
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
    global _ex = ex
    f = @eval (_DF) -> begin
        _col_dict = Dict()
        $ex
        DataFrame(_col_dict)
    end
    f(df)
end

function DataFrame(d::Associative)
    # Find the first position with maximum length in the Dict.
    # I couldn't get findmax to work here.
    ## (Nrow,maxpos) = findmax(map(length, values(d)))
    lengths = map(length, values(d))
    maxpos = find(lengths .== max(lengths))[1]
    keymaxlen = keys(d)[maxpos]
    Nrow = length(d[keymaxlen])
    # Start with a blank DataFrame
    df = DataFrame()
    # Assign the longest column to set the overall nrows.
    df[string(keymaxlen)] = d[keymaxlen]
    # Now assign them all.
    for (k,v) in d
        if contains([1,Nrow], length(v))
            df[string(k)] = v     # string(k) forces string column names
        else
            println("Warning: Column $(string(k)) ignored: mismatched column lengths")
        end
    end
    df
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


# add function curries to ease pipelining:
with(e::Expr) = x -> with(x, e)
within(e::Expr) = x -> within(x, e)
within!(e::Expr) = x -> within!(x, e)
summarise(e::Expr) = x -> summarise(x, e)

# TODO add versions of each of these for Dict's

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
    result, where
end


type GroupedDataFrame{T}
    parent::DataFrame{T}
    cols::Vector{T}      # columns used for sorting
    idx::Vector{Int}     # indexing vector when sorted by the given columns
    starts::Vector{Int}  # starts of groups
    ends::Vector{Int}    # ends of groups 
end

#
# Split
#
function groupby(df::DataFrame{ASCIIString}, cols::Vector{ASCIIString})
    ## a subset of Wes McKinney's algorithm here:
    ##     http://wesmckinney.com/blog/?p=489
    dv = PooledDataVec(df[cols[1]])
    x = copy(dv.refs)
    ngroups = length(dv.pool)
    for j = 2:length(cols)
        dv = PooledDataVec(df[cols[j]])
        for i = 1:nrow(df)
            x[i] += (dv.refs[i] - 1) * ngroups
        end
        ngroups = ngroups * length(dv.pool)
        # TODO if ngroups is really big, shrink it
    end
    (idx, starts) = groupsort_indexer(x, ngroups)
    ends = [starts[2:end] - 1]
    GroupedDataFrame(df, cols, idx, starts[1:end-1], ends)
end
groupby(d::DataFrame{ASCIIString}, cols::ASCIIString) = groupby(d, [cols])

# add a function curry
groupby(cols::Vector{ASCIIString}) = x -> groupby(x, cols)



start(gd::GroupedDataFrame) = 1
next(gd::GroupedDataFrame, state::Int) = 
    (sub(gd.parent, gd.idx[gd.starts[state]:gd.ends[state]]),
     state + 1)
done(gd::GroupedDataFrame, state::Int) = state > length(gd.starts)
length(gd::GroupedDataFrame) = length(gd.starts)
ref(gd::GroupedDataFrame, idx::Int) = sub(gd.parent, gd.idx[gd.starts[idx]:gd.ends[idx]]) 

function show(io, gd::GroupedDataFrame)
    println(io, typeof(gd), " ", length(gd.starts), " groups in DataFrame:")
    show(io, gd.parent)
end

#
# Apply / map
#

# map() sweeps along groups
function map(f::Function, gd::GroupedDataFrame)
    [f(d) for d in gd]
end

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

# summarise() sweeps along groups and applies summarise to each group
function summarise(gd::GroupedDataFrame, ex::Expr)  
    x = [summarise(d, ex) for d in gd]
    # There must be a better way to do this.
    ## idx = [fill(gdx, nrow(x[gdx])) for gdx in 1:length(x)]  # not quite right - an array in an array
    # In R, it's: rep(1:length(a), sapply(a,length))
    Nrow = sum(nrow, x)
    idx = fill(0, Nrow)
    i = 1
    for gdx in 1:length(x)
        for kdx in 1:nrow(x[gdx])
            idx[i] = gdx
            i += 1
        end
    end
    keydf = gd.parent[gd.idx[gd.starts[idx]], gd.cols]
    resdf = rbind(x...)
    cbind(keydf, resdf)
end

# default pipelines:
map(f::Function, x::SubDataFrame) = f(x)
(|)(x::GroupedDataFrame, e::Expr) = summarise(x, e)   
## (|)(x::GroupedDataFrame, f::Function) = map(f, x)

# apply a function to each column in a DataFrame
colwise(f::Function, d::AbstractDataFrame) = [f(d[idx]) for idx in 1:ncol(d)]
colwise(f::Function, d::GroupedDataFrame) = map(colwise(f), d)
colwise(f::Function) = x -> colwise(f, x)
colwise(fns::Vector{Function}, d::AbstractDataFrame) = [f(d[idx]) for f in fns, idx in 1:ncol(d)]
colwise(fns::Vector{Function}, d::GroupedDataFrame) = map(colwise(f), d)
colwise(fns::Vector{Function}) = x -> colwise(f, x)

# by() convenience function
by(d::AbstractDataFrame, cols::Vector{ASCIIString}, f::Function) = map(f, groupby(d, cols))
by(d::AbstractDataFrame, cols::Vector{ASCIIString}, e::Expr) = summarise(groupby(d, cols), e)

