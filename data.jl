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
    refs::Vector{Uint64}
    pool::Vector{T}
    # TODO: ordering
    # TODO: meta-data for dummy conversion
    
    filter::Bool
    replace::Bool
    replaceVal::T
    
    function PooledDataVec{T}(refs::Vector{Uint64}, pool::Vector{T}, f::Bool, r::Bool, v::T)
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
PooledDataVec{T}(re::Vector{Uint64}, p::Vector{T}, f::Bool, r::Bool, v::T) = PooledDataVec{T}(re, p, f, r, v)

# how do you construct one? well, from the same sigs as a DataVec!
PooledDataVec{T}(d::Vector{T}, m::AbstractVector{Bool}) = PooledDataVec{T}(d, m, false, false, zero(T))
function PooledDataVec{T}(d::Vector{T}, m::AbstractVector{Bool}, f::Bool, r::Bool, v::T)  
    # algorithm... start with a null pool and a pre-allocated refs, plus hash from T to Int.
    # iterate over d. If in pool already, set the refs accordingly. If new, add to pool then set refs.
    newrefs = Array(Uint64, length(d))
    newpool = Array(T, 0)
    poolref = Dict{T,Uint64}(0)
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

# TODO: copy_to
# TODO: similar


# properties
size(v::DataVec) = size(v.data)
size(v::PooledDataVec) = size(v.refs)
length(v::DataVec) = length(v.data)
length(v::PooledDataVec) = length(v.refs)
isna(v::DataVec) = v.na
isna(v::PooledDataVec) = v.refs == 0
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
        x.refs[x.refs == fromidx] = toidx
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
    
    x.refs[x.refs == fromidx] = 0
    
    return NA
end
function replace!{T}(x::PooledDataVec{T}, fromval::NAtype, toval::T)
    toidx = _find_first(x.pool, toval)
    # if toval is in the pool, just do the assignment
    if toidx != 0
        x.refs[x.refs == 0] = toidx
    else
        # otherwise, toval is new, add it to the pool
        push(x.pool, toval)
        x.refs[x.refs == 0] = length(x.pool)
    end
    
    return toval
end

# things to deal with unwanted NAs -- lower case returns the base type, with overhead,
# mixed case returns an iterator
nafilter{T}(v::DataVec{T}) = v.data[!v.na]
nareplace{T}(v::DataVec{T}, r::T) = [v.na[i] ? r : v.data[i] for i = 1:length(v.data)]
# TODO PooledDataVec

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




# ## DataFrame - a list of heterogeneous Data vectors with row and col names.
# columns are a vector, which means that operations that insert/delete columns
# are O(n).
# row and col names must be the right length, but can be "nothing".
type DataFrame{RT,CT}
    columns::Vector{Any}
    rownames::Vector{RT}
    colnames::Vector{CT}
    
    # inner constructor requires everything to be the right types, checks lengths
    function DataFrame(cols::Vector, rn::Vector{RT}, cn::Vector{CT})  
        # all cols
        if !all([isa(c, DataVec) | c = cols])
            error("DataFrame inner constructor requires all columns be DataVecs already")
        end
          
        # all columns have to be the same length
        if !all(map(length, cols) == length(cols[1]))
            error("all columns in a DataFrame have to be the same length")
        end
        
        # rownames has to be the same length as the columns
        if length(rn) != length(cols[1])
            error("rownames must be the same length as columns")
        end
        
        # colnames has to be the same length as columns vector
        if length(cn) != length(cols)
            error("colnames must be the same length as the number of columns")
        end
        
        new(cols, rn, cn)
    end
end
# constructors 
# if we already have DataVecs, but no names
nothings(n) = fill(nothing, n) # TODO: move elsewhere?
DataFrame(cs::Vector) = DataFrame(cs, nothings(length(cs[1])), nothings(length(cs)))
# if we have DataVecs and names
DataFrame{RT,CT}(cs::Vector, rn::Vector{RT}, cn::Vector{CT}) = DataFrame{RT,CT}(cs, rn, cn)
# if we have DataVecs and colnames (note can't just have rownames -- hm)
DataFrame{RT,CT}(cs::Vector, cn::Vector{CT}) = DataFrame{RT,CT}(cs, nothings(length(cs[1])), cn)

# if we have something else, convert each value in this tuple to a DataVec and pass it in, hoping for the best
DataFrame(vals...) = DataFrame([DataVec(x) | x = vals])
# if we have a matrix, create a tuple of columns and pass that in
DataFrame{T}(m::Array{T,2}) = DataFrame([DataVec(squeeze(m[:,i])) | i = 1:size(m)[2]])
# 
# 

# TODO: move
function idxFirstEqual{T}(x::Vector{T}, y::T)
    for i = 1:length(x)
        if x[i] == y
            return i
        end
    end
    return nothing
end


nrow(df::DataFrame) = length(df.columns[1])
ncol(df::DataFrame) = length(df.columns)
names(df::DataFrame) = colnames(df)
colnames(df::DataFrame) = df.colnames
rownames(df::DataFrame) = df.rownames
size(df::DataFrame) = (nrow(df), ncol(df))
size(df::DataFrame, i::Integer) = i==1 ? nrow(df) : (i==2 ? ncol(df) : error("DataFrames have two dimensions only"))

# get columns by name, position
ref(df::DataFrame, i::Int) = df.columns[i]
ref{RT,CT}(df::DataFrame{RT,CT}, name::CT) = df.columns[idxFirstEqual(df.colnames, name)] # TODO make faster
ref{RT,CT}(df::DataFrame{RT,CT}, names::Vector{CT}) = df[[idxFirstEqual(df.colnames, n)::Int | n = names]]
ref(df::DataFrame, is::Vector{Int}) = DataFrame(df.columns[is], df.rownames, df.colnames[is])
ref(df::DataFrame, rng::Range1) = DataFrame(df.columns[rng], df.rownames, df.colnames[rng])
ref(df::DataFrame, pos::Vector{Bool}) = DataFrame(df.columns[pos], df.rownames, df.colnames[pos])

# get slices
# row slices
ref(df::DataFrame, r::Int, rng::Range1) = DataFrame({DataVec[df.columns[c][r]] | c = rng}, 
                                                    [df.rownames[r]], 
                                                    df.colnames[rng])
ref(df::DataFrame, r::Int, cs::Vector{Int}) = DataFrame({DataVec[df.columns[c][r]] | c = cs}, 
                                                        [df.rownames[r]], 
                                                        df.colnames[cs])
ref{RT,CT}(df::DataFrame{RT,CT}, r::Int, cs::Vector{CT}) = DataFrame({DataVec[df.columns[c][r]] | c = cs}, 
                                                                     [df.rownames[r]], 
                                                                     df.colnames[[idxFirstEqual(df.colnames, c)::Int | c = cs]])
ref(df::DataFrame, r::Int, cs::Vector{Bool}) = df[cs][r,:] # possibly slow, but pretty

# 2-D slices
# rows are vector of indexes
ref(df::DataFrame, rs::Vector{Int}, cs::Vector{Int}) = DataFrame({DataVec(df.columns[c][rs]) | c = cs}, 
                                                                 df.rownames[rs], 
                                                                 df.colnames[cs])
ref(df::DataFrame, rs::Vector{Int}, rng::Range1) = DataFrame({DataVec(df.columns[c][rs]) | c = rng}, 
                                                             df.rownames[rs], 
                                                             df.colnames[rng])
ref(df::DataFrame, rs::Vector{Int}, cs::Vector{Bool}) = df[cs][rs,:] # slow way
ref{RT,CT}(df::DataFrame{RT,CT}, rs::Vector{Int}, cs::Vector{CT}) = df[cs][rs,:] # slow way
ref(df::DataFrame, rs::Vector{Int}, c::Int) = df[rs, [c]] # delegate
ref{RT,CT}(df::DataFrame{RT,CT}, rs::Vector{Int}, name::CT) = df[rs, [name]] # delegate
# TODO: other types of row indexing with 2-D slices
# rows are range, vector of booleans, name, or vector of names
# is there a macro way to define all of these??
ref(df::DataFrame, rr::Range1, cr::Range1) = DataFrame({DataVec(df.columns[c][rr]) | c = cr}, 
                                                             df.rownames[rr], 
                                                             df.colnames[cr])


head(df::DataFrame, r::Int) = df[1:r, :]
head(df::DataFrame) = head(df, 6)
tail(df::DataFrame, r::Int) = df[(nrow(df)-r+1):nrow(df), :]
tail(df::DataFrame) = tail(df, 6)


# get singletons. TODO: nicer error handling
# TODO: deal with oddness if row/col types are ints
ref(df::DataFrame, r::Int, c::Int) = df.columns[c][r]
ref{RT,CT}(df::DataFrame{RT,CT}, rn::RT, c::Int) = df.columns[c][idxFirstEqual(df.rownames, rn)]
ref{RT,CT}(df::DataFrame{RT,CT}, r::Int, cn::CT) = df.columns[idxFirstEqual(df.colnames, cn)][r]
ref{RT,CT}(df::DataFrame{RT,CT}, rn::RT, cn::CT) = df.columns[idxFirstEqual(df.colnames, cn)][idxFirstEqual(df.rownames, rn)]


# to print a DataFrame, find the max string length of each column
# and get the max rowname width
# then print the column names with an appropriate buffer
# then row-by-row print with an appropriate buffer
maxShowLength(v::Vector) = length(v) > 0 ? max([length(string(x)) | x = v]) : 0
maxShowLength(dv::DataVec) = max([length(string(x)) | x = dv])
function show(io, df::DataFrame)
    # if we don't have row names, use indexes
    if eltype(df.rownames) == Nothing
        rowNames = [sprintf("[%d,]", r) | r = 1:nrow(df)]
    else
        rowNames = df.rownames
    end
    
    rownameWidth = maxShowLength(rowNames)
    
    # if we don't have columns names, use indexes
    # note that column names in R are obligatory
    if eltype(df.colnames) == Nothing
        colNames = [sprintf("[,%d]", c) | c = 1:ncol(df)]
    else
        colNames = df.colnames
    end
    
    colWidths = [max(length(string(colNames[c])), maxShowLength(df.columns[c])) | c = 1:ncol(df)]
    
    header = strcat(repeat(" ", rownameWidth+1),
                    join([lpad(string(colNames[i]), colWidths[i]+1, " ") | i = 1:ncol(df)], ""))
    println(io, header)
    
    for i = 1:min(100, nrow(df)) # TODO
        rowname = rpad(string(rowNames[i]), rownameWidth+1, " ")
        line = strcat(rowname,
                      join([lpad(string(df[i,c]), colWidths[c]+1, " ") | c = 1:ncol(df)], ""))
        println(io, line)
    end
end

# get the structure of a DF
# TODO: return a string or something instead of printing?
str(df::DataFrame) = str(OUTPUT_STREAM::IOStream, df)
function str(io, df::DataFrame)
    println(io, sprintf("%d observations of %d variables", nrow(df), ncol(df)))

    if eltype(df.colnames) == Nothing
        colNames = [sprintf("[,%d]", c) | c = 1:ncol(df)]
    else
        colNames = df.colnames
    end
    
    # foreach column, print the column name or index, the type, and then print the first elements of 
    # the column until the total column width would exceed a constant
    maxPrintedWidth = 60
    for c in 1:ncol(df)
        printedWidth = 0
        colstr = strcat(string(colNames[c]), ": ", string(eltype(df[c])), " ")
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

# summarize the columns of a DF
# if the column's base type derives from Number, 
# compute min, 1st quantile, median, mean, 3rd quantile, and max
# filtering NAs, which are reported separately
# if boolean, report trues, falses, and NAs
# if anything else, punt.
# Note that R creates a summary object, which has a print method. That's
# a reasonable alternative to this. The summary() functions in show.jl
# return a string.
summary(dv::DataVec) = summary(OUTPUT_STREAM::IOStream, dv)
summary(df::DataFrame) = summary(OUTPUT_STREAM::IOStream, df)
function summary{T<:Number}(io, dv::DataVec{T})
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
function summary{T}(io, dv::DataVec{T})
    # if nothing else, just give the length and element type and NA count
    println(io, "Length: $(length(dv))")
    println(io, "Type  : $(string(eltype(dv)))")
    println(io, "NAs   : $(sum(isna(dv)))")
end

# TODO: clever layout in rows
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
function csvDataFrame(filename)
    dat = csvread(filename)
    
    # if the first row looks like strings, chop it off and process it as the 
    # column names
    if all([typeof(x)==ASCIIString | x = dat[1,:]])
        colNames = [_remove_quotes(x) | x = dat[1,:]]
        dat = dat[2:,:]
    else
        # null column names
        colNames = []
    end
    
    # foreach column, if everything is either numeric or empty string, then build a numeric DataVec
    # otherwise build a string DataVec
    cols = Array(Any, size(dat,2))
    for c = 1:size(dat,2)
        nas = [x == "" | x = dat[:,c]]
        # iterate over the column, ignoring null strings, promoting as we go, until we're done
        # or we hit a non-numeric value, in which case short-circuit
        colType = None
        colIsNum = true
        for r = 1:size(dat,1)
            v = dat[r,c]
            if v != ""
                if isa(v,Number)
                    colType = promote_type(colType, typeof(v))
                else
                    colIsNum = false
                    break
                end
            end
        end
        # build DataVecs
        # TODO: special-case booleans
        if (colIsNum && colType != None)
            # this is annoying to have to pre-allocate the array, but comprehensions don't
            # seem to get the type right
            tmpcol = Array(colType, size(dat,1))
            for r = 1:length(tmpcol)
                tmpcol[r] = dat[r,c] == "" ? false : dat[r,c] # false is the smallest numeric 0
            end
            cols[c] = DataVec(tmpcol, nas)
        else # TODO: other types of strings?
            cols[c] = DataVec([string(_remove_quotes(x))::ASCIIString | x = dat[:,c]], nas)
        end
    end
    
    # combine the columns into a DataFrame and return
    DataFrame(cols, nothings(size(dat,1)), colNames)
end

