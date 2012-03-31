## definitions for "Data" types which can contain NAs
## Inspirations:
## R's NAs
## Panda's discussion of NAs: http://pandas.pydata.org/pandas-docs/stable/missing_data.html
## NumPy's analysis of the issue: https://github.com/numpy/numpy/blob/master/doc/neps/missing-data.rst

## Abstract type is DataVec, which is a parameterized type that wraps an vector of a type and a (bit) array
## for the mask. 

bitstype 64 Mask # TODO

type DataVec{T}
    data::Vector{T}
    na::AbstractVector{Bool} # TODO use a bit array
    
    filter::Bool
    replace::Bool # replace supercedes filter, if both true
    replaceVal::T
    
    # sanity checks
    function DataVec{T}(d::Vector{T}, m::Vector{Bool}, f::Bool, r::Bool, v::T) 
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
DataVec{T}(d::Vector{T}, m::Vector{Bool}, f::Bool, r::Bool, v::T) = DataVec{T}(d, m, f, r, v)

type NAtype; end
const NA = NAtype()
show(x::NAtype) = print("NA")

type NAException <: Exception
    msg::String
end

length(x::NAtype) = 0
size(x::NAtype) = (0,)

# TODO: Move me to a more appropriate spot
# this allows zero(String) to work
oftype(::Type{ASCIIString},c) = repeat(" ",c)


# constructor from type
function ref(::Type{DataVec}, vals...)
    lenvals = length(vals)
    # first, iterate over vals to find the most generic non-NA type
    toptype = None
    for i = 1:lenvals
        if vals[i] != NA
            toptype = promote_type(toptype, typeof(vals[i]))
        end
    end
    
    # TODO: confirm that this type has a zero() 
    
    # then, allocate vectors
    ret = DataVec(Array(toptype, lenvals), falses(lenvals))
    # copy from vals into data and mask
    for i = 1:lenvals
        if vals[i] == NA
            ret.data[i] = zero(toptype)
            ret.na[i] = true
        else
            ret.data[i] = vals[i]
            # ret.na[i] = false (default)
        end
    end
    
    return ret
end

# constructor from base type object
DataVec(x::Vector) = DataVec(x, falses(length(x)))

# TODO: copy_to
# TODO: similar


# properties
size(v::DataVec) = size(v.data)
length(v::DataVec) = length(v.data)
isna(v::DataVec) = v.na
eltype{T}(v::DataVec{T}) = T

# equality, respecting NAs, should be pretty fast
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

# range access
function ref(x::DataVec, r::Range1)
    DataVec(x.data[r], x.na[r])
end

# logical access -- note that unlike Array logical access, this throws an error if
# the index vector is not the same size as the data vector
function ref(x::DataVec, ind::Vector{Bool})
    if length(x) != length(ind)
        throw(ArgumentError("boolean index is not the same size as the DataVec"))
    end
    DataVec(x.data[ind], x.na[ind])
end

# array index access
function ref(x::DataVec, ind::Vector{Int})
    DataVec(x.data[ind], x.na[ind])
end

# assign variants
function assign{T}(x::DataVec{T}, v::T, i::Int)
    x.data[i] = v
    x.na[i] = false
    return x[i]
end
function assign{T}(x::DataVec{T}, v::T, is::Vector{Int})
    x.data[is] = v
    x.na[is] = false
    return x[is] # this could get slow -- maybe not...
end
function assign{T}(x::DataVec{T}, vs::Vector{T}, is::Vector{Int})
    if length(is) != length(vs)
        throw(ArgumentError("can't assign when index and data vectors are different length"))
    end
    x.data[is] = vs
    x.na[is] = false
    return x[is]
end
function assign{T}(x::DataVec{T}, v::T, mask::Vector{Bool})
    x.data[mask] = v
    x.na[mask] = false
    return x[mask]
end
function assign{T}(x::DataVec{T}, vs::Vector{T}, mask::Vector{Bool})
    if sum(mask) != length(vs)
        throw(ArgumentError("can't assign when boolean trues and data vectors are different length"))
    end
    x.data[mask] = vs
    x.na[mask] = false
    return x[mask]
end
function assign{T}(x::DataVec{T}, v::T, rng::Range1)
    x.data[rng] = v
    x.na[rng] = false
    return x[rng]
end
function assign{T}(x::DataVec{T}, vs::Vector{T}, rng::Range1)
    if length(rng) != length(vs)
        throw(ArgumentError("can't assign when index and data vectors are different length"))
    end
    x.data[rng] = vs
    x.na[rng] = false
    return x[rng]
end
assign{T}(x::DataVec{T}, n::NAtype, i::Int) = begin (x.na[i] = true); return x[i]; end
assign{T}(x::DataVec{T}, n::NAtype, is::Vector{Int}) = begin (x.na[is] = true); x[is]; end
assign{T}(x::DataVec{T}, n::NAtype, mask::Vector{Bool}) = begin (x.na[mask] = true); x[mask]; end
assign{T}(x::DataVec{T}, n::NAtype, rng::Range1) = begin (x.na[rng] = true); x[rng]; end


# things to deal with unwanted NAs -- lower case returns the base type, with overhead,
# mixed case returns an iterator
nafilter{T}(v::DataVec{T}) = v.data[!v.na]
nareplace{T}(v::DataVec{T}, r::T) = [v.na[i] ? r : v.data[i] | i = 1:length(v.data)]

# naFilter redefines a new DataVec with a flipped bit that determines how start/next/done operate
naFilter{T}(v::DataVec{T}) = DataVec(v.data, v.na, true, false, v.replaceVal)

# naReplace is similar to naFilter, but with a replacement value
naReplace{T}(v::DataVec{T}, rv::T) = DataVec(v.data, v.na, false, true, rv)

# If neither the filter or replace flags are set, the iterator will return an NA
# when it hits an NA. If one or the other are set, it'll skip/replace the NA.
start(x::DataVec) = 1
function next(x::DataVec, state::Int)
    # if filter is set, iterate til we find a non-NA value
    if x.filter
        for i = state:length(x.data)
            if !x.na[i]
                return (x.data[i], i+1)
            end
        end
        error("called next(DataVec) without calling done() first")
    elseif x.replace
        if x.na[state]
            return (x.replaceVal, state+1)
        else
            return (x.data[state], state+1)
        end
    else
        if x.na[state]
            return(NA, state+1)
        else
            return(x.data[state], state+1)
        end
    end
end
function done(x::DataVec, state::Int)
    # if filter is set, iterate til we find a non-NA value
    if x.filter
        for i = state:length(x.data)
            if !x.na[i]
                return false
            end
        end
        return true
    else # just check lengths
        return state > length(x.data)
    end
end

# can promote in theory based on data type
promote_rule{S,T}(::Type{DataVec{S}}, ::Type{T}) = promote_rule(S, T)
promote_rule{T}(::Type{DataVec{T}}, ::Type{T}) = T

# convert to the base type, but only if there are no NAs
function convert{T}(::Type{T}, x::DataVec{T})
    if (any(x.na))
        throw(NAException("Cannot convert DataVec with NAs to base type -- naFilter or naReplace them first"))
    else
        return x.data
    end
end

# a DataVec is not a StridedArray, so sum() and similar picks up the itr version, which will work without 
# additional code

# print
show(x::DataVec) = show_comma_array(x, '[', ']') 

# ## DataTable - a list of heterogeneous Data vectors with row and col names
# 
# 
# 

