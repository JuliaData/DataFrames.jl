##############################################################################
##
## DataArray is a generalization of DataVec
##
##############################################################################

##############################################################################
##
## DataArray type definition
##
##############################################################################

abstract AbstractDataArray{T}

type DataArray{T} <: AbstractDataArray{T}
    data::Array{T}
    na::BitArray

    # Sanity check that new data values and missingness metadata match
    function DataArray(new_data::Array{T}, is_missing::BitArray)
        if size(new_data) != size(is_missing)
            error("Data and missingness arrays must be the same size!")
        end
        new(new_data, is_missing)
    end
end

##############################################################################
##
## DataArray constructors
##
##############################################################################

# Need to redefine inner constructor as outer constuctor
DataArray{T}(d::Array{T}, n::BitArray) = DataArray{T}(d, n)

# Convert Array{Bool}'s to BitArray's to save space
DataArray{T}(d::Array{T}, m::Array{Bool}) = DataArray{T}(d, bitpack(m))

# Explicitly convert an existing array to a DataArray w/ no NA's
DataArray(x::Array) = DataArray(x, falses(size(x)))

# A no-op constructor
DataArray(d::DataArray) = d

# Construct an all-NA DataArray of a specific type
DataArray(t::Type, n::Int64, p::Int64) = DataArray(Array(t, n, p), trues(n, p))

# copy does a deep copy
copy{T}(dm::DataArray{T}) = DataArray{T}(copy(dm.data), copy(dm.na))

# TODO: copy_to

##############################################################################
##
## Basic size properties of all Data* objects
##
##############################################################################

size(v::DataArray) = size(v.data)
ndims(v::DataArray) = ndims(v.data)
numel(v::DataArray) = numel(v.data)
eltype{T}(v::DataArray{T}) = T

##############################################################################
##
## A new predicate: isna()
##
##############################################################################

isna(v::DataArray) = v.na

##############################################################################
##
## ref()/assign() definitions
##
##############################################################################

# single-element access
ref{T}(a::DataArray{T}, i::Number) = a.na[i] ? NA : a.data[i]
ref{T}(a::DataArray{T}, i::Number, j::Number) = a.na[i, j] ? NA : a.data[i, j]
ref{T}(a::DataArray{T}, i::Number, j::Number, k::Number) = a.na[i, j, k] ? NA : a.data[i, j, k]
ref{T}(a::DataArray{T}, i::Number...) = a.na[i] ? NA : a.data[i]

# range access
function ref(x::DataArray, r1::Range1)
    DataArray(x.data[r1], x.na[r1])
end
function ref(x::DataArray, r1::Range1, r2::Range1)
    DataArray(x.data[r1, r2], x.na[r1, r2])
end
#...

# logical access
function ref(x::DataArray, ind::Array{Bool})
    DataArray(x.data[ind], x.na[ind])
end

# array index access
function ref(x::DataArray, ind::Array{Int})
    DataArray(x.data[ind], x.na[ind])
end

# assign variants
# x[3] = "cat"
function assign{S, T}(x::DataArray{S}, v::T, i::Int)
    x.data[i] = v
    x.na[i] = false
    return x[i]
end

# x[[3, 4]] = "cat"
function assign{S, T}(x::DataArray{S}, v::T, is::Array{Int})
    x.data[is] = v
    x.na[is] = false
    return x[is]
end

# x[[3, 4]] = ["cat", "dog"]
function assign{S, T}(x::DataArray{S}, vs::Array{T}, is::Array{Int})
    x.data[is] = vs
    x.na[is] = false
    return x[is]
end

# x[[true, false, true]] = "cat"
function assign{S, T}(x::DataArray{S}, v::T, mask::Array{Bool})
    x.data[mask] = v
    x.na[mask] = false
    return x[mask]
end

# x[[true, false, true]] = ["cat", "dog"]
function assign{S, T}(x::DataArray{S}, vs::Array{T}, mask::Array{Bool})
    x.data[mask] = vs
    x.na[mask] = false
    return x[mask]
end

# x[2:3] = "cat"
function assign{S, T}(x::DataArray{S}, v::T, rng::Range1)
    x.data[rng] = v
    x.na[rng] = false
    return x[rng]
end

# x[2:3] = ["cat", "dog"]
function assign{S, T}(x::DataArray{S}, vs::Array{T}, rng::Range1)
    x.data[rng] = vs
    x.na[rng] = false
    return x[rng]
end

# x[3] = NA
assign{T}(x::DataArray{T}, n::NAtype, i::Int) = begin (x.na[i] = true); return NA; end

# x[[3,5]] = NA
assign{T}(x::DataArray{T}, n::NAtype, is::Array{Int}) = begin (x.na[is] = true); return x[is]; end

# x[[true, false, true]] = NA
assign{T}(x::DataArray{T}, n::NAtype, mask::Array{Bool}) = begin (x.na[mask] = true); return x[mask]; end

# x[2:3] = NA
assign{T}(x::DataArray{T}, n::NAtype, rng::Range1) = begin (x.na[rng] = true); return x[rng]; end

##############################################################################
##
## Conversion and promotion
##
##############################################################################

# TODO: Abstract? Pooled?

# Can promote in theory based on data type

promote_rule{T, T}(::Type{DataArray{T}}, ::Type{T}) = promote_rule(T, T)
promote_rule{S, T}(::Type{DataArray{S}}, ::Type{T}) = promote_rule(S, T)
promote_rule{T}(::Type{DataArray{T}}, ::Type{T}) = T

function convert{T}(::Type{T}, x::DataArray{T})
    if any_na(x)
        err = "Cannot convert DataArray  with NA's to base type"
        throw(NAException(err))
    else
        return x.data
    end
end
function convert{S, T}(::Type{S}, x::DataArray{T})
    if any_na(x)
        err = "Cannot convert DataArray with NA's to base type"
        throw(NAException(err))
    else
        return convert(S, x.data)
    end
end

##############################################################################
##
## String representations and printing
##
##############################################################################

function string(x::DataArray)
    tmp = join(x, ", ")
    return "[$tmp]"
end

show(io, x::DataArray) = Base.show_comma_array(io, x, '[', ']')

function repl_show(io::IO, dv::DataArray)
    s = size(dv)
    print("BLAH!")
end

##############################################################################
##
## Convenience predicates: any_na, isnan, isfinite
##
##############################################################################

function any_na(dv::DataArray)
    for i in 1:numel(dv)
        if dv.na[i]
            return true
        end
    end
    return false
end

function isnan(dv::DataArray)
    new_data = isnan(dv.data)
    DataArray(new_data, dv.na)
end

function isfinite(dv::DataArray)
    new_data = isfinite(dv.data)
    DataArray(new_data, dv.na)
end
