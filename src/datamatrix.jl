##############################################################################
##
## DataMatrix is a 2D generalization of DataVec
##
##############################################################################

##############################################################################
##
## DataMatrix type definition
##
##############################################################################

abstract AbstractDataMatrix{T} <: AbstractMatrix{T}

type DataMatrix{T} <: AbstractDataMatrix{T}
    data::Matrix{T}
    na::BitMatrix

    # Sanity check that new data values and missingness metadata match
    function DataMatrix(new_data::Matrix{T}, is_missing::BitMatrix)
        if size(new_data) != size(is_missing)
            error("Data and missingness matrices must be the same size!")
        end
        new(new_data, is_missing)
    end
end

##############################################################################
##
## DataMatrix constructors
##
##############################################################################

# Need to redefine inner constructor as outer constuctor
DataMatrix{T}(d::Matrix{T}, n::BitMatrix) = DataMatrix{T}(d, n)

# Convert Vector{Bool}'s to BitArray's to save space
DataMatrix{T}(d::Matrix{T}, m::Matrix{Bool}) = DataMatrix{T}(d, bitpack(m))

# Explicitly convert an existing matrix to a DataMatrix w/ no NA's
DataMatrix(x::Matrix) = DataMatrix(x, falses(size(x)))

# Explicitly convert a BitMatrix to a Matrix before wrapping with a DataMatrix
DataMatrix(x::BitMatrix, m::BitMatrix) = DataMatrix(convert(Matrix{Bool}, x), m)

# Explicitly convert a BitMatrix to a DataMatrix w/ no NA's
DataMatrix(x::BitMatrix) = DataMatrix(convert(Matrix{Bool}, x), falses(size(x)))

# A no-op constructor
DataMatrix(d::DataMatrix) = d

# Construct an all-NA DataMatrix of a specific type
DataMatrix(t::Type, n::Int64, p::Int64) = DataMatrix(Array(t, n, p), trues(n, p))

# Construct an all-NA DataMatrix of Float64's
DataMatrix(n::Int64, p::Int64) = DataMatrix(Array(Float64, n, p), trues(n, p))

# Construct an empty DataMatrix of a specific type
DataMatrix(t::Type) = DataMatrix(Array(t, 0, 0), trues(0, 0))

# Construct an empty DataMatrix of Float64's
DataMatrix() = DataMatrix(Array(Float64, 0, 0), trues(0, 0))

# Initialized constructors with 0's, 1's
for (f, basef) in ((:dmzeros, :zeros), (:dmones, :ones), (:dmeye, :eye))
    @eval begin
        ($f)(n::Int64, p::Int64) = DataMatrix(($basef)(n, p), falses(n, p))
        ($f)(t::Type, n::Int64, p::Int64) = DataMatrix(($basef)(t, n, p), falses(n, p))
    end
end

# Initialized constructors with false's or true's
for (f, basef) in ((:dmfalses, :falses), (:dmtrues, :trues))
    @eval begin
        ($f)(n::Int64, p::Int64) = DataMatrix(($basef)(n, p), falses(n, p))
    end
end

# Single-argument dmeye
for (f, basef) in ((:dmeye, :eye), )
    @eval begin
        ($f)(n::Int64) = DataMatrix(($basef)(n), falses(n, n))
    end
end

# Initialized constructors based on diagonal
for (f, basef) in ((:dmdiagm, :diagm), )
    @eval begin
        ($f)(vals::Vector) = DataMatrix(($basef)(vals), falses(length(vals), length(vals)))
    end
end

##############################################################################
##
## Copying operations
##
##############################################################################

# copy does a deep copy
copy{T}(dm::DataMatrix{T}) = DataMatrix{T}(copy(dm.data), copy(dm.na))

# TODO: copy_to

##############################################################################
##
## Basic size properties of DataMatrix objects
##
##############################################################################

size(v::DataMatrix) = size(v.data)
ndims(v::DataMatrix) = ndims(v.data)
numel(v::DataMatrix) = numel(v.data)
eltype{T}(v::DataMatrix{T}) = T

##############################################################################
##
## A new predicate: isna()
##
##############################################################################

isna(v::DataMatrix) = v.na

##############################################################################
##
## ref() definitions
##
##############################################################################

# single-element access
ref{T}(a::DataMatrix{T}, i::Int, j::Int) = a.na[i, j] ? NA : a.data[i, j]

# single-element access without regard to size
ref{T}(a::DataMatrix{T}, i::Int) = a.na[i] ? NA : a.data[i]

# int w/ range access
function ref(x::DataMatrix, i::Int, r2::Range1)
    DataMatrix(x.data[i, r2], x.na[i, r2])
end

# range w/ int access
function ref(x::DataMatrix, r1::Range1, j::Int)
    DataVec(x.data[r1, j], x.na[r1, j])
end

# range access
function ref(x::DataMatrix, r1::Range1, r2::Range1)
    DataMatrix(x.data[r1, r2], x.na[r1, r2])
end

# logical access
function ref(x::DataMatrix, ind1::Vector{Bool}, ind2::Vector{Bool})
    DataMatrix(x.data[ind1, ind2], x.na[ind1, ind2])
end

# array index access
function ref(x::DataMatrix, ind1::Vector{Int}, ind2::Vector{Int})
    DataMatrix(x.data[ind1, ind2], x.na[ind1, ind2])
end

##############################################################################
##
## assign() definitions
##
##############################################################################

#
# Assignment of NA's
#

# single-element access without regard to size
function assign{T}(a::DataMatrix{T}, n::NAtype, i::Int)
    a.na[i] = true
    return NA
end

# x[3, 1] = NA
function assign{T}(x::DataMatrix{T}, n::NAtype, i::Int, j::Int)
	x.na[i, j] = true
	return NA
end

# x[[3,5], [1, 1]] = NA
function assign{T}(x::DataMatrix{T}, n::NAtype, ind1::Vector{Int}, ind2::Vector{Int})
	x.na[ind1, ind2] = true
	return NA
end

# x[[true, false, true], [true, true, true]] = NA
function assign{T}(x::DataMatrix{T}, n::NAtype, ind1::Vector{Bool}, ind2::Vector{Bool})
	x.na[ind1, ind2] = true
	return NA
end

# x[2:3, 1:2] = NA
function assign{T}(x::DataMatrix{T}, n::NAtype, ind1::Range1, ind2::Range1)
	x.na[ind1, ind2] = true
	return NA
end

#
# Generic assignments
#

# x[3] = "cat"
function assign{S, T}(a::DataMatrix{S}, v::T, i::Int)
    a.data[i] = v
    a.na[i] = false
    return NA
end

# x[3, 1] = "cat"
function assign{S, T}(x::DataMatrix{S}, v::T, i::Int, j::Int)
    x.data[i, j] = v
    x.na[i, j] = false
    return x[i, j]
end

# x[[3, 4], [1, 2]] = "cat"
function assign{S, T}(x::DataMatrix{S}, v::T, ind1::Vector{Int}, ind2::Vector{Int})
    x.data[ind1, ind2] = v
    x.na[ind1, ind2] = false
    return x[ind1, ind2]
end

# x[[3, 4], [1, 2]] = ["cat", "dog"]
function assign{S, T}(x::DataMatrix{S}, vals::Vector{T}, ind1::Vector{Int}, ind2::Vector{Int})
    x.data[ind1, ind2] = vals
    x.na[ind1, ind2] = false
    return x[ind1, ind2]
end

# x[[true, false, true], [false, false]] = "cat"
function assign{S, T}(x::DataMatrix{S}, v::T, ind1::Vector{Bool}, ind2::Vector{Bool})
    x.data[ind1, ind2] = v
    x.na[ind1, ind2] = false
    return x[ind1, ind2]
end

# x[[true, false, true], [false, false]] = ["cat", "dog"]
function assign{S, T}(x::DataMatrix{S}, vals::Vector{T}, ind1::Vector{Bool}, ind2::Vector{Bool})
    x.data[ind1, ind2] = vals
    x.na[ind1, ind2] = false
    return x[ind1, ind2]
end

# x[2:3, 1:2] = "cat"
function assign{S, T}(x::DataMatrix{S}, v::T, ind1::Range1, ind2::Range1)
    x.data[ind1, ind2] = v
    x.na[ind1, ind2] = false
    return x[ind1, ind2]
end

# x[2:3, 1:2] = ["cat", "dog"]
function assign{S, T}(x::DataMatrix{S}, vals::Vector{T}, ind1::Range1, ind2::Range1)
    x.data[ind1, ind2] = vals
    x.na[ind1, ind2] = false
    return x[ind1, ind2]
end

##############################################################################
##
## Conversion and promotion
##
##############################################################################

promote_rule{T, T}(::Type{DataMatrix{T}}, ::Type{T}) = promote_rule(T, T)
promote_rule{S, T}(::Type{DataMatrix{S}}, ::Type{T}) = promote_rule(S, T)
promote_rule{T}(::Type{DataMatrix{T}}, ::Type{T}) = T

function convert{N}(::Type{BitArray{N}}, dm::DataMatrix{BitArray{N}})
	error("How in the world did you get here?!")
end

function convert{N,T}(::Type{BitArray{N}}, dm::DataMatrix{T})
	error("Don't try to convert a DataMatrix to a BitArray")
end

function convert{T}(::Type{T}, x::DataMatrix{T})
    if any_na(x)
        err = "Cannot convert DataMatrix  with NA's to base type"
        throw(NAException(err))
    else
        return x.data
    end
end

function convert{S, T}(::Type{S}, x::DataMatrix{T})
    if any_na(x)
        err = "Cannot convert DataMatrix with NA's to base type"
        throw(NAException(err))
    else
        return convert(S, x.data)
    end
end

function DataMatrix(df::DataFrame)
    ts = coltypes(df)
    for t in ts
        if !(t <: Number)
            error("Convert convert a non-numeric DataFrame to a DataMatrix")
        end
    end
    n, p = size(df)
    dm = dmzeros(n, p)
    for i in 1:n
        for j in 1:p
            dm[i, j] = df[i, j]
        end
    end
    return dm
end

##############################################################################
##
## Iteration
##
##############################################################################

function start{T}(dm::DataMatrix{T})
	return 1
end

function next{T}(dm::DataMatrix{T}, ind::Int)
	return (dm[ind], ind + 1)
end

function done{T}(dm::DataMatrix{T}, ind::Int)
	return ind > numel(dm)
end

##############################################################################
##
## String representations and printing
##
##############################################################################

# Now correctly inherited from AbstractMatrix

##############################################################################
##
## Convenience predicates: any_na, isnan, isfinite
##
##############################################################################

function any_na(dm::DataMatrix)
    for i in 1:numel(dm)
        if dm.na[i]
            return true
        end
    end
    return false
end

function isnan(dm::DataMatrix)
    new_data = isnan(dm.data)
    DataMatrix(new_data, dm.na)
end

function isfinite(dm::DataMatrix)
    new_data = isfinite(dm.data)
    DataMatrix(new_data, dm.na)
end

function diag{T}(dm::DataMatrix{T})
    return DataVec(diag(dm.data), diag(dm.na))
end

nrow{T}(dm::DataMatrix{T}) = size(dm, 1)
ncol{T}(dm::DataMatrix{T}) = size(dm, 2)

function matrix{T}(dm::DataMatrix{T})
    if any_na(dm)
        error("Can't convert a DataMatrix with missing entries")
    else
        return dm.data
    end
end
