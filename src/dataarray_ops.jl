
# NA operations for DataArray using NumericExtensions

using NumericExtensions
import NumericExtensions.evaluate, NumericExtensions.result_type
import NumericExtensions.reduced_size

import Base.sum, Base.prod, Base.max, Base.min, Base.cumsum, Base.cumprod

typealias DimSpec Union(Int, (Int, Int))

# Functors to return additive and multiplicative identity for NA
type NA2Zero <: BinaryFunctor; end
evaluate(::NA2Zero, x, y::Bool) = y ? zero(typeof(x)) : x
result_type{T<:Number}(::NA2Zero, ::Type{T}, ::Type{Bool}) = T

type NA2One <: BinaryFunctor; end
evaluate(::NA2One, x, y::Bool) = y ? one(typeof(x)) : x
result_type{T<:Number}(::NA2One, ::Type{T}, ::Type{Bool}) = T

type NA2Min <: BinaryFunctor; end
evaluate(::NA2Min, x, y::Bool) = y ? typemin(typeof(x)) : x
result_type{T<:Number}(::NA2Min, ::Type{T}, ::Type{Bool}) = T

type NA2Max <: BinaryFunctor; end
evaluate(::NA2Max, x, y::Bool) = y ? typemax(typeof(x)) : x
result_type{T<:Number}(::NA2Max, ::Type{T}, ::Type{Bool}) = T


# Functor to return true if data is NOT NA (useful to get the number of non-NA)
# entries.
type NotNA <: UnaryFunctor; end
evaluate(::NotNA, y::Bool) = !y
result_type(::NotNA, ::Type{Bool}) = Bool

# Can make this a noop when NumericExtensions handles BitArrays.
macro BA(x)
    :(convert(Array{Bool,ndims($x)}, $x))
end

# TODO: Make handling NA depend on a flag
# TODO: Implement median, std, var, mad, norm, skewness, kurtosis
# TODO: Implement inplace versions of functions that don't need to allocate
#       space for result.
# Think about median as it's a bit tricky b/c have to actually get rid of the
# NAs which I'm not sure is possible with mapreduce.
#

# These are placeholders for now b/c NumericExtensions doesn't work with
# BitArrays.

# Entire Array
sum{T<:Number}(da::DataArray{T}) = isempty(da) ? zero(T) : 
    mapreduce(NA2Zero(), Add(), da.data, @BA(da.na))
prod{T<:Number}(da::DataArray{T}) = isempty(da) ? one(T) :
    mapreduce(NA2One(), Multiply(), da.data, @BA(da.na))
max{T<:Number}(da::DataArray{T}) = isempty(da) ? throw(ArgumentError("Empty error not allowed")) :
    mapreduce(NA2Min(), Max(), da.data, @BA(da.na))
min{T<:Number}(da::DataArray{T}) = isempty(da) ? throw(ArgumentError("Empty error not allowed")) :
    mapreduce(NA2Max(), Min(), da.data, @BA(da.na))

# Reduce along dimensions
sum{T<:Number}(da::DataArray{T}, dims::DimSpec) = isempty(da) ? zeros(T, reduced_size(size(da), dims)) :
    mapreduce(NA2Zero(), Add(), da.data, @BA(da.na), dims)
prod{T<:Number}(da::DataArray{T}, dims::DimSpec) = isempty(da) ? ones(T, reduced_size(size(da), dims)) :
    mapreduce(NA2One(), Multiply(), da.data, @BA(da.na), dims)
max{T<:Number}(da::DataArray{T}, dims::DimSpec) = isempty(da) ? throw(ArguemntError("Empty array not allowed")) :
    mapreduce(NA2Min(), Max(), da.data, @BA(da.na), dims)
min{T<:Number}(da::DataArray{T}, dims::DimSpec) = isempty(da) ? throw(ArguementError("Empty array not allowed")) :
    mapreduce(NA2Max(), Min(), da.data, @BA(da.na), dims)

function mean{T<:Number}(da::DataArray{T})
    if isempty(da)
        return zero(T)
    end
    na = @BA(da.na)
    s = mapreduce(NA2Zero(), Add(), da.data, na)
    nn = mapreduce(NotNA(), Add(), na)
    s ./ nn
end
function mean{T<:Number}(da::DataArray{T}, dims::DimSpec)
    if isempty(da)
        return zeros(T, reduced_size(size(da), dims))
    end
    na = @BA(da.na)
    s = mapreduce(NA2Zero(), Add(), da.data, na, dims)
    nn = mapreduce(NotNA(), Add(), na, dims)
    map(Divide(), s, nn)
end


# Dimensionless version only defined for vectors
function cumsum{T<:Number}(da::DataArray{T,1})
    if isempty(da)
        return zero(T)
    end
    c = DataArray(Array(T,size(da.data)), da.na)
    mapscan!(c.data, NA2Zero(), Add(), da.data, @BA(da.na))
    c
end
function cumsum{T<:Number}(da::DataArray{T}, dims::DimSpec)
    if isempty(da)
        return zeros(T, reduced_size(size(da), dims))
    end
    c = DataArray(Array(T,size(da.data)), da.na)
    mapscan!(c.data, NA2Zero(), Add(), da.data, @BA(da.na), dims)
    c
end

# Dimensionless version only defined for vectors
function cumprod{T<:Number}(da::DataArray{T,1})
    if isempty(da)
        return one(T)
    end
    c = DataArray(Array(T,size(da.data)), da.na)
    mapscan!(c.data, NA2One(), Multiply(), da.data, @BA(da.na))
    c
end
function cumprod{T<:Number}(da::DataArray{T}, dims::DimSpec)
    if isempty(da)
        return ones(T, reduced_size(size(da), dims))
    end
    c = DataArray(Array(T,size(da.data)), da.na)
    mapscan!(c.data, NA2One(), Multiply(), da.data, @BA(da.na), dims)
    c
end


# Basically just copy the var, std, etc. functions from Numeric Extensions as
# they're hand-coded there.



# Inplace versions
