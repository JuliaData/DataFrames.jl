unary_operators = [:(+), :(-), :(!), :(*)]

elementary_functions = [:abs, :sign, :acos, :acosh, :asin,
                        :asinh, :atan, :atanh, :sin, :sinh,
                        :cos, :cosh, :tan, :tanh, :ceil, :floor,
                        :round, :trunc, :exp, :exp2, :expm1, :log, :log10, :log1p,
                        :log2, :exponent, :sqrt, :gamma, :lgamma, :digamma,
                        :erf, :erfc]

vectorized_comparison_operators = [(:(.==), :(==)), (:(.!=), :(!=)),
                                   (:(.>), :(>)), (:(.>=), :(>=)),
                                   (:(.<), :(<)), (:(.<=), :(<=))]

binary_operators = [:(+), :(.+), :(-), :(.-), :(*), :(.*), :(/), :(./),
                    :(.^), :(div), :(mod), :(fld), :(rem)]

arithmetic_operators = [:(+), :(.+), :(-), :(.-), :(*), :(.*), :(/), :(./),
                        :(.^), :(div), :(mod), :(fld), :(rem)]
                        
array_arithmetic_operators = [:(+), :(.+), :(-), :(.-), :(.*), :(.^)]

biscalar_operators = [:(max), :(min)]

unary_vector_operators = [:min, :max, :prod, :sum, :mean, :median, :std,
                          :var, :mad, :norm, :skewness, :kurtosis]

# TODO: dist, iqr, rle, inverse_rle

pairwise_vector_operators = [:diff, :reldiff, :percent_change]

cumulative_vector_operators = [:cumprod, :cumsum, :cumsum_kbn, :cummin, :cummax]

ffts = [:fft]

binary_vector_operators = [:dot, :cor, :cov,
                           :cor_spearman, :cov_spearman]

rowwise_operators = [:rowmins, :rowmaxs, :rowprods, :rowsums,
                     :rowmeans, :rowmedians, :rowstds, :rowvars,
                     :rowffts, :rownorms]

columnar_operators = [:colmins, :colmaxs, :colprods, :colsums,
                      :colmeans, :colmedians, :colstds, :colvars,
                      :colffts, :colnorms]

boolean_operators = [:any, :all]

# Unary operators for DataFrames and DataArrays
macro dataframe_unary(f)
    esc(:($(f)(d::DataFrame) = DataFrame([$(f)(d[i]) for i=1:size(d, 2)], deepcopy(index(d)))))
end

# Binary operators
macro dataframe_binary(f)
    esc(quote
        function $(f)(a::DataFrame, b::DataFrame)
            if size(a) != size(b); error("argument dimensions must match"); end
            DataFrame([$(f)(a[i], b[i]) for i=1:size(a, 2)], deepcopy(index(a)))
        end
        @swappable $(f)(a::DataFrame, b::Union(Number, String)) = 
            DataFrame([$(f)(a[i], b) for i=1:size(a, 2)], deepcopy(index(a)))
        @swappable $(f)(a::DataFrame, b::NAtype) = 
            DataFrame([$(f)(a[i], b) for i=1:size(a, 2)], deepcopy(index(a)))
    end)
end

# Swap arguments to fname() anywhere in AST. Returns the number of
# arguments swapped
function swapargs(ast::Expr, fname::Symbol)
    if ast.head == :call &&
       (ast.args[1] == fname ||
        (isa(ast.args[1], Expr) && ast.args[1].head == :curly &&
         ast.args[1].args[1] == fname)) &&
       length(ast.args) == 3

        ast.args[2], ast.args[3] = ast.args[3], ast.args[2]
        1
    else
        n = 0
        for arg in ast.args
            n += swapargs(arg, fname)
        end
        n
    end
end
function swapargs(ast, fname::Symbol)
    ast
    0
end

# Return a block consisting of both the given function and a copy of
# the function in which arguments to the function itself and any
# 2-argument calls to a function of the same name are swapped
macro swappable(func, syms...)
    if (func.head != :function && func.head != :(=)) ||
       func.args[1].head != :call || length(func.args[1].args) != 3
        error("@swappable may only be applied to functions of two arguments")
    end
    
    func2 = deepcopy(func)
    fname = func2.args[1].args[1]
    if isa(fname, Expr)
        if fname.head == :curly
            fname = fname.args[1]
        else
            error("Unexpected function name $fname")
        end
    end

    for s in unique([fname, syms...])
        if swapargs(func2, s) < 1
            error("No argument swapped")
        end
    end
    esc(Expr(:block, func, func2))
end

# Unary operators, NA
for f in (:(+), :(-), :(!), :(*))
    @eval $(f)(d::NAtype) = NA
end

# Unary operators, DataArrays. Definitions in base should be adequate
# for AbstractDataArrays. These are just optimizations
!(d::DataArray{Bool}) = DataArray(!d.data, copy(d.na))
-(d::DataArray) = DataArray(-d.data, copy(d.na))

# Unary operators, DataFrames
@dataframe_unary !
@dataframe_unary -
# As in Base, these are identity operators
for f in (:(+), :(*))
    @eval $(f)(d::DataFrame) = d
end

# Treat ctranspose and * in a special way for now
for f in (:ctranspose, :transpose)
    @eval $(f)(d::DataArray) = DataArray($(f)(d.data), d.na')
end

# Propagates NA's
# For a dissenting view,
# http://radfordneal.wordpress.com/2011/05/21/slowing-down-matrix-multiplication-in-r/
# But we're getting 10x R while maintaining NA's
for (adata, bdata) in ((true, false), (false, true), (true, true))
    @eval begin
        function (*)(a::$(adata ? :(Union(DataVector, DataMatrix)) : :(Union(Vector, Matrix))),
                     b::$(bdata ? :(Union(DataVector, DataMatrix)) : :(Union(Vector, Matrix))))
            c = $(adata ? :(a.data) : :a) * $(bdata ? :(b.data) : :b)
            res = DataArray(c, falses(size(c)))
            # Propagation can be made more efficient by storing record of corrupt
            # rows and columns, then doing fast edits.
            $(if adata
                quote
                    n1 = size(a, 1)
                    p1 = size(a, 2)
                    corrupt_rows = falses(n1)
                    for j in 1:p1, i in 1:n1
                        # Propagate NA's
                        # Corrupt all rows based on i
                        corrupt_rows[i] |= a.na[i, j]
                    end
                    res.na[corrupt_rows, :] = true
                end
            end)
            $(if bdata
                quote
                    n2 = size(b, 1)
                    p2 = size(b, 2)
                    corrupt_cols = falses(p2)
                    for j in 1:p2, i in 1:n2
                        # Propagate NA's
                        # Corrupt all columns based on j
                        corrupt_cols[j] |= b.na[i, j]
                    end
                    res.na[:, corrupt_cols] = true
                end
            end)
            res
        end
    end
end

#
# Elementary functions
#
# XXX: The below should be revisited once we have a way to infer what
# the proper return type of an array should be.

# Apply unary operator to non-NA members of a DataArray or
# AbstractDataArray
macro dataarray_unary(f, intype, outtype)
    esc(quote
        function $(f){T<:$(intype)}(d::DataArray{T})
            data = similar(d.data, $(outtype))
            for i = 1:length(data)
                if !d.na[i]
                    data[i] = $(f)(d[i])
                end
            end
            DataArray(data, copy(d.na))
        end
        function $(f){T<:$(intype)}(adv::AbstractDataArray{T})
            # XXX Make sure similar actually works for AbstractDataArray types
            # defined in DataFrames
            res = similar(adv, $(outtype))
            for i = 1:length(adv)
                res[i] = ($f)(adv[i])
            end
            res
        end
    end)
end

# One-argument elementary functions that return the same type as their
# inputs
for f in (:abs, :sign)
    @eval begin
        $(f)(::NAtype) = NA
        @dataarray_unary $(f) Number T
        @dataframe_unary $(f)
    end
end

# One-argument elementary functions that always return floating points
for f in (:acos, :acosh, :asin, :asinh, :atan, :atanh, :sin, :sinh, :cos,
          :cosh, :tan, :tanh, :ceil, :floor, :round, :trunc, :exp, :exp2,
          :expm1, :log, :log10, :log1p, :log2, :exponent, :sqrt, :gamma,
          :lgamma, :digamma, :erf, :erfc)
    @eval begin
        ($f)(::NAtype) = NA
        @dataarray_unary $(f) FloatingPoint T
        @dataarray_unary $(f) Real Float64
        @dataframe_unary $(f)
    end
end

# Elementary functions that take varargs
for f in (:round, :ceil, :floor, :trunc)
    @eval begin
        ($f)(::NAtype, args...) = NA

        function $(f){T<:Number}(d::DataArray{T}, args...)
            data = similar(d.data)
            for i = 1:length(data)
                if !d.na[i]
                    data[i] = $(f)(d[i], args...)
                end
            end
            DataArray(data, copy(d.na))
        end
        function $(f){T<:Number}(adv::AbstractDataArray{T}, args...)
            # XXX Make sure similar actually works for
            # AbstractDataArray types defined in DataFrames
            res = similar(adv)
            for i = 1:length(adv)
                res[i] = ($f)(adv[i], args...)
            end
            res
        end
        $(f)(d::DataFrame, args...) = 
                DataFrame([$(f)(d[i], args...) for i=1:size(d, 2)], deepcopy(index(d)))
    end
end

#
# Bit operators
#

@swappable (&)(a::NAtype, b::Bool) = b ? NA : false
@swappable (|)(a::NAtype, b::Bool) = b ? true : NA
@swappable ($)(a::NAtype, b::Bool) = NA

# To avoid ambiguity warning
@swappable (|)(a::NAtype, b::Function) = NA

for f in (:&, :|, :$)
    @eval begin
        # Scalar with NA
        ($f)(::NAtype, ::NAtype) = NA
        @swappable ($f)(::NAtype, b) = NA
        # DataFrame
        @dataframe_binary $(f)
    end
end

# DataArray with DataArray
(&)(a::DataArray{Bool}, b::DataArray{Bool}) =
    DataArray(a.data & b.data, (a.na & b.data) | (b.na & a.data))
(|)(a::DataArray{Bool}, b::DataArray{Bool}) =
    DataArray(a.data | b.data, (a.na & !b.data) | (b.na & !a.data))
($)(a::DataArray{Bool}, b::DataArray{Bool}) =
    DataArray(a.data $ b.data, a.na | b.na)

# DataArray with non-DataArray
# Need explicit definition for BitArray to avoid ambiguity
for t in (:(BitArray), :(Union(AbstractArray{Bool}, Bool)))
    @eval begin
        @swappable (&)(a::DataArray{Bool}, b::$t) = DataArray(a.data & b, a.na & b)
        @swappable (|)(a::DataArray{Bool}, b::$t) = DataArray(a.data | b, a.na & !b)
        @swappable ($)(a::DataArray{Bool}, b::$t) = DataArray(a.data $ b, a.na)
    end
end

#
# Comparison operators
#

# Binary operators with one scalar argument
macro dataarray_binary_scalar(vectorfunc, scalarfunc, outtype)
    esc(Expr(:block,
        # DataArray and AbstractDataArray with scalar
        # XXX It would be really nice to make this work with arbitrary
        # types, but doing so results in a bunch of method ambiguity
        # warnings
        {
            quote
                @swappable function $(vectorfunc)(a::DataArray, b::$t)
                    res = DataArray(similar(a.data, $outtype), copy(a.na))
                    for i = 1:length(a)
                        if !res.na[i]
                            res.data[i] = $(scalarfunc)(a.data[i], b)
                        end
                    end
                    res
                end $scalarfunc
                @swappable function $(vectorfunc)(a::AbstractDataArray, b::$t)
                    res = similar(a, $outtype)
                    for i = 1:length(a)
                        res[i] = $(scalarfunc)(a[i], b)
                    end
                    res
                end $scalarfunc
            end
            for t in (:String, :Number)
        }...
    ))
end

# Binary operators with two array arguments
macro dataarray_binary_array(vectorfunc, scalarfunc, outtype)
    esc(Expr(:block,
        # DataArray with other array
        {
            quote
                function $(vectorfunc)(a::$(adata ? :DataArray : :AbstractArray),
                                       b::$(bdata ? :DataArray : :AbstractArray))
                    res = DataArray(Array($outtype, promote_shape(size(a), size(b))), $narule)
                    for i = 1:length(res)
                        if !res.na[i]
                            res.data[i] = $(scalarfunc)($(adata ? :(a.data) : :a)[i],
                                                        $(bdata ? :(b.data) : :b)[i])
                        end
                    end
                    res
                end
            end
            for (adata, bdata, narule) in ((true, true, :(a.na | b.na)),
                                           (true, false, :(copy(a.na))),
                                           (false, true, :(copy(b.na))))
        }...,
        # AbstractDataArray with other array
        # Definitinons with DataArray necessary to avoid ambiguity
        {
            quote
                function $(vectorfunc)(a::$atype, b::$btype)
                    res = similar($(asim ? :a : :b), $outtype, promote_shape(size(a), size(b)))
                    for i = 1:length(a)
                        res[i] = $(scalarfunc)(a[i], b[i])
                    end
                    res
                end
            end
            for (asim, atype, btype) in ((true, :DataArray, :AbstractDataArray),
                                         (false, :AbstractDataArray, :DataArray),
                                         (true, :AbstractDataArray, :AbstractDataArray),
                                         (true, :AbstractDataArray, :AbstractArray),
                                         (false, :AbstractDataArray, :AbstractArray))
        }...,
    ))
end

isless(::NAtype, ::NAtype) = false
isless(::NAtype, b) = true
isless(a, ::NAtype) = false

# This is for performance only; the definition in Base is sufficient
# for AbstractDataArrays
function isequal(a::DataArray, b::DataArray)
    if size(a) != size(b) || a.na != b.na
        return false
    end
    for i = 1:length(a)
        if !a.na[i] && a.data[i] != b.data[i]
            return false
        end
    end
    return true
end
function isequal(df1::AbstractDataFrame, df2::AbstractDataFrame)
    if size(df1, 2) != size(df2, 2)
        return false
    end
    for idx in 1:size(df1, 2)
        if !isequal(df1[idx], df2[idx])
            return false
        end
    end
    return true
end

for sf in (:(==), :(!=), :(>), :(>=), :(<), :(<=))
    vf = symbol(".$sf")

    @eval begin
        # Array with NA
        @swappable $(vf){T,N}(::NAtype, b::AbstractArray{T,N}) =
            DataArray(Array(Bool, size(b)), trues(size(b)))

        # Scalar with NA
        $(vf)(::NAtype, ::NAtype) = NA
        $(sf)(::NAtype, ::NAtype) = NA
        @swappable $(vf)(::NAtype, b) = NA
        @swappable $(sf)(::NAtype, b) = NA

        @dataarray_binary_scalar $vf $sf Bool
        @dataarray_binary_array $vf $sf Bool

        $(sf)(a::AbstractDataArray, b::AbstractDataArray) =
            error("$sf not defined for AbstractDataArrays. Try $vf")

        # DataFrame
        @dataframe_binary $vf
    end
end

#
# Binary operators
#
.^(::MathConst{:e}, B::DataArray) = exp(B)
.^(::MathConst{:e}, B::AbstractDataArray) = exp(B)
for f in arithmetic_operators
    @eval begin
        # Array with NA
        @swappable $(f){T,N}(::NAtype, b::AbstractArray{T,N}) =
            DataArray(Array(T, size(b)), trues(size(b)))

        # Scalar with NA
        ($f)(::NAtype, ::NAtype) = NA
        @swappable ($f)(d::NAtype, x::Number) = NA

        @swappable ($f)(A::BitArray, B::AbstractDataArray) = ($f)(bitunpack(A), B)
        @swappable ($f)(A::BitArray, B::DataArray) = ($f)(bitunpack(A), B)
        @dataarray_binary_scalar $f $f promote_type(eltype(a), eltype(b))

        @dataframe_binary $f
    end
end

^(::NAtype, ::NAtype) = NA
^(a, ::NAtype) = NA
^(::NAtype, ::Integer) = NA
^(::NAtype, ::Number) = NA

# for arithmetic, NAs propagate
for (vf, sf) in ((:(+), :(+)), (:(.+), :(+)), (:(-), :(-)), (:(.-), :(-)), (:(.*), :(*)),
                 (:(.^), :(^)))
    @eval begin
        @dataarray_binary_array $vf $sf promote_type(eltype(a), eltype(b))
    end
end
@dataarray_binary_array (./) (/) promote_type(eltype(a), eltype(b))

for f in biscalar_operators
    @eval begin
        function ($f)(d::NAtype, e::NAtype)
            return NA
        end
        function ($f){T <: Number}(d::NAtype, x::T)
            return NA
        end
        function ($f){T <: Number}(x::T, d::NAtype)
            return NA
        end
    end
end

for f in pairwise_vector_operators
    @eval begin
        function ($f)(dv::DataVector)
            n = length(dv)
            new_data = ($f)(dv.data)
            new_na = falses(n - 1)
            for i = 2:(n - 1)
                if isna(dv[i])
                    new_na[i - 1] = true
                    new_na[i] = true
                end
            end
            if isna(dv[n])
                new_na[n - 1] = true
            end
            return DataArray(new_data, new_na)
        end
    end
end

for f in cumulative_vector_operators
    @eval begin
        function ($f)(dv::DataVector)
            new_data = ($f)(dv.data)
            new_na = falses(length(dv))
            hitna = false
            for i = 1:length(dv)
                if isna(dv[i])
                    hitna = true
                end
                if hitna
                    new_na[i] = true
                end
            end
            return DataArray(new_data, new_na)
        end
    end
end

for f in unary_vector_operators
    @eval begin
        function ($f)(dv::DataVector)
            if any(isna(dv))
                return NA
            else
                return ($f)(dv.data)
            end
        end
    end
end

for f in ffts
    @eval begin
        function ($f)(dv::DataVector)
            if any(isna(dv))
                return NA
            else
                return ($f)(dv.data)
            end
        end
    end
end

for f in binary_vector_operators
    @eval begin
        function ($f)(dv1::DataVector, dv2::DataVector)
            if any(isna(dv1)) || any(isna(dv2))
                return NA
            else
                return ($f)(dv1.data, dv2.data)
            end
        end
    end
end

for (f, colf) in ((:min, :colmins),
                  (:max, :colmaxs),
                  (:prod, :colprods),
                  (:sum, :colsums),
                  (:mean, :colmeans),
                  (:median, :colmedians),
                  (:std, :colstds),
                  (:var, :colvars),
                  (:fft, :colffts), # TODO: Remove and/or fix
                  (:norm, :colnorms))
    @eval begin
        function ($colf)(df::AbstractDataFrame)
            p = ncol(df)
            res = DataFrame()
            for j in 1:p
                res[j] = DataArray(($f)(df[j]))
            end
            colnames!(res, colnames(df))
            return res
        end
        function ($colf)(dm::AbstractDataMatrix)
            n, p = nrow(dm), ncol(dm)
            res = datazeros(p)
            for j in 1:p
                res[j] = ($f)(dm[:, j])
            end
            return res
        end
    end
end

for (f, rowf) in ((:min, :rowmins),
                  (:max, :rowmaxs),
                  (:prod, :rowprods),
                  (:sum, :rowsums),
                  (:mean, :rowmeans),
                  (:median, :rowmedians),
                  (:std, :rowstds),
                  (:var, :rowvars),
                  (:fft, :rowffts), # TODO: Remove and/or fix
                  (:norm, :rownorms))
    @eval begin
        function ($rowf)(dm::DataMatrix)
            n, p = nrow(dm), ncol(dm)
            res = datazeros(n)
            for i in 1:n
                res[i] = ($f)(DataArray(reshape(dm.data[i, :], p), reshape(dm.na[i, :], p)))
            end
            return res
        end
    end
end

#
# Boolean operators
#

function all{T}(dv::AbstractDataArray{T})
    for i in 1:length(dv)
        if isna(dv[i])
            return NA
        end
        if !dv[i]
            return false
        end
    end
    return true
end

function any{T}(dv::AbstractDataArray{T})
    has_na = false
    for i in 1:length(dv)
        if !isna(dv[i])
            if dv[i]
                return true
            end
        else
            has_na = true
        end
    end
    if has_na
        return NA
    else
        return false
    end
end

function all(df::AbstractDataFrame)
    for i in 1:nrow(df)
        for j in 1:ncol(df)
            if isna(df[i, j])
                return NA
            end
            if !df[i, j]
                return false
            end
        end
    end
    return true
end

function any(df::AbstractDataFrame)
    has_na = false
    for i in 1:nrow(df)
        for j in 1:ncol(df)
            if !isna(df[i, j])
                if df[i, j]
                    return true
                end
            else
                has_na = true
            end
        end
    end
    if has_na
        return NA
    else
        return false
    end
end

function Stats.range{T}(dv::AbstractDataVector{T})
    return DataVector[min(dv), max(dv)]
end

function rle{T}(v::AbstractVector{T})
    n = length(v)
    current_value = v[1]
    current_length = 1
    values = similar(v, n)
    total_values = 1
    lengths = Array(Int16, n)
    total_lengths = 1
    for i in 2:n
        if v[i] == current_value
            current_length += 1
        else
            values[total_values] = current_value
            total_values += 1
            lengths[total_lengths] = current_length
            total_lengths += 1
            current_value = v[i]
            current_length = 1
        end
    end
    values[total_values] = current_value
    lengths[total_lengths] = current_length
    return (values[1:total_values], lengths[1:total_lengths])
end

function rle{T}(v::AbstractDataVector{T})
    n = length(v)
    current_value = v[1]
    current_length = 1
    values = DataArray(T, n)
    total_values = 1
    lengths = Array(Int16, n)
    total_lengths = 1
    for i in 2:n
        if isna(v[i]) || isna(current_value)
            if isna(v[i]) && isna(current_value)
                current_length += 1
            else
                values[total_values] = current_value
                total_values += 1
                lengths[total_lengths] = current_length
                total_lengths += 1
                current_value = v[i]
                current_length = 1
            end
        else
            if v[i] == current_value
                current_length += 1
            else
                values[total_values] = current_value
                total_values += 1
                lengths[total_lengths] = current_length
                total_lengths += 1
                current_value = v[i]
                current_length = 1
            end
        end
    end
    values[total_values] = current_value
    lengths[total_lengths] = current_length
    return (values[1:total_values], lengths[1:total_lengths])
end

## inverse run-length encoding
function inverse_rle{T, I <: Integer}(values::AbstractVector{T}, lengths::Vector{I})
    total_n = sum(lengths)
    pos = 0
    res = similar(values, total_n)
    n = length(values)
    for i in 1:n
        v = values[i]
        l = lengths[i]
        for j in 1:l
            pos += 1
            res[pos] = v
        end
    end
    return res
end
