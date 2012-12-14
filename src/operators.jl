# This is multiplicative analog of diff
function reldiff{T}(v::Vector{T})
    n = length(v)
    res = Array(T, n - 1)
    for i in 2:n
        res[i - 1] = v[i] / v[i - 1]
    end
    return res
end

# Diff scaled by previous value
function percent_change{T}(v::Vector{T})
    n = length(v)
    res = Array(T, n - 1)
    for i in 2:n
        res[i - 1] = (v[i] - v[i - 1]) / v[i - 1]
    end
    return res
end

unary_operators = [:(+), :(-), :(!)]

numeric_unary_operators = [:(+), :(-)]

logical_unary_operators = [:(!)]

elementary_functions = [:abs, :sign, :acos, :acosh, :asin,
                        :asinh, :atan, :atanh, :sin, :sinh,
                        :cos, :cosh, :tan, :tanh, :ceil, :floor,
                        :round, :trunc, :exp, :log, :log10, :log1p,
                        :log2, :logb, :sqrt]

comparison_operators = [:(==), :(.==), :(!=), :(.!=), :isless,
                        :(>), :(.>), :(>=), :(.>=), :(<), :(.<),
                        :(<=), :(.<=)]

scalar_comparison_operators = [:(==), :(!=), :isless, :(>), :(>=),
                               :(<), :(<=)]

array_comparison_operators = [:(.==), :(.!=), :(.>), :(.>=), :(.<), :(.<=)]

vectorized_comparison_operators = [(:(.==), :(==)), (:(.!=), :(!=)),
                                   (:(.>), :(>)), (:(.>=), :(>=)),
                                   (:(.<), :(<)), (:(.<=), :(<=))]

binary_operators = [:(+), :(.+), :(-), :(.-), :(*), :(.*), :(/), :(./),
                    :(.^), :(div), :(mod), :(fld), :(rem),
                    :(&), :(|), :($)]

induced_binary_operators = [:(^)]

arithmetic_operators = [:(+), :(.+), :(-), :(.-), :(*), :(.*), :(/), :(./),
                        :(.^), :(div), :(mod), :(fld), :(rem)]

induced_arithmetic_operators = [:(^)]

biscalar_operators = [:(max), :(min)]

scalar_arithmetic_operators = [:(+), :(-), :(*), :(/),
                               :(div), :(mod), :(fld), :(rem)]

induced_scalar_arithmetic_operators = [:(^)]

array_arithmetic_operators = [:(+), :(.+), :(-), :(.-), :(.*), :(./), :(.^)]

bit_operators = [:(&), :(|), :($)]

unary_vector_operators = [:min, :max, :prod, :sum, :mean, :median, :std,
                          :var, :norm]

pairwise_vector_operators = [:diff, :reldiff, :percent_change]

cumulative_vector_operators = [:cumprod, :cumsum, :cumsum_kbn, :cummin, :cummax]

ffts = [:fft]

binary_vector_operators = [:dot, :cor_pearson, :cov_pearson,
                           :cor_spearman, :cov_spearman]

rowwise_operators = [:rowmins, :rowmaxs, :rowprods, :rowsums,
                     :rowmeans, :rowmedians, :rowstds, :rowvars,
                     :rowffts, :rownorms]

columnar_operators = [:colmins, :colmaxs, :colprods, :colsums,
                      :colmeans, :colmedians, :colstds, :colvars,
                      :colffts, :colnorms]

boolean_operators = [:any, :all]

for f in unary_operators
    @eval begin
        function ($f)(d::NAtype)
            return NA
        end
        function ($f){T}(dv::DataVec{T})
            res = deepcopy(dv)
            for i in 1:length(dv)
                res[i] = ($f)(dv[i])
            end
            return res
        end
        function ($f){T}(dm::DataMatrix{T})
            res = deepcopy(dm)
            for i in 1:numel(dm)
                res[i] = ($f)(dm[i])
            end
            return res
        end
        function ($f)(df::DataFrame)
            res = deepcopy(df)
            n, p = nrow(df), ncol(df)
            for j in 1:p
                if typeof(df[j]).parameters[1] <: Number
                    for i in 1:n
                        res[i, j] = ($f)(df[i, j])
                    end
                else
                    for i in 1:n
                        res[i, j] = NA
                    end
                end
            end
            return res
        end
    end
end

# Treat ctranspose and * in a special way for now
function ctranspose{T}(dv::DataVec{T})
    return DataMatrix(dv.data', dv.na')
end

function ctranspose{T}(dm::DataMatrix{T})
    return DataMatrix(dm.data', dm.na')
end

# TODO: Check there are no better algorithms
function (*){S <: Real, T <: Real}(a::DataVec{S}, b::DataMatrix{T})
    if size(b, 1) != 1
        error("DataVec and matrix sizes must match")
    end
    n, p = length(a), size(b, 2)
    res = dmzeros(n, p)
    for i in 1:n
        for j in 1:p
            res[i, j] = a[i] * b[j]
        end
    end
    return res
end

function (*){S <: Real, T <: Real}(a::Vector{S}, b::DataMatrix{T})
    if size(b, 1) != 1
        error("Vector and matrix sizes must match")
    end
    n, p = length(a), size(b, 2)
    res = dmzeros(n, p)
    for i in 1:n
        for j in 1:p
            res[i, j] = a[i] * b[j]
        end
    end
    return res
end

# TODO: Check there are no better algorithms
function (*){S <: Real, T <: Real}(a::DataMatrix{S}, b::DataVec{T})
    if size(a, 2) != length(b)
        error("The number of columns of the DataMatrix must match the length of the DataVec")
    end
    n, p = size(a, 1), length(b)
    res = dvzeros(n)
    for i in 1:n
        res[i] = 0.0
        for j in 1:p
            res[i] += a[i, j] * b[j]
        end
    end
    return res
end

# TODO: Check there are no better algorithms
function (*){S <: Real, T <: Real}(a::DataMatrix{S}, b::Vector{T})
    if size(a, 2) != length(b)
        error("The number of columns of the DataMatrix must match the length of the Vector")
    end
    n, p = size(a, 1), length(b)
    res = dvzeros(n)
    for i in 1:n
        res[i] = 0.0
        for j in 1:p
            res[i] += a[i, j] * b[j]
        end
    end
    return res
end

# Propagates NA's
# For a dissenting view,
# http://radfordneal.wordpress.com/2011/05/21/slowing-down-matrix-multiplication-in-r/
# But we're getting 10x R while maintaining NA's
function (*){S <: Real, T <: Real}(a::DataMatrix{S}, b::DataMatrix{T})
    n1, p1 = size(a)
    n2, p2 = size(b)
    if p1 != n2
        error("DataMatrix sizes must align for matrix multiplication")
    end
    res = DataMatrix(a.data * b.data, falses(n1, p2))
    # Propagation can be made more efficient by storing record of corrupt
    # rows and columns, then doing fast edits.
    corrupt_rows = falses(n1)
    corrupt_cols = falses(p2)
    for i in 1:n1
        for j in 1:p1
            if a.na[i, j]
                # Propagate NA's
                # Corrupt all rows based on i
                corrupt_rows[i] = true
            end
        end
    end
    for i in 1:n2
        for j in 1:p2
            if b.na[i, j]
                # Propagate NA's
                # Corrupt all columns based on j
                corrupt_cols[j] = true
            end
        end
    end
    for i in 1:n1
        if corrupt_rows[i]
            res.na[i, :] = true
        end
    end
    for j in 1:p2
        if corrupt_cols[j]
            res.na[:, j] = true
        end
    end
    return res
end

function (*){S <: Real, T <: Real}(a::DataMatrix{S}, b::Matrix{T})
    n1, p1 = size(a)
    n2, p2 = size(b)
    if p1 != n2
        error("DataMatrix and Matrix sizes must align for matrix multiplication")
    end
    res = DataMatrix(a.data * b, falses(n1, p2))
    for i in 1:n1
        for j in 1:p1
            if a.na[i, j]
                # Propagate NA's
                # Corrupt all rows based on i
                res.na[i, :] = true
            end
        end
    end
    return res
end

function (*){S <: Real, T <: Real}(a::Matrix{S}, b::DataMatrix{T})
    n1, p1 = size(a)
    n2, p2 = size(b)
    if p1 != n2
        error("Matrix and DataMatrix sizes must align for matrix multiplication")
    end
    res = DataMatrix(a * b.data, falses(n1, p2))
    for i in 1:n2
        for j in 1:p2
            if b.na[i, j]
                # Propagate NA's
                # Corrupt all columns based on j
                res.na[:, j] = true
            end
        end
    end
    return res
end

for f in elementary_functions
    @eval begin
        function ($f)(d::NAtype)
            return NA
        end
        function ($f){T}(dv::DataVec{T})
            n = length(dv)
            res = DataVec(Array(T, n), falses(n))
            for i = 1:n
                res[i] = ($f)(dv[i])
            end
            return res
        end
        function ($f){T}(adv::AbstractDataVec{T})
            res = deepcopy(adv)
            for i = 1:length(adv)
                if isna(adv[i])
                    res[i] = NA
                else
                    res[i] = ($f)(adv[i])
                end
            end
            return res
        end
        function ($f){T}(dm::DataMatrix{T})
            res = DataMatrix(Array(T, size(dm)), falses(size(dm)))
            for i = 1:numel(dm)
                res[i] = ($f)(dm[i])
            end
            return res
        end
        function ($f)(df::DataFrame)
            n, p = nrow(df), ncol(df)
            res = DataFrame(coltypes(df), colnames(df), n)
            for j in 1:p
                if eltype(df[j]) <: Number
                    for i in 1:n
                        res[i, j] = ($f)(df[i, j])
                    end
                else
                    for i in 1:n
                        res[i, j] = NA
                    end
                end
            end
            return res
        end
    end
end

for f in comparison_operators
    @eval begin
        function ($f)(d::NAtype, e::NAtype)
            return NA
        end
        function ($f){T <: Union(String, Number)}(d::NAtype, x::T)
            return NA
        end
        function ($f){T <: Union(String, Number)}(x::T, d::NAtype)
            return NA
        end
    end
end

for (f, scalarf) in vectorized_comparison_operators
    @eval begin
        function ($f){S, T <: Union(String, Number)}(a::DataVec{S}, v::T)
            res = DataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                if isna(a[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(a[i], v)
                end
            end
            return res
        end
        function ($f){S <: Union(String, Number), T}(v::S, a::DataVec{T})
            res = DataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                if isna(a[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(v, a[i])
                end
            end
            return res
        end
        function ($f){S, T <: Union(String, Number)}(a::PooledDataVec{S}, v::T)
            res = PooledDataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                if isna(a[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(a[i], v)
                end
            end
            return res
        end
        function ($f){S <: Union(String, Number), T}(v::S, a::PooledDataVec{T})
            res = PooledDataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                if isna(a[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(v, a[i])
                end
            end
            return res
        end
        function ($f){T}(a::AbstractDataVec{T}, v::NAtype)
            res = DataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                res[i] = NA
            end
            res
        end
        function ($f){T}(v::NAtype, a::AbstractDataVec{T})
            res = DataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                res[i] = NA
            end
            res
        end
        function ($f){T}(a::PooledDataVec{T}, v::NAtype)
            res = PooledDataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                res[i] = NA
            end
            res
        end
        function ($f){T}(v::NAtype, a::PooledDataVec{T})
            res = PooledDataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                res[i] = NA
            end
            res
        end
        function ($f){T <: Number}(a::DataFrame, v::T)
            res = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: Number
                    for i in 1:n
                        res[i, j] = isna(a[i, j]) ? NA : ($scalarf)(a[i, j], v)
                    end
                else
                    for i in 1:n
                        res[i, j] = NA
                    end
                end
            end
            return res
        end
        function ($f){T <: Number}(v::T, a::DataFrame)
            res = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: Number
                    for i = 1:n
                        res[i, j] = isna(a[i, j]) ? NA : ($scalarf)(a[i, j], v)
                    end
                else
                    for i = 1:n
                        res[i, j] = NA
                    end
                end
            end
            return res
        end
        function ($f){T <: String}(a::DataFrame, v::T)
            res = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: String
                    for i in 1:n
                        res[i, j] = isna(a[i, j]) ? NA : ($scalarf)(a[i, j], v)
                    end
                else
                    for i in 1:n
                        res[i, j] = NA
                    end
                end
            end
            return res
        end
        function ($f){T <: String}(v::T, a::DataFrame)
            res = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: String
                    for i = 1:n
                        res[i, j] = isna(a[i, j]) ? NA : ($scalarf)(a[i, j], v)
                    end
                else
                    for i = 1:n
                        res[i, j] = NA
                    end
                end
            end
            return res
        end
        function ($f)(a::DataFrame, v::NAtype)
            res = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                for i in 1:n
                    res[i, j] = NA
                end
            end
            return res
        end
        function ($f)(v::NAtype, a::DataFrame)
            res = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                for i = 1:n
                    res[i, j] = NA
                end
            end
            return res
        end
    end
end

for f in scalar_comparison_operators
    @eval begin
        function ($f){S, T}(a::AbstractDataVec{S}, b::AbstractDataVec{T})
            error(strcat(string($f), " not defined for DataVecs. Try .", string($f)))
        end
        function ($f){S, T}(a::DataMatrix{S}, b::DataMatrix{T})
            error(strcat(string($f), " not defined for DataMatrix's. Try .", string($f)))
        end
        function ($f)(a::AbstractDataFrame, b::AbstractDataFrame)
            error(strcat(string($f), " not defined for DataFrames. Try .", string($f)))
        end
    end
end

for (f, scalarf) in vectorized_comparison_operators
    @eval begin
        function ($f){S, T}(a::AbstractDataVec{S}, b::AbstractDataVec{T})
            res = DataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                if isna(a[i]) || isna(b[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(a[i], b[i])
                end
            end
            return res
        end
        function ($f){S, T}(a::AbstractDataVec{S}, b::Vector{T})
            res = DataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                if isna(a[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(a[i], b[i])
                end
            end
            return res
        end
        function ($f){S, T}(a::Vector{S}, b::AbstractDataVec{T})
            res = DataVec(Array(Bool, length(a)), BitArray(length(a)))
            for i in 1:length(a)
                if isna(b[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(a[i], b[i])
                end
            end
            return res
        end
        function ($f){S, T}(a::DataMatrix{S}, b::DataMatrix{T})
            res = DataMatrix(Array(Bool, size(a)), BitArray(size(a)))
            for i in 1:numel(a)
                if isna(a[i]) || isna(b[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(a[i], b[i])
                end
            end
            return res
        end
        function ($f){S, T}(a::DataMatrix{S}, b::Matrix{T})
            res = DataMatrix(Array(Bool, size(a)), BitArray(size(a)))
            for i in 1:numel(a)
                if isna(a[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(a[i], b[i])
                end
            end
            return res
        end
        function ($f){S, T}(a::Matrix{S}, b::DataMatrix{T})
            res = DataMatrix(Array(Bool, size(a)), BitArray(size(a)))
            for i in 1:numel(a)
                if isna(b[i])
                    res[i] = NA
                else
                    res[i] = ($scalarf)(a[i], b[i])
                end
            end
            return res
        end
        function ($f)(a::DataFrame, b::DataFrame)
            n, p = nrow(a), ncol(a)
            if n != nrow(b) || p != ncol(b)
                error("DataFrames must have matching sizes for comparisons")
            end
            # Tries to preserve types from a
            results = DataFrame(Array(Bool, size(a)))
            # TODO: Test that types match across a and b
            for j in 1:p
                for i in 1:n
                    results[i, j] = ($scalarf)(a[i, j], b[i, j])
                end
            end
            return results
        end
    end
end

for f in binary_operators
    @eval begin
        function ($f)(d::NAtype, e::NAtype)
            return NA
        end
        function ($f){T <: Union(Number, String)}(d::NAtype, x::T)
            return NA
        end
        function ($f){T <: Union(Number, String)}(x::T, d::NAtype)
            return NA
        end
        function ($f){T}(A::Number, B::DataVec{T})
            res = DataVec(Array(promote_type(typeof(A),T), length(B)), B.na)
            for i in 1:length(B)
                res.data[i] = ($f)(A, B.data[i])
            end
            return res
        end
        function ($f){T}(A::DataVec{T}, B::Number)
            res = DataVec(Array(promote_type(typeof(B),T), length(A)), A.na)
            for i in 1:length(A)
                res.data[i] = ($f)(A.data[i], B)
            end
            return res
        end
        function ($f){T}(A::NAtype, B::DataVec{T})
            res = DataVec(Array(typeof(B).parameters[1], length(B)), B.na)
            for i in 1:length(B)
                res.data[i] = B.data[i]
                res.na[i] = true
            end
            return res
        end
        function ($f){T}(A::DataVec{T}, B::NAtype)
            res = DataVec(Array(typeof(A).parameters[1], length(A)), A.na)
            for i in 1:length(A)
                res.data[i] = A.data[i]
                res.na[i] = true
            end
            return res
        end
        function ($f)(df::DataFrame, x::Union(Number, NAtype))
            n, p = nrow(df), ncol(df)
            # Tries to preserve types
            results = deepcopy(df)
            for j in 1:p
                if typeof(df[j]).parameters[1] <: Number
                    for i in 1:n
                        results[i, j] = ($f)(df[i, j], x)
                    end
                else
                    for i in 1:n
                        results[i, j] = NA
                    end
                end
            end
            return results
        end
        function ($f)(x::Union(Number, NAtype), df::DataFrame)
            n, p = nrow(df), ncol(df)
            # Tries to preserve types
            results = deepcopy(df)
            for j in 1:p
                if typeof(df[j]).parameters[1] <: Number
                    for i in 1:n
                        results[i, j] = ($f)(x, df[i, j])
                    end
                else
                    for i in 1:n
                        results[i, j] = NA
                    end
                end
            end
            return results
        end
        function ($f){T <: Union(String, Number)}(d::NAtype, x::T)
            return NA
        end
        function ($f){T <: Union(String, Number)}(x::T, d::NAtype)
            return NA
        end
        function ($f){T,N}(a::AbstractArray{T,N}, d::NAtype)
            return NA
        end
        function ($f){T,N}(d::NAtype, a::AbstractArray{T,N})
            return NA
        end
    end
end

for f in induced_binary_operators
    @eval begin
        function ($f)(d::NAtype, e::NAtype)
            return NA
        end
        function ($f)(d::Union(String, Number), e::NAtype)
            return NA
        end
        function ($f)(d::NAtype, e::FloatingPoint)
            return NA
        end
    end
end

# for arithmetic, NAs propagate
for f in array_arithmetic_operators
    @eval begin
        function ($f){S, T}(A::DataVec{S}, B::DataVec{T})
            if (length(A) != length(B))
                error("DataVec lengths must match")
            end
            res = DataVec(Array(promote_type(S, T),
                                length(A)),
                          BitArray(length(A)))
            for i in 1:length(A)
                res.na[i] = (A.na[i] || B.na[i])
                res.data[i] = ($f)(A.data[i], B.data[i])
            end
            return res
        end
        function ($f){S, T}(A::DataMatrix{S}, B::DataMatrix{T})
            if size(A) != size(B)
                error("DataMatrix sizes must match")
            end
            res = DataMatrix(Array(promote_type(S, T), size(A)),
                             BitArray(size(A)))
            for i in 1:numel(A)
                res.na[i] = (A.na[i] || B.na[i])
                res.data[i] = ($f)(A.data[i], B.data[i])
            end
            return res
        end
        function ($f)(a::DataFrame, b::DataFrame)
            n, p = nrow(a), ncol(a)
            if n != nrow(b) || p != ncol(b)
                error("DataFrames must have matching sizes for arithmetic")
            end
            # Tries to preserve types from a
            results = deepcopy(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: Number
                    for i in 1:n
                        results[i, j] = ($f)(a[i, j], b[i, j])
                    end
                else
                    for i in 1:n
                        results[i, j] = NA
                    end
                end
            end
            return results
        end
    end
end

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
        function ($f)(dv::DataVec)
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
            return DataVec(new_data, new_na)
        end
    end
end

for f in cumulative_vector_operators
    @eval begin
        function ($f)(dv::DataVec)
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
            return DataVec(new_data, new_na)
        end
    end
end

for f in unary_vector_operators
    @eval begin
        function ($f)(dv::DataVec)
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
        function ($f)(dv::DataVec)
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
        function ($f)(dv1::DataVec, dv2::DataVec)
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
                  (:fft, :colffts),
                  (:norm, :colnorms))
    @eval begin
        function ($colf)(df::DataFrame)
            res = DataFrame(coltypes(df), colnames(df), 1)
            p = ncol(df)
            for j in 1:p
                res[:, p] = ($f)(df[:, p])
            end
            return res
        end
        function ($colf){T}(dm::DataMatrix{T})
            n, p = nrow(dm), ncol(dm)
            res = dvzeros(p)
            for j in 1:p
                res[j] = ($f)(DataVec(dm.data[:, j], dm.na[:, j]))
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
                  (:fft, :rowffts),
                  (:norm, :rownorms))
    @eval begin
        function ($rowf){T}(dm::DataMatrix{T})
            n, p = nrow(dm), ncol(dm)
            res = dvzeros(n)
            for i in 1:n
                res[i] = ($f)(DataVec(reshape(dm.data[i, :], p), reshape(dm.na[i, :], p)))
            end
            return res
        end
    end
end

#
# Boolean operators
#

function all{T}(dv::DataVec{T})
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

function any{T}(dv::DataVec{T})
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

function all(df::DataFrame)
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

function any(df::DataFrame)
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

# isequal() should for Data*
# * If missingness differs, underlying values are irrelevant
# * If both entries are NA, underlying values are irrelevant
function isequal{T}(a::DataVec{T}, b::DataVec{T})
    if length(a) != length(b)
        return false
    else
        for i = 1:length(a)
            if a.na[i] != b.na[i]
                return false
            elseif !a.na[i] && !b.na[i] && (a.data[i] != b.data[i])
                return false
            end
        end
    end
    return true
end
function isequal{T}(a::PooledDataVec{T}, b::PooledDataVec{T})
    if length(a) != length(b)
        return false
    else
        for i = 1:length(a)
            # Will we speed this up by looking under hood?
            if isna(a[i])
                if !isna(b[i])
                    return false
                end
            else
                if isna(b[i])
                    return false
                end
                if a[i] != b[i]
                    return false
                end
            end
        end
    end
    return true
end
function isequal{T}(a::AbstractDataVec{T}, b::AbstractDataVec{T})
    if length(a) != length(b)
        return false
    else
        for i = 1:length(a)
            if isna(a[i])
                if !isna(b[i])
                    return false
                end
            else
                if isna(b[i])
                    return false
                end
                if a[i] != b[i]
                    return false
                end
            end
        end
    end
    return true
end
function isequal(df1::AbstractDataFrame, df2::AbstractDataFrame)
    if ncol(df1) != ncol(df2)
        return false
    end
    for idx in 1:ncol(df1)
        if !isequal(df1[idx], df2[idx])
            return false
        end
    end
    return true
end

function range{T}(dv::DataVec{T})
    return DataVec([min(dv), max(dv)], falses(2))
end

