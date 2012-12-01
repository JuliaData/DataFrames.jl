# Missing unary operators on NA's
for f in (:(!), :(+), :(-))
    @eval begin
        function ($f)(d::NAtype)
            return NA
        end
    end
end

# Missing unary operators on DataVec's
for f in (:(!), :(+), :(-))
    @eval begin
        function ($f)(dv::DataVec)
            res = deepcopy(dv)
            for i in 1:length(dv)
                res[i] = ($f)(dv[i])
            end
            return res
        end
    end
end

# Base functions
# These provide an additional mechanism for vectorizing
# DataVec and DataFrame operations. Should try out performance
for f in (:abs, :sign, :acos, :acosh, :asin, :asinh,
          :atan, :atan2, :atanh, :sin, :sinh, :cos,
          :cosh, :tan, :tanh, :ceil, :floor,
          :round, :trunc, :signif, :exp, :log,
          :log10, :log1p, :log2, :logb, :sqrt)
    @eval begin
        function ($f)(d::NAtype)
            return NA
        end
    end
end

# Quick isless hack
function isless{S, T}(a::AbstractDataVec{S}, v::T)
    ret = DataVec(Array(Bool, length(a)), BitArray(length(a)), naRule(a), false)
    for i = 1:length(a)
        ret[i] = isna(a[i]) ? NA : isless(a[i], v)
    end
    ret
end
function isless{S, T}(v::S, a::AbstractDataVec{T})
    ret = DataVec(Array(Bool, length(a)), BitArray(length(a)), naRule(a), false)
    for i = 1:length(a)
        ret[i] = isna(a[i]) ? NA : isless(v, a[i])
    end
    ret
end

# element-wise symmetric (in)equality operators
for (f, scalarf) in ((:(.==), :(==)), (:(.!=), :(!=)))
    @eval begin    
        function ($f){S, T}(a::AbstractDataVec{S}, v::T)
            # allocate a DataVec for the return value, then assign into it
            ret = DataVec(Array(Bool,length(a)), BitArray(length(a)), naRule(a), false)
            for i = 1:length(a)
                ret[i] = isna(a[i]) ? NA : ($scalarf)(a[i], v)
            end
            ret
        end
        ($f){S, T}(v::S, a::AbstractDataVec{T}) = ($f)(a::AbstractDataVec{T}, v::S)
    end
end

# element-wise antisymmetric (in)equality operators
for (f, scalarf, scalarantif) in ((:(.<), :(<), :(>)),
                                  (:(.>), :(>), :(<)),
                                  (:(.<=),:(<=), :(>=)),
                                  (:(.>=), :(>=), :(<=)))
    @eval begin    
        function ($f){S, T}(a::AbstractDataVec{S}, v::T)
            # allocate a DataVec for the return value, then assign into it
            ret = DataVec(Array(Bool,length(a)), BitArray(length(a)), naRule(a), false)
            for i = 1:length(a)
                ret[i] = isna(a[i]) ? NA : ($scalarf)(a[i], v)
            end
            ret
        end
        function ($f){S, T}(v::S, a::AbstractDataVec{T})
            # allocate a DataVec for the return value, then assign into it
            ret = DataVec(Array(Bool,length(a)), BitArray(length(a)), naRule(a), false)
            for i = 1:length(a)
                ret[i] = isna(a[i]) ? NA : ($scalarantif)(a[i], v)
            end
            ret
        end
    end
end

# for arithmetic, NAs propagate
for f in (:+, :.+, :-, :.-, :.*, :./, :.^,
          :div, :mod, :fld, :rem, :max, :min,
          :&, :|, :$)
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

for f in (:+, :.+, :-, :.-, :*, :.*, :/, :./, :^, :.^,
          :div, :mod, :fld, :rem, :max, :min,
          :&, :|, :$)
    @eval begin
        function ($f){T}(A::NAtype, B::DataVec{T})
            res = deepcopy(B)
            for i in 1:length(B)
                res[i] = ($f)(A, B[i])
            end
            return res
        end
        function ($f){T}(A::DataVec{T}, B::NAtype)
            res = deepcopy(A)
            for i in 1:length(A)
                res[i] = ($f)(A[i], B)
            end
            return res
        end
    end
end

for f in (:*, :/)
    @eval begin
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

for f in (:^, )
    @eval begin
        function ($f){T}(A::Number, B::DataVec{T})
            F = DataVec(Array(promote_type(typeof(A),T), length(B)), B.na)
            for i=1:length(B)
                F.data[i] = ($f)(A, B.data[i])
            end
            return F
        end
    end
end

# Multiplication is a special case
function (*){S, T}(A::DataVec{S}, B::DataVec{T})
    n = length(A)
    if n != length(B)
        error("DataVec's must have the same length")
    end
    res = 0.0
    for i in 1:n
        if isna(A[i]) || isna(B[i])
            return NA
        end
        res += A[i] * B[i]
    end
    return res
end
dot{S, T}(A::DataVec{S}, B::DataVec{T}) = (*){S, T}(A::DataVec{S}, B::DataVec{T})

# Vectorized arithmetic operations
for f in (:abs, :sign, :acos, :acosh, :asin, :asinh,
          :atan, :atan2, :atanh, :sin, :sinh, :cos,
          :cosh, :tan, :tanh, :ceil, :floor,
          :round, :trunc, :signif, :exp, :log,
          :log10, :log1p, :log2, :logb, :sqrt)
    @eval begin
        function ($f)(adv::AbstractDataVec)
            ret = deepcopy(adv)
            for i = 1:length(adv)
                if isna(adv[i])
                    ret[i] = NA
                else
                    ret[i] = ($f)(adv[i])
                end
            end
            return ret
        end
    end
end

# Dyadic arithmetic operations
for f in (:diff, )
    @eval begin
        function ($f)(dv::DataVec)
            n = length(dv)
            new_data = ($f)(dv.data)
            new_na = bitfalses(n - 1)
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

# Sequential arithmetic operations
for f in (:cumprod, :cumsum, :cumsum_kbn)
    @eval begin
        function ($f)(dv::DataVec)
            new_data = ($f)(dv.data)
            new_na = bitfalses(length(dv))
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

# Global arithmetic operations
# Tolerate no NA's
for f in (:min, :max, :prod, :sum,
          :mean, :median,
          :std, :var,
          :fft, :norm)
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

# Two-column arithmetic operations
# Tolerate no NA's in either column
for f in (:cor_pearson, :cov_pearson,
          :cor_spearman, :cov_spearman)
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

# Scalar-DF + DF-Scalar operators
# Scalar <: Real or Scalar is NA
# Maybe add DataNumber = Union(Number, NAtype)?
for f in (:(+), :(.+), :(-), :(.-), :(*), :(.*),
          :(/), :(./), :(^), :(.^))
    @eval begin
        function ($f)(df::DataFrame, x::Union(Number, NAtype))
            n, p = nrow(df), ncol(df)
            # Tries to preserve types
            results = deepcopy(df)
            # Could instead do and only return Float64
            # results = DataFrame(Float64, n, p)
            for j in 1:p
                if typeof(df[j]).parameters[1] <: Real
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
            # Could instead do and only return Float64
            # results = DataFrame(Float64, n, p)
            for j in 1:p
                if typeof(df[j]).parameters[1] <: Real
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
    end
end

# DF-DF operators
for f in (:(+), :(.+), :(-), :(.-), :(.*), :(./), :(.^))
    @eval begin
        function ($f)(a::DataFrame, b::DataFrame)
            n, p = nrow(a), ncol(a)
            if n != nrow(b) || p != ncol(b)
                error("DataFrames must have matching sizes for arithmetic")
            end
            # Tries to preserve types from a
            results = deepcopy(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: Real
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

# Unary operators and Base functions on DataFrame's
for f in (:(!), :(+), :(-),
          :abs, :sign, :acos, :acosh, :asin, :asinh,
          :atan, :atan2, :atanh, :sin, :sinh, :cos,
          :cosh, :tan, :tanh, :ceil, :floor,
          :round, :trunc, :signif, :exp, :log,
          :log10, :log1p, :log2, :logb, :sqrt)
    @eval begin
        function ($f)(df::DataFrame)
            res = deepcopy(df)
            n, p = nrow(df), ncol(df)
            for j in 1:p
                if typeof(df[j]).parameters[1] <: Real
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

# Quick isless hack(s)
# Need to pass NA's for non-numeric columns
function isless{T <: Number}(a::DataFrame, v::T)
    ret = DataFrame(Array(Bool, size(a)))
    n, p = nrow(a), ncol(a)
    for j in 1:p
        if typeof(a[j]).parameters[1] <: Number
            for i in 1:n
                ret[i, j] = isna(a[i, j]) ? NA : isless(a[i, j], v)
            end
        else
            for i in 1:n
                ret[i, j] = NA
            end
        end
    end
    return ret
end
function isless{T <: Number}(v::T, a::DataFrame)
    ret = DataFrame(Array(Bool, size(a)))
    n, p = nrow(a), ncol(a)
    for j in 1:p
        if typeof(a[j]).parameters[1] <: Number
            for i = 1:n
                ret[i, j] = isna(a[i, j]) ? NA : isless(a[i, j], v)
            end
        else
            for i = 1:n
                ret[i, j] = NA
            end
        end
    end
    return ret
end
function isless{T <: String}(a::DataFrame, v::T)
    ret = DataFrame(Array(Bool, size(a)))
    n, p = nrow(a), ncol(a)
    for j in 1:p
        if typeof(a[j]).parameters[1] <: String
            for i in 1:n
                ret[i, j] = isna(a[i, j]) ? NA : isless(a[i, j], v)
            end
        else
            for i in 1:n
                ret[i, j] = NA
            end
        end
    end
    return ret
end
function isless{T <: String}(v::T, a::DataFrame)
    ret = DataFrame(Array(Bool, size(a)))
    n, p = nrow(a), ncol(a)
    for j in 1:p
        if typeof(a[j]).parameters[1] <: String
            for i = 1:n
                ret[i, j] = isna(a[i, j]) ? NA : isless(a[i, j], v)
            end
        else
            for i = 1:n
                ret[i, j] = NA
            end
        end
    end
    return ret
end

for f in (:(.==), :(.!=), :(.<), :(.<=), :(.>), :(.>=))
    @eval begin
        function ($f){T <: Number}(a::DataFrame, v::T)
            ret = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: Number
                    for i in 1:n
                        ret[i, j] = isna(a[i, j]) ? NA : ($f)(a[i, j], v)
                    end
                else
                    for i in 1:n
                        ret[i, j] = NA
                    end
                end
            end
            return ret
        end
        function ($f){T <: Number}(v::T, a::DataFrame)
            ret = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: Number
                    for i = 1:n
                        ret[i, j] = isna(a[i, j]) ? NA : ($f)(a[i, j], v)
                    end
                else
                    for i = 1:n
                        ret[i, j] = NA
                    end
                end
            end
            return ret
        end
        function ($f){T <: String}(a::DataFrame, v::T)
            ret = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: String
                    for i in 1:n
                        ret[i, j] = isna(a[i, j]) ? NA : ($f)(a[i, j], v)
                    end
                else
                    for i in 1:n
                        ret[i, j] = NA
                    end
                end
            end
            return ret
        end
        function ($f){T <: String}(v::T, a::DataFrame)
            ret = DataFrame(Array(Bool, size(a)))
            n, p = nrow(a), ncol(a)
            for j in 1:p
                if typeof(a[j]).parameters[1] <: String
                    for i = 1:n
                        ret[i, j] = isna(a[i, j]) ? NA : ($f)(a[i, j], v)
                    end
                else
                    for i = 1:n
                        ret[i, j] = NA
                    end
                end
            end
            return ret
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
    end
end

# Missing binary operators on NA's
for f in (:(==), :(!=), :(<), :(>), :(<=), :(>=), :(.>), :(.>=),
          :max, :min,
          :(+), :(-), :(*), :(/), :(^),
          :(&), :(|), :(\), :(./), :(.\), :(.*), :(.^),
          :(.+), :(.-), :(.==), :(.!=), :(.<), :(.<=),
          :div, :fld, :rem, :mod)
    @eval begin
        function ($f){T}(d::NAtype, x::T)
            return NA
        end
        function ($f){T}(x::T, d::NAtype)
            return NA
        end
    end
end

