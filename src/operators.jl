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

pairwise_vector_operators = [:diff]

cumulative_vector_operators = [:cumprod, :cumsum, :cumsum_kbn]

ffts = [:fft]

binary_vector_operators = [:dot, :cor_pearson, :cov_pearson,
                           :cor_spearman, :cov_spearman]

columnar_operators = [:colmins, :colmaxs, :colprods, :colsums,
                      :colmeans, :colmedians, :colstds, :colvars,
                      :colffts, :colnorms]

boolean_operators = [:any, :all]

for f in unary_operators
    @eval begin
        function ($f)(d::NAtype)
            return NA
        end
        function ($f)(dv::DataVec)
            res = deepcopy(dv)
            for i in 1:length(dv)
                res[i] = ($f)(dv[i])
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

for f in elementary_functions
    @eval begin
        function ($f)(d::NAtype)
            return NA
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
        function ($f){S, T}(a::AbstractDataVec{S}, b::AbstractDataVec{T})
            res = DataVec(Array(Bool, length(a)),
                          BitArray(length(a)),
                          naRule(a),
                          false)
            for i in 1:length(a)
                res[i] = isna(a[i]) ? NA : ($f)(a[i], b[i])
            end
            return res
        end
        function ($f){S, T <: Union(String, Number)}(a::AbstractDataVec{S}, v::T)
            res = DataVec(Array(Bool,length(a)),
                          BitArray(length(a)),
                          naRule(a),
                          false)
            for i in 1:length(a)
                res[i] = isna(a[i]) ? NA : ($f)(a[i], v)
            end
            return res
        end
        function ($f){S <: Union(String, Number), T}(v::S, a::AbstractDataVec{T})
            res = DataVec(Array(Bool, length(a)),
                          BitArray(length(a)),
                          naRule(a),
                          false)
            for i in 1:length(a)
                res[i] = isna(a[i]) ? NA : ($f)(v, a[i])
            end
            res
        end
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

for f in cumulative_vector_operators
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
