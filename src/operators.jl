const unary_operators = [:(+), :(-), :(!), :(*)]

const numeric_unary_operators = [:(+), :(-)]

const logical_unary_operators = [:(!)]

const elementary_functions = [:abs, :sign, :acos, :acosh, :asin,
                              :asinh, :atan, :atanh, :sin, :sinh,
                              :cos, :cosh, :tan, :tanh, :ceil, :floor,
                              :round, :trunc, :exp, :exp2, :expm1, :log, :log10, :log1p,
                              :log2, :exponent, :sqrt, :gamma, :lgamma, :digamma,
                              :erf, :erfc]

const two_argument_elementary_functions = [:round, :ceil, :floor, :trunc]

const special_comparison_operators = [:isless]

const comparison_operators = [:(==), :(.==), :(!=), :(.!=),
                              :(>), :(.>), :(>=), :(.>=), :(<), :(.<),
                              :(<=), :(.<=)]

const scalar_comparison_operators = [:(==), :(!=), :(>), :(>=),
                                     :(<), :(<=)]

const array_comparison_operators = [:(.==), :(.!=), :(.>), :(.>=), :(.<), :(.<=)]

const vectorized_comparison_operators = [(:(.==), :(==)), (:(.!=), :(!=)),
                                         (:(.>), :(>)), (:(.>=), :(>=)),
                                         (:(.<), :(<)), (:(.<=), :(<=))]

const binary_operators = [:(+), :(.+), :(-), :(.-), :(*), :(.*), :(/), :(./),
                          :(.^), :(div), :(mod), :(fld), :(rem)]

const induced_binary_operators = [:(^)]

const arithmetic_operators = [:(+), :(.+), :(-), :(.-), :(*), :(.*), :(/), :(./),
                              :(.^), :(div), :(mod), :(fld), :(rem)]

const induced_arithmetic_operators = [:(^)]

const biscalar_operators = [:(max), :(min)]

const scalar_arithmetic_operators = [:(+), :(-), :(*), :(/),
                                     :(div), :(mod), :(fld), :(rem)]

const induced_scalar_arithmetic_operators = [:(^)]

const array_arithmetic_operators = [:(+), :(.+), :(-), :(.-), :(.*), :(.^)]

const bit_operators = [:(&), :(|), :($)]

const unary_vector_operators = [:minimum, :maximum, :prod, :sum, :mean, :median, :std,
                                :var, :mad, :norm, :skewness, :kurtosis]


# TODO: dist, iqr, rle, inverse_rle

const pairwise_vector_operators = [:diff, :reldiff, :percent_change]

const cumulative_vector_operators = [:cumprod, :cumsum, :cumsum_kbn, :cummin, :cummax]

const ffts = [:fft]

const binary_vector_operators = [:dot, :cor, :cov, :cor_spearman]

const rowwise_operators = [:rowmins, :rowmaxs, :rowprods, :rowsums,
                           :rowmeans, :rowmedians, :rowstds, :rowvars,
                           :rowffts, :rownorms]

const columnar_operators = [:colmins, :colmaxs, :colprods, :colsums,
                            :colmeans, :colmedians, :colstds, :colvars,
                            :colffts, :colnorms]

const boolean_operators = [:any, :all]

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

#
# Unary operator macros for DataFrames and DataArrays
#

macro dataframe_unary(f)
    esc(:($(f)(d::DataFrame) = DataFrame([$(f)(d[i]) for i=1:size(d, 2)], deepcopy(index(d)))))
end

#
# Binary operator macros for DataFrames and DataArrays
#

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

# Unary operators, DataFrames
@dataframe_unary !
@dataframe_unary -
# As in Base, these are identity operators
for f in (:(+), :(*))
    @eval $(f)(d::DataFrame) = d
end

#
# Elementary functions
#
# XXX: The below should be revisited once we have a way to infer what
# the proper return type of an array should be.

# One-argument elementary functions that return the same type as their
# inputs
for f in (:abs, :sign)
    @eval begin
        @dataframe_unary $(f)
    end
end

# One-argument elementary functions that always return floating points
for f in (:acos, :acosh, :asin, :asinh, :atan, :atanh, :sin, :sinh, :cos,
          :cosh, :tan, :tanh, :exp, :exp2, :expm1, :log, :log10, :log1p,
          :log2, :exponent, :sqrt, :gamma, :lgamma, :digamma, :erf, :erfc)
    @eval begin
        @dataframe_unary $(f)
    end
end

# Elementary functions that take varargs
for f in (:round, :ceil, :floor, :trunc)
    @eval begin
        $(f)(d::DataFrame, args::Integer...) = 
            DataFrame([$(f)(d[i], args...) for i=1:size(d, 2)], deepcopy(index(d)))
    end
end

#
# Bit operators
#

for f in (:&, :|, :$)
    @eval begin
        # DataFrame
        @dataframe_binary $(f)
    end
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

for sf in scalar_comparison_operators
    vf = symbol(".$sf")
    @eval begin
        @dataframe_binary $vf
    end
end

#
# Binary operators
#

for f in arithmetic_operators
    @eval begin
        @dataframe_binary $f
    end
end

for f in (:minimum, :maximum, :prod, :sum, :mean, :median, :std, :var, :norm)
    colf = symbol("col$(f)s")
    rowf = symbol("row$(f)s")
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
    end
end

#
# Boolean operators
#

function all(df::AbstractDataFrame)
    for i in 1:size(df, 2)
        x = all(df[i])
        if isna(x)
            return NA
        end
        if !x
            return false
        end
    end
    true
end

function any(df::AbstractDataFrame)
    has_na = false
    for i in 1:size(df, 2)
        x = any(df[i])
        if !isna(x)
            if x
                return true
            end
        else
            has_na = true
        end
    end
    has_na ? NA : false
end
