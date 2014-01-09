module TestOperators
    using Base.Test
    using DataArrays
    using DataFrames
    using Stats

    macro test_da_pda(da, code)
        esc(quote
            let $da = copy($da)
                $code
            end
            let $da = PooledDataArray($da)
                $code
            end
        end)
    end

    unary_operators = [(+), (-), (!)]

    numeric_unary_operators = [(+), (-)]

    logical_unary_operators = [(!)]

    elementary_functions = [abs, sign, acos, acosh, asin,
                            asinh, atan, atanh, sin, sinh,
                            cos, cosh, tan, tanh, ceil, floor,
                            round, trunc, exp, exp2, expm1, log, log10, log1p,
                            log2, exponent, sqrt, gamma, lgamma, digamma,
                            erf, erfc]

    special_comparison_operators = [isless]

    comparison_operators = [(==), (.==), (!=), (.!=),
                            (>), (.>), (>=), (.>=), (<), (.<),
                            (<=), (.<=)]

    scalar_comparison_operators = [(==), (!=), (>), (>=),
                                   (<), (<=)]

    array_comparison_operators = [(.==), (.!=), (.>), (.>=), (.<), (.<=)]

    binary_operators = [(+), (.+), (-), (.-), (*), (.*), (/), (./),
                        (.^), (div), (mod), (fld), (rem),
                        (&), (|), ($)]

    induced_binary_operators = [(^)]

    arithmetic_operators = [(+), (.+), (-), (.-), (*), (.*), (/), (./),
                            (.^), (div), (mod), (fld), (rem)]

    induced_arithmetic_operators = [(^)]

    biscalar_operators = [(max), (min)]

    scalar_arithmetic_operators = [(+), (-), (*), (/),
                                   (div), (mod), (fld), (rem)]

    induced_scalar_arithmetic_operators = [(^)]

    array_arithmetic_operators = [(+), (.+), (-), (.-), (.*), (./), (.^)]

    bit_operators = [(&), (|), ($)]

    unary_vector_operators = [minimum, maximum, prod, sum, mean, median, std, var, norm]

    ex_tunary_vector_operators = [mad, skewness, kurtosis]

    pairwise_vector_operators = [diff]#, percent_change]

    cumulative_vector_operators = [cumprod, cumsum, cumsum_kbn, cummin, cummax]

    ffts = [fft]

    binary_vector_operators = [dot, cor, cov, corspearman]

    columnar_operators = [colmins, colmaxs, colprods, colsums,
                          colmeans, colmedians, colstds, colvars,
                          colnorms]

    boolean_operators = [any, all]

    # Unary operators on DataFrame's should be equivalent to elementwise
    # application of those same operators
    df = DataFrame(quote
                       A = [1, 2, 3, 4]
                       B = [1.0, pi, pi, e]
                   end)
    for f in numeric_unary_operators
        for i in 1:nrow(df)
            for j in 1:ncol(df)
                @assert f(df)[i, j] == f(df[i, j])
            end
        end
    end
    df = DataFrame(quote
                       A = [true, false, true, false]
                   end)
    for f in logical_unary_operators
        for i in 1:nrow(df)
            for j in 1:ncol(df)
                @assert f(df)[i, j] == f(df[i, j])
            end
        end
    end

    # Elementary functions on DataFrames's
    N = 5
    df = DataFrame(A = DataArray(ones(N)),
                   B = DataArray(ones(N)))
    for f in elementary_functions
        for i in 1:nrow(df)
            for j in 1:ncol(df)
                  @assert f(df)[i, j] == f(df[i, j])
            end
        end
    end

    # Broadcasting operations between NA's and DataFrames's
    N = 5
    df = DataFrame(A = DataArray(ones(N)),
                   B = DataArray(ones(N)))
    for f in arithmetic_operators
        for i in 1:nrow(df)
            for j in 1:ncol(df)
                @assert isna(f(df, NA)[i, j])
                @assert isna(f(NA, df)[i, j])
            end
        end
    end

    # Broadcasting operations between scalars and DataFrames's
    N = 5
    df = DataFrame(A = DataArray(ones(N)),
                   B = DataArray(ones(N)))
    for f in arithmetic_operators
        for i in 1:nrow(df)
            for j in 1:ncol(df)
                @assert f(df, 1)[i, j] == f(df[i, j], 1)
                @assert f(1, df)[i, j] == f(1, df[i, j])
            end
        end
    end

    # Binary operations on pairs of DataFrame's
    # TODO: Test in the presence of in-operable types like Strings
    N = 5
    df = DataFrame(A = DataArray(ones(N)),
                   B = DataArray(ones(N)))
    for f in array_arithmetic_operators
        for i in 1:nrow(df)
            for j in 1:ncol(df)
                @assert isna(f(df, df)[i, j]) && isna(df[i, j]) ||
                        f(df, df)[i, j] == f(df[i, j], df[i, j])
            end
        end
    end

    # TODO: Columnar operators on DataFrame's

    # Boolean operators on DataFrames's
    N = 5
    df = DataFrame(A = DataArray(falses(N)))
    @assert any(df) == false
    @assert any(!df) == true
    @assert all(df) == false
    @assert all(!df) == true

    df = DataFrame(A = DataArray(falses(N)))
    df[3, 1] = true
    @assert any(df) == true
    @assert all(df) == false

    df = DataFrame(A = DataArray(falses(N)))
    df[2, 1] = NA
    df[3, 1] = true
    @assert any(df) == true
    @assert all(df) == false

    dv = @data [false, NA]
    dv[1] = NA

    df = DataFrame(A = DataArray(falses(N)))
    df[2, 1] = NA
    @assert isna(any(df))
    @assert all(df) == false

    df = DataFrame(A = DataArray(falses(N)))
    df[1, 1] = NA
    @assert isna(any(dv))
    @assert isna(all(dv))

    # Is this a genuine special case?
    @assert isna(NA ^ 2.0)

    #
    # Equality tests
    #
    df = DataFrame({dv})
    alt_dv = @data [false, NA]
    alt_dv[1] = NA
    alt_df = DataFrame({alt_dv})
    # @assert isequal(DataFrame({dv}) .== DataFrame({dv}), DataFrame({DataVector[true, NA]}))

    @assert all(isna(NA .== df))
    @assert all(isna(df .== NA))
end
