using Base.Test
using DataFrames

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

let
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

    unary_vector_operators = [minimum, maximum, prod, sum, mean, median, std,
                              var, mad, norm, skewness, kurtosis]

    pairwise_vector_operators = [diff, percent_change]

    cumulative_vector_operators = [cumprod, cumsum, cumsum_kbn, cummin, cummax]

    ffts = [fft]

    binary_vector_operators = [dot, cor, cov, cor_spearman]

    columnar_operators = [colmins, colmaxs, colprods, colsums,
                          colmeans, colmedians, colstds, colvars,
                          colnorms]

    boolean_operators = [any, all]

    # All unary operators return NA when evaluating NA
    for f in unary_operators
        @assert isna(f(NA))
    end

    # All elementary functions return NA when evaluating NA
    for f in elementary_functions
        @assert isna(f(NA))
    end

    # All comparison operators return NA when comparing NA with NA
    # All comparison operators return NA when comparing scalars with NA
    # All comparison operators return NA when comparing NA with scalars
    for f in comparison_operators
        @assert isna(f(NA, NA))
        @assert isna(f(NA, 1))
        @assert isna(f(1, NA))
    end

    # All arithmetic operators return NA when operating on two NA's
    # All arithmetic operators return NA when operating on a scalar and an NA
    # All arithmetic operators return NA when operating on an NA and a scalar
    for f in arithmetic_operators
        @assert isna(f(NA, NA))
        @assert isna(f(1, NA))
        @assert isna(f(NA, 1))
    end

    # All bit operators return NA when operating on two NA's
    # All bit operators return NA when operating on a scalar and an NA
    # All bit operators return NA when operating on an NA and a scalar
    for f in bit_operators
        @assert isna(f(NA, NA))
        @assert isna(f(1, NA))
        @assert isna(f(NA, 1))
    end

    # Unary operators on DataVector's should be equivalent to elementwise
    # application of those same operators
    N = 5
    dv = dataones(N)
    @test_da_pda dv begin
        for f in numeric_unary_operators
            for i in 1:length(dv)
                @assert f(dv)[i] == f(dv[i])
            end
        end
    end
    dv = datatrues(N)
    @test_da_pda dv begin
        for f in logical_unary_operators
            for i in 1:length(dv)
                @assert f(dv)[i] == f(dv[i])
            end
        end
    end

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

    # Elementary functions on DataVector's
    N = 5
    dv = dataones(N)
    @test_da_pda dv begin
        for f in elementary_functions
            for i in 1:length(dv)
                @assert f(dv)[i] == f(dv[i])
            end
        end
    end

    # Elementary functions on DataFrames's
    N = 5
    df = DataFrame(quote
                       A = dataones($(N))
                       B = dataones($(N))
                   end)
    for f in elementary_functions
        for i in 1:nrow(df)
            for j in 1:ncol(df)
                  @assert f(df)[i, j] == f(df[i, j])
            end
        end
    end

    # Broadcasting operations between NA's and DataVector's
    N = 5
    dv = dataones(N)
    @test_da_pda dv begin
        for f in arithmetic_operators
            for i in 1:length(dv)
                @assert isna(f(dv, NA)[i])
                @assert isna(f(NA, dv)[i])
            end
        end
    end

    # Broadcasting operations between NA's and DataVector's
    N = 5
    dv = dataones(N)
    @test_da_pda dv begin
        for f in arithmetic_operators
            for i in 1:length(dv)
                @assert isna(f(dv, NA)[i])
                @assert isna(f(NA, dv)[i])
            end
        end
    end

    # Broadcasting operations between scalars and DataVector's
    N = 5
    dv = dataones(N)
    @test_da_pda dv begin
        for f in arithmetic_operators
            for i in 1:length(dv)
                @assert f(dv, 1)[i] == f(dv[i], 1)
                @assert f(1, dv)[i] == f(1, dv[i])
            end
        end
    end
    dv = DataVector[false, true, false, true, false]
    for f in bit_operators
        for i in 1:length(dv)
            @assert f(dv, true)[i] == f(dv[i], true)
            @assert f(true, dv)[i] == f(true, dv[i])
        end
    end

    # Broadcasting operations between NA's and DataFrames's
    N = 5
    df = DataFrame(quote
                       A = dataones($(N))
                       B = dataones($(N))
                   end)
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
    df = DataFrame(quote
                       A = dataones($(N))
                       B = dataones($(N))
                   end)
    for f in arithmetic_operators
        for i in 1:nrow(df)
            for j in 1:ncol(df)
                @assert f(df, 1)[i, j] == f(df[i, j], 1)
                @assert f(1, df)[i, j] == f(1, df[i, j])
            end
        end
    end

    # Binary operations on (DataVector, Vector) or (Vector, DataVector)
    N = 5
    v = ones(N)
    dv = dataones(N)
    dv[1] = NA
    @test_da_pda dv begin
        for f in array_arithmetic_operators
            for i in 1:length(dv)
                @assert isna(f(v, dv)[i]) && isna(dv[i]) ||
                        f(v, dv)[i] == f(v[i], dv[i])
                @assert isna(f(dv, v)[i]) && isna(dv[i]) ||
                        f(dv, v)[i] == f(dv[i], v[i])
            end
        end
    end

    # Binary operations on pairs of DataVector's
    N = 5
    dv = dataones(N)
    dv[1] = NA
    @test_da_pda dv begin
        for f in array_arithmetic_operators
            for i in 1:length(dv)
                @assert isna(f(dv, dv)[i]) && isna(dv[i]) ||
                        f(dv, dv)[i] == f(dv[i], dv[i])
            end
        end
    end

    # Binary operations on pairs of DataFrame's
    # TODO: Test in the presence of in-operable types like Strings
    N = 5
    df = DataFrame(quote
                       A = dataones($(N))
                       B = dataones($(N))
                   end)
    for f in array_arithmetic_operators
        for i in 1:nrow(df)
            for j in 1:ncol(df)
                @assert isna(f(df, df)[i, j]) && isna(df[i, j]) ||
                        f(df, df)[i, j] == f(df[i, j], df[i, j])
            end
        end
    end

    # Unary vector operators on DataVector's
    N = 5
    dv = dataones(5)
    for f in unary_vector_operators
        @assert isequal(f(dv), f(dv.data))
    end
    dv[1] = NA
    for f in unary_vector_operators
        @assert isna(f(dv))
    end

    # Pairwise vector operators on DataVector's
    N = 5
    dv = DataVector[911, 269, 835.0, 448, 772]
    for f in pairwise_vector_operators
        @assert isequal(f(dv), f(dv.data))
    end
    dv = DataVector[NA, 269, 835.0, 448, 772]
    for f in pairwise_vector_operators
        v = f(dv)
        @assert isna(v[1])
        @assert isequal(v[2:4], f(dv.data)[2:4])
    end
    dv = DataVector[911, NA, 835.0, 448, 772]
    for f in pairwise_vector_operators
        v = f(dv)
        @assert isna(v[1])
        @assert isna(v[2])
        @assert isequal(v[3:4], f(dv.data)[3:4])
    end
    dv = DataVector[911, 269, 835.0, 448, NA]
    for f in pairwise_vector_operators
        v = f(dv)
        @assert isna(v[4])
        @assert isequal(v[1:3], f(dv.data)[1:3])
    end

    # Cumulative vector operators on DataVector's
    N = 5
    dv = dataones(N)
    for f in cumulative_vector_operators
        for i in 1:length(dv)
            @assert f(dv)[i] == f(dv.data)[i]
        end
    end
    dv[4] = NA
    for f in cumulative_vector_operators
        for i in 1:3
            @assert f(dv)[i] == f(dv.data)[i]
        end
        for i in 4:N
            @assert isna(f(dv)[i])
        end
    end

    # FFT's on DataVector's
    N = 5
    dv = dataones(5)
    for f in ffts
        @assert f(dv) == f(dv.data)
    end
    dv[1] = NA
    for f in ffts
        @assert isna(f(dv))
    end

    # Binary vector operators on DataVector's
    N = 5
    dv = dataones(5)
    for f in binary_vector_operators
        @assert f(dv, dv) == f(dv.data, dv.data) ||
                (isnan(f(dv, dv)) && isnan(f(dv.data, dv.data)))
    end
    dv[1] = NA
    for f in binary_vector_operators
        @assert isna(f(dv, dv))
    end

    # TODO: Columnar operators on DataFrame's

    # Boolean operators on DataVector's
    N = 5
    @assert any(datafalses(N)) == false
    @assert any(datatrues(N)) == true
    @assert all(datafalses(N)) == false
    @assert all(datatrues(N)) == true
    @assert any(PooledDataArray(datafalses(N))) == false
    @assert any(PooledDataArray(datatrues(N))) == true
    @assert all(PooledDataArray(datafalses(N))) == false
    @assert all(PooledDataArray(datatrues(N))) == true

    dv = datafalses(N)
    dv[3] = true
    @test_da_pda dv begin
        @assert any(dv) == true
        @assert all(dv) == false
    end

    dv = datafalses(N)
    dv[2] = NA
    dv[3] = true
    @test_da_pda dv begin
        @assert any(dv) == true
        @assert all(dv) == false
    end

    dv = datafalses(N)
    dv[2] = NA
    @test_da_pda dv begin
        @assert isna(any(dv))
        @assert all(dv) == false
    end

    dv = datafalses(1)
    dv[1] = NA
    @test_da_pda dv begin
        @assert isna(any(dv))
        @assert isna(all(dv))
    end

    # Boolean operators on DataFrames's
    N = 5
    df = DataFrame(quote
                       A = datafalses($(N))
                   end)
    @assert any(df) == false
    @assert any(!df) == true
    @assert all(df) == false
    @assert all(!df) == true

    df = DataFrame(quote
                       A = datafalses($(N))
                   end)
    df[3, 1] = true
    @assert any(df) == true
    @assert all(df) == false

    df = DataFrame(quote
                       A = datafalses($(N))
                   end)
    df[2, 1] = NA
    df[3, 1] = true
    @assert any(df) == true
    @assert all(df) == false

    df = DataFrame(quote
                       A = datafalses($(N))
                   end)
    df[2, 1] = NA
    @assert isna(any(df))
    @assert all(df) == false

    df = DataFrame(quote
                     A = datafalses($(N))
                   end)
    df[1, 1] = NA
    @assert isna(any(dv))
    @assert isna(all(dv))

    # Is this a genuine special case?
    @assert isna(NA ^ 2.0)

    #
    # Equality tests
    #

    dv = DataVector[1, NA]
    alt_dv = DataVector[2, NA]
    pdv = PooledDataArray(DataVector[1, NA])
    alt_pdv = PooledDataArray(DataVector[2, NA])
    df = DataFrame({dv})
    alt_df = DataFrame({alt_dv})

    @assert isna(NA == NA)
    @assert isna(NA != NA)

    # @assert isna(dv == dv) # SHOULD RAISE ERROR
    # @assert isna(dv != dv) # SHOULD RAISE ERROR

    # @assert isna(df == df) # SHOULD RAISE ERROR
    # @assert isna(df != df) # SHOULD RAISE ERROR

    @assert isequal(dv, dv)
    @assert isequal(pdv, pdv)
    @assert isequal(df, df)

    @assert !isequal(dv, alt_dv)
    @assert !isequal(pdv, alt_pdv)
    @assert !isequal(df, alt_df)

    @assert isequal(DataVector[1, NA] .== DataVector[1, NA], DataVector[true, NA])
    @assert isequal(PooledDataVector[1, NA] .== PooledDataVector[1, NA], DataVector[true, NA])
    @assert isequal(DataFrame({dv}) .== DataFrame({dv}), DataFrame({DataVector[true, NA]}))

    @assert all(isna(NA .== dataones(5)))
    @assert all(isna(dataones(5) .== NA))
    @assert all(isna(NA .== PooledDataArray(dataones(5))))
    @assert all(isna(PooledDataArray(dataones(5)) .== NA))

    @assert all(isna(NA .== df))
    @assert all(isna(df .== NA))

    # Run length encoding
    dv = dataones(5)
    dv[3] = NA

    v, l = rle(dv)
    @assert isequal(v, DataVector[1.0, NA, 1.0])
    @assert (l == [2, 1, 2])

    rdv = inverse_rle(v, l)
    @assert isequal(dv, rdv)
end
