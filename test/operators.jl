require("extras/test.jl")

load("DataFrames")
using DataFrames

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

pairwise_vector_operators = [:diff, :percent_change]

cumulative_vector_operators = [:cumprod, :cumsum, :cumsum_kbn, :cummin, :cummax]

ffts = [:fft]

binary_vector_operators = [:dot, :cor_pearson, :cov_pearson,
                           :cor_spearman, :cov_spearman]

columnar_operators = [:colmins, :colmaxs, :colprods, :colsums,
                      :colmeans, :colmedians, :colstds, :colvars,
                      :colffts, :colnorms]

boolean_operators = [:any, :all]

# All unary operators return NA when evaluating NA
for f in unary_operators
  @eval begin
    @assert isna(($f)(NA))
  end
end

# All elementary functions return NA when evaluating NA
for f in elementary_functions
  @eval begin
    @assert isna(($f)(NA))
  end
end

# All comparison operators return NA when comparing NA with NA
# All comparison operators return NA when comparing scalars with NA
# All comparison operators return NA when comparing NA with scalars
for f in comparison_operators
  @eval begin
    @assert isna(($f)(NA, NA))
    @assert isna(($f)(NA, 1))
    @assert isna(($f)(1, NA))
  end
end

# All arithmetic operators return NA when operating on two NA's
# All arithmetic operators return NA when operating on a scalar and an NA
# All arithmetic operators return NA when operating on an NA and a scalar
for f in arithmetic_operators
  @eval begin
    @assert isna(($f)(NA, NA))
    @assert isna(($f)(1, NA))
    @assert isna(($f)(NA, 1))
  end
end

# All bit operators return NA when operating on two NA's
# All bit operators return NA when operating on a scalar and an NA
# All bit operators return NA when operating on an NA and a scalar
for f in bit_operators
  @eval begin
    @assert isna(($f)(NA, NA))
    @assert isna(($f)(1, NA))
    @assert isna(($f)(NA, 1))
  end
end

# Unary operators on DataVec's should be equivalent to elementwise
# application of those same operators
N = 5
dv = dvones(N)
for f in numeric_unary_operators
  @eval begin
    for i in 1:length(dv)
      @assert (($f)(dv))[i] == ($f)(dv[i])
    end
  end
end
dv = dvtrues(N)
for f in logical_unary_operators
  @eval begin
    for i in 1:length(dv)
      @assert (($f)(dv))[i] == ($f)(dv[i])
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
  @eval begin
    for i in 1:nrow(df)
      for j in 1:ncol(df)
        @assert (($f)(df))[i, j] == ($f)(df[i, j])
      end
    end
  end
end
df = DataFrame(quote
                 A = [true, false, true, false]
               end)
for f in logical_unary_operators
  @eval begin
    for i in 1:nrow(df)
      for j in 1:ncol(df)
        @assert (($f)(df))[i, j] == ($f)(df[i, j])
      end
    end
  end
end

# Elementary functions on DataVec's
N = 5
dv = dvones(N)
for f in elementary_functions
  @eval begin
    for i in 1:length(dv)
      @assert (($f)(dv))[i] == ($f)(dv[i])
    end
  end
end

# Elementary functions on DataFrames's
N = 5
df = DataFrame(quote
                 A = dvones($(N))
                 B = dvones($(N))
               end)
for f in elementary_functions
  @eval begin
    for i in 1:nrow(df)
      for j in 1:ncol(df)
        @assert (($f)(df))[i, j] == ($f)(df[i, j])
      end
    end
  end
end

# Broadcasting operations between NA's and DataVec's
N = 5
dv = dvones(N)
for f in arithmetic_operators
  @eval begin
    for i in 1:length(dv)
      @assert isna((($f)(dv, NA))[i])
      @assert isna((($f)(NA, dv))[i])
    end
  end
end

# Broadcasting operations between NA's and DataVec's
N = 5
dv = dvones(N)
for f in arithmetic_operators
  @eval begin
    for i in 1:length(dv)
      @assert isna((($f)(dv, NA))[i])
      @assert isna((($f)(NA, dv))[i])
    end
  end
end

# Broadcasting operations between scalars and DataVec's
N = 5
dv = dvones(N)
for f in arithmetic_operators
  @eval begin
    for i in 1:length(dv)
      @assert (($f)(dv, 1))[i] == ($f)(dv[i], 1)
      @assert (($f)(1, dv))[i] == ($f)(1, dv[i])
    end
  end
end

# Broadcasting operations between NA's and DataFrames's
N = 5
df = DataFrame(quote
                 A = dvones($(N))
                 B = dvones($(N))
               end)
for f in arithmetic_operators
  @eval begin
    for i in 1:nrow(df)
      for j in 1:ncol(df)
        @assert isna((($f)(df, NA))[i, j])
        @assert isna((($f)(NA, df))[i, j])
      end
    end
  end
end

# Broadcasting operations between scalars and DataFrames's
N = 5
df = DataFrame(quote
                 A = dvones($(N))
                 B = dvones($(N))
               end)
for f in arithmetic_operators
  @eval begin
    for i in 1:nrow(df)
      for j in 1:ncol(df)
        @assert (($f)(df, 1))[i, j] == ($f)(df[i, j], 1)
        @assert (($f)(1, df))[i, j] == ($f)(1, df[i, j])
      end
    end
  end
end

# Binary operations on pairs of DataVec's
N = 5
dv = dvones(N)
dv[1] = NA
for f in array_arithmetic_operators
  @eval begin
    for i in 1:length(dv)
      @assert isna(($f)(dv, dv)[i]) && isna(dv[i]) ||
              (($f)(dv, dv))[i] == ($f)(dv[i], dv[i])
    end
  end
end

# Binary operations on pairs of DataFrame's
# TODO: Test in the presence of in-operable types like Strings
N = 5
df = DataFrame(quote
                 A = dvones($(N))
                 B = dvones($(N))
               end)
for f in array_arithmetic_operators
  @eval begin
    for i in 1:nrow(df)
      for j in 1:ncol(df)
        @assert isna(($f)(df, df)[i, j]) && isna(df[i, j]) ||
                (($f)(df, df))[i, j] == ($f)(df[i, j], df[i, j])
      end
    end
  end
end

# Unary vector operators on DataVec's
N = 5
dv = dvones(5)
for f in unary_vector_operators
  @eval begin
    @assert ($f)(dv) == ($f)(dv.data)
  end
end
dv[1] = NA
for f in unary_vector_operators
  @eval begin
    @assert isna(($f)(dv))
  end
end

# TODO: Pairwise vector operators on DataVec's

# Cumulative vector operators on DataVec's
N = 5
dv = dvones(N)
for f in cumulative_vector_operators
  @eval begin
    for i in 1:length(dv)
      @assert (($f)(dv))[i] == ($f)(dv.data)[i]
    end
  end
end
dv[4] = NA
for f in cumulative_vector_operators
  @eval begin
    for i in 1:3
      @assert (($f)(dv))[i] == ($f)(dv.data)[i]
    end
    for i in 4:N
      @assert isna((($f)(dv))[i])
    end
  end
end

# FFT's on DataVec's
N = 5
dv = dvones(5)
for f in ffts
  @eval begin
    @assert ($f)(dv) == ($f)(dv.data)
  end
end
dv[1] = NA
for f in ffts
  @eval begin
    @assert isna(($f)(dv))
  end
end

# Binary vector operators on DataVec's
N = 5
dv = dvones(5)
for f in binary_vector_operators
  @eval begin
    @assert (($f)(dv, dv) == ($f)(dv.data, dv.data)) ||
            (isnan(($f)(dv, dv)) && isnan(($f)(dv.data, dv.data)))
  end
end
dv[1] = NA
for f in binary_vector_operators
  @eval begin
    @assert isna(($f)(dv, dv))
  end
end

# TODO: Columnar operators on DataFrame's

# Boolean operators on DataVec's
N = 5
@assert any(dvfalses(N)) == false
@assert any(dvtrues(N)) == true
@assert all(dvfalses(N)) == false
@assert all(dvtrues(N)) == true

dv = dvfalses(N)
dv[3] = true
@assert any(dv) == true
@assert all(dv) == false

dv = dvfalses(N)
dv[2] = NA
dv[3] = true
@assert any(dv) == true
@assert all(dv) == false

dv = dvfalses(N)
dv[2] = NA
@assert isna(any(dv))
@assert all(dv) == false

dv = dvfalses(1)
dv[1] = NA
@assert isna(any(dv))
@assert isna(all(dv))

# Boolean operators on DataFrames's
N = 5
df = DataFrame(quote
                 A = dvfalses($(N))
               end)
@assert any(df) == false
@assert any(!df) == true
@assert all(df) == false
@assert all(!df) == true

df = DataFrame(quote
                 A = dvfalses($(N))
               end)
df[3, 1] = true
@assert any(df) == true
@assert all(df) == false

df = DataFrame(quote
                 A = dvfalses($(N))
               end)
df[2, 1] = NA
df[3, 1] = true
@assert any(df) == true
@assert all(df) == false

df = DataFrame(quote
                 A = dvfalses($(N))
               end)
df[2, 1] = NA
@assert isna(any(df))
@assert all(df) == false

df = DataFrame(quote
                 A = dvfalses($(N))
               end)
df[1, 1] = NA
@assert isna(any(dv))
@assert isna(all(dv))

# Is this a genuine special case?
@assert isna(NA ^ 2.0)

#
# Equality tests
#

dv = DataVec[1, NA]
alt_dv = DataVec[2, NA]
df = DataFrame({dv})
alt_df = DataFrame({alt_dv})

@assert isna(NA == NA)
@assert isna(NA != NA)

# @assert isna(dv == dv) # SHOULD RAISE ERROR
# @assert isna(dv != dv) # SHOULD RAISE ERROR

# @assert isna(df == df) # SHOULD RAISE ERROR
# @assert isna(df != df) # SHOULD RAISE ERROR

@assert isequal(dv, dv)
@assert isequal(df, df)

@assert !isequal(dv, alt_dv)
@assert !isequal(df, alt_df)

@assert isequal(DataVec[1, NA] .== DataVec[1, NA], DataVec[true, NA])
@assert isequal(PooledDataVec[1, NA] .== PooledDataVec[1, NA], DataVec[true, NA])
@assert isequal(DataFrame({dv}) .== DataFrame({dv}), DataFrame({DataVec[true, NA]}))

@assert all(isna(NA .== dvones(5)))
@assert all(isna(dvones(5) .== NA))

@assert all(isna(NA .== df))
@assert all(isna(df .== NA))
