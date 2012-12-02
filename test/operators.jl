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

arithmetic_operators = [:(+), :(.+), :(-), :(.-), :(*), :(.*), :(/), :(./),
                        :(^), :(.^), :(div), :(mod), :(fld), :(rem),
                        :(max), :(min)]

scalar_arithmetic_operators = [:(+), :(.+), :(-), :(.-), :(*), :(.*), :(/), :(./),
                        :(^), :(.^), :(div), :(mod), :(fld), :(rem),
                        :(max), :(min)]

array_arithmetic_operators = [:(+), :(.+), :(-), :(.-), :(*), :(.*), :(/), :(./),
                        :(^), :(.^), :(div), :(mod), :(fld), :(rem),
                        :(max), :(min)]

bit_operators = [:(&), :(|), :($)]

# logical_operators = [:(&&), :(||), :($$)]

unary_vector_operators = [:diff, :cumprod, :cumsum, :cumsum_kbn,
                          :min, :max, :prod, :sum, :mean, :median,
                          :std, :var, :fft, :norm]

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
for f in comparison_operators
  @eval begin
    @assert isna(($f)(NA, NA))
  end
end

# All comparison operators return NA when comparing scalars with NA
for f in comparison_operators
  @eval begin
    @assert isna(($f)(NA, 1))
  end
end

# All comparison operators return NA when comparing NA with scalars
for f in comparison_operators
  @eval begin
    @assert isna(($f)(1, NA))
  end
end

# All arithmetic operators return NA when operating on two NA's
for f in arithmetic_operators
  @eval begin
    @assert isna(($f)(NA, NA))
  end
end

# All arithmetic operators return NA when operating on a scalar and an NA
for f in arithmetic_operators
  @eval begin
    @assert isna(($f)(1, NA))
  end
end

# All arithmetic operators return NA when operating on an NA and a scalar
for f in arithmetic_operators
  @eval begin
    @assert isna(($f)(NA, 1))
  end
end

# All bit operators return NA when operating on two NA's
for f in bit_operators
  @eval begin
    @assert isna(($f)(NA, NA))
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
df = DataFrame(quote
                 A = dvones(N)
                 B = dvones(N)
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
