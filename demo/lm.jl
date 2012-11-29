load("Distributions")
using Distributions

type OLSResults
  call::Formula
  predictors::Matrix
  predictor_names::Vector
  responses::Matrix
  coefficients::Matrix
  std_errors::Vector
  t_stats::Matrix
  p_values::Matrix
  predictions::Matrix
  residuals::Matrix
  r_squared::Float64
end

function print(results::OLSResults)
  function p_value_stars(p_value::Float64)
    if 0.05 < p_value < 0.1
      return "."
    elseif 0.01 < p_value < 0.05
      return "*"
    elseif 0.001 < p_value < 0.01
      return "**"
    elseif p_value < 0.001
      return "***"
    else
      return " "
    end
  end  
  println()
  println()
  println("Call:")
  println("lm($(results.call))") # Should print df as well.
  println()
  println("Coefficients:")
  println()
  @printf(" %12.s %12s %12s %12s %12s\n", "Term", "Estimate", "Std. Error", "t value", "Pr(>|t|)")
  N = size(results.coefficients, 1)
  for i = 1:N
    @printf(" %12.s  ", results.predictor_names[i])
    print(join(map(z -> @sprintf("%12f", z),
                    {results.coefficients[i, 1],
                     results.std_errors[i, 1],
                     results.t_stats[i, 1],
                     results.p_values[i, 1]}), " "))
    print(" ")
    println(p_value_stars(results.p_values[i, 1]))
  end
  println("---")
  println("Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1")
  println()  
  @printf("Adjusted R-squared: %0.4f", results.r_squared)
  println()
  println()
  #Residual standard error: 0.04082 on 2 degrees of freedom
  #Multiple R-squared: 0.9994,	Adjusted R-squared: 0.9985 
  #F-statistic:  1081 on 3 and 2 DF,  p-value: 0.0009244 
end

# minimal first version: support y ~ x1 + x2 + log(x3)
function lm(ex::Expr, df::AbstractDataFrame)
  call = Formula(ex)
  mf = model_frame(call, df)
  mm = model_matrix(mf)
  x = mm.model
  y = mm.response
  n = size(x, 1)
  p = size(x, 2)
  coefficients = inv(x' * x) * x' * y
  predictions = x * coefficients
  residuals = y - predictions
  degrees = n - p
  sigma = sum(residuals.^2) / degrees
  covariance = sigma * inv(x' * x)
  t_values = coefficients ./ diag(covariance).^(1/2)
  p_values = map(t -> 2 * (1 - cdf(TDist(degrees), t)), t_values)
  r_squared = 1.0 - sigma / var(y)
  OLSResults(call,
             x,
             mm.model_colnames,
             y,
             coefficients,
             diag(covariance).^(1/2),
             t_values,
             p_values,
             predictions,
             residuals,
             r_squared)
end
