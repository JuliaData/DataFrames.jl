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
  r_squared::Float
end

function print(results::OLSResults)
  println()
  println()
  println("Call to lm(): $(results.call)")
  println()
  println("Fitted Model:")
  println()
  printf(" %16.s  %8.s  %8.s  %8.s  %8.s\n", "Term", "Estimate", "Std. Error", "t", "p-Value")
  N = size(results.coefficients, 1)
  for i = 1:N
    printf(" %16.s  ", results.predictor_names[i])
    println(join(map(z -> sprintf("%5.7f", z), {results.coefficients[i, 1],
                  results.std_errors[i, 1],
                  results.t_stats[i, 1],
                  results.p_values[i, 1]}), "  "))
  end
  println()
  printf("R-squared: %0.4f", results.r_squared)
  println()
  println()
end

# minimal first version: support y ~ x1 + x2 + log(x3)
function lm(ex::Expr, df::DataFrame)
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
  t_values = coefficients ./ (diag(covariance) / sqrt(n))
  r_squared = 1.0 - sigma / var(y)
  OLSResults(call,
             x,
             mm.model_colnames,
             y,
             coefficients,
             diag(covariance),
             t_values,
             t_values,
             predictions,
             residuals,
             r_squared)
end
