require("Distributions")
require("DataFrames")
using DataFrames
using Distributions

import Base.show

abstract AbstractLinearModel

type LinearModel <: AbstractLinearModel
  formula::Formula
  predictors::Matrix
  predictor_names::Vector
  response::Vector
  response_name::String
  coefficients::Vector
  predictions::Vector
  residuals::Vector
  df::Int
  xtxchol::Base.CholeskyDensePivoted
  restriction_matrix::Matrix
  restriction_vector::Vector
end

type LinearModelInference <: AbstractLinearModel
  formula::Formula
  predictors::Matrix
  predictor_names::Vector
  response::Vector
  response_name::String
  coefficients::Vector
  predictions::Vector
  residuals::Vector
  df::Int
  xtxchol::Base.CholeskyDensePivoted
  std_errors::Vector
  t_statistics::Vector
  p_values::Vector
  r_squared::Float64
  finite_sample::Bool
end

type TestResult
  test_statistic::Float64
  distribution::ASCIIString
  df::Vector{Int}
  p_value::Float64
end

# type RestrictedLinearModel <: AbstractLinearModel
#   formula::Formula
#   predictors::Matrix
#   predictor_names::Vector
#   response::Vector
#   response_name::String
#   coefficients::Vector
#   predictions::Vector
#   residuals::Vector
#   df::Int
#   restriction_matrix::Matrix
#   restriction_vector::Vector
# end

function show(io::Any, fit::AbstractLinearModel)
  @printf("\n%s\n\nCoefficients:\n", fit.formula)
  println(join([@sprintf("%12s", pn) for pn in fit.predictor_names]))
  print(join([@sprintf("%12f", est) for est in fit.coefficients]))
end

function show(io::Any, obj::LinearModelInference)
  @printf("\n%s\n\nCoefficients:\n", obj.formula)
  @printf("         Term    Estimate  Std. Error     %s value    Pr(>|%s|)\n", obj.finite_sample ? 't' : 'z', obj.finite_sample ? 't' : 'z')
  N = size(obj.coefficients, 1)
  for i = 1:N
    @printf(" %12s%12.5f%12.5f%12.3f%12.3f %-3s\n",
      obj.predictor_names[i],
      obj.coefficients[i],
      obj.std_errors[i],
      obj.t_statistics[i],
      obj.p_values[i],
      p_value_stars(obj.p_values[i]))
  end
  @printf("---\nSignif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1\n\nR-squared: %0.4f\n", obj.r_squared)
end

function show(io::Any, obj::TestResult)
  if length(obj.df) == 1
    @printf("%8s   Pr(>%s)\n%8.3f%8.3f", 
      @sprintf("%s(%d)", obj.distribution, obj.df[1]), obj.distribution, obj.test_statistic, obj.p_value)
  else
    @printf("%8s   Pr(>%s)\n%8.3f%8.3f", 
      @sprintf("%s(%d,%d)", obj.distribution, obj.df[1], obj.df[2]), obj.distribution, obj.test_statistic, obj.p_value)
  end
end

function lm(call::Expr, df::AbstractDataFrame)
  formula = Formula(call)
  mf = model_frame(formula, df)
  mm = model_matrix(mf)
  x = mm.model
  xtxchol = cholpd(x'x, 'L')
  y = reshape(mm.response, size(mm.response, 1))
  coefficients = xtxchol \ (x'y)
  predictions = x * coefficients
  residuals = y - predictions
  LinearModel(formula,
             x,
             mm.model_colnames,
             y,
             mm.response_colnames[1],
             coefficients,
             predictions,
             residuals,
             size(x, 1) - size(x, 2),
             xtxchol,
             eye(size(x, 2)),
             zeros(size(x, 2)))
end

function restrict(fit::AbstractLinearModel, restriction_matrix::Matrix, restriction_vector::Vector)
  if length(restriction_vector) < size(restriction_matrix, 1)
    if length(restriction_vector) != size(restriction_matrix, 2) || size(restriction_matrix, 1) != size(fit.predictors, 2) error("Illegal restrictions") end
    H = null(restriction_matrix')
    h = restriction_matrix'\restriction_vector
  elseif length(restriction_vector) == size(restriction_matrix, 1)
    if length(restriction_vector) != size(fit.predictors, 2) error("Illegal restrictions") end
    H = restriction_matrix
    h = restriction_vector
  else
    error("Illegal restrictions")
  end
  x = fit.predictors*H
  xtxchol = cholpd(x'x, 'L')
  y = fit.response - fit.predictors*h
  coefficients = H*(xtxchol \ (x'y)) + h
  predictions = fit.predictors * coefficients
  residuals = fit.response - predictions
  LinearModel(fit.formula,
                        fit.predictors,
                        fit.predictor_names,
                        fit.response,
                        fit.response_name,
                        coefficients,
                        predictions,
                        residuals,
                        size(x, 1) - size(x, 2),
                        xtxchol,
                        H,
                        h)
end

function cov(fit::AbstractLinearModel) 
  e = fit.residuals
  xhsqrt = fit.xtxchol.LR \ fit.restriction_matrix'
  return dot(e, e) / (size(fit.predictors, 1) - size(fit.predictors, 2)) * xhsqrt'xhsqrt
end

function covHC(fit::AbstractLinearModel)
  xeps = (fit.predictors * fit.restriction_matrix) .* repmat(fit.residuals, 1, size(fit.restriction_matrix, 2))
  varsqrt = xeps * (fit.xtxchol \ fit.restriction_matrix')
  return varsqrt'varsqrt
end

function inference(fit::AbstractLinearModel, covest::Function, finite_sample::Bool)
  parcov = covest(fit)
  std_errors = sqrt(diag(parcov))
  t_values = fit.coefficients ./ std_errors
  if finite_sample
    p_values = 2.0 * (1.0 - cdf(TDist(size(fit.predictors, 1) - size(fit.predictors, 2)), abs(t_values)))
  else
    p_values = 2.0 * (1.0 - cdf(Normal(), abs(t_values)))
  end    
  r_squared = 1.0 - var(fit.residuals) / var(fit.response - fit.predictors*fit.restriction_vector)
  LinearModelInference(fit.formula,
                      fit.predictors,
                      fit.predictor_names,
                      fit.response,
                      fit.response_name,
                      fit.coefficients,
                      fit.predictions,
                      fit.residuals,
                      fit.df,
                      fit.xtxchol,
                      std_errors,
                      t_values,
                      p_values,
                      r_squared,
                      finite_sample)
end
inference(fit) = inference(fit, cov, true)
inference_robust(fit) = inference(fit, covHC, false)

function logLik(fit::AbstractLinearModel)
  T, k = size(fit.predictors)
  return -0.5 * T * log(dot(fit.residuals, fit.residuals) / T)
end

function lrtest(fit0::AbstractLinearModel, fitA::AbstractLinearModel)
  ll0 = logLik(fit0)
  llA = logLik(fitA)
  tst = 2.0 * (llA - ll0)
  df = size(fitA.predictors, 2) - size(fit0.predictors, 2)
  return TestResult(tst, "Chisq", [df], 1.0 - cdf(Chisq(df), tst))
end

function waldtest(fit::AbstractLinearModel, restriction_matrix::Matrix, restriction_vector::Vector, covest::Function, finite_sample::Bool)
  tmpfit = restrict(fit, restriction_matrix, restriction_vector)
  tstsqrt = chol(covest(fit), 'L') \ (fit.coefficients - tmpfit.coefficients)
  tst = dot(tstsqrt, tstsqrt)
  df = (tmpfit.df - fit.df)
  if finite_sample
    tst /= df
    p_value = 1.0 - cdf(FDist(df, length(fit.response) - df), tst)
  else
    p_value = 1.0 - cdf(Chisq(df), tst)
  end
  return TestResult(tst, 
                    finite_sample ? "F" : "Chisq", 
                    finite_sample ? [tmpfit.df - fit.df, fit.df] : [tmpfit.df - fit.df], p_value)
end
waldtest(fit, restriction_matrix, restriction_vector) = waldtest(fit, restriction_matrix, restriction_vector, cov, true)
waldtest_robust(fit, restriction_matrix, restriction_vector) = waldtest(fit, restriction_matrix, restriction_vector, covHC, false)

function p_value_stars(p_value::Float64)
  if p_value < 0.001
    return "***"
  elseif p_value < 0.01
    return "**"
  elseif p_value < 0.05
    return "*"
  elseif p_value < 0.1
    return "."
  else
    return " "
  end
end  
