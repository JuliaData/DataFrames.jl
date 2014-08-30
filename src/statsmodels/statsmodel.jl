##############################################################################
#
# A macro for doing delegation
#
# This macro call
#
#     @delegate MyContainer.elems [:size, :length, :ndims, :endof]
#
# produces this block of expressions
#
#     size(a::MyContainer) = size(a.elems)
#     length(a::MyContainer) = length(a.elems)
#     ndims(a::MyContainer) = ndims(a.elems)
#     endof(a::MyContainer) = endof(a.elems)
#
##############################################################################

macro delegate(source, targets)
    typename = esc(source.args[1])
    fieldname = esc(Expr(:quote, source.args[2].args[1]))
    funcnames = targets.args
    n = length(funcnames)
    result = quote begin end end
    for i in 1:n
        funcname = esc(funcnames[i])
        f = quote
            ($funcname)(a::($typename), args...) = ($funcname)(a.($fieldname), args...)
        end
        push!(result.args[2].args, f)
    end
    return result
end

# Wrappers for DataFrameStatisticalModel and DataFrameRegressionModel
immutable DataFrameStatisticalModel{M,T} <: StatisticalModel
    model::M
    mf::ModelFrame
    mm::ModelMatrix{T}
end

immutable DataFrameRegressionModel{M,T} <: RegressionModel
    model::M
    mf::ModelFrame
    mm::ModelMatrix{T}
end

for (modeltype, dfmodeltype) in ((:StatisticalModel, DataFrameStatisticalModel),
                                 (:RegressionModel, DataFrameRegressionModel))
    @eval begin
        function StatsBase.fit{T<:$modeltype}(::Type{T}, f::Formula, df::AbstractDataFrame,
                                              args...; kwargs...)
            mf = ModelFrame(f, df)
            mm = ModelMatrix(mf)
            y = model_response(mf)
            $dfmodeltype(fit(T, mm.m, y, args...; kwargs...), mf, mm)
        end
    end
end

# Delegate functions from StatsBase that use our new types
typealias DataFrameModels Union(DataFrameStatisticalModel, DataFrameRegressionModel)
@delegate DataFrameModels.model [StatsBase.coef, StatsBase.confint, StatsBase.deviance,
                                 StatsBase.loglikelihood, StatsBase.nobs, StatsBase.stderr,
                                 StatsBase.vcov]
@delegate DataFrameRegressionModel.model [StatsBase.residuals, StatsBase.model_response,
                                          StatsBase.predict, StatsBase.predict!]

# Predict function that takes data frame as predictor instead of matrix
function StatsBase.predict(mm::DataFrameRegressionModel, df::AbstractDataFrame)
    # copy terms remove outcome if present
    newTerms = Terms(mm.mf.terms)
    removeResponse!(newTerms)
    # create new model frame/matrix
    newX = ModelMatrix(ModelFrame(newTerms, df)).m
    predict(mm, newX)
end

# coeftable implementation
function StatsBase.coeftable(model::DataFrameModels)
    ct = coeftable(model.model)
    cfnames = coefnames(model.mf)
    if length(ct.rownms) == length(cfnames)
        ct.rownms = cfnames
    end
    ct
end

# show function that delegates to coeftable
function Base.show(io::IO, model::DataFrameModels)
    try
        ct = coeftable(model)
        println(io, "$(typeof(model)):\n\nCoefficients:")
        show(io, ct)
    catch e
        if isa(e, String) && beginswith(e, "coeftable is not defined")
            show(io, model.model)
        else
            rethrow(e)
        end
    end
end
