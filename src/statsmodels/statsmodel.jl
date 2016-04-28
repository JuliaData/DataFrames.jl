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
typealias DataFrameModels @compat(Union{DataFrameStatisticalModel, DataFrameRegressionModel})
@delegate DataFrameModels.model [StatsBase.coef, StatsBase.confint,
                                 StatsBase.deviance, StatsBase.nulldeviance,
                                 StatsBase.loglikelihood, StatsBase.nullloglikelihood,
                                 StatsBase.df, StatsBase.df_residual, StatsBase.nobs,
                                 StatsBase.stderr, StatsBase.vcov]
@delegate DataFrameRegressionModel.model [StatsBase.residuals, StatsBase.model_response,
                                          StatsBase.predict, StatsBase.predict!]
# Need to define these manually because of ambiguity using @delegate
StatsBase.R2(mm::DataFrameRegressionModel) = R2(mm.model)
StatsBase.adjR2(mm::DataFrameRegressionModel) = adjR2(mm.model)
StatsBase.R2(mm::DataFrameRegressionModel, variant::Symbol) = R2(mm.model, variant)
StatsBase.adjR2(mm::DataFrameRegressionModel, variant::Symbol) = adjR2(mm.model, variant)

# Predict function that takes data frame as predictor instead of matrix
function StatsBase.predict(mm::DataFrameRegressionModel, df::AbstractDataFrame)
    # copy terms, removing outcome if present (ModelFrame will complain if a
    # term is not found in the DataFrame and we don't want to remove elements with missing y)
    newTerms = remove_response(mm.mf.terms)
    # create new model frame/matrix
    mf = ModelFrame(newTerms, df)
    newX = ModelMatrix(mf, mm.mf.df[1:0,:]).m
    yp = predict(mm, newX)
    out = DataArray(eltype(yp), size(df, 1))
    out[mf.msng] = yp
    return(out)
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
        println(io, "$(typeof(model))")
        println(io)
        println(io, Formula(model.mf.terms))
        println(io)
        println(io,"Coefficients:")
        show(io, ct)
    catch e
        if isa(e, ErrorException) && contains(e.msg, "coeftable is not defined")
            show(io, model.model)
        else
            rethrow(e)
        end
    end
end
