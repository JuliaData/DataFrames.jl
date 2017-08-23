import TableTraits
import DataValues
import NamedTuples

# T is the type of the elements produced
# TS is a tuple type that stores the columns of the DataFrame
immutable DataFrameIterator{T, TS}
    df::DataFrame
    # This field hols a tuple with the columns of the DataFrame.
    # Having a tuple of the columns here allows the iterator
    # functions to access the columns in a type stable way.
    columns::TS
end

TableTraits.isiterable(x::DataFrame) = true
TableTraits.isiterabletable(x::DataFrame) = true

function TableTraits.getiterator(df::DataFrame)
    col_expressions = Array{Expr,1}()
    df_columns_tuple_type = Expr(:curly, :Tuple)
    for i in 1:length(df.columns)
        if isa(df.columns[i], DataArray)
            push!(col_expressions, Expr(:(::), names(df)[i], DataValues.DataValue{eltype(df.columns[i])}))
        else
            push!(col_expressions, Expr(:(::), names(df)[i], eltype(df.columns[i])))
        end
        push!(df_columns_tuple_type.args, typeof(df.columns[i]))
    end
    t_expr = NamedTuples.make_tuple(col_expressions)

    t2 = :(DataFrameIterator{Float64,Float64})
    t2.args[2] = t_expr
    t2.args[3] = df_columns_tuple_type

    t = eval(t2)

    e_df = t(df, (df.columns...))

    return e_df
end

function Base.length{T,TS}(iter::DataFrameIterator{T,TS})
    return size(iter.df,1)
end

function Base.eltype{T,TS}(iter::DataFrameIterator{T,TS})
    return T
end

function Base.start{T,TS}(iter::DataFrameIterator{T,TS})
    return 1
end

@generated function Base.next{T,TS}(iter::DataFrameIterator{T,TS}, state)
    constructor_call = Expr(:call, :($T))
    for i in 1:length(iter.types[2].types)
        if iter.parameters[1].parameters[i] <: DataValues.DataValue
            push!(constructor_call.args, :(isna(columns[$i],i) ? $(iter.parameters[1].parameters[i])() : $(iter.parameters[1].parameters[i])(columns[$i][i])))
        else
            push!(constructor_call.args, :(columns[$i][i]))
        end
    end

    quote
        i = state
        columns = iter.columns
        a = $constructor_call
        return a, state+1
    end
end

function Base.done{T,TS}(iter::DataFrameIterator{T,TS}, state)
    return state>size(iter.df,1)
end

# Sink

@generated function _tabletraits_fill_df(columns, enumerable)
    n = length(columns.types)
    push_exprs = Expr(:block)
    for i in 1:n
        if columns.parameters[i] <: DataArray
            ex = :( push!(columns[$i], isnull(i[$i]) ? DataArrays.NA : get(i[$i])) )
        else
            ex = :( push!(columns[$i], i[$i]) )
        end
        push!(push_exprs.args, ex)
    end

    quote
        for i in enumerable
            $push_exprs
        end
    end
end

function _construct_dataframe_from_iterabletable(x)
    iter = TableTraits.getiterator(x)

    T = eltype(iter)
    if !(T<:NamedTuples.NamedTuple)
        error("Can only collect a NamedTuple iterator into a DataFrame")
    end

    column_types = TableTraits.column_types(iter)
    column_names = TableTraits.column_names(iter)

    columns = []
    for t in column_types
        if isa(t, TypeVar)
            push!(columns, Array{Any}(0))
        elseif t <: DataValues.DataValue
            push!(columns, DataArray(t.parameters[1],0))
        else
            push!(columns, Array{t}(0))
        end
    end
    df = DataFrame(columns, fieldnames(T))
    _tabletraits_fill_df((df.columns...), iter)
    return df
end

function DataFrame(x::Array{T,1}) where {T<:NamedTuples.NamedTuple}
    return _construct_dataframe_from_iterabletable(x)
end

function DataFrame(x)
    if TableTraits.isiterabletable(x)
        return _construct_dataframe_from_iterabletable(x)        
    else
        return convert(DataFrame, x)
    end
end

function ModelFrame(f::Formula, source; kwargs...)
    TableTraits.isiterabletable(source) || error()
    return ModelFrame(f, DataFrame(source); kwargs...)
end

function StatsBase.fit{T<:StatsBase.StatisticalModel}(::Type{T}, f::Formula, source, args...; contrasts::Dict = Dict(), kwargs...)
    TableTraits.isiterabletable(source) || error()
    mf = ModelFrame(f, source, contrasts=contrasts)
    mm = ModelMatrix(mf)
    y = model_response(mf)
    DataFrameStatisticalModel(fit(T, mm.m, y, args...; kwargs...), mf, mm)
end

function StatsBase.fit{T<:StatsBase.RegressionModel}(::Type{T}, f::Formula, source, args...; contrasts::Dict = Dict(), kwargs...)
    TableTraits.isiterabletable(source) || error()
    mf = ModelFrame(f, source, contrasts=contrasts)
    mm = ModelMatrix(mf)
    y = model_response(mf)
    DataFrameRegressionModel(fit(T, mm.m, y, args...; kwargs...), mf, mm)
end