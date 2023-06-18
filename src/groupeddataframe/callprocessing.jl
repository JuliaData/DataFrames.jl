abstract type FirstColCount end

struct FirstMultiCol <: FirstColCount end
struct FirstSingleCol <: FirstColCount end

firstcoltype(firstmulticol::Bool) =
    firstmulticol ? FirstMultiCol() : FirstSingleCol()

# Wrapping automatically adds column names when the value returned
# by the user-provided function lacks them
wrap(x::AbstractDataFrame) = x
wrap(x::DataFrameRow) = x
wrap(x::Tables.AbstractRow) = x
wrap(x::NamedTuple) = x
function wrap(x::NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}})
    if !isempty(x)
        len1 = length(x[1])
        for i in 2:length(x)
            length(x[i]) == len1 || throw(DimensionMismatch("all vectors returned in a " *
                                                            "NamedTuple must have the same length"))
        end
    end
    return x
end
wrap(x::AbstractMatrix) =
    NamedTuple{Tuple(gennames(size(x, 2)))}(Tuple(view(x, :, i) for i in 1:size(x, 2)))
wrap(x::Any) = (x1=x,)

const ERROR_ROW_COUNT = "return value must not change its kind " *
                        "(single row or variable number of rows) across groups"

const ERROR_COL_COUNT = "function must return only single-column values, " *
                        "or only multiple-column values"

wrap_table(x::Any, ::FirstSingleCol) =
    throw(ArgumentError(ERROR_ROW_COUNT))
wrap_table(x::Any, ::FirstMultiCol) =
    throw(ArgumentError(ERROR_ROW_COUNT))
wrap_table(x::Union{NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}},
                    AbstractDataFrame, AbstractMatrix}, ::FirstSingleCol) =
        throw(ArgumentError(ERROR_COL_COUNT))
wrap_table(x::Union{NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}},
                    AbstractDataFrame, AbstractMatrix}, ::FirstMultiCol) =
    wrap(x)
wrap_table(x::AbstractVector, ::FirstSingleCol) = wrap(x)
wrap_table(x::AbstractVector, ::FirstMultiCol) = throw(ArgumentError(ERROR_COL_COUNT))

wrap_row(x::DataFrameRow, ::FirstSingleCol) = throw(ArgumentError(ERROR_COL_COUNT))
wrap_row(x::DataFrameRow, ::FirstMultiCol) = wrap(x)

wrap_row(x::Tables.AbstractRow, ::FirstSingleCol) = throw(ArgumentError(ERROR_COL_COUNT))
wrap_row(x::Tables.AbstractRow, ::FirstMultiCol) = wrap(x)

wrap_row(x::Any, ::FirstSingleCol) = wrap(x)
# NamedTuple is not possible in this branch
wrap_row(x::Any, ::FirstMultiCol) = throw(ArgumentError(ERROR_COL_COUNT))

wrap_row(x::Union{AbstractArray{<:Any, 0}, Ref}, ::FirstMultiCol) =
    throw(ArgumentError(ERROR_COL_COUNT))
wrap_row(x::Union{AbstractArray{<:Any, 0}, Ref}, ::FirstSingleCol) =
    (x1 = x[],)
# note that also NamedTuple() is correctly captured by this definition
# as it is more specific than the one below
wrap_row(::Union{AbstractVecOrMat, AbstractDataFrame,
                 NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}}, ::FirstSingleCol) =
    throw(ArgumentError(ERROR_ROW_COUNT))
wrap_row(::Union{AbstractVecOrMat, AbstractDataFrame,
                 NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}}, ::FirstMultiCol) =
    throw(ArgumentError(ERROR_ROW_COUNT))

wrap_row(x::NamedTuple, ::FirstSingleCol) = throw(ArgumentError(ERROR_COL_COUNT))

function wrap_row(x::NamedTuple, ::FirstMultiCol)
    if any(v -> v isa AbstractVector, x)
        throw(ArgumentError("mixing single values and vectors in a named tuple is not allowed"))
    end
    return x
end

# idx, starts and ends are passed separately to avoid cost of field access in tight loop
# Manual unrolling of Tuple is used as it turned out more efficient than @generated
# for small number of columns passed.
# For more than 4 columns `map` is slower than @generated
# but this case is probably rare and if huge number of columns is passed @generated
# has very high compilation cost
function do_call(f::Base.Callable, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::Tuple{}, i::Integer)
    if f isa ByRow
        return [f.fun() for _ in 1:(ends[i] - starts[i] + 1)]
    else
        return f()
    end
end

function do_call(f::Base.Callable, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::Tuple{AbstractVector}, i::Integer)
    idx = view(idx, starts[i]:ends[i])
    return f(view(incols[1], idx))
end

function do_call(f::Base.Callable, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::NTuple{2, AbstractVector}, i::Integer)
    idx = view(idx, starts[i]:ends[i])
    return f(view(incols[1], idx), view(incols[2], idx))
end

function do_call(f::Base.Callable, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::NTuple{3, AbstractVector}, i::Integer)
    idx = view(idx, starts[i]:ends[i])
    return f(view(incols[1], idx), view(incols[2], idx), view(incols[3], idx))
end

function do_call(f::Base.Callable, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::NTuple{4, AbstractVector}, i::Integer)
    idx = view(idx, starts[i]:ends[i])
    return f(view(incols[1], idx), view(incols[2], idx), view(incols[3], idx),
             view(incols[4], idx))
end

function do_call(f::Base.Callable, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::Tuple, i::Integer)
    idx = view(idx, starts[i]:ends[i])
    return f(map(c -> view(c, idx), incols)...)
end

function do_call(f::Base.Callable, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::NamedTuple, i::Integer)
    if f isa ByRow && isempty(incols)
        return [f.fun(NamedTuple()) for _ in 1:(ends[i] - starts[i] + 1)]
    else
        idx = view(idx, starts[i]:ends[i])
        return f(map(c -> view(c, idx), incols))
    end
end

function do_call(f::Base.Callable, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::Nothing, i::Integer)
    idx = view(idx, starts[i]:ends[i])
    return f(view(parent(gd), idx, :))
end
