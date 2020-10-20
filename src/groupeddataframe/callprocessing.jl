# Wrapping automatically adds column names when the value returned
# by the user-provided function lacks them
wrap(x::Union{AbstractDataFrame, DataFrameRow}) = x
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

wrap_table(x::Any, ::Val) =
    throw(ArgumentError(ERROR_ROW_COUNT))
function wrap_table(x::Union{NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}},
                             AbstractDataFrame, AbstractMatrix},
                             ::Val{firstmulticol}) where firstmulticol
    if !firstmulticol
        throw(ArgumentError(ERROR_COL_COUNT))
    end
    return wrap(x)
end

function wrap_table(x::AbstractVector, ::Val{firstmulticol}) where firstmulticol
    if firstmulticol
        throw(ArgumentError(ERROR_COL_COUNT))
    end
    return wrap(x)
end

function wrap_row(x::Any, ::Val{firstmulticol}) where firstmulticol
    # NamedTuple is not possible in this branch
    if (x isa DataFrameRow) ⊻ firstmulticol
        throw(ArgumentError(ERROR_COL_COUNT))
    end
    return wrap(x)
end

function wrap_row(x::Union{AbstractArray{<:Any, 0}, Ref},
                  ::Val{firstmulticol}) where firstmulticol
    if firstmulticol
        throw(ArgumentError(ERROR_COL_COUNT))
    end
    return (x1 = x[],)
end

# note that also NamedTuple() is correctly captured by this definition
# as it is more specific than the one below
wrap_row(::Union{AbstractVecOrMat, AbstractDataFrame,
                 NamedTuple{<:Any, <:Tuple{Vararg{AbstractVector}}}}, ::Val) =
    throw(ArgumentError(ERROR_ROW_COUNT))

function wrap_row(x::NamedTuple, ::Val{firstmulticol}) where firstmulticol
    if any(v -> v isa AbstractVector, x)
        throw(ArgumentError("mixing single values and vectors in a named tuple is not allowed"))
    end
    if !firstmulticol
        throw(ArgumentError(ERROR_COL_COUNT))
    end
    return x
end

# idx, starts and ends are passed separately to avoid cost of field access in tight loop
# Manual unrolling of Tuple is used as it turned out more efficient than @generated
# for small number of columns passed.
# For more than 4 columns `map` is slower than @generated
# but this case is probably rare and if huge number of columns is passed @generated
# has very high compilation cost
function do_call(f::Any, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::Tuple{}, i::Integer)
    if f isa ByRow
        return [f.fun() for _ in 1:(ends[i] - starts[i] + 1)]
    else
        return f()
    end
end

function do_call(f::Any, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::Tuple{AbstractVector}, i::Integer)
    idx = idx[starts[i]:ends[i]]
    return f(view(incols[1], idx))
end

function do_call(f::Any, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::NTuple{2, AbstractVector}, i::Integer)
    idx = idx[starts[i]:ends[i]]
    return f(view(incols[1], idx), view(incols[2], idx))
end

function do_call(f::Any, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::NTuple{3, AbstractVector}, i::Integer)
    idx = idx[starts[i]:ends[i]]
    return f(view(incols[1], idx), view(incols[2], idx), view(incols[3], idx))
end

function do_call(f::Any, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::NTuple{4, AbstractVector}, i::Integer)
    idx = idx[starts[i]:ends[i]]
    return f(view(incols[1], idx), view(incols[2], idx), view(incols[3], idx),
             view(incols[4], idx))
end

function do_call(f::Any, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::Tuple, i::Integer)
    idx = idx[starts[i]:ends[i]]
    return f(map(c -> view(c, idx), incols)...)
end

function do_call(f::Any, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::NamedTuple, i::Integer)
    if f isa ByRow && isempty(incols)
        return [f.fun(NamedTuple()) for _ in 1:(ends[i] - starts[i] + 1)]
    else
        idx = idx[starts[i]:ends[i]]
        return f(map(c -> view(c, idx), incols))
    end
end

function do_call(f::Any, idx::AbstractVector{<:Integer},
                 starts::AbstractVector{<:Integer}, ends::AbstractVector{<:Integer},
                 gd::GroupedDataFrame, incols::Nothing, i::Integer)
    idx = idx[starts[i]:ends[i]]
    return f(view(parent(gd), idx, :))
end
