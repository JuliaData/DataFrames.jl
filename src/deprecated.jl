import Base: @deprecate

@deprecate DataFrame!(args...; kwargs...) DataFrame(args...; copycols=false, kwargs...)

# TODO: remove these definitions in year 2021
by(args...; kwargs...) = throw(ArgumentError("by function was removed from DataFrames.jl. " *
                                             "Use the `combine(groupby(...), ...)` or `combine(f, groupby(...))` instead."))

aggregate(args...; kwargs...) = throw(ArgumentError("aggregate function was removed from DataFrames.jl. " *
                                                    "Use the `combine` function instead."))

export categorical, categorical!
function CategoricalArrays.categorical(df::AbstractDataFrame,
                                       cols::Union{ColumnIndex, MultiColumnIndex};
                                       compress::Union{Bool, Nothing}=nothing)
    if compress === nothing
        compress = false
        categoricalstr = "categorical"
    else
        categoricalstr = "(x -> categorical(x, compress=$compress))"
    end
    if cols isa AbstractVector{<:Union{AbstractString, Symbol}}
        Base.depwarn("`categorical(df, cols)` is deprecated. " *
                     "Use `transform(df, cols .=> $categoricalstr, renamecols=false)` instead.",
                     :categorical)
        return transform(df, cols .=> (x -> categorical(x, compress=compress)), renamecols=false)
    elseif cols isa Union{AbstractString, Symbol}
        Base.depwarn("`categorical(df, cols)` is deprecated. " *
                     "Use `transform(df, cols => $categoricalstr, renamecols=false)` instead.",
                     :categorical)
        return transform(df, cols => (x -> categorical(x, compress=compress)), renamecols=false)
    else
        Base.depwarn("`categorical(df, cols)` is deprecated. " *
                     "Use `transform(df, names(df, cols) .=> $categoricalstr, renamecols=false)` instead.",
                     :categorical)
        return transform(df, names(df, cols) .=> (x -> categorical(x, compress=compress)), renamecols=false)
    end
end

function CategoricalArrays.categorical(df::AbstractDataFrame,
                                       cols::Union{Type, Nothing}=nothing;
                                       compress::Bool=false)
    if compress === nothing
        compress = false
        categoricalstr = "categorical"
    else
        categoricalstr = "categorical(x, compress=$compress)"
    end
    if cols === nothing
        cols = Union{AbstractString, Missing}
        Base.depwarn("`categorical(df)` is deprecated. " *
                     "Use `transform(df, names(df, $cols) .=> $categoricalstr, renamecols=false)` instead.",
                     :categorical)
    else
        Base.depwarn("`categorical(df, T)` is deprecated. " *
                     "Use transform(df, names(df, T) .=> $categoricalstr, renamecols=false)` instead.",
                     :categorical)
    end
    return transform(df, names(df, cols) .=> (x -> categorical(x, compress=compress)), renamecols=false)
end

function categorical!(df::DataFrame, cols::Union{ColumnIndex, MultiColumnIndex};
                      compress::Union{Bool, Nothing}=nothing)
    if compress === nothing
        compress = false
        categoricalstr = "categorical"
    else
        categoricalstr = "(x -> categorical(x, compress=$compress))"
    end
    if cols isa AbstractVector{<:Union{AbstractString, Symbol}}
        Base.depwarn("`categorical!(df, cols)` is deprecated. " *
                     "Use `transform!(df, cols .=> $categoricalstr, renamecols=false)` instead.",
                     :categorical!)
        return transform!(df, cols .=> (x -> categorical(x, compress=compress)), renamecols=false)
    elseif cols isa Union{AbstractString, Symbol}
        Base.depwarn("`categorical!(df, cols)` is deprecated. " *
                     "Use `transform!(df, cols => $categoricalstr, renamecols=false)` instead.",
                     :categorical!)
        return transform!(df, cols => (x -> categorical(x, compress=compress)), renamecols=false)
    else
        Base.depwarn("`categorical!(df, cols)` is deprecated. " *
                     "Use `transform!(df, names(df, cols) .=> $categoricalstr, renamecols=false)` instead.",
                     :categorical!)
        return transform!(df, names(df, cols) .=> (x -> categorical(x, compress=compress)), renamecols=false)
    end
end

function categorical!(df::DataFrame, cols::Union{Type, Nothing}=nothing;
                      compress::Bool=false)
    if compress === nothing
        compress = false
        categoricalstr = "categorical"
    else
        categoricalstr = "(x -> categorical(x, compress=$compress))"
    end
    if cols === nothing
        cols = Union{AbstractString, Missing}
        Base.depwarn("`categorical!(df)` is deprecated. " *
                     "Use `transform!(df, names(df, $cols) .=> $categoricalstr, renamecols=false)` instead.",
                     :categorical!)
    else
        Base.depwarn("`categorical!(df, T)` is deprecated. " *
                     "Use `transform!(df, names(df, T) .=> $categoricalstr, renamecols=false)` instead.",
                     :categorical!)
    end
    return transform!(df, names(df, cols) .=> (x -> categorical(x, compress=compress)), renamecols=false)
end

@deprecate DataFrame(pairs::NTuple{N, Pair}; makeunique::Bool=false,
          copycols::Bool=true) where {N} DataFrame(pairs..., makeunique=makeunique, copycols=copycols)
@deprecate DataFrame(columns::NTuple{N, AbstractVector}, cnames::NTuple{N, Symbol}; makeunique::Bool=false,
          copycols::Bool=true) where {N} DataFrame(collect(columns), collect(cnames);
              makeunique=makeunique, copycols=copycols)
@deprecate DataFrame(columns::NTuple{N, AbstractVector}, cnames::NTuple{N, AbstractString}; makeunique::Bool=false,
                     copycols::Bool=true) where {N} DataFrame(collect(AbstractVector, columns), [Symbol(c) for c in cnames];
                                                              makeunique=makeunique, copycols=copycols)
@deprecate DataFrame(columns::NTuple{N, AbstractVector};
                     copycols::Bool=true) where {N} DataFrame(collect(AbstractVector, columns),
                                                              gennames(length(columns)), copycols=copycols)
@deprecate DataFrame(columns::AbstractMatrix, cnames::AbstractVector{Symbol} = gennames(size(columns, 2));
                     makeunique::Bool=false) DataFrame(AbstractVector[columns[:, i] for i in 1:size(columns, 2)],
                                                       cnames; makeunique=makeunique, copycols=false)

@deprecate DataFrame(columns::AbstractMatrix, cnames::AbstractVector{<:AbstractString};
                     makeunique::Bool=false) DataFrame(AbstractVector[columns[:, i] for i in 1:size(columns, 2)],
                                                       Symbol.(cnames); makeunique=makeunique, copycols=false)

function DataFrame(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol},
                   nrows::Integer=0; makeunique::Bool=false)::DataFrame where T<:Type
    Base.depwarn("`DataFrame` constructor with passed eltypes is deprecated. " *
                 "Pass explicitly created columns to a `DataFrame` constructor instead.",
                     :DataFrame)
    columns = AbstractVector[elty >: Missing ?
                             fill!(Tables.allocatecolumn(elty, nrows), missing) :
                             Tables.allocatecolumn(elty, nrows)
                             for elty in column_eltypes]
    return DataFrame(columns, Index(convert(Vector{Symbol}, cnames),
                     makeunique=makeunique), copycols=false)
end

DataFrame(column_eltypes::AbstractVector{<:Type},
          cnames::AbstractVector{<:AbstractString},
          nrows::Integer=0; makeunique::Bool=false) =
    DataFrame(column_eltypes, Symbol.(cnames), nrows; makeunique=makeunique)
