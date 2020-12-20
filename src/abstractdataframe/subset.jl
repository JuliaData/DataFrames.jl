# subset allows a transformation specification without a target column name or a column

_process_subset_pair(i::Int, a::ColumnIndex) = a => Symbol(:x, i)
_process_subset_pair(i::Int, @nospecialize(a::Pair{<:Any, <:Base.Callable})) =
    first(a) => last(a) => Symbol(:x, i)
_process_subset_pair(i::Int, a) =
    throw(ArgumentError("condition specifier $a is not supported by `subset`"))

function _get_subset_conditions(df::Union{AbstractDataFrame, GroupedDataFrame},
                                @nospecialize(args), skipmissing::Bool)
    conditions = Any[_process_subset_pair(i, a) for (i, a) in enumerate(args)]

    isempty(conditions) && throw(ArgumentError("at least one condition must be passed"))

    if df isa AbstractDataFrame
        df_conditions = select(df, conditions..., copycols=!(df isa DataFrame))
    else
        df_conditions = select(df, conditions...,
                               copycols=!(parent(df) isa DataFrame), keepkeys=false)
    end
    test_type = skipmissing ? Union{Missing, Bool} : Bool
    for col in eachcol(df_conditions)
        if !(eltype(col) <: test_type)
            throw(ArgumentError("each transformation must produce a vector whose " *
                                "eltype is subtype of $test_type"))
        end
    end
    @assert ncol(df_conditions) == length(conditions)
    if ncol(df_conditions) == 1
        return .===(df_conditions[!, 1], true)
    else
        return .===(.&(eachcol(df_conditions)...), true)
    end
end

"""
    subset(df::AbstractDataFrame, args...; skipmissing::Bool=false, view::Bool=false)
    subset(gdf::GroupedDataFrame, args...; skipmissing::Bool=false, view::Bool=false)

Return a copy of data frame `df` or parent of `gdf` containing only rows for
which all values produced by transformation(s) `args` for a given row are `true`.

If `skipmissing=true`, returning `missing` in `args` is allowed and corresponding rows are
dropped. If `skipmissing=false` (the default) an error is thrown if `missing` is
present.

Each argument passed in `args` can be either a single column selector or a
`source_columns => function` transformation specifier following the rules
described for [`select`](@ref).

Note that as opposed to [`filter`](@ref) the `subset` function works on whole
columns (or all rows in groups for `GroupedDataFrame`) and by default drops rows
for which condition is false.

If `view=true` a `SubDataFrame` view  is returned instead of a `DataFrame`.

If a `GroupedDataFrame` is passed then it must include all groups present in the
`parent` data frame, like in [`select!`](@ref).

See also: [`subset!`](@ref), [`filter`](@ref), [`filter!`](@ref),  [`select`](@ref)

# Examples

```
julia> df = DataFrame(id=1:4, x=[true, false, true, false], y=[true, true, false, false],
                      z=[true, true, missing, missing], v=[1, 2, 11, 12])
4×5 DataFrame
│ Row │ id    │ x    │ y    │ z       │ v     │
│     │ Int64 │ Bool │ Bool │ Bool?   │ Int64 │
├─────┼───────┼──────┼──────┼─────────┼───────┤
│ 1   │ 1     │ 1    │ 1    │ 1       │ 1     │
│ 2   │ 2     │ 0    │ 1    │ 1       │ 2     │
│ 3   │ 3     │ 1    │ 0    │ missing │ 11    │
│ 4   │ 4     │ 0    │ 0    │ missing │ 12    │

julia> subset(df, :x)
2×5 DataFrame
│ Row │ id    │ x    │ y    │ z       │ v     │
│     │ Int64 │ Bool │ Bool │ Bool?   │ Int64 │
├─────┼───────┼──────┼──────┼─────────┼───────┤
│ 1   │ 1     │ 1    │ 1    │ 1       │ 1     │
│ 2   │ 3     │ 1    │ 0    │ missing │ 11    │

julia> subset(df, :v => x -> x .> 3)
2×5 DataFrame
│ Row │ id    │ x    │ y    │ z       │ v     │
│     │ Int64 │ Bool │ Bool │ Bool?   │ Int64 │
├─────┼───────┼──────┼──────┼─────────┼───────┤
│ 1   │ 3     │ 1    │ 0    │ missing │ 11    │
│ 2   │ 4     │ 0    │ 0    │ missing │ 12    │

julia> subset(df, :x, :y => ByRow(!))
1×5 DataFrame
│ Row │ id    │ x    │ y    │ z       │ v     │
│     │ Int64 │ Bool │ Bool │ Bool?   │ Int64 │
├─────┼───────┼──────┼──────┼─────────┼───────┤
│ 1   │ 3     │ 1    │ 0    │ missing │ 11    │

julia> subset(df, :x, :z, skipmissing=true)
1×5 DataFrame
│ Row │ id    │ x    │ y    │ z     │ v     │
│     │ Int64 │ Bool │ Bool │ Bool? │ Int64 │
├─────┼───────┼──────┼──────┼───────┼───────┤
│ 1   │ 1     │ 1    │ 1    │ 1     │ 1     │

julia> subset(df, :x, :z)
ERROR: ArgumentError: each transformation must return a vector whose eltype is subtype of Bool

julia> subset(groupby(df, :y), :v => x -> x .> minimum(x))
2×5 DataFrame
│ Row │ id    │ x    │ y    │ z       │ v     │
│     │ Int64 │ Bool │ Bool │ Bool?   │ Int64 │
├─────┼───────┼──────┼──────┼─────────┼───────┤
│ 1   │ 2     │ 0    │ 1    │ 1       │ 2     │
│ 2   │ 4     │ 0    │ 0    │ missing │ 12    │
```
"""
@inline function subset(df::AbstractDataFrame, @nospecialize(args...);
                        skipmissing::Bool=false, view::Bool=false)
    row_selector = _get_subset_conditions(df, args, skipmissing)
    return view ? Base.view(df, row_selector, :) : df[row_selector, :]
end

@inline function subset(gdf::GroupedDataFrame, @nospecialize(args...);
                        skipmissing::Bool=false, view::Bool=false)
    row_selector = _get_subset_conditions(gdf, args, skipmissing)
    df = parent(gdf)
    return view ? Base.view(df, row_selector, :) : df[row_selector, :]
end

"""
    subset!(df::AbstractDataFrame, args...; skipmissing::Bool=false)
    subset!(gdf::GroupedDataFrame{DataFrame}, args..., skipmissing::Bool=false)

Update data frame `df` or the parent of `gdf` in place to contain only rows for
which all values produced by transformation(s) `args` for a given row is `true`.

If `skipmissing=true` returing `missing` in `args` is allowed and these rows are
dropped. If `skipmissing=false` (the default) an error is thrown if `missing` is
present.

Each argument passed in `args` can be either a single column selector or a
`source_columns => function` transformation specifier following the rules
described for [`select`](@ref).

Note that as opposed to [`filter!`](@ref) the `subset!` function works on whole
columns (or all rows in groups for `GroupedDataFrame`) and by default drops rows
for which contition is false.

If `GroupedDataFrame` is subsetted then it must include all groups present in the
`parent` data frame, like in [`select!`](@ref).

See also: [`subset`](@ref), [`filter`](@ref), [`filter!`](@ref)

# Examples

```
julia> df = DataFrame(id=1:4, x=[true, false, true, false], y=[true, true, false, false])
4×3 DataFrame
│ Row │ id    │ x    │ y    │
│     │ Int64 │ Bool │ Bool │
├─────┼───────┼──────┼──────┤
│ 1   │ 1     │ 1    │ 1    │
│ 2   │ 2     │ 0    │ 1    │
│ 3   │ 3     │ 1    │ 0    │
│ 4   │ 4     │ 0    │ 0    │

julia> subset!(copy(df), :x, :y => ByRow(!));

julia> df
4×3 DataFrame
│ Row │ id    │ x    │ y    │
│     │ Int64 │ Bool │ Bool │
├─────┼───────┼──────┼──────┤
│ 1   │ 1     │ 1    │ 1    │
│ 2   │ 2     │ 0    │ 1    │
│ 3   │ 3     │ 1    │ 0    │
│ 4   │ 4     │ 0    │ 0    │

julia> df = DataFrame(id=1:4, y=[true, true, false, false], v=[1, 2, 11, 12]);

julia> subset!(groupby(df, :y), :v => x -> x .> minimum(x));

julia> df
2×3 DataFrame
│ Row │ id    │ y    │ v     │
│     │ Int64 │ Bool │ Int64 │
├─────┼───────┼──────┼───────┤
│ 1   │ 2     │ 1    │ 2     │
│ 2   │ 4     │ 0    │ 12    │

julia> df = DataFrame(id=1:4, x=[true, false, true, false],
                      z=[true, true, missing, missing], v=1:4);

julia> subset!(df, :x, :z)
ERROR: ArgumentError: each transformation must return a vector whose eltype is subtype of Bool

julia> subset!(df, :x, :z, skipmissing=true);

julia> df
1×4 DataFrame
│ Row │ id    │ x    │ z     │ v     │
│     │ Int64 │ Bool │ Bool? │ Int64 │
├─────┼───────┼──────┼───────┼───────┤
│ 1   │ 1     │ 1    │ 1     │ 1     │

julia> df = DataFrame(id=1:4, x=[true, false, true, false], y=[true, true, false, false],
                      z=[true, true, missing, missing], v=[1, 2, 11, 12]);

julia> subset!(groupby(df, :y), :v => x -> x .> minimum(x));

julia> df
2×5 DataFrame
│ Row │ id    │ x    │ y    │ z       │ v     │
│     │ Int64 │ Bool │ Bool │ Bool?   │ Int64 │
├─────┼───────┼──────┼──────┼─────────┼───────┤
│ 1   │ 2     │ 0    │ 1    │ 1       │ 2     │
│ 2   │ 4     │ 0    │ 0    │ missing │ 12    │
```
"""
function subset!(df::AbstractDataFrame, @nospecialize(args...); skipmissing::Bool=false)
    row_selector = _get_subset_conditions(df, args, skipmissing)
    return delete!(df, findall(!, row_selector))
end

function subset!(gdf::GroupedDataFrame, @nospecialize(args...); skipmissing::Bool=false)
    row_selector = _get_subset_conditions(gdf, args, skipmissing)
    df = parent(gdf)
    return delete!(df, findall(!, row_selector))
end
