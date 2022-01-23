_and() = throw(ArgumentError("at least one condition must be passed"))
_and(x::Bool) = x
_and(x::Bool, y::Bool...) = x && _and(y...)

function _and(x::Any...)
    loc = findfirst(x -> !(x isa Bool), x)
    # we know x has positive length and must contain non-boolean
    @assert !isnothing(loc)
    xv = x[loc]
    if ismissing(xv)
        throw(ArgumentError("missing was returned in condition number $loc " *
                            "but only true or false are allowed; pass " *
                            "skipmissing=true to skip missing values"))
    else
        throw(ArgumentError("value $xv was returned in condition number $loc" *
                            " but only true or false are allowed"))
    end
end

_and_long(x::Bool, y::Bool) = x && y

function _and_long(x, y)
    v = x isa Bool ? y : x
    @assert !(v isa Bool)
    if ismissing(v)
        throw(ArgumentError("missing was returned " *
                            "but only true or false are allowed; pass " *
                            "skipmissing=true to skip missing values"))
    else
        throw(ArgumentError("value $v was returned" *
                            " but only true or false are allowed"))
    end
end

_and_missing() = throw(ArgumentError("at least one condition must be passed"))
_and_missing(x::Bool) = x
_and_missing(x::Bool, y::Union{Bool, Missing}...) = x && _and_missing(y...)
_and_missing(x::Missing, y::Union{Bool, Missing}...) = false

function _and_missing(x::Any...)
    loc = findfirst(x -> !(x isa Union{Bool, Missing}), x)
    # we know x has positive length and must contain non-boolean
    @assert !isnothing(loc)
    xv = x[loc]
    throw(ArgumentError("value $xv was returned in condition number $loc " *
                        "but only true, false, or missing are allowed"))
end

_and_long_missing(x::Bool, y::Bool) = x && y
_and_long_missing(x::Bool, y::Missing) = false
_and_long_missing(x::Missing, y::Union{Bool, Missing}) = false

function _and_long_missing(x, y)
    v = x isa Union{Missing, Bool} ? y : x
    @assert !(v isa Union{Missing, Bool})
    throw(ArgumentError("value $v was returned " *
                        "but only true, false, or missing are allowed"))
end

# we are guaranteed that ByRow returns a vector
# this workaround is needed for 0-argument ByRow
assert_bool_vec(fun::ByRow) = fun

function assert_bool_vec(@nospecialize(fun))
    return function(x...)
        val = fun(x...)
        if !(val isa AbstractVector)
            throw(ArgumentError("functions passed to `subset` must return an AbstractVector."))
        end
        return val
    end
end

function _get_subset_conditions(df::Union{AbstractDataFrame, GroupedDataFrame},
                                (args,)::Ref{Any}, skipmissing::Bool)
    cs_vec = []
    for v in map(x -> broadcast_pair(df isa GroupedDataFrame ? parent(df) : df, x), args)
        if v isa AbstractVecOrMat{<:Pair}
            append!(cs_vec, v)
        elseif v isa MultiColumnIndex
            append!(cs_vec, names(df, v))
        else
            push!(cs_vec, v)
        end
    end

    # subset allows a transformation specification without a target column name or a column
    conditions = Any[if a isa ColumnIndex
                         a => Symbol(:x, i)
                     elseif a isa Pair{<:Any, <:Base.Callable}
                         first(a) => assert_bool_vec(last(a)) => Symbol(:x, i)
                     else
                         throw(ArgumentError("condition specifier $a is not supported by `subset`"))
                     end for (i, a) in enumerate(cs_vec)]
    isempty(conditions) && throw(ArgumentError("at least one condition must be passed"))

    if df isa AbstractDataFrame
        df_conditions = select(df, conditions..., copycols=!(df isa DataFrame))
    else
        df_conditions = select(df, conditions...,
                               copycols=!(parent(df) isa DataFrame), keepkeys=false)
    end

    @assert ncol(df_conditions) == length(conditions)

    cols = eachcol(df_conditions)
    # with many columns, process each column sequentially to avoid large compilation time
    if length(conditions) > 16
        if skipmissing
            cond = _and_long_missing.(cols[1], cols[2])
            for i in 3:length(conditions)
                cond .= _and_long_missing.(cond, cols[i])
            end
        else
            cond = _and_long.(cols[1], cols[2])
            for i in 3:length(conditions)
                cond .= _and_long.(cond, cols[i])
            end
        end
    else
        if skipmissing
            cond = _and_missing.(cols...)
        else
            cond = _and.(cols...)
        end
    end

    @assert eltype(cond) === Bool
    return cond
end

"""
    subset(df::AbstractDataFrame, args...; skipmissing::Bool=false, view::Bool=false)
    subset(gdf::GroupedDataFrame, args...; skipmissing::Bool=false, view::Bool=false,
           ungroup::Bool=true)

Return a copy of data frame `df` or parent of `gdf` containing only rows for
which all values produced by transformation(s) `args` for a given row are
`true`. All transformations must produce vectors containing `true` or `false`
(and optionally `missing` if `skipmissing=true`).

Each argument passed in `args` can be any specifier following the rules
described for [`select`](@ref) with the restriction that:
* specifying target column name is not allowed as `subset` does not create new
  columns;
* every passed transformation must return a scalar or a vector (returning
  `AbstractDataFrame`, `NamedTuple`, `DataFrameRow` or `AbstractMatrix`
  is not supported).

If `skipmissing=false` (the default) `args` are required to produce vectors
containing only `Bool` values. If `skipmissing=true`, additionally `missing` is
allowed and it is treated as `false` (i.e. rows for which one of the conditions
returns `missing` are skipped).

If `view=true` a `SubDataFrame` view  is returned instead of a `DataFrame`.

If `ungroup=false` the resulting data frame is re-grouped based on the same
grouping columns as `gdf` and a `GroupedDataFrame` is returned.

If a `GroupedDataFrame` is passed then it must include all groups present in the
`parent` data frame, like in [`select!`](@ref).

!!! note

    Note that as the `subset` function works in exactly the same way as other
    transformation functions defined in DataFrames.jl this is the preferred way to
    subset rows of a data frame or grouped data frame. In particular it uses a
    different set of rules for specifying transformations than [`filter`](@ref)
    which is implemented in DataFrames.jl to ensure support for the
    standard Julia API for collections.

See also: [`subset!`](@ref), [`filter`](@ref), [`select`](@ref)

# Examples

```jldoctest
julia> df = DataFrame(id=1:4, x=[true, false, true, false],
                      y=[true, true, false, false],
                      z=[true, true, missing, missing], v=[1, 2, 11, 12])
4×5 DataFrame
 Row │ id     x      y      z        v
     │ Int64  Bool   Bool   Bool?    Int64
─────┼─────────────────────────────────────
   1 │     1   true   true     true      1
   2 │     2  false   true     true      2
   3 │     3   true  false  missing     11
   4 │     4  false  false  missing     12

julia> subset(df, :x)
2×5 DataFrame
 Row │ id     x     y      z        v
     │ Int64  Bool  Bool   Bool?    Int64
─────┼────────────────────────────────────
   1 │     1  true   true     true      1
   2 │     3  true  false  missing     11

julia> subset(df, :v => x -> x .> 3)
2×5 DataFrame
 Row │ id     x      y      z        v
     │ Int64  Bool   Bool   Bool?    Int64
─────┼─────────────────────────────────────
   1 │     3   true  false  missing     11
   2 │     4  false  false  missing     12

julia> subset(df, :x, :y => ByRow(!))
1×5 DataFrame
 Row │ id     x     y      z        v
     │ Int64  Bool  Bool   Bool?    Int64
─────┼────────────────────────────────────
   1 │     3  true  false  missing     11

julia> subset(df, :x, :z, skipmissing=true)
1×5 DataFrame
 Row │ id     x     y     z      v
     │ Int64  Bool  Bool  Bool?  Int64
─────┼─────────────────────────────────
   1 │     1  true  true   true      1

julia> subset(df, :x, :z)
ERROR: ArgumentError: missing was returned in condition number 2 but only true or false are allowed; pass skipmissing=true to skip missing values

julia> subset(groupby(df, :y), :v => x -> x .> minimum(x))
2×5 DataFrame
 Row │ id     x      y      z        v
     │ Int64  Bool   Bool   Bool?    Int64
─────┼─────────────────────────────────────
   1 │     2  false   true     true      2
   2 │     4  false  false  missing     12
```
"""
function subset(df::AbstractDataFrame, @nospecialize(args...);
                skipmissing::Bool=false, view::Bool=false)
    row_selector = _get_subset_conditions(df, Ref{Any}(args), skipmissing)
    return view ? Base.view(df, row_selector, :) : df[row_selector, :]
end

function subset(gdf::GroupedDataFrame, @nospecialize(args...);
                skipmissing::Bool=false, view::Bool=false,
                        ungroup::Bool=true)
    row_selector = _get_subset_conditions(gdf, Ref{Any}(args), skipmissing)
    df = parent(gdf)
    res = view ? Base.view(df, row_selector, :) : df[row_selector, :]
    # TODO: in some cases it might be faster to groupby gdf.groups[row_selector]
    return ungroup ? res : groupby(res, groupcols(gdf))
end

"""
    subset!(df::AbstractDataFrame, args...; skipmissing::Bool=false)
    subset!(gdf::GroupedDataFrame{DataFrame}, args..., skipmissing::Bool=false,
            ungroup::Bool=true)

Update data frame `df` or the parent of `gdf` in place to contain only rows for
which all values produced by transformation(s) `args` for a given row is `true`.
All transformations must produce vectors containing `true` or `false` (and
optionally `missing` if `skipmissing=true`).

Each argument passed in `args` can be any specifier following the rules
described for [`select`](@ref) with the restriction that:
* specifying target column name is not allowed as `subset!` does not create new
  columns;
* every passed transformation must return a scalar or a vector (returning
  `AbstractDataFrame`, `NamedTuple`, `DataFrameRow` or `AbstractMatrix`
  is not supported).

If `skipmissing=false` (the default) `args` are required to produce vectors
containing only `Bool` values. If `skipmissing=true`, additionally `missing` is
allowed and it is treated as `false` (i.e. rows for which one of the conditions
returns `missing` are skipped).

If `ungroup=false` the resulting data frame is re-grouped based on the same
grouping columns as `gdf` and a `GroupedDataFrame` is returned.

If `GroupedDataFrame` is subsetted then it must include all groups present in
the `parent` data frame, like in [`select!`](@ref). In this case the passed
`GroupedDataFrame` is updated to have correct groups after its parent is
updated.

!!! note

    Note that as the `subset!` function works in exactly the same way as other
    transformation functions defined in DataFrames.jl this is the preferred way to
    subset rows of a data frame or grouped data frame. In particular it uses a
    different set of rules for specifying transformations than [`filter!`](@ref)
    which is implemented in DataFrames.jl to ensure support for the
    standard Julia API for collections.

See also: [`subset`](@ref), [`filter!`](@ref), [`select!`](@ref)

# Examples

```jldoctest
julia> df = DataFrame(id=1:4, x=[true, false, true, false], y=[true, true, false, false])
4×3 DataFrame
 Row │ id     x      y
     │ Int64  Bool   Bool
─────┼─────────────────────
   1 │     1   true   true
   2 │     2  false   true
   3 │     3   true  false
   4 │     4  false  false

julia> subset!(df, :x, :y => ByRow(!));

julia> df
1×3 DataFrame
 Row │ id     x     y
     │ Int64  Bool  Bool
─────┼────────────────────
   1 │     3  true  false

julia> df = DataFrame(id=1:4, y=[true, true, false, false], v=[1, 2, 11, 12]);

julia> subset!(groupby(df, :y), :v => x -> x .> minimum(x));

julia> df
2×3 DataFrame
 Row │ id     y      v
     │ Int64  Bool   Int64
─────┼─────────────────────
   1 │     2   true      2
   2 │     4  false     12

julia> df = DataFrame(id=1:4, x=[true, false, true, false],
                      z=[true, true, missing, missing], v=1:4)
4×4 DataFrame
 Row │ id     x      z        v
     │ Int64  Bool   Bool?    Int64
─────┼──────────────────────────────
   1 │     1   true     true      1
   2 │     2  false     true      2
   3 │     3   true  missing      3
   4 │     4  false  missing      4

julia> subset!(df, :x, :z)
ERROR: ArgumentError: missing was returned in condition number 2 but only true or false are allowed; pass skipmissing=true to skip missing values

julia> subset!(df, :x, :z, skipmissing=true);

julia> df
1×4 DataFrame
 Row │ id     x     z      v
     │ Int64  Bool  Bool?  Int64
─────┼───────────────────────────
   1 │     1  true   true      1

julia> df = DataFrame(id=1:4, x=[true, false, true, false], y=[true, true, false, false],
                      z=[true, true, missing, missing], v=[1, 2, 11, 12]);

julia> subset!(groupby(df, :y), :v => x -> x .> minimum(x));

julia> df
2×5 DataFrame
 Row │ id     x      y      z        v
     │ Int64  Bool   Bool   Bool?    Int64
─────┼─────────────────────────────────────
   1 │     2  false   true     true      2
   2 │     4  false  false  missing     12
```
"""
function subset!(df::AbstractDataFrame, @nospecialize(args...); skipmissing::Bool=false)
    row_selector = _get_subset_conditions(df, Ref{Any}(args), skipmissing)
    return deleteat!(df, findall(!, row_selector))
end

function subset!(gdf::GroupedDataFrame, @nospecialize(args...); skipmissing::Bool=false,
                 ungroup::Bool=true)
    ngroups = length(gdf)
    groups = gdf.groups
    lazy_lock = gdf.lazy_lock
    row_selector = _get_subset_conditions(gdf, Ref{Any}(args), skipmissing)
    df = parent(gdf)
    res = deleteat!(df, findall(!, row_selector))
    if nrow(res) == length(groups) # we have not removed any rows
        return ungroup ? res : gdf
    end
    newgroups = groups[row_selector]

    # TODO: add threading support
    seen = fill(false, ngroups)
    @inbounds for gix in newgroups
        @assert gix > 0 # having dropped groups in gdf is not allowed here
        seen[gix] = true
    end

    if sum(seen) < ngroups # subset! has dropped some groups
        remap = cumsum(seen)
        @inbounds for i in eachindex(newgroups)
            newgroups[i] = remap[newgroups[i]]
        end
        ngroups = remap[end]
    end

    # update GroupedDataFrame indices in a thread safe way
    Threads.lock(lazy_lock) do
        gdf.groups = newgroups
        gdf.idx = nothing
        gdf.starts = nothing
        gdf.ends = nothing
        gdf.ngroups = ngroups
        gdf.keymap = nothing
    end
    return ungroup ? res : gdf
end
