"""
    nonunique(df::AbstractDataFrame; keep::Symbol=:first)
    nonunique(df::AbstractDataFrame, cols; keep::Symbol=:first)

Return a `Vector{Bool}` in which `true` entries indicate duplicate rows.

If `keep=:first` (the default) a row is a duplicate if there exists a prior
row with all columns containing equal values (according to `isequal`).

If `keep=:last` a row is a duplicate if there exists a subsequent row with all
columns containing equal values (according to `isequal`).

If `keep=:only` a row is a duplicate if there exists any other row with all
columns containing equal values (according to `isequal`).

See also [`unique`](@ref) and [`unique!`](@ref).

# Arguments
- `df` : `AbstractDataFrame`
- `cols` : a selector specifying the column(s) or their transformations to
  compare. Can be any column selector or transformation accepted by
  [`select`](@ref) that returns at least one column if `df` has at least one
  column.

# Examples

```jldoctest
julia> df = DataFrame(i=1:4, x=[1, 2, 1, 2])
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> df = vcat(df, df)
8×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
   5 │     1      1
   6 │     2      2
   7 │     3      1
   8 │     4      2

julia> nonunique(df)
8-element Vector{Bool}:
 0
 0
 0
 0
 1
 1
 1
 1

julia> nonunique(df, keep=:last)
8-element Vector{Bool}:
 1
 1
 1
 1
 0
 0
 0
 0

julia> nonunique(df, 2)
8-element Vector{Bool}:
 0
 0
 1
 1
 1
 1
 1
 1
```
"""
function nonunique(df::AbstractDataFrame; keep::Symbol=:first)
    if !(keep in (:first, :last, :only))
        throw(ArgumentError("`keep` must be :first, :last, or :none"))
    end
    ncol(df) == 0 && return Bool[]
    res = fill(true, nrow(df))
    if keep == :first
        gslots = row_group_slots!(ntuple(i -> df[!, i], ncol(df)), Val(false),
                                  nothing, false, nothing)[3]
        # unique rows are the first encountered group representatives,
        # nonunique are everything else
        @inbounds for g_row in gslots
            (g_row > 0) && (res[g_row] = false)
        end
        return res
    else
        # TODO: this can be potentially optimized in the future,
        #       but the use of this code is expected to be rare
        #       so currently a simple implementation is provided
        #       that is already visibly faster than using groupby and combine 
        gdf = groupby(df, All())
        idx = gdf.idx
        @assert length(gdf.starts) == length(gdf.ends)
        if keep == :last
            for (s, e) in zip(gdf.starts, gdf.ends)
                # keep last index in a group
                res[idx[e]] = false
            end
        else
            @assert keep == :only
            for (s, e) in zip(gdf.starts, gdf.ends)
                # set to false if s == e
                res[idx[e]] = s != e
            end
        end
    end
    return res
end

function nonunique(df::AbstractDataFrame, cols; keep::Symbol=:first)
    udf = _try_select_no_copy(df, cols)
    if ncol(df) > 0 && ncol(udf) == 0
         throw(ArgumentError("finding duplicate rows in data frame when " *
                             "`cols` selects no columns is not allowed"))
    else
        return nonunique(udf, keep=keep)
    end
end

"""
    allunique(df::AbstractDataFrame, cols=:)

Return `true` if all rows of `df` are not duplicated. Two rows are duplicate if
all their columns contain equal values (according to `isequal`).

See also [`unique`](@ref) and [`nonunique`](@ref).

# Arguments
- `df` : `AbstractDataFrame`
- `cols` : a selector specifying the column(s) or their transformations to compare.
  Can be any column selector or transformation accepted by [`select`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(i=1:4, x=[1, 2, 1, 2])
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> allunique(df)
true

julia> allunique(df, :x)
false

julia> allunique(df, :i => ByRow(isodd))
false
```
"""
function Base.allunique(df::AbstractDataFrame, cols=:)
    udf = _try_select_no_copy(df, cols)
    nrow(udf) == 0 && return true
    return row_group_slots!(ntuple(i -> udf[!, i], ncol(udf)),
                            Val(false), nothing, false, nothing)[1] == nrow(df)
end

"""
    unique(df::AbstractDataFrame; view::Bool=false, keep::Symbol=:first)
    unique(df::AbstractDataFrame, cols; view::Bool=false, keep::Symbol=:first)

If `keep=:first` (the default) return a data frame containing only the first
occurrence of unique rows in `df`.

If `keep=:last` return a data frame containing only the last occurrence of
unique rows in `df`.

If `keep=:only` return a data frame containing only rows that are unique in `df`
(in case of duplicate rows all are dropped).

When `cols` is specified, the returned `DataFrame` contains complete rows,
retaining in each case the first occurrence of a given combination of values
in selected columns or their transformations. `cols` can be any column
selector or transformation accepted by [`select`](@ref).

If `view=false` a freshly allocated `DataFrame` is returned,
and if `view=true` then a `SubDataFrame` view into `df` is returned.

# Arguments
- `df` : the AbstractDataFrame
- `cols` :  column indicator (`Symbol`, `Int`, `Vector{Symbol}`, `Regex`, etc.)
specifying the column(s) to compare.

$METADATA_FIXED

See also: [`unique!`](@ref), [`nonunique`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(i=1:4, x=[1, 2, 1, 2])
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> df = vcat(df, df)
8×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
   5 │     1      1
   6 │     2      2
   7 │     3      1
   8 │     4      2

julia> unique(df)   # doesn't modify df
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> unique(df, 2)
2×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2

julia> unique(df, keep=:only)
0×2 DataFrame
 Row │ i      x     
     │ Int64  Int64
─────┴──────────────
```
"""
@inline function Base.unique(df::AbstractDataFrame; view::Bool=false,
                             keep::Symbol=:first)
    rowidxs = (!).(nonunique(df, keep=keep))
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

@inline function Base.unique(df::AbstractDataFrame, cols; view::Bool=false,
                             keep::Symbol=:first)
    rowidxs = (!).(nonunique(df, cols, keep=keep))
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

"""
    unique!(df::AbstractDataFrame; keep::Symbol=:first)
    unique!(df::AbstractDataFrame, cols; keep::Symbol=:first)

If `keep=:first` (the default) update `df` in place to contain only the first
occurrence of unique rows in `df`.

If `keep=:last` update `df` in place to contain only the last occurrence of
unique rows in `df`.

If `keep=:only` update `df` in place to contain only rows that are unique in `df`
(in case of duplicate rows all are dropped).

When `cols` is specified, the returned `DataFrame` contains complete rows,
retaining in each case the first occurrence of a given combination of values
in selected columns or their transformations. `cols` can be any column
selector or transformation accepted by [`select`](@ref).

# Arguments
- `df` : the AbstractDataFrame
- `cols` :  column indicator (`Symbol`, `Int`, `Vector{Symbol}`, `Regex`, etc.)
specifying the column(s) to compare.

$METADATA_FIXED

See also: [`unique!`](@ref), [`nonunique`](@ref).

# Examples

```jldoctest
julia> df = DataFrame(i=1:4, x=[1, 2, 1, 2])
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> df = vcat(df, df)
8×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2
   5 │     1      1
   6 │     2      2
   7 │     3      1
   8 │     4      2

julia> unique!(df)  # modifies df
4×2 DataFrame
 Row │ i      x
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      1
   4 │     4      2

julia> unique(df, keep=:only)
0×2 DataFrame
 Row │ i      x     
     │ Int64  Int64
─────┴──────────────
```
"""
Base.unique!(df::AbstractDataFrame; keep::Symbol=:first) =
    deleteat!(df, _findall(nonunique(df, keep=keep)))
Base.unique!(df::AbstractDataFrame, cols::AbstractVector; keep::Symbol=:first) =
    deleteat!(df, _findall(nonunique(df, cols, keep=keep)))
Base.unique!(df::AbstractDataFrame, cols; keep::Symbol=:first) =
    deleteat!(df, _findall(nonunique(df, cols, keep=keep)))

