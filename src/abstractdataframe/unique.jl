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
    cols = ntuple(i -> df[!, i], ncol(df))
    if keep == :first
        # if we can take advantage of references pass groups to avoid generating hashes
        rpa = refpool_and_array.(cols)
        refpools = first.(rpa)
        refarrays = last.(rpa)
        if isnothing(refpools) || isnothing(refarrays)
            ngroups, _, gslots, _ = row_group_slots!(cols, Val(true), nothing,
                                                     false, nothing)
            # unique rows are the first encountered group representatives,
            # nonunique are everything else
            cseen = 0
            @inbounds for g_row in gslots
                if g_row > 0
                    res[g_row] = false
                    # this check slows down the process when all rows are unique
                    # but speeds up when we have duplicates
                    cseen += 1
                    cseen == ngroups && break
                end
            end
        else
            groups = Vector{Int}(undef, nrow(df))
            ngroups = row_group_slots!(cols, refpools, refarrays,
                                       Val(false), groups, false, false)[1]
            seen = fill(false, ngroups)
            cseen = 0
            for i in 1:nrow(df)
                g = groups[i]
                if !seen[g]
                    seen[g] = true
                    res[i] = false
                    cseen += 1
                    cseen == ngroups && break
                end
            end
        end
    else
        groups = Vector{Int}(undef, nrow(df))
        ngroups = row_group_slots!(cols, Val(false), groups, false, nothing)[1]
        if keep == :last
            seen = fill(false, ngroups)
            cseen = 0
            for i in nrow(df):-1:1
                g = groups[i]
                if !seen[g]
                    seen[g] = true
                    res[i] = false
                    cseen += 1
                    cseen == ngroups && break
                end
            end
        else
            @assert keep == :only
            # -1 indicates that we have not seen the group yet
            # positive value indicates the first position we have seen the group
            # 0 indicates that we have seen the group at least twice
            firstseen = fill(-1, ngroups)
            for i in 1:nrow(df)
                g = groups[i]
                j = firstseen[g]
                if j == -1
                    # this is possibly non duplicate row
                    firstseen[g] = i
                    res[i] = false
                elseif j > 0
                    # the row had duplicate
                    res[j] = true
                    firstseen[g] = 0
                end
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

julia> unique!(copy(df))  # modifies df
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

