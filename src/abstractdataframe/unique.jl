"""
    nonunique(df::AbstractDataFrame; keep::Symbol=:first)
    nonunique(df::AbstractDataFrame, cols; keep::Symbol=:first)

Return a `Vector{Bool}` in which `true` entries indicate duplicate rows.

Duplicate rows are those for which at least another row contains equal values
(according to `isequal`) for all columns in `cols` (by default, all columns).
If `keep=:first` (the default), only the first occurrence of a set of duplicate
rows is indicated with a `false` entry.
If `keep=:last`, only the last occurrence of a set of duplicate rows is
indicated with a `false` entry.
If `keep=:noduplicates`, only rows without any duplicates are indicated with a
`false` entry.

# Arguments
- `df` : `AbstractDataFrame`
- `cols` : a selector specifying the column(s) or their transformations to
  compare. Can be any column selector or transformation accepted by
  [`select`](@ref) that returns at least one column if `df` has at least one
  column.

See also [`unique`](@ref) and [`unique!`](@ref).

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
    if !(keep in (:first, :last, :noduplicates))
        throw(ArgumentError("`keep` must be :first, :last, or :noduplicates"))
    end
    nrow(df) == 0 && return Bool[]
    res = fill(true, nrow(df))
    cols = ntuple(i -> df[!, i], ncol(df))
    if keep == :first
        rpa = refpool_and_array.(cols)
        refpools = first.(rpa)
        refarrays = last.(rpa)
        # if refarray cannot be used, we can avoid allocating a groups vector
        if any(isnothing, refpools) || any(isnothing, refarrays)
            _, _, gslots, _ = row_group_slots!(cols, Val(true), nothing,
                                               false, nothing, false)
            # unique rows are the first encountered group representatives,
            # nonunique are everything else
            @inbounds for g_row in gslots
                g_row > 0 && (res[g_row] = false)
            end
        else # faster refarray method but allocates a groups vector
            groups = Vector{Int}(undef, nrow(df))
            ngroups = row_group_slots!(cols, refpools, refarrays,
                                       Val(false), groups, false, false, false)[1]
            seen = fill(false, ngroups)
            for i in 1:nrow(df)
                g = groups[i]
                if !seen[g]
                    seen[g] = true
                    res[i] = false
                end
            end
        end
    else
       # always allocate a group vector, use refarray automatically if possible
        groups = Vector{Int}(undef, nrow(df))
        ngroups = row_group_slots!(cols, Val(false), groups, false, nothing, false)[1]
        if keep == :last
            seen = fill(false, ngroups)
            for i in nrow(df):-1:1
                g = groups[i]
                if !seen[g]
                    seen[g] = true
                    res[i] = false
                end
            end
        else
            @assert keep == :noduplicates
            # -1 indicates that we have not seen the group yet
            # positive value indicates the first position we have seen the group
            # 0 indicates that we have seen the group at least twice
            firstseen = fill(-1, ngroups)
            for i in 1:nrow(df)
                g = groups[i]
                j = firstseen[g]
                if j == -1
                    # this is possibly a non duplicate row
                    firstseen[g] = i
                    res[i] = false
                elseif j > 0
                    # the row had a duplicate
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
    end
    return nonunique(udf, keep=keep)
end

"""
    allunique(df::AbstractDataFrame, cols=:)

Return `true` if none of the rows of `df` are duplicated. Two rows are
duplicates if all their columns contain equal values (according to `isequal`)
for all columns in `cols` (by default, all columns).

# Arguments
- `df` : `AbstractDataFrame`
- `cols` : a selector specifying the column(s) or their transformations to
  compare. Can be any column selector or transformation accepted by
  [`select`](@ref).

See also [`unique`](@ref) and [`nonunique`](@ref).

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
                            Val(false), nothing, false, nothing, true)[1] == nrow(df)
end

# avoid invoking Base.allunique(f, iterator) introduced in Julia 1.11

Base.allunique(df::AbstractDataFrame, cols::Tuple) =
    invoke(Base.allunique, Tuple{AbstractDataFrame, Any}, df, cols)

"""
    unique(df::AbstractDataFrame; view::Bool=false, keep::Symbol=:first)
    unique(df::AbstractDataFrame, cols; view::Bool=false, keep::Symbol=:first)

Return a data frame containing only unique rows in `df`.

Non-unique (duplicate) rows are those for which at least another row contains
equal values (according to `isequal`) for all columns in `cols` (by default,
all columns).
If `keep=:first` (the default), only the first occurrence of a set of duplicate
rows is kept.
If `keep=:last`, only the last occurrence of a set of duplicate rows is kept.
If `keep=:noduplicates`, only rows without any duplicates are kept.

If `view=false` a freshly allocated `DataFrame` is returned, and if `view=true`
then a `SubDataFrame` view into `df` is returned.

# Arguments
- `df` : the AbstractDataFrame
- `cols` : a selector specifying the column(s) or their transformations to
  compare. Can be any column selector or transformation accepted by
  [`select`](@ref) that returns at least one column if `df` has at least one
  column.

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

julia> unique(df, keep=:noduplicates)
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

Update `df` in-place to contain only unique rows.

Non-unique (duplicate) rows are those for which at least another row contains
equal values (according to `isequal`) for all columns in `cols` (by default,
all columns).
If `keep=:first` (the default), only the first occurrence of a set of duplicate
rows is kept.
If `keep=:last`, only the last occurrence of a set of duplicate rows is kept.
If `keep=:noduplicates`, only rows without any duplicates are kept.

# Arguments
- `df` : the AbstractDataFrame
- `cols` :  column indicator (`Symbol`, `Int`, `Vector{Symbol}`, `Regex`, etc.)
  specifying the column(s) to compare. Can be any column selector or
  transformation accepted by [`select`](@ref) that returns at least one column
  if `df` has at least one column.

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

julia> unique(df, keep=:noduplicates)
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
