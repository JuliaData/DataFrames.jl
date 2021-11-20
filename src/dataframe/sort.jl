
"""
    sort!(df::AbstractDataFrame, cols=All();
          alg::Union{Algorithm, Nothing}=nothing, lt=isless, by=identity,
          rev::Bool=false, order::Ordering=Forward)

Sort data frame `df` by column(s) `cols`.
Sorting on multiple columns is done lexicographicallly.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).
If `cols` selects no columns, sort `df` on all columns
(this behaviour is deprecated and will change in future versions).

If `alg` is `nothing` (the default), the most appropriate algorithm is
chosen automatically among `TimSort`, `MergeSort` and `RadixSort` depending
on the type of the sorting columns and on the number of rows in `df`.

If `rev` is `true`, reverse sorting is performed. To enable reverse sorting
only for some columns, pass `order(c, rev=true)` in `cols`, with `c` the
corresponding column index (see example below).

See other methods for a description of other keyword arguments.

# Examples
```jldoctest
julia> df = DataFrame(x = [3, 1, 2, 1], y = ["b", "c", "a", "b"])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> sort!(df, :x)
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     1  c
   2 │     1  b
   3 │     2  a
   4 │     3  b

julia> sort!(df, [:x, :y])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     1  b
   2 │     1  c
   3 │     2  a
   4 │     3  b

julia> sort!(df, [:x, :y], rev=true)
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a
   3 │     1  c
   4 │     1  b

julia> sort!(df, [:x, order(:y, rev=true)])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     1  c
   2 │     1  b
   3 │     2  a
   4 │     3  b
```
"""
function Base.sort!(df::DataFrame, cols=All(); alg=nothing,
                    lt=isless, by=identity, rev=false, order=Forward)
    if !(isa(by, Function) || eltype(by) <: Function)
        msg = "'by' must be a Function or a vector of Functions. " *
              "Perhaps you wanted 'cols'."
        throw(ArgumentError(msg))
    end
    # exclude AbstractVector as in that case cols can contain order(...) clauses
    if cols isa MultiColumnIndex && !(cols isa AbstractVector)
        cols = index(df)[cols]
    end
    ord = ordering(df, cols, lt, by, rev, order)
    _alg = Sort.defalg(df, ord; alg=alg, cols=cols)
    return sort!(df, _alg, ord)
end

function Base.sort!(df::DataFrame, a::Base.Sort.Algorithm, o::Base.Sort.Ordering)
    p = _sortperm(df, a, o)
    pp = similar(p)
    c = _columns(df)

    for (i, col) in enumerate(c)
        # Check if this column has been sorted already
        if any(j -> c[j]===col, 1:i-1)
            continue
        end

        copyto!(pp, p)
        Base.permute!!(col, pp)
    end
    return df
end
