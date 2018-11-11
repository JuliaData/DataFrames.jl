
"""
    sort!(df::AbstractDataFrame, cols;
          alg::Union{Algorithm, Nothing}=nothing, lt=isless, by=identity,
          rev::Bool=false, order::Ordering=Forward)

Sort data frame `df` by column(s) `cols`.
`cols` can be either a `Symbol` or `Integer` column index, or
a tuple or vector of such indices.

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
│ Row │ x     │ y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 3     │ b      │
│ 2   │ 1     │ c      │
│ 3   │ 2     │ a      │
│ 4   │ 1     │ b      │

julia> sort!(df, :x)
4×2 DataFrame
│ Row │ x     │ y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ c      │
│ 2   │ 1     │ b      │
│ 3   │ 2     │ a      │
│ 4   │ 3     │ b      │

julia> sort!(df, (:x, :y))
4×2 DataFrame
│ Row │ x     │ y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ b      │
│ 2   │ 1     │ c      │
│ 3   │ 2     │ a      │
│ 4   │ 3     │ b      │

julia> sort!(df, (:x, :y), rev=true)
4×2 DataFrame
│ Row │ x     │ y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 3     │ b      │
│ 2   │ 2     │ a      │
│ 3   │ 1     │ c      │
│ 4   │ 1     │ b      │

julia> sort!(df, (:x, order(:y, rev=true)))
4×2 DataFrame
│ Row │ x     │ y      │
│     │ Int64 │ String │
├─────┼───────┼────────┤
│ 1   │ 1     │ c      │
│ 2   │ 1     │ b      │
│ 3   │ 2     │ a      │
│ 4   │ 3     │ b      │
```
"""
function Base.sort!(df::DataFrame, cols_new=[]; cols=[], alg=nothing,
                    lt=isless, by=identity, rev=false, order=Forward)
    if !(isa(by, Function) || eltype(by) <: Function)
        msg = "'by' must be a Function or a vector of Functions. Perhaps you wanted 'cols'."
        throw(ArgumentError(msg))
    end
    if cols != []
        Base.depwarn("sort!(df, cols=cols) is deprecated, use sort!(df, cols) instead",
                     :sort!)
        cols_new = cols
    end
    ord = ordering(df, cols_new, lt, by, rev, order)
    _alg = Sort.defalg(df, ord; alg=alg, cols=cols_new)
    sort!(df, _alg, ord)
end

function Base.sort!(df::DataFrame, a::Base.Sort.Algorithm, o::Base.Sort.Ordering)
    p = sortperm(df, a, o)
    pp = similar(p)
    c = _columns(df)

    for (i,col) in enumerate(c)
        # Check if this column has been sorted already
        if any(j -> c[j]===col, 1:i-1)
            continue
        end

        copyto!(pp,p)
        Base.permute!!(col, pp)
    end
    df
end
