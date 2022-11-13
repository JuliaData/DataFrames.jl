"""
    reverse(df::AbstractDataFrame, start=1, stop=nrow(df))

Return a data frame containing the rows in `df` in reversed order.
If `start` and `stop` are provided, only rows in the `start:stop` range are affected.

$METADATA_FIXED

# Examples

```jldoctest
julia> df = DataFrame(a=1:5, b=6:10, c=11:15)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15

julia> reverse(df)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     4      9     14
   3 │     3      8     13
   4 │     2      7     12
   5 │     1      6     11

julia> reverse(df, 2, 3)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     3      8     13
   3 │     2      7     12
   4 │     4      9     14
   5 │     5     10     15
```
"""
Base.reverse(df::AbstractDataFrame, start::Integer=1, stop::Integer=nrow(df)) =
    mapcols(x -> reverse(x, start, stop), df)

"""
    reverse!(df::AbstractDataFrame, start=1, stop=nrow(df))

Mutate data frame in-place to reverse its row order.
If `start` and `stop` are provided, only rows in the `start:stop` range are affected.

`reverse!` will produce a correct result even if some columns of passed data frame
are identical (checked with `===`). Otherwise, if two columns share some part of
memory but are not identical (e.g. are different views of the same parent
vector) then `reverse!` result might be incorrect.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

# Examples

```jldoctest
julia> df = DataFrame(a=1:5, b=6:10, c=11:15)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15

julia> reverse!(df)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     4      9     14
   3 │     3      8     13
   4 │     2      7     12
   5 │     1      6     11

julia> reverse!(df, 2, 3)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     3      8     13
   3 │     4      9     14
   4 │     2      7     12
   5 │     1      6     11
```
"""
function Base.reverse!(df::AbstractDataFrame, start::Integer=1, stop::Integer=nrow(df))
    toskip = Set{Int}()
    seen_cols = IdDict{Any, Nothing}()
    for (i, col) in enumerate(eachcol(df))
        if haskey(seen_cols, col)
            push!(toskip, i)
        else
            seen_cols[col] = nothing
        end
    end

    for (i, col) in enumerate(eachcol(df))
        if !(i in toskip)
            reverse!(col, start, stop)
        end
    end
    _drop_all_nonnote_metadata!(parent(df))
    return df
end

function _permutation_helper!(fun::Union{typeof(Base.permute!!), typeof(Base.invpermute!!)},
                              df::AbstractDataFrame, p::AbstractVector{<:Integer})
    nrow(df) != length(p) &&
        throw(DimensionMismatch("Permutation does not have a correct length " *
                                "(expected $(nrow(df)) but got $(length(p)))"))

    cp = _compile_permutation!(Base.copymutable(p))

    if isempty(cp)
        _drop_all_nonnote_metadata!(parent(df))
        return df
    end

    if fun === Base.invpermute!!
        reverse!(@view cp[1:end-1])
    end

    seen_cols = IdDict{Any, Nothing}()
    for col in eachcol(df)
        if !haskey(seen_cols, col)
            seen_cols[col] = nothing
            _cycle_permute!(col, cp)
        end
    end

    _drop_all_nonnote_metadata!(parent(df))
    return df
end

# convert a classical permutation to zero terminated cycle
# notation, zeroing the original permutation in the process.
function _compile_permutation!(p::AbstractVector{<:Integer})
    firstindex(p) == 1 ||
        throw(ArgumentError("Permutation vectors must have 1-based indexing"))
    # this length is sufficient because we do not record 1-cycles,
    # so the worst case is all 2-cycles. One extra element gives the
    # algorithm leeway to defer error detection without unsafe reads.
    # trace _compile_permutation!([3,3,1]) for example.
    out = similar(p, 3 * length(p) ÷ 2 + 1)
    out_len = 0
    start = 0
    count = length(p)
    @inbounds while count > 0
        start = findnext(!iszero, p, start + 1)
        start isa Int || throw(ArgumentError("Passed vector p is not a valid permutation"))
        last_k = p[start]
        count -= 1
        last_k == start && continue
        out_len += 1
        out[out_len] = last_k
        p[start] = 0
        start < last_k <= length(p) || throw(ArgumentError("Passed vector p is not a valid permutation"))
        out_len += 1
        k = out[out_len] = p[last_k]
        while true
            count -= 1
            p[last_k] = 0
            last_k = k
            start <= k <= length(p) || throw(ArgumentError("Passed vector p is not a valid permutation"))
            out_len += 1
            k = out[out_len] = p[k]
            k == 0 && break
        end
        last_k == start || throw(ArgumentError("Passed vector p is not a valid permutation"))
    end
    return resize!(out, out_len)
end

# Permute a vector `v` based on a permutation `p` listed in zero terminated
# cycle notation. For example, the permutation 1 -> 2, 2 -> 3, 3 -> 1, 4 -> 6,
# 5 -> 5, 6 -> 4 is traditionally expressed as [2, 3, 1, 6, 5, 4] but in cycle
# notation is expressed as [1, 2, 3, 0, 4, 6, 0]
function _cycle_permute!(v::AbstractVector, p::AbstractVector{<:Integer})
    i = firstindex(p)
    @inbounds while i < lastindex(p)
        last_p_i = p[i]
        start = v[last_p_i]
        while true
            i += 1
            p_i = p[i]
            p_i == 0 && break
            v[last_p_i] = v[p_i]
            last_p_i = p_i
        end
        v[last_p_i] = start
        i += 1
    end
    return v
end

"""
    permute!(df::AbstractDataFrame, p)

Permute data frame `df` in-place, according to permutation `p`.
Throws `ArgumentError` if `p` is not a permutation.

To return a new data frame instead of permuting `df` in-place, use `df[p, :]`.

`permute!` will produce a correct result even if some columns of passed data frame
or permutation `p` are identical (checked with `===`). Otherwise, if two columns share
some part of memory but are not identical (e.g. are different views of the same parent
vector) then `permute!` result might be incorrect.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

# Examples

```jldoctest
julia> df = DataFrame(a=1:5, b=6:10, c=11:15)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15

julia> permute!(df, [5, 3, 1, 2, 4])
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     3      8     13
   3 │     1      6     11
   4 │     2      7     12
   5 │     4      9     14
```
"""
Base.permute!(df::AbstractDataFrame, p::AbstractVector{<:Integer}) =
    _permutation_helper!(Base.permute!!, df, p)

"""
    invpermute!(df::AbstractDataFrame, p)

Like [`permute!`](@ref), but the inverse of the given permutation is applied.

`invpermute!` will produce a correct result even if some columns of passed data
frame or permutation `p` are identical (checked with `===`). Otherwise, if two
columns share some part of memory but are not identical (e.g. are different views
of the same parent vector) then `invpermute!` result might be incorrect.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

# Examples

```jldoctest
julia> df = DataFrame(a=1:5, b=6:10, c=11:15)
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15

julia> permute!(df, [5, 3, 1, 2, 4])
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     5     10     15
   2 │     3      8     13
   3 │     1      6     11
   4 │     2      7     12
   5 │     4      9     14

julia> invpermute!(df, [5, 3, 1, 2, 4])
5×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      6     11
   2 │     2      7     12
   3 │     3      8     13
   4 │     4      9     14
   5 │     5     10     15
```
"""
Base.invpermute!(df::AbstractDataFrame, p::AbstractVector{<:Integer}) =
    _permutation_helper!(Base.invpermute!!, df, p)

"""
    shuffle([rng=GLOBAL_RNG,] df::AbstractDataFrame)

Return a copy of `df` with randomly permuted rows.
The optional `rng` argument specifies a random number generator.

$METADATA_FIXED

# Examples

```jldoctest
julia> using Random

julia> rng = MersenneTwister(1234);

julia> shuffle(rng, DataFrame(a=1:5, b=1:5))
5×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     2      2
   2 │     1      1
   3 │     4      4
   4 │     3      3
   5 │     5      5
```
"""
Random.shuffle(df::AbstractDataFrame) =
    df[randperm(nrow(df)), :]
Random.shuffle(r::AbstractRNG, df::AbstractDataFrame) =
    df[randperm(r, nrow(df)), :]

"""
    shuffle!([rng=GLOBAL_RNG,] df::AbstractDataFrame)

Randomly permute rows of `df` in-place.
The optional `rng` argument specifies a random number generator.

`shuffle!` will produce a correct result even if some columns of passed data frame
are identical (checked with `===`). Otherwise, if two columns share some part of
memory but are not identical (e.g. are different views of the same parent
vector) then `shuffle!` result might be incorrect.

$METADATA_FIXED
Metadata having other styles is dropped (from parent data frame when `df` is a `SubDataFrame`).

# Examples

```jldoctest
julia> using Random

julia> rng = MersenneTwister(1234);

julia> shuffle!(rng, DataFrame(a=1:5, b=1:5))
5×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     2      2
   2 │     1      1
   3 │     4      4
   4 │     3      3
   5 │     5      5
```
"""
Random.shuffle!(df::AbstractDataFrame) =
    permute!(df, randperm(nrow(df)))
Random.shuffle!(r::AbstractRNG, df::AbstractDataFrame) =
    permute!(df, randperm(r, nrow(df)))

