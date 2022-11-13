"""
    filter(fun, df::AbstractDataFrame; view::Bool=false)
    filter(cols => fun, df::AbstractDataFrame; view::Bool=false)

Return a data frame containing only rows from `df` for which `fun` returns
`true`.

If `cols` is not specified then the predicate `fun` is passed `DataFrameRow`s.
Elements of a `DataFrameRow` may be accessed with dot syntax or column indexing inside `fun`.

If `cols` is specified then the predicate `fun` is passed elements of the
corresponding columns as separate positional arguments, unless `cols` is an
`AsTable` selector, in which case a `NamedTuple` of these arguments is passed.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR), and
column duplicates are allowed if a vector of `Symbol`s, strings, or integers is
passed.

If `view=false` a freshly allocated `DataFrame` is returned. If `view=true` then
a `SubDataFrame` view into `df` is returned.

Passing `cols` leads to a more efficient execution of the operation for large
data frames.

!!! note

    This method is defined so that DataFrames.jl implements the Julia API for
    collections, but it is generally recommended to use the [`subset`](@ref)
    function instead as it is consistent with other DataFrames.jl functions
    (as opposed to `filter`).

$METADATA_FIXED

See also: [`filter!`](@ref)

# Examples

```jldoctest
julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> filter(row -> row.x > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter(row -> row["x"] > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter(:x => x -> x > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter([:x, :y] => (x, y) -> x == 1 || y == "b", df)
3×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b

julia> filter(AsTable(:) => nt -> nt.x == 1 || nt.y == "b", df)
3×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b
```
"""
@inline function Base.filter(f, df::AbstractDataFrame; view::Bool=false)
    rowidxs = _filter_helper(f, eachrow(df))
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

@inline function Base.filter((cols, f)::Pair, df::AbstractDataFrame; view::Bool=false)
    int_cols = index(df)[cols] # it will be AbstractVector{Int} or Int
    if length(int_cols) == 0
        rowidxs = [f() for _ in axes(df, 1)]
    else
        rowidxs = _filter_helper(f, (df[!, i] for i in int_cols)...)
    end
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

# this method is needed to allow for passing duplicate columns
@inline function Base.filter((cols, f)::Pair{<:Union{AbstractVector{<:Integer},
                                                     AbstractVector{<:AbstractString},
                                                     AbstractVector{<:Symbol}}},
                             df::AbstractDataFrame; view::Bool=false)
    if length(cols) == 0
        rowidxs = [f() for _ in axes(df, 1)]
    else
        rowidxs = _filter_helper(f, (df[!, i] for i in cols)...)
    end
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

_filter_helper(f, cols...)::AbstractVector{Bool} = ((x...) -> f(x...)::Bool).(cols...)

@inline function Base.filter((cols, f)::Pair{AsTable}, df::AbstractDataFrame;
                             view::Bool=false)
    cols = index(df)[cols.cols]
    df_tmp = select(df, cols, copycols=false)
    if ncol(df_tmp) == 0
        rowidxs = [f(NamedTuple()) for _ in axes(df, 1)]
    else
        rowidxs = _filter_helper_astable(f, Tables.namedtupleiterator(df_tmp))
    end
    return view ? Base.view(df, rowidxs, :) : df[rowidxs, :]
end

_filter_helper_astable(f, nti::Tables.NamedTupleIterator)::AbstractVector{Bool} = (x -> f(x)::Bool).(nti)

"""
    filter!(fun, df::AbstractDataFrame)
    filter!(cols => fun, df::AbstractDataFrame)

Remove rows from data frame `df` for which `fun` returns `false`.

If `cols` is not specified then the predicate `fun` is passed `DataFrameRow`s.
Elements of a `DataFrameRow` may be accessed with dot syntax or column indexing inside `fun`.

If `cols` is specified then the predicate `fun` is passed elements of the
corresponding columns as separate positional arguments, unless `cols` is an
`AsTable` selector, in which case a `NamedTuple` of these arguments is passed.
`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR), and
column duplicates are allowed if a vector of `Symbol`s, strings, or integers is
passed.

Passing `cols` leads to a more efficient execution of the operation for large
data frames.

!!! note

    This method is defined so that DataFrames.jl implements the Julia API for
    collections, but it is generally recommended to use the [`subset!`](@ref)
    function instead as it is consistent with other DataFrames.jl functions
    (as opposed to `filter!`).

$METADATA_FIXED

See also: [`filter`](@ref)

# Examples

```jldoctest
julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"])
4×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     2  a
   4 │     1  b

julia> filter!(row -> row.x > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter!(row -> row["x"] > 1, df)
2×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     2  a

julia> filter!(:x => x -> x == 3, df)
1×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b

julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"]);

julia> filter!([:x, :y] => (x, y) -> x == 1 || y == "b", df)
3×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b

julia> df = DataFrame(x=[3, 1, 2, 1], y=["b", "c", "a", "b"]);

julia> filter!(AsTable(:) => nt -> nt.x == 1 || nt.y == "b", df)
3×2 DataFrame
 Row │ x      y
     │ Int64  String
─────┼───────────────
   1 │     3  b
   2 │     1  c
   3 │     1  b
```
"""
Base.filter!(f::Function, df::AbstractDataFrame) = deleteat!(df, findall(!f, eachrow(df)))
Base.filter!((col, f)::Pair{<:ColumnIndex}, df::AbstractDataFrame) =
    _filter!_helper(df, f, df[!, col])
Base.filter!((cols, f)::Pair{<:AbstractVector{Symbol}}, df::AbstractDataFrame) =
    filter!([index(df)[col] for col in cols] => f, df)
Base.filter!((cols, f)::Pair{<:AbstractVector{<:AbstractString}}, df::AbstractDataFrame) =
    filter!([index(df)[col] for col in cols] => f, df)
Base.filter!((cols, f)::Pair, df::AbstractDataFrame) =
    filter!(index(df)[cols] => f, df)
Base.filter!((cols, f)::Pair{<:AbstractVector{Int}}, df::AbstractDataFrame) =
    _filter!_helper(df, f, (df[!, i] for i in cols)...)

function _filter!_helper(df::AbstractDataFrame, f, cols...)
    if length(cols) == 0
        rowidxs = findall(x -> !f(), axes(df, 1))
    else
        rowidxs = findall(((x...) -> !(f(x...)::Bool)).(cols...))
    end
    return deleteat!(df, rowidxs)
end

function Base.filter!((cols, f)::Pair{<:AsTable}, df::AbstractDataFrame)
    cols = index(df)[cols.cols]
    dff = select(df, cols, copycols=false)
    if ncol(dff) == 0
        return deleteat!(df, findall(x -> !f(NamedTuple()), axes(df, 1)))
    else
        return _filter!_helper_astable(df, Tables.namedtupleiterator(dff), f)
    end
end

_filter!_helper_astable(df::AbstractDataFrame, nti::Tables.NamedTupleIterator, f) =
    deleteat!(df, _findall((x -> !(f(x)::Bool)).(nti)))

