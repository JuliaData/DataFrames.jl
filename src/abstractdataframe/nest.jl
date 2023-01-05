"""
    nest(gdf::GroupedDataFrame, cols::Pair{<:AbstractString}...)
    nest(gdf::GroupedDataFrame, cols::Pair{Symbol}...)
    nest(gdf::GroupedDataFrame)

Return a data frame with one row for each group in `gdf` where
or or more columns contain a data frame of the rows that belong to that group.

Every `cols` argument must be a pair `column_selector => column_name`.
If no `cols` are passed, then by default `valuecols(gdf) => :data`
nesting is performed.

The returned data frame has all grouping columns of `gdf`, followed by one
or more columns where `column_name` is the name of the column storing data frames,
and every data frame consists of columns picked by `column_selector` values
computed for each group of `gdf`.

TODO: metadata

# Examples

julia> df = DataFrame(id = ["b", "a", "a", "c", "b", "b"],
                      x = 1:6, y = 11:16, z='a':'f')
6×4 DataFrame
 Row │ id      x      y      z    
     │ String  Int64  Int64  Char 
─────┼────────────────────────────
   1 │ b           1     11  a
   2 │ a           2     12  b
   3 │ a           3     13  c
   4 │ c           4     14  d
   5 │ b           5     15  e
   6 │ b           6     16  f

julia> n1 = nest(groupby(df, :id))
3×2 DataFrame
 Row │ id      data
     │ String  DataFrame     
─────┼───────────────────────
   1 │ b       3×3 DataFrame 
   2 │ a       2×3 DataFrame 
   3 │ c       1×3 DataFrame 

julia> n1.data
3-element Vector{DataFrame}:
 3×3 DataFrame
 Row │ x      y      z    
     │ Int64  Int64  Char 
─────┼────────────────────
   1 │     1     11  a
   2 │     5     15  e
   3 │     6     16  f
 2×3 DataFrame
 Row │ x      y      z    
     │ Int64  Int64  Char 
─────┼────────────────────
   1 │     2     12  b
   2 │     3     13  c
 1×3 DataFrame
 Row │ x      y      z    
     │ Int64  Int64  Char 
─────┼────────────────────
   1 │     4     14  d

julia> n2 = nest(groupby(df, :id), [:z, :x] => :zx)
3×2 DataFrame
 Row │ id      zx
     │ String  DataFrame     
─────┼───────────────────────
   1 │ b       3×2 DataFrame 
   2 │ a       2×2 DataFrame 
   3 │ c       1×2 DataFrame 

julia> n2.zx
3-element Vector{DataFrame}:
 3×2 DataFrame
 Row │ z     x     
     │ Char  Int64 
─────┼─────────────
   1 │ a         1
   2 │ e         5
   3 │ f         6
 2×2 DataFrame
 Row │ z     x     
     │ Char  Int64 
─────┼─────────────
   1 │ b         2
   2 │ c         3
 1×2 DataFrame
 Row │ z     x     
     │ Char  Int64 
─────┼─────────────
   1 │ d         4

julia> n3 = nest(groupby(df, :id), :x => :x, [:y, :z] => :yz)
3×3 DataFrame
 Row │ id      x              yz
     │ String  DataFrame      DataFrame     
─────┼──────────────────────────────────────
   1 │ b       3×1 DataFrame  3×2 DataFrame 
   2 │ a       2×1 DataFrame  2×2 DataFrame 
   3 │ c       1×1 DataFrame  1×2 DataFrame 

julia> n3.x
3-element Vector{DataFrame}:
 3×1 DataFrame
 Row │ x     
     │ Int64 
─────┼───────
   1 │     1
   2 │     5
   3 │     6
 2×1 DataFrame
 Row │ x     
     │ Int64 
─────┼───────
   1 │     2
   2 │     3
 1×1 DataFrame
 Row │ x     
     │ Int64 
─────┼───────
   1 │     4

julia> n3.yz
3-element Vector{DataFrame}:
 3×2 DataFrame
 Row │ y      z    
     │ Int64  Char 
─────┼─────────────
   1 │    11  a
   2 │    15  e
   3 │    16  f
 2×2 DataFrame
 Row │ y      z    
     │ Int64  Char 
─────┼─────────────
   1 │    12  b
   2 │    13  c
 1×2 DataFrame
 Row │ y      z    
     │ Int64  Char
─────┼─────────────
   1 │    14  d
"""
nest(gdf::GroupedDataFrame, cols::Pair{<:Any, <:AbstractString}...) =
    combine(gdf, (sdf -> (; Symbol(dst) => select(sdf, index(sdf)[src]))
                  for (src, dst) in cols)...)
nest(gdf::GroupedDataFrame, cols::Pair{<:Any, Symbol}...) =
    combine(gdf, (sdf -> (; dst => select(sdf, index(sdf)[src]))
                  for (src, dst) in cols)...)
nest(gdf::GroupedDataFrame) = nest(gdf, valuecols(gdf) => :data)

"""
    unnest(df::AbstractDataFrame, src::ColumnIndex...;
           cols::Union{Symbol, AbstractVector{Symbol},
                       AbstractVector{<:AbstractString}}=:setequal,
           promote::Bool=true,
           makeunique::Bool=false, flatten::Bool=true)

Extract the contents of one or more columns `cols` in `df` that contain data frames,
returning a data frame with as many rows and columns as the nested data frames contain,
in addition to original columns.
The newly created columns are stored at the end of the data frame (and the `src` columns are
dropped).

Each `src` column must contain a `NamedTuple`, a `DataFrameRow`, a
`Tables.AbstractRow`, or Tables.jl table.

`cols` (default `:setequal`) and `promote` (default `true`) keyword arguments
have the same meaning as in [`push!`](@ref).

If `makeunique=false` (the default) produced column names must be unique.
If `makeunique=true` then duplicate column names will be suffixed with `_i`
(`i` starting at `1` for the first duplicate).

If `flatten=true` (the default) then newly created columns are flattened
using [`flatten`](@ref) with `scalar=Missing` keyword argument.

TODO: metadata

"""
function unnest(df::AbstractDataFrame, src::ColumnIndex...;
                cols::Union{Symbol, AbstractVector{Symbol},
                            AbstractVector{<:AbstractString}}=:setequal,
                promote::Bool=(cols in [:union, :subset]),
                makeunique::Bool=false, flatten::Bool=true)
    ref_df = select(df, Not(collect(Any, src)))
    col_count = ncol(ref_df)
    for idx in src
        col = df[!, idx]
        tmp_df = DataFrame()
        for v in col
            if v isa DataFrame # produce DataFrameRow
                # if flatten=false make a copy to avoid aliases
                v = DataFrame([n => [flatten ? c : copy(c)]
                               for (n, c) in pairs(eachcol(v))],
                              copycols=false) |> only
            elseif Tables.istable(v) # produce NamedTuple
                v = Tables.columntable(v)
            end
            push!(tmp_df, v, cols=cols, promote=promote)
        end
        hcat!(ref_df, tmp_df, makeunique=makeunique, copycols=false)
    end
    return if flatten
        DataFrames.flatten(ref_df, col_count+1:ncol(ref_df), scalar=Missing)
    else
        ref_df
    end
end

"""
    flatten(df::AbstractDataFrame, cols; scalar::Type)

When columns `cols` of data frame `df` have iterable elements that define
`length` (for example a `Vector` of `Vector`s), return a `DataFrame` where each
element of each `col` in `cols` is flattened, meaning the column corresponding
to `col` becomes a longer vector where the original entries are concatenated.
Elements of row `i` of `df` in columns other than `cols` will be repeated
according to the length of `df[i, col]`. These lengths must therefore be the
same for each `col` in `cols`, or else an error is raised. Note that these
elements are not copied, and thus if they are mutable changing them in the
returned `DataFrame` will affect `df`.

`cols` can be any column selector ($COLUMNINDEX_STR; $MULTICOLUMNINDEX_STR).

If `scalar` is passed then values that have this type in flattened columns
are treated as scalars and broadcasted as many times as is needed to match
lengths of values stored in other columns. One row is produced if all
corresponding values are scalars.

$METADATA_FIXED

# Examples

```jldoctest
julia> df1 = DataFrame(a=[1, 2], b=[[1, 2], [3, 4]], c=[[5, 6], [7, 8]])
2×3 DataFrame
 Row │ a      b       c
     │ Int64  Array…  Array…
─────┼───────────────────────
   1 │     1  [1, 2]  [5, 6]
   2 │     2  [3, 4]  [7, 8]

julia> flatten(df1, :b)
4×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Array…
─────┼──────────────────────
   1 │     1      1  [5, 6]
   2 │     1      2  [5, 6]
   3 │     2      3  [7, 8]
   4 │     2      4  [7, 8]

julia> flatten(df1, [:b, :c])
4×3 DataFrame
 Row │ a      b      c
     │ Int64  Int64  Int64
─────┼─────────────────────
   1 │     1      1      5
   2 │     1      2      6
   3 │     2      3      7
   4 │     2      4      8

julia> df2 = DataFrame(a=[1, 2], b=[("p", "q"), ("r", "s")])
2×2 DataFrame
 Row │ a      b
     │ Int64  Tuple…
─────┼───────────────────
   1 │     1  ("p", "q")
   2 │     2  ("r", "s")

julia> flatten(df2, :b)
4×2 DataFrame
 Row │ a      b
     │ Int64  String
─────┼───────────────
   1 │     1  p
   2 │     1  q
   3 │     2  r
   4 │     2  s

julia> df3 = DataFrame(a=[1, 2], b=[[1, 2], [3, 4]], c=[[5, 6], [7]])
2×3 DataFrame
 Row │ a      b       c
     │ Int64  Array…  Array…
─────┼───────────────────────
   1 │     1  [1, 2]  [5, 6]
   2 │     2  [3, 4]  [7]

julia> flatten(df3, [:b, :c])
ERROR: ArgumentError: Lengths of iterables stored in columns :b and :c are not the same in row 2

julia> df4 = DataFrame(a=[1, 2, 3],
                       b=[[1, 2], missing, missing],
                       c=[[5, 6], missing, [7, 8]])
3×3 DataFrame
 Row │ a      b        c       
     │ Int64  Array…?  Array…? 
─────┼─────────────────────────
   1 │     1  [1, 2]   [5, 6]
   2 │     2  missing  missing 
   3 │     3  missing  [7, 8]

julia> flatten(df4, [:b, :c], scalar=Missing)
5×3 DataFrame
 Row │ a      b        c       
     │ Int64  Int64?   Int64?  
─────┼─────────────────────────
   1 │     1        1        5
   2 │     1        2        6
   3 │     2  missing  missing 
   4 │     3  missing        7
   5 │     3  missing        8
```
"""
function flatten(df::AbstractDataFrame,
                 cols::Union{ColumnIndex, MultiColumnIndex};
                 scalar::Type=Union{})
    _check_consistency(df)

    idxcols = index(df)[cols]
    if isempty(idxcols)
        cdf = copy(df)
        _drop_all_nonnote_metadata!(cdf)
        return cdf
    end

    col1 = first(idxcols)
    lengths = Int[length_maybe_scalar(x, scalar) for x in df[!, col1]]
    for (i, coli) in enumerate(idxcols)
        i == 1 && continue
        update_lengths!(lengths, df[!, coli], scalar, df, col1, coli)
    end

    # handle case where in all columns we had a scalar
    # in this case we keep it one time
    for i in 1:length(lengths)
        lengths[i] == -1 && (lengths[i] = 1)
    end

    new_df = similar(df[!, Not(cols)], sum(lengths))
    for name in _names(new_df)
        repeat_lengths!(new_df[!, name], df[!, name], lengths)
    end
    length(idxcols) > 1 && sort!(idxcols)
    for col in idxcols
        col_to_flatten = df[!, col]
        fast_path = eltype(col_to_flatten) isa AbstractVector &&
                    !isempty(col_to_flatten)
        flattened_col = if fast_path
                reduce(vcat, col_to_flatten)
            elseif scalar === Union{}
                collect(Iterators.flatten(col_to_flatten))
            else
                collect(Iterators.flatten(v isa scalar ? Iterators.repeated(v, l) : v
                                          for (l, v) in zip(lengths, col_to_flatten)))
            end
        insertcols!(new_df, col, _names(df)[col] => flattened_col)
    end

    _copy_all_note_metadata!(new_df, df)
    return new_df
end

length_maybe_scalar(v, scalar::Type) = v isa scalar ? -1 : length(v)

function update_lengths!(lengths::Vector{Int}, col::AbstractVector, scalar::Type,
                         df::AbstractDataFrame, col1, coli)
    for (i, v) in enumerate(col)
        lv = length_maybe_scalar(v, scalar)
        lv == -1 && continue
        if lengths[i] == -1
            lengths[i] = lv
        elseif lengths[i] != lv
            colnames = _names(df)
            throw(ArgumentError("Lengths of iterables stored in columns :$(colnames[col1]) " *
                                "and :$(colnames[coli]) are not the same in row $i"))
        end
    end
end

function repeat_lengths!(longnew::AbstractVector, shortold::AbstractVector,
                         lengths::AbstractVector{Int})
    counter = 1
    @inbounds for i in eachindex(shortold)
        l = lengths[i]
        longnew[counter:(counter + l - 1)] .= Ref(shortold[i])
        counter += l
    end
end

