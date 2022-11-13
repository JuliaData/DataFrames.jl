"""
    hcat(df::AbstractDataFrame...;
         makeunique::Bool=false, copycols::Bool=true)

Horizontally concatenate data frames.

If `makeunique=false` (the default) column names of passed objects must be unique.
If `makeunique=true` then duplicate column names will be suffixed
with `_i` (`i` starting at 1 for the first duplicate).

If `copycols=true` (the default) then the `DataFrame` returned by `hcat` will
contain copied columns from the source data frames.
If `copycols=false` then it will contain columns as they are stored in the
source (without copying). This option should be used with caution as mutating
either the columns in sources or in the returned `DataFrame` might lead to
the corruption of the other object.

Metadata: `hcat` propagates table-level `:note`-style metadata for keys that are present
in all passed data frames and have the same value;
it propagates column-level `:note`-style metadata.

# Example
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> df2 = DataFrame(A=4:6, B=4:6)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     4      4
   2 │     5      5
   3 │     6      6

julia> df3 = hcat(df1, df2, makeunique=true)
3×4 DataFrame
 Row │ A      B      A_1    B_1
     │ Int64  Int64  Int64  Int64
─────┼────────────────────────────
   1 │     1      1      4      4
   2 │     2      2      5      5
   3 │     3      3      6      6

julia> df3.A === df1.A
false

julia> df3 = hcat(df1, df2, makeunique=true, copycols=false);

julia> df3.A === df1.A
true
```
"""
function Base.hcat(df::AbstractDataFrame; makeunique::Bool=false, copycols::Bool=true)
    df = DataFrame(df, copycols=copycols)
    _drop_all_nonnote_metadata!(df)
    return df
end

# TODO: after deprecation remove AbstractVector methods
Base.hcat(df::AbstractDataFrame, x::AbstractVector; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(DataFrame(df, copycols=copycols), x, makeunique=makeunique, copycols=copycols)
Base.hcat(x::AbstractVector, df::AbstractDataFrame; makeunique::Bool=false, copycols::Bool=true) =
    hcat!(x, df, makeunique=makeunique, copycols=copycols)
Base.hcat(df1::AbstractDataFrame, df2::AbstractDataFrame;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(DataFrame(df1, copycols=copycols), df2,
          makeunique=makeunique, copycols=copycols)
Base.hcat(df::AbstractDataFrame, x::Union{AbstractVector, AbstractDataFrame},
          y::Union{AbstractVector, AbstractDataFrame}...;
          makeunique::Bool=false, copycols::Bool=true) =
    hcat!(hcat(df, x, makeunique=makeunique, copycols=copycols), y...,
          makeunique=makeunique, copycols=copycols)

"""
    vcat(dfs::AbstractDataFrame...;
         cols::Union{Symbol, AbstractVector{Symbol},
                     AbstractVector{<:AbstractString}}=:setequal,
         source::Union{Nothing, Symbol, AbstractString,
                       Pair{<:Union{Symbol, AbstractString}, <:AbstractVector}}=nothing)

Vertically concatenate `AbstractDataFrame`s.

The `cols` keyword argument determines the columns of the returned data frame:

* `:setequal`: require all data frames to have the same column names
  disregarding order. If they appear in different orders, the order of the first
  provided data frame is used.
* `:orderequal`: require all data frames to have the same column names and in
  the same order.
* `:intersect`: only the columns present in *all* provided data frames are kept.
  If the intersection is empty, an empty data frame is returned.
* `:union`: columns present in *at least one* of the provided data frames are
  kept. Columns not present in some data frames are filled with `missing` where
  necessary.
* A vector of `Symbol`s or strings: only listed columns are kept. Columns not
  present in some data frames are filled with `missing` where necessary.

The `source` keyword argument, if not `nothing` (the default), specifies the
additional column to be added in the last position in the resulting data frame
that will identify the source data frame. It can be a `Symbol` or an
`AbstractString`, in which case the identifier will be the number of the passed
source data frame, or a `Pair` consisting of a `Symbol` or an `AbstractString`
and of a vector specifying the data frame identifiers (which do not have to be
unique). The name of the source column is not allowed to be present in any
source data frame.

The order of columns is determined by the order they appear in the included data
frames, searching through the header of the first data frame, then the second,
etc.

The element types of columns are determined using `promote_type`, as with `vcat`
for `AbstractVector`s.

`vcat` ignores empty data frames when composing the result (except for
metadata), making it possible to initialize an empty data frame at the beginning
of a loop and `vcat` onto it.

Metadata: `vcat` propagates table-level `:note`-style metadata for keys that are
present in all passed data frames and have the same value. `vcat` propagates
column-level `:note`-style metadata for keys that are present in all passed data
frames that contain this column and have the same value.

# Example
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> df2 = DataFrame(A=4:6, B=4:6)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     4      4
   2 │     5      5
   3 │     6      6

julia> df3 = DataFrame(A=7:9, C=7:9)
3×2 DataFrame
 Row │ A      C
     │ Int64  Int64
─────┼──────────────
   1 │     7      7
   2 │     8      8
   3 │     9      9

julia> df4 = DataFrame()
0×0 DataFrame

julia> vcat(df1, df2)
6×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     4      4
   5 │     5      5
   6 │     6      6

julia> vcat(df1, df3, cols=:union)
6×3 DataFrame
 Row │ A      B        C
     │ Int64  Int64?   Int64?
─────┼─────────────────────────
   1 │     1        1  missing
   2 │     2        2  missing
   3 │     3        3  missing
   4 │     7  missing        7
   5 │     8  missing        8
   6 │     9  missing        9

julia> vcat(df1, df3, cols=:intersect)
6×1 DataFrame
 Row │ A
     │ Int64
─────┼───────
   1 │     1
   2 │     2
   3 │     3
   4 │     7
   5 │     8
   6 │     9

julia> vcat(df4, df1)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> vcat(df1, df2, df3, df4, cols=:union, source="source")
9×4 DataFrame
 Row │ A      B        C        source
     │ Int64  Int64?   Int64?   Int64
─────┼─────────────────────────────────
   1 │     1        1  missing       1
   2 │     2        2  missing       1
   3 │     3        3  missing       1
   4 │     4        4  missing       2
   5 │     5        5  missing       2
   6 │     6        6  missing       2
   7 │     7  missing        7       3
   8 │     8  missing        8       3
   9 │     9  missing        9       3

julia> vcat(df1, df2, df4, df3, cols=:union, source=:source => 'a':'d')
9×4 DataFrame
 Row │ A      B        C        source
     │ Int64  Int64?   Int64?   Char
─────┼─────────────────────────────────
   1 │     1        1  missing  a
   2 │     2        2  missing  a
   3 │     3        3  missing  a
   4 │     4        4  missing  b
   5 │     5        5  missing  b
   6 │     6        6  missing  b
   7 │     7  missing        7  d
   8 │     8  missing        8  d
   9 │     9  missing        9  d
```
"""
Base.vcat(dfs::AbstractDataFrame...;
          cols::Union{Symbol, AbstractVector{Symbol},
                      AbstractVector{<:AbstractString}}=:setequal,
          source::Union{Nothing, SymbolOrString,
                           Pair{<:SymbolOrString, <:AbstractVector}}=nothing) =
    reduce(vcat, dfs; cols=cols, source=source)

"""
    reduce(::typeof(vcat),
           dfs::Union{AbstractVector{<:AbstractDataFrame},
                      Tuple{AbstractDataFrame, Vararg{AbstractDataFrame}}};
           cols::Union{Symbol, AbstractVector{Symbol},
                       AbstractVector{<:AbstractString}}=:setequal,
           source::Union{Nothing, Symbol, AbstractString,
                         Pair{<:Union{Symbol, AbstractString}, <:AbstractVector}}=nothing)

Efficiently reduce the given vector or tuple of `AbstractDataFrame`s with
`vcat`.

The column order, names, and types of the resulting `DataFrame`, and the
behavior of `cols` and `source` keyword arguments follow the rules specified for
[`vcat`](@ref) of `AbstractDataFrame`s.

Metadata: `vcat` propagates table-level `:note`-style metadata for keys that are
present in all passed data frames and have the same value. `vcat` propagates
column-level `:note`-style metadata for keys that are present in all passed data
frames that contain this column and have the same value.

# Example
```jldoctest
julia> df1 = DataFrame(A=1:3, B=1:3)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3

julia> df2 = DataFrame(A=4:6, B=4:6)
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     4      4
   2 │     5      5
   3 │     6      6

julia> df3 = DataFrame(A=7:9, C=7:9)
3×2 DataFrame
 Row │ A      C
     │ Int64  Int64
─────┼──────────────
   1 │     7      7
   2 │     8      8
   3 │     9      9

julia> reduce(vcat, (df1, df2))
6×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      1
   2 │     2      2
   3 │     3      3
   4 │     4      4
   5 │     5      5
   6 │     6      6

julia> reduce(vcat, [df1, df2, df3], cols=:union, source=:source)
9×4 DataFrame
 Row │ A      B        C        source
     │ Int64  Int64?   Int64?   Int64
─────┼─────────────────────────────────
   1 │     1        1  missing       1
   2 │     2        2  missing       1
   3 │     3        3  missing       1
   4 │     4        4  missing       2
   5 │     5        5  missing       2
   6 │     6        6  missing       2
   7 │     7  missing        7       3
   8 │     8  missing        8       3
   9 │     9  missing        9       3
```
"""
function Base.reduce(::typeof(vcat),
                     dfs::Union{AbstractVector{<:AbstractDataFrame},
                                Tuple{AbstractDataFrame, Vararg{AbstractDataFrame}}};
                     cols::Union{Symbol, AbstractVector{Symbol},
                                 AbstractVector{<:AbstractString}}=:setequal,
                     source::Union{Nothing, SymbolOrString,
                                   Pair{<:SymbolOrString, <:AbstractVector}}=nothing)
    res = _vcat(AbstractDataFrame[df for df in dfs if ncol(df) != 0]; cols=cols)
    # only handle table-level metadata, as column-level metadata was done in _vcat
    _merge_matching_table_note_metadata!(res, dfs)

    if source !== nothing
        len = length(dfs)
        if source isa SymbolOrString
            col, vals = source, 1:len
        else
            @assert source isa Pair{<:SymbolOrString, <:AbstractVector}
            col, vals = source
        end

        if columnindex(res, col) > 0
            idx = findfirst(df -> columnindex(df, col) > 0, dfs)
            @assert idx !== nothing
            throw(ArgumentError("source column name :$col already exists in data frame " *
                                " passed in position $idx"))
        end

        if len != length(vals)
            throw(ArgumentError("number of passed source identifiers ($(length(vals)))" *
                                "does not match the number of data frames ($len)"))
        end

        source_vec = Tables.allocatecolumn(eltype(vals), nrow(res))
        @assert firstindex(source_vec) == 1 && lastindex(source_vec) == nrow(res)
        start = 1
        for (v, df) in zip(vals, dfs)
            stop = start + nrow(df) - 1
            source_vec[start:stop] .= Ref(v)
            start = stop + 1
        end

        @assert start == nrow(res) + 1
        insertcols!(res, col => source_vec)
    end

    return res
end

function _vcat(dfs::AbstractVector{AbstractDataFrame};
               cols::Union{Symbol, AbstractVector{Symbol},
                           AbstractVector{<:AbstractString}}=:setequal)
    # note that empty DataFrame() objects are dropped from dfs before we call _vcat
    if isempty(dfs)
        cols isa Symbol && return DataFrame()
        return DataFrame([col => Missing[] for col in cols])
    end
    # Array of all headers
    allheaders = map(names, dfs)
    # Array of unique headers across all data frames
    uniqueheaders = unique(allheaders)
    # All symbols present across all headers
    unionunique = union(uniqueheaders...)
    # List of symbols present in all dataframes
    intersectunique = intersect(uniqueheaders...)

    if cols === :orderequal
        header = unionunique
        if length(uniqueheaders) > 1
            throw(ArgumentError("when `cols=:orderequal` all data frames need to " *
                                "have the same column names and be in the same order"))
        end
    elseif cols === :setequal || cols === :equal
        # an explicit error is thrown as :equal was supported in the past
        if cols === :equal
            throw(ArgumentError("`cols=:equal` is not supported. " *
                                "Use `:setequal` instead."))
        end

        header = unionunique
        coldiff = setdiff(unionunique, intersectunique)

        if !isempty(coldiff)
            # if any DataFrames are a full superset of names, skip them
            let header=header     # julia #15276
                filter!(u -> !issetequal(u, header), uniqueheaders)
            end
            estrings = map(enumerate(uniqueheaders)) do (i, head)
                matching = findall(h -> head == h, allheaders)
                headerdiff = setdiff(coldiff, head)
                badcols = join(headerdiff, ", ", " and ")
                args = join(matching, ", ", " and ")
                return "column(s) $badcols are missing from argument(s) $args"
            end
            throw(ArgumentError(join(estrings, ", ", ", and ")))
        end
    elseif cols === :intersect
        header = intersectunique
    elseif cols === :union
        header = unionunique
    elseif cols isa Symbol
        throw(ArgumentError("Invalid `cols` value :$cols. " *
                            "Only `:orderequal`, `:setequal`, `:intersect`, " *
                            "`:union`, or a vector of column names is allowed."))
    elseif cols isa AbstractVector{Symbol}
        header = cols
    else
        @assert cols isa AbstractVector{<:AbstractString}
        header = Symbol.(cols)
    end

    if isempty(header)
        out_df = DataFrame()
    else
        all_cols = Vector{AbstractVector}(undef, length(header))
        for (i, name) in enumerate(header)
            newcols = map(dfs) do df
                if hasproperty(df, name)
                    return df[!, name]
                else
                    Iterators.repeated(missing, nrow(df))
                end
            end

            lens = map(length, newcols)
            T = mapreduce(eltype, promote_type, newcols)
            all_cols[i] = Tables.allocatecolumn(T, sum(lens))
            offset = 1
            for j in 1:length(newcols)
                copyto!(all_cols[i], offset, newcols[j])
                offset += lens[j]
            end
        end

        out_df = DataFrame(all_cols, header, copycols=false)
    end

    # here we process column-level metadata, table-level metadata is processed in reduce

    # first check if all data frames do not have column-level metadata
    # in which case we do not have to do anything
    all(df -> getfield(parent(df), :colmetadata) === nothing, dfs) && return out_df

    for colname in _names(out_df)
        if length(dfs) == 1
            df1 = dfs[1]
            hasproperty(df1, colname) && _copy_col_note_metadata!(out_df, colname, df1, colname)
        else
            start = findfirst(x -> hasproperty(x, colname), dfs)
            start === nothing && continue
            df_start = dfs[start]
            for key_start in colmetadatakeys(df_start, colname)
                meta_val_start, meta_style_start = colmetadata(df_start, colname, key_start, style=true)
                if meta_style_start === :note
                    good_key = true
                    for i in start+1:length(dfs)
                        dfi = dfs[i]
                        if hasproperty(dfi, colname)
                            if key_start in colmetadatakeys(dfi, colname)
                                meta_vali, meta_stylei = colmetadata(dfi, colname, key_start, style=true)
                                if !(meta_stylei === :note && isequal(meta_val_start, meta_vali))
                                    good_key = false
                                    break
                                end
                            else
                                good_key = false
                                break
                            end
                        end
                    end
                    good_key && colmetadata!(out_df, colname, key_start, meta_val_start, style=:note)
                end
            end
        end
    end

    return out_df
end

"""
    repeat(df::AbstractDataFrame; inner::Integer = 1, outer::Integer = 1)

Construct a data frame by repeating rows in `df`. `inner` specifies how many
times each row is repeated, and `outer` specifies how many times the full set
of rows is repeated.

$METADATA_FIXED

# Example
```jldoctest
julia> df = DataFrame(a=1:2, b=3:4)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> repeat(df, inner=2, outer=3)
12×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     1      3
   3 │     2      4
   4 │     2      4
   5 │     1      3
   6 │     1      3
   7 │     2      4
   8 │     2      4
   9 │     1      3
  10 │     1      3
  11 │     2      4
  12 │     2      4
```
"""
function Base.repeat(df::AbstractDataFrame; inner::Integer=1, outer::Integer=1)
    inner < 0 && throw(ArgumentError("inner keyword argument must be non-negative"))
    outer < 0 && throw(ArgumentError("outer keyword argument must be non-negative"))
    return mapcols(x -> repeat(x, inner = Int(inner), outer = Int(outer)), df)
end

"""
    repeat(df::AbstractDataFrame, count::Integer)

Construct a data frame by repeating each row in `df` the number of times
specified by `count`.

$METADATA_FIXED

# Example
```jldoctest
julia> df = DataFrame(a=1:2, b=3:4)
2×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4

julia> repeat(df, 2)
4×2 DataFrame
 Row │ a      b
     │ Int64  Int64
─────┼──────────────
   1 │     1      3
   2 │     2      4
   3 │     1      3
   4 │     2      4
```
"""
function Base.repeat(df::AbstractDataFrame, count::Integer)
    count < 0 && throw(ArgumentError("count must be non-negative"))
    return mapcols(x -> repeat(x, Int(count)), df)
end

