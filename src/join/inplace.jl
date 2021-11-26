"""
    leftjoin!(df1, df2; on, makeunique=false, source=nothing,
              matchmissing=:error)


Perform a left join of two data frame objects by updating the `df1` with the
joined columns from `df2`.

A left join includes all rows from `df1` and leaves all rows and columns from `df1`
untouched. Note that each row in `df1` must have at most one match in `df2`. Otherwise,
this function would not be able to execute the join in-place since new rows would need to be
added to `df1`.

# Arguments
- `df1`, `df2`: the `AbstractDataFrames` to be joined

# Keyword Arguments
- `on` : A column name to join `df1` and `df2` on. If the columns on which
  `df1` and `df2` will be joined have different names, then a `left=>right`
  pair can be passed. It is also allowed to perform a join on multiple columns,
  in which case a vector of column names or column name pairs can be passed
  (mixing names and pairs is allowed).
- `makeunique` : if `false` (the default), an error will be raised
  if duplicate names are found in columns not joined on;
  if `true`, duplicate names will be suffixed with `_i`
  (`i` starting at 1 for the first duplicate).
- `source` : Default: `nothing`. If a `Symbol` or string, adds indicator
  column with the given name, for whether a row appeared in only `df1` (`"left_only"`)
  or in both (`"both"`). If the name is already in use,
  the column name will be modified if `makeunique=true`.
- `matchmissing` : if equal to `:error` throw an error if `missing` is present
  in `on` columns; if equal to `:equal` then `missing` is allowed and missings are
  matched; if equal to `:notequal` then missings are dropped in `df2` `on` columns;
  `isequal` is used for comparisons of rows for equality

The columns added to `df1` from `df2` will support missing values.

It is not allowed to join on columns that contain `NaN` or `-0.0` in real or
imaginary part of the number. If you need to perform a join on such values use
CategoricalArrays.jl and transform a column containing such values into a
`CategoricalVector`.

See also: [`leftjoin`](@ref).

# Examples
```jldoctest
julia> name = DataFrame(ID=[1, 2, 3], Name=["John Doe", "Jane Doe", "Joe Blogs"])
3×2 DataFrame
 Row │ ID     Name
     │ Int64  String
─────┼──────────────────
   1 │     1  John Doe
   2 │     2  Jane Doe
   3 │     3  Joe Blogs

julia> job = DataFrame(ID=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ ID     Job
     │ Int64  String
─────┼───────────────
   1 │     1  Lawyer
   2 │     2  Doctor
   3 │     4  Farmer

julia> leftjoin!(name, job, on = :ID)
3×3 DataFrame
 Row │ ID     Name       Job
     │ Int64  String     String?
─────┼───────────────────────────
   1 │     1  John Doe   Lawyer
   2 │     2  Jane Doe   Doctor
   3 │     3  Joe Blogs  missing

julia> job2 = DataFrame(identifier=[1, 2, 4], Job=["Lawyer", "Doctor", "Farmer"])
3×2 DataFrame
 Row │ identifier  Job
     │ Int64       String
─────┼────────────────────
   1 │          1  Lawyer
   2 │          2  Doctor
   3 │          4  Farmer

julia> leftjoin!(name, job2, on = :ID => :identifier, makeunique=true, source=:source)
3×5 DataFrame
 Row │ ID     Name       Job      Job_1    source
     │ Int64  String     String?  String?  String
─────┼───────────────────────────────────────────────
   1 │     1  John Doe   Lawyer   Lawyer   both
   2 │     2  Jane Doe   Doctor   Doctor   both
   3 │     3  Joe Blogs  missing  missing  left_only
```
"""
function leftjoin!(df1::AbstractDataFrame, df2::AbstractDataFrame;
                   on::Union{<:OnType, AbstractVector}=Symbol[], makeunique::Bool=false,
                   source::Union{Nothing, Symbol, AbstractString}=nothing,
                   matchmissing::Symbol=:error)

    _check_consistency(df1)
    _check_consistency(df2)

    if !is_column_insertion_allowed(df1)
        throw(ArgumentError("leftjoin! is only supported if `df1` is a `DataFrame`, " *
                            "or a SubDataFrame created with `:` as column selector"))
    end

    if on == []
        throw(ArgumentError("Missing join argument 'on'."))
    end

    joiner = DataFrameJoiner(df1, df2, on, matchmissing, :left)

    right_noon_names = names(joiner.dfr, Not(joiner.right_on))
    if !(makeunique || isempty(intersect(right_noon_names, names(df1))))
        throw(ArgumentError("the following columns are present in both " *
                            "left and right data frames but not listed in `on`: " *
                            join(intersect(right_noon_names, names(df1)), ", ") *
                            ". Pass makeunique=true to add a suffix automatically to " *
                            "columns names from the right data frame."))
    end

    left_ixs_inner, right_ixs_inner = find_inner_rows(joiner)

    right_ixs = _map_leftjoin_ixs(nrow(df1), left_ixs_inner, right_ixs_inner)

    # TODO: consider adding threading support in the future
    for colname in right_noon_names
        rcol = joiner.dfr[!, colname] # note that joiner.dfr does not have to be df2
        rcol_joined = compose_joined_rcol!(rcol, similar_missing(rcol, nrow(df1)),
                                          right_ixs)
        # if df1 isa SubDataFrame we must copy columns
        insertcols!(df1, colname => rcol_joined, makeunique=makeunique,
                    copycols=!(df1 isa DataFrame))
    end

    if source !== nothing
        pool = ["left_only", "right_only", "both"]
        invpool = Dict{String, UInt32}("left_only" => 1,
                                       "right_only" => 2,
                                       "both" => 3)
        indicatorcol = PooledArray(PooledArrays.RefArray(UInt32.(2 .* (right_ixs .> 0) .+ 1)),
                                   invpool, pool)

        unique_indicator = source
        if makeunique
            try_idx = 0
            while hasproperty(df1, unique_indicator)
                try_idx += 1
                unique_indicator = Symbol(source, "_", try_idx)
            end
        end

        if hasproperty(df1, unique_indicator)
            throw(ArgumentError("joined data frame already has column " *
                                ":$unique_indicator. Pass makeunique=true to " *
                                "make it unique using a suffix automatically."))
        end
        df1[!, unique_indicator] = indicatorcol
    end
    return df1
end

function _map_leftjoin_ixs(out_len::Int,
                           left_ixs_inner::Vector{Int},
                           right_ixs_inner::Vector{Int})
    right_ixs = zeros(Int, out_len)
    @inbounds for (li, ri) in zip(left_ixs_inner, right_ixs_inner)
        if right_ixs[li] > 0
            throw(ArgumentError("duplicate rows found in right table"))
        end
        right_ixs[li] = ri
    end
    return right_ixs
end

function compose_joined_rcol!(rcol::AbstractVector,
                              rcol_joined::AbstractVector,
                              right_ixs::Vector{Int})
    @assert length(rcol_joined) == length(right_ixs)
    @inbounds for (i, idx) in enumerate(right_ixs)
        if idx > 0
            rcol_joined[i] = rcol[idx]
        end
    end
    return rcol_joined
end
