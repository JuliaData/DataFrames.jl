"""
    describe(df::AbstractDataFrame; cols=:)
    describe(df::AbstractDataFrame, stats::Union{Symbol, Pair}...; cols=:)

Return descriptive statistics for a data frame as a new `DataFrame`
where each row represents a variable and each column a summary statistic.

# Arguments
- `df` : the `AbstractDataFrame`
- `stats::Union{Symbol, Pair}...` : the summary statistics to report.
  Arguments can be:
    - A symbol from the list `:mean`, `:std`, `:min`, `:q25`,
      `:median`, `:q75`, `:max`, `:eltype`, `:nunique`, `:nuniqueall`, `:first`,
      `:last`, `:nnonmissing`, and `:nmissing`. The default statistics used are
      `:mean`, `:min`, `:median`, `:max`, `:nmissing`, and `:eltype`.
    - `:detailed` as the only `Symbol` argument to return all statistics
      except `:first`, `:last`, `:nuniqueall`, and `:nnonmissing`.
    - `:all` as the only `Symbol` argument to return all statistics.
    - A `function => name` pair where `name` is a `Symbol` or string. This will
      create a column of summary statistics with the provided name.
- `cols` : a keyword argument allowing to select only a subset or transformation
  of columns from `df` to describe. Can be any column selector or transformation
  accepted by [`select`](@ref).

# Details
For `Real` columns, compute the mean, standard deviation, minimum, first
quantile, median, third quantile, and maximum. If a column does not derive from
`Real`, `describe` will attempt to calculate all statistics, using `nothing` as
a fall-back in the case of an error.

When `stats` contains `:nunique`, `describe` will report the
number of unique values in a column. If a column's base type derives from `Real`,
`:nunique` will return `nothing`s. Use `:nuniqueall` to report the number of
unique values in all columns.

Missing values are filtered in the calculation of all statistics, however the
column `:nmissing` will report the number of missing values of that variable
and `:nnonmissing` the number of non-missing values.

If custom functions are provided, they are called repeatedly with the vector
corresponding to each column as the only argument. For columns allowing for
missing values, the vector is wrapped in a call to `skipmissing`: custom
functions must therefore support such objects (and not only vectors), and cannot
access missing values.

Metadata: this function drops all metadata.

# Examples

```jldoctest
julia> df = DataFrame(i=1:10, x=0.1:0.1:1.0, y='a':'j');

julia> describe(df)
3×7 DataFrame
 Row │ variable  mean    min  median  max  nmissing  eltype
     │ Symbol    Union…  Any  Union…  Any  Int64     DataType
─────┼────────────────────────────────────────────────────────
   1 │ i         5.5     1    5.5     10          0  Int64
   2 │ x         0.55    0.1  0.55    1.0         0  Float64
   3 │ y                 a            j           0  Char

julia> describe(df, :min, :max)
3×3 DataFrame
 Row │ variable  min  max
     │ Symbol    Any  Any
─────┼────────────────────
   1 │ i         1    10
   2 │ x         0.1  1.0
   3 │ y         a    j

julia> describe(df, :min, sum => :sum)
3×3 DataFrame
 Row │ variable  min  sum
     │ Symbol    Any  Union…
─────┼───────────────────────
   1 │ i         1    55
   2 │ x         0.1  5.5
   3 │ y         a

julia> describe(df, :min, sum => :sum, cols=:x)
1×3 DataFrame
 Row │ variable  min      sum
     │ Symbol    Float64  Float64
─────┼────────────────────────────
   1 │ x             0.1      5.5
```
"""
DataAPI.describe(df::AbstractDataFrame,
                 stats::Union{Symbol, Pair{<:Base.Callable, <:SymbolOrString}}...;
                 cols=:) =
    _describe(select(df, cols, copycols=false), Any[s for s in stats])

DataAPI.describe(df::AbstractDataFrame; cols=:) =
    _describe(select(df, cols, copycols=false),
              Any[:mean, :min, :median, :max, :nmissing, :eltype])

function _describe(df::AbstractDataFrame, stats::AbstractVector)
    predefined_funs = Symbol[s for s in stats if s isa Symbol]

    allowed_fields = [:mean, :std, :min, :q25, :median, :q75, :max,
                      :nunique, :nuniqueall, :nmissing, :nnonmissing,
                      :first, :last, :eltype]

    if predefined_funs == [:all]
        predefined_funs = allowed_fields
        i = findfirst(s -> s == :all, stats)
        splice!(stats, i, allowed_fields) # insert in the stats vector to get a good order
    elseif predefined_funs == [:detailed]
        predefined_funs = [:mean, :std, :min, :q25, :median, :q75,
                           :max, :nunique, :nmissing, :eltype]
        i = findfirst(s -> s == :detailed, stats)
        splice!(stats, i, predefined_funs) # insert in the stats vector to get a good order
    elseif :all in predefined_funs || :detailed in predefined_funs
        throw(ArgumentError("`:all` and `:detailed` must be the only `Symbol` argument."))
    elseif !issubset(predefined_funs, allowed_fields)
        not_allowed = join(setdiff(predefined_funs, allowed_fields), ", :")
        allowed_msg = "\nAllowed fields are: :" * join(allowed_fields, ", :")
        throw(ArgumentError(":$not_allowed not allowed." * allowed_msg))
    end

    custom_funs = Any[s[1] => Symbol(s[2]) for s in stats if s isa Pair]

    ordered_names = [s isa Symbol ? s : Symbol(last(s)) for s in stats]

    if !allunique(ordered_names)
        df_ord_names = DataFrame(ordered_names = ordered_names)
        duplicate_names = unique(ordered_names[nonunique(df_ord_names)])
        throw(ArgumentError("Duplicate names not allowed. Duplicated value(s) are: " *
                            ":$(join(duplicate_names, ", "))"))
    end

    # Put the summary stats into the return data frame
    data = DataFrame()
    data.variable = propertynames(df)

    # An array of Dicts for summary statistics
    col_stats_dicts = map(eachcol(df)) do col
        if eltype(col) >: Missing
            t = skipmissing(col)
            d = get_stats(t, predefined_funs)
            get_stats!(d, t, custom_funs)
        else
            d = get_stats(col, predefined_funs)
            get_stats!(d, col, custom_funs)
        end

        if :nmissing in predefined_funs
            d[:nmissing] = count(ismissing, col)
        end

        if :nnonmissing in predefined_funs
            d[:nnonmissing] = count(!ismissing, col)
        end

        if :first in predefined_funs
            d[:first] = isempty(col) ? nothing : first(col)
        end

        if :last in predefined_funs
            d[:last] = isempty(col) ? nothing : last(col)
        end

        if :eltype in predefined_funs
            d[:eltype] = eltype(col)
        end

        return d
    end

    for stat in ordered_names
        # for each statistic, loop through the columns array to find values
        # letting the comprehension choose the appropriate type
        data[!, stat] = [d[stat] for d in col_stats_dicts]
    end

    return data
end

# Compute summary statistics
# use a dict because we don't know which measures the user wants
# Outside of the `describe` function due to something with 0.7
function get_stats(@nospecialize(col::Union{AbstractVector, Base.SkipMissing}),
                   stats::AbstractVector{Symbol})
    d = Dict{Symbol, Any}()

    if :q25 in stats || :median in stats || :q75 in stats
        # types that do not support basic arithmetic (like strings) will only fail
        # after sorting the data, so check this beforehand to fail early
        T = eltype(col)
        if isconcretetype(T) && !hasmethod(-, Tuple{T, T})
            d[:q25] = d[:median] = d[:q75] = nothing
        else
            mcol = Base.copymutable(col)
            if :q25 in stats
                d[:q25] = try quantile!(mcol, 0.25) catch; nothing; end
            end
            if :median in stats
                d[:median] = try quantile!(mcol, 0.50) catch; nothing; end
            end
            if :q75 in stats
                d[:q75] = try quantile!(mcol, 0.75) catch; nothing; end
            end
        end
    end

    if :min in stats || :max in stats
        ex = try extrema(col) catch; (nothing, nothing) end
        d[:min] = ex[1]
        d[:max] = ex[2]
    end

    if :mean in stats || :std in stats
        m = try mean(col) catch end
        # we can add non-necessary things to d, because we choose what we need
        # in the main function
        d[:mean] = m

        if :std in stats
            d[:std] = try std(col, mean = m) catch end
        end
    end

    if :nunique in stats
        if eltype(col) <: Real
            d[:nunique] = nothing
        else
            d[:nunique] = try length(Set(col)) catch end
        end
    end

    if :nuniqueall in stats
        d[:nuniqueall] = try length(Set(col)) catch end
    end

    return d
end

function get_stats!(d::Dict, @nospecialize(col::Union{AbstractVector, Base.SkipMissing}),
                    stats::Vector{Any})
    for stat in stats
        d[stat[2]] = try stat[1](col) catch end
    end
end

