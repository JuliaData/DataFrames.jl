abstract type AbstractAggregate end

struct Reduce{O, C, A} <: AbstractAggregate
    op::O
    condf::C
    adjust::A
    checkempty::Bool
end
Reduce(f, condf=nothing, adjust=nothing) = Reduce(f, condf, adjust, false)

check_aggregate(f::Any, ::AbstractVector) = f
check_aggregate(f::typeof(sum), ::AbstractVector{<:Union{Missing, Number}}) =
    Reduce(Base.add_sum)
check_aggregate(f::typeof(sum∘skipmissing), ::AbstractVector{<:Union{Missing, Number}}) =
    Reduce(Base.add_sum, !ismissing)
check_aggregate(f::typeof(prod), ::AbstractVector{<:Union{Missing, Number}}) =
    Reduce(Base.mul_prod)
check_aggregate(f::typeof(prod∘skipmissing), ::AbstractVector{<:Union{Missing, Number}}) =
    Reduce(Base.mul_prod, !ismissing)
check_aggregate(f::typeof(maximum), ::AbstractVector{<:Union{Missing, Real}}) =
    Reduce(max)
check_aggregate(f::typeof(maximum∘skipmissing), ::AbstractVector{<:Union{Missing, Real}}) =
    Reduce(max, !ismissing, nothing, true)
check_aggregate(f::typeof(minimum), ::AbstractVector{<:Union{Missing, Real}}) =
    Reduce(min)
check_aggregate(f::typeof(minimum∘skipmissing), ::AbstractVector{<:Union{Missing, Real}}) =
    Reduce(min, !ismissing, nothing, true)
check_aggregate(f::typeof(mean), ::AbstractVector{<:Union{Missing, Number}}) =
    Reduce(Base.add_sum, nothing, /)
check_aggregate(f::typeof(mean∘skipmissing), ::AbstractVector{<:Union{Missing, Number}}) =
    Reduce(Base.add_sum, !ismissing, /)

# Other aggregate functions which are not strictly reductions
struct Aggregate{F, C} <: AbstractAggregate
    f::F
    condf::C
end
Aggregate(f) = Aggregate(f, nothing)

check_aggregate(f::typeof(var), ::AbstractVector{<:Union{Missing, Number}}) =
    Aggregate(var)
check_aggregate(f::typeof(var∘skipmissing), ::AbstractVector{<:Union{Missing, Number}}) =
    Aggregate(var, !ismissing)
check_aggregate(f::typeof(std), ::AbstractVector{<:Union{Missing, Number}}) =
    Aggregate(std)
check_aggregate(f::typeof(std∘skipmissing), ::AbstractVector{<:Union{Missing, Number}}) =
    Aggregate(std, !ismissing)
check_aggregate(f::typeof(first), v::AbstractVector) =
    eltype(v) === Any ? f : Aggregate(first)
check_aggregate(f::typeof(first),
                ::AbstractVector{<:Union{Missing, MULTI_COLS_TYPE, AbstractVector}}) = f
check_aggregate(f::typeof(first∘skipmissing), v::AbstractVector) =
    eltype(v) === Any ? f : Aggregate(first, !ismissing)
check_aggregate(f::typeof(first∘skipmissing),
                ::AbstractVector{<:Union{Missing, MULTI_COLS_TYPE, AbstractVector}}) = f
check_aggregate(f::typeof(last), v::AbstractVector) =
    eltype(v) === Any ? f : Aggregate(last)
check_aggregate(f::typeof(last),
                ::AbstractVector{<:Union{Missing, MULTI_COLS_TYPE, AbstractVector}}) = f
check_aggregate(f::typeof(last∘skipmissing), v::AbstractVector) =
    eltype(v) === Any ? f : Aggregate(last, !ismissing)
check_aggregate(f::typeof(last∘skipmissing),
                ::AbstractVector{<:Union{Missing, MULTI_COLS_TYPE, AbstractVector}}) = f
check_aggregate(f::typeof(length), ::AbstractVector) = Aggregate(length)

# SkipMissing does not support length

# Use a strategy similar to reducedim_init from Base to get the vector of the right type
function groupreduce_init(op, condf, adjust,
                          incol::AbstractVector{U}, gd::GroupedDataFrame) where U
    T = Base.promote_union(U)

    if op === Base.add_sum
        initf = zero
    elseif op === Base.mul_prod
        initf = one
    else
        throw(ErrorException("Unrecognized op $op"))
    end

    Tnm = nonmissingtype(T)
    if isconcretetype(Tnm) && applicable(initf, Tnm)
        tmpv = initf(Tnm)
        initv = op(tmpv, tmpv)
        if adjust isa Nothing
            x = Tnm <: AbstractIrrational ? float(initv) : initv
        else
            x = adjust(initv, 1)
        end
        if condf === !ismissing
            V = typeof(x)
        else
            V = U >: Missing ? Union{typeof(x), Missing} : typeof(x)
        end
        # here we are sure that only Base.add_sum or Base.mul_prod are performed
        # so we always fall back to Vector as output column type
        v = Tables.allocatecolumn(V, length(gd))
        fill!(v, x)
        return v
    else
        # do not try to determine the narrowest possible type nor starting value
        # as this is not possible to do correctly in general without processing
        # groups; it will get fixed later in groupreduce!; later we
        # will make use of the fact that this vector is filled with #undef
        # while above the vector is filled with a concrete value
        return Vector{Any}(undef, length(gd))
    end
end

for (op, initf) in ((:max, :typemin), (:min, :typemax))
    @eval begin
        function groupreduce_init(::typeof($op), condf, adjust,
                                  incol::AbstractVector{T}, gd::GroupedDataFrame) where T
            @assert isnothing(adjust)
            S = nonmissingtype(T)
            # !ismissing check is purely an optimization to avoid a copy later
            outcol = similar(incol, condf === !ismissing ? S : T, length(gd))
            # Comparison is possible only between CatValues from the same pool
            resT = typeof(outcol)
            if nameof(resT) === :CategoricalArray && nameof(parentmodule(resT)) === :CategoricalArrays
                # we know that CategoricalArray has `pool` field
                outcol.pool = incol.pool
            end
            # It is safe to use a non-missing init value
            # since missing will poison the result if present
            # we assume here that groups are non-empty (current design assures this)
            # + workaround for https://github.com/JuliaLang/julia/issues/36978
            if isconcretetype(S) && hasmethod($initf, Tuple{S}) && !(S <: Irrational)
                fill!(outcol, $initf(S))
            else
                fillfirst!(condf, outcol, incol, gd)
            end
            return outcol
        end
    end
end

function copyto_widen!(res::AbstractVector{T}, x::AbstractVector) where T
    @inbounds for i in eachindex(res, x)
        val = x[i]
        S = typeof(val)
        if S <: T || promote_type(S, T) <: T
            res[i] = val
        else
            newres = Tables.allocatecolumn(promote_type(S, T), length(x))
            return copyto_widen!(newres, x)
        end
    end
    return res
end

function groupreduce!_helper(res::AbstractVector, f, op, condf, adjust, checkempty::Bool,
                             incol::AbstractVector, groups::Vector{Int}, counts::Vector{Int},
                             batches)
    for batch in batches
        # Allow other tasks to do garbage collection while this one runs
        GC.safepoint()

        @inbounds for i in batch
            gix = groups[i]
            x = incol[i]
            if gix > 0 && (condf === nothing || condf(x))
                # this check should be optimized out if U is not Any
                if eltype(res) === Any && !isassigned(res, gix)
                    res[gix] = f(x, gix)
                else
                    res[gix] = op(res[gix], f(x, gix))
                end
                # this check is optimized out by constant propagation
                if adjust !== nothing || checkempty
                    counts[gix] += 1
                end
            end
        end
    end
end

function groupreduce!(res::AbstractVector, f, op, condf, adjust, checkempty::Bool,
                      incol::AbstractVector, gd::GroupedDataFrame)
    n = length(gd)
    if adjust !== nothing || checkempty
        counts = zeros(Int, n)
    else
        counts = Int[]
    end
    groups = gd.groups
    batchsize = Threads.nthreads() > 1 ? 100_000 : typemax(Int)
    batches = Iterators.partition(eachindex(incol, groups), batchsize)

    groupreduce!_helper(res, f, op, condf, adjust, checkempty,
                             incol, groups, counts, batches)
    # handle the case of an uninitialized reduction
    if eltype(res) === Any
        if op === Base.add_sum
            initf = zero
        elseif op === Base.mul_prod
            initf = one
        else
            initf = x -> throw(ErrorException("Unrecognized op $op"))
        end
        @inbounds for gix in eachindex(res)
            if !isassigned(res, gix)
                res[gix] = initf(nonmissingtype(eltype(incol)))
            end
        end
    end
    if adjust !== nothing
        res .= adjust.(res, counts)
    end
    if checkempty && any(iszero, counts)
        throw(ArgumentError("some groups contain only missing values"))
    end
    # Reallocate Vector created in groupreduce_init with min or max
    # for CategoricalVector
    resT = typeof(res)
    if nameof(resT) === :CategoricalArray && nameof(parentmodule(resT)) === :CategoricalArrays
        @assert op === min || op === max
        # we know that CategoricalArray has `pool` field
        @assert res.pool === incol.pool
        res.pool = copy(incol.pool)
    end
    if isconcretetype(eltype(res))
        return res
    else
        return copyto_widen!(Tables.allocatecolumn(typeof(first(res)), n), res)
    end
end

# function barrier works around type instability of groupreduce_init due to applicable
groupreduce(f, op, condf, adjust, checkempty::Bool,
            incol::AbstractVector, gd::GroupedDataFrame) =
    groupreduce!(groupreduce_init(op, condf, adjust, incol, gd),
                 f, op, condf, adjust, checkempty, incol, gd)
# Avoids the overhead due to Missing when computing reduction
groupreduce(f, op, condf::typeof(!ismissing), adjust, checkempty::Bool,
            incol::AbstractVector, gd::GroupedDataFrame) =
    groupreduce!(disallowmissing(groupreduce_init(op, condf, adjust, incol, gd)),
                 f, op, condf, adjust, checkempty, incol, gd)

(r::Reduce)(incol::AbstractVector, gd::GroupedDataFrame) =
    groupreduce((x, i) -> x, r.op, r.condf, r.adjust, r.checkempty, incol, gd)

function (agg::Aggregate{typeof(var)})(incol::AbstractVector, gd::GroupedDataFrame)
    means = groupreduce((x, i) -> x, Base.add_sum, agg.condf, /, false, incol, gd)
    z = zero(eltype(incol)) - zero(eltype(means))
    S = typeof((abs2(z) + abs2(z))/2)
    # !ismissing check is purely an optimization to avoid a copy later
    T = eltype(incol) >: Missing && agg.condf !== !ismissing ?
        T = Union{Missing, S} : S
    res = zeros(T, length(gd))
    return groupreduce!(res, (x, i) -> @inbounds(abs2(x - means[i])), +, agg.condf,
                        (x, l) -> l <= 1 ? x/0 : x/(l-1),
                        false, incol, gd)
end

function (agg::Aggregate{typeof(std)})(incol::AbstractVector, gd::GroupedDataFrame)
    outcol = Aggregate(var, agg.condf)(incol, gd)
    if typeof(sqrt(zero(eltype(outcol)))) === eltype(outcol)
        return map!(sqrt, outcol, outcol)
    else
        return map(sqrt, outcol)
    end
end

for f in (first, last)
    function (agg::Aggregate{typeof(f)})(incol::AbstractVector, gd::GroupedDataFrame)
        n = length(gd)
        outcol = similar(incol, n)
        fillfirst!(agg.condf, outcol, incol, gd, rev=agg.f === last)
        if isconcretetype(eltype(outcol))
            return outcol
        else
            return copyto_widen!(Tables.allocatecolumn(typeof(first(outcol)), n), outcol)
        end
    end
end

function (agg::Aggregate{typeof(length)})(incol::AbstractVector, gd::GroupedDataFrame)
    if getfield(gd, :idx) === nothing
        lens = zeros(Int, length(gd))
        @inbounds for gix in gd.groups
            gix > 0 && (lens[gix] += 1)
        end
        return lens
    else
        return gd.ends .- gd.starts .+ 1
    end
end

isagg((col, (fun, outcol))::Pair{<:ColumnIndex, <:Pair{<:Any, <:SymbolOrString}}, gdf::GroupedDataFrame) =
    check_aggregate(fun, parent(gdf)[!, col]) isa AbstractAggregate
isagg(::Any, gdf::GroupedDataFrame) = false
