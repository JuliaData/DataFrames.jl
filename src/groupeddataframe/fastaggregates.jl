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
check_aggregate(f::typeof(maximum),
                ::AbstractVector{<:Union{Missing, MULTI_COLS_TYPE, AbstractVector}}) = f
check_aggregate(f::typeof(maximum), v::AbstractVector{<:Union{Missing, Real}}) =
    eltype(v) === Any ? f : Reduce(max)
check_aggregate(f::typeof(maximum∘skipmissing),
                ::AbstractVector{<:Union{Missing, MULTI_COLS_TYPE, AbstractVector}}) = f
check_aggregate(f::typeof(maximum∘skipmissing), v::AbstractVector{<:Union{Missing, Real}}) =
    eltype(v) === Any ? f : Reduce(max, !ismissing, nothing, true)
check_aggregate(f::typeof(minimum),
                ::AbstractVector{<:Union{Missing, MULTI_COLS_TYPE, AbstractVector}}) = f
check_aggregate(f::typeof(minimum), v::AbstractVector{<:Union{Missing, Real}}) =
    eltype(v) === Any ? f : Reduce(min)
check_aggregate(f::typeof(minimum∘skipmissing),
                ::AbstractVector{<:Union{Missing, MULTI_COLS_TYPE, AbstractVector}}) = f
check_aggregate(f::typeof(minimum∘skipmissing), v::AbstractVector{<:Union{Missing, Real}}) =
    eltype(v) === Any ? f : Reduce(min, !ismissing, nothing, true)
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
        v = similar(incol, V, length(gd))
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
            outcolT = typeof(outcol).name
            if outcolT.name === :CategoricalArray &&
                nameof(outcolT.module) === :CategoricalArrays
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

function groupreduce!(res::AbstractVector, f, op, condf, adjust, checkempty::Bool,
                      incol::AbstractVector, gd::GroupedDataFrame, nthreads::Integer)
    n = length(gd)
    groups = gd.groups
    if adjust !== nothing || checkempty
        counts = zeros(Int, n)
    end
    nt = min(nthreads, Threads.nthreads())
    if nt <= 1 || axes(incol) != axes(groups)
        # Operate on array blocks smaller than 3MB so that they fit in the CPU cache
        npasses = sizeof(res) ÷ 3_000_000
        for j in 1:npasses
            start = 1 + ((j - 1) * length(groups)) ÷ npasses
            stop = (j * length(groups)) ÷ npasses
            @inbounds for i in eachindex(incol, groups)
                gix = groups[i]
                gix in start:stop || continue
                x = incol[i]
                if gix > 0 && (condf === nothing || condf(x))
                    # this check should be optimized out if U is not Any
                    if eltype(res) === Any && !isassigned(res, gix)
                        res[gix] = f(x, gix)
                    else
                        res[gix] = op(res[gix], f(x, gix))
                    end
                    if adjust !== nothing || checkempty
                        counts[gix] += 1
                    end
                end
            end
        end
    else
        res_vec = Vector{typeof(res)}(undef, nt)
        # needs to be always allocated to fix type instability with @threads
        counts_vec = Vector{Vector{Int}}(undef, nt)
        res_vec[1] = res
        if adjust !== nothing || checkempty
            counts_vec[1] = counts
        end
        for i in 2:nt
            res_vec[i] = copy(res)
            if adjust !== nothing || checkempty
                counts_vec[i] = zeros(Int, n)
            end
        end
        Threads.@threads for tid in 1:nt
            res′ = res_vec[tid]
            if adjust !== nothing || checkempty
                counts′ = counts_vec[tid]
            end
            start = 1 + ((tid - 1) * length(groups)) ÷ nt
            stop = (tid * length(groups)) ÷ nt
            @inbounds for i in start:stop
                gix = groups[i]
                x = incol[i]
                if gix > 0 && (condf === nothing || condf(x))
                    # this check should be optimized out if U is not Any
                    if eltype(res′) === Any && !isassigned(res′, gix)
                        res′[gix] = f(x, gix)
                    else
                        res′[gix] = op(res′[gix], f(x, gix))
                    end
                    if adjust !== nothing || checkempty
                        counts′[gix] += 1
                    end
                end
            end
        end
        for i in 2:length(res_vec)
            res .= op.(res, res_vec[i])
        end
        if adjust !== nothing || checkempty
            for i in 2:length(counts_vec)
                counts .+= counts_vec[i]
            end
        end
    end
    # handle the case of an unitialized reduction
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
    resT = typeof(res).name
    if resT.name === :CategoricalArray &&
        nameof(resT.module) === :CategoricalArrays
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
            incol::AbstractVector, gd::GroupedDataFrame,
            nthreads::Integer) =
    groupreduce!(groupreduce_init(op, condf, adjust, incol, gd),
                 f, op, condf, adjust, checkempty, incol, gd, nthreads)
# Avoids the overhead due to Missing when computing reduction
groupreduce(f, op, condf::typeof(!ismissing), adjust, checkempty::Bool,
            incol::AbstractVector, gd::GroupedDataFrame,
            nthreads::Integer) =
    groupreduce!(disallowmissing(groupreduce_init(op, condf, adjust, incol, gd)),
                 f, op, condf, adjust, checkempty, incol, gd, nthreads)

(r::Reduce)(incol::AbstractVector, gd::GroupedDataFrame;
            nthreads::Integer=NTHREADS) =
    groupreduce((x, i) -> x, r.op, r.condf, r.adjust, r.checkempty, incol, gd, nthreads)

# this definition is missing in Julia 1.0 LTS and is required by aggregation for var
# TODO: remove this when we drop 1.0 support
if VERSION < v"1.1"
    Base.zero(::Type{Missing}) = missing
end

function (agg::Aggregate{typeof(var)})(incol::AbstractVector, gd::GroupedDataFrame;
                                       nthreads::Integer=NTHREADS)
    means = groupreduce((x, i) -> x, Base.add_sum, agg.condf, /, false,
                        incol, gd, nthreads)
    # !ismissing check is purely an optimization to avoid a copy later
    if eltype(means) >: Missing && agg.condf !== !ismissing
        T = Union{Missing, real(eltype(means))}
    else
        T = real(eltype(means))
    end
    res = zeros(T, length(gd))
    return groupreduce!(res, (x, i) -> @inbounds(abs2(x - means[i])), +, agg.condf,
                        (x, l) -> l <= 1 ? oftype(x / (l-1), NaN) : x / (l-1),
                        false, incol, gd, nthreads)
end

function (agg::Aggregate{typeof(std)})(incol::AbstractVector, gd::GroupedDataFrame;
                                       nthreads::Integer=NTHREADS)
    outcol = Aggregate(var, agg.condf)(incol, gd; nthreads=nthreads)
    if eltype(outcol) <: Union{Missing, Rational}
        return sqrt.(outcol)
    else
        return map!(sqrt, outcol, outcol)
    end
end

for f in (:first, :last)
    # Without using @eval the presence of a keyword argument triggers a Julia bug
    @eval begin
        function (agg::Aggregate{typeof($f)})(incol::AbstractVector, gd::GroupedDataFrame;
                                              nthreads::Integer=NTHREADS)
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
end

function (agg::Aggregate{typeof(length)})(incol::AbstractVector, gd::GroupedDataFrame;
                                          nthreads::Integer=NTHREADS)
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
