# Formulas for representing and working with linear-model-type expressions
# Original by Harlan D. Harris.  Later modifications by John Myles White
# and Douglas M. Bates.

## Formulas are written as expressions and parsed by the Julia parser.
## For example :(y ~ a + b + log(c))
## In Julia the & operator is used for an interaction.  What would be written
## in R as y ~ a + b + a:b is written :(y ~ a + b + a&b) in Julia.
## The equivalent R expression, y ~ a*b, is the same in Julia

## The lhs of a one-sided formula is 'nothing'
## The rhs of a formula can be 1

type Formula
    lhs::@compat(Union{Symbol, Expr, Void})
    rhs::@compat(Union{Symbol, Expr, Integer})
end

macro ~(lhs, rhs)
    ex = Expr(:call,
              :Formula,
              Base.Meta.quot(lhs),
              Base.Meta.quot(rhs))
    return ex
end

type Terms
    terms::Vector
    eterms::Vector        # evaluation terms
    factors::Matrix{Int8} # maps terms to evaluation terms
    order::Vector{Int}    # orders of rhs terms
    response::Bool        # indicator of a response, which is eterms[1] if present
    intercept::Bool       # is there an intercept column in the model matrix?
end

type ModelFrame
    df::AbstractDataFrame
    terms::Terms
    msng::BitArray
end

type ModelMatrix{T <: @compat(Union{Float32, Float64})}
    m::Matrix{T}
    assign::Vector{Int}
end

Base.size(mm::ModelMatrix) = size(mm.m)
Base.size(mm::ModelMatrix, dim...) = size(mm.m, dim...)

function Base.show(io::IO, f::Formula)
    print(io,
          string("Formula: ",
                 f.lhs == nothing ? "" : f.lhs, " ~ ", f.rhs))
end

## Return, as a vector of symbols, the names of all the variables in
## an expression or a formula
function allvars(ex::Expr)
    if ex.head != :call error("Non-call expression encountered") end
    cc=Symbol[]
    for i in ex.args[2:end] cc=append!(cc,allvars(i)) end
    cc
end
allvars(f::Formula) = unique(vcat(allvars(f.rhs), allvars(f.lhs)))
allvars(sym::Symbol) = [sym]
allvars(v::Any) = Array(Symbol, 0)

# special operators in formulas
const specials = Set([:+, :-, :*, :/, :&, :|, :^])

function dospecials(ex::Expr)
    if ex.head != :call error("Non-call expression encountered") end
    a1 = ex.args[1]
    if !(a1 in specials) return ex end
    excp = copy(ex)
    excp.args = vcat(a1,map(dospecials, ex.args[2:end]))
    if a1 != :* return excp end
    aa = excp.args
    a2 = aa[2]
    a3 = aa[3]
    if length(aa) > 3
        excp.args = vcat(a1, aa[3:end])
        a3 = dospecials(excp)
    end
    ## this order of expansion gives the R-style ordering of interaction
    ## terms (after sorting in increasing interaction order) for higher-
    ## order interaction terms (e.g. x1 * x2 * x3 should expand to x1 +
    ## x2 + x3 + x1&x2 + x1&x3 + x2&x3 + x1&x2&x3)
    :($a2 + $a2 & $a3 + $a3)
end
dospecials(a::Any) = a

## Distribution of & over +
const distributive = @compat Dict(:& => :+)

distribute(ex::Expr) = distribute!(copy(ex))
distribute(a::Any) = a
## apply distributive property in-place
function distribute!(ex::Expr)
    if ex.head != :call error("Non-call expression encountered") end
    [distribute!(a) for a in ex.args[2:end]]
    ## check that top-level can be distributed
    a1 = ex.args[1]
    if a1 in keys(distributive)

        ## which op is being DISTRIBUTED (e.g. &, *)?
        distributed_op = a1
        ## which op is doing the distributing (e.g. +)?
        distributing_op = distributive[a1]

        ## detect distributing sub-expression (first arg is, e.g. :+)
        is_distributing_subex(e) =
            typeof(e)==Expr && e.head == :call && e.args[1] == distributing_op

        ## find first distributing subex
        first_distributing_subex = findfirst(is_distributing_subex, ex.args)
        if first_distributing_subex != 0
            ## remove distributing subexpression from args
            subex = splice!(ex.args, first_distributing_subex)

            newargs = Any[distributing_op]
            ## generate one new sub-expression, which calls the distributed operation
            ## (e.g. &) on each of the distributing sub-expression's arguments, plus
            ## the non-distributed arguments of the original expression.
            for a in subex.args[2:end]
                new_subex = copy(ex)
                push!(new_subex.args, a)
                ## need to recurse here, in case there are any other
                ## distributing operations in the sub expression
                distribute!(new_subex)
                push!(newargs, new_subex)
            end
            ex.args = newargs
        end
    end
    ex
end
distribute!(a::Any) = a

const associative = Set([:+,:*,:&])       # associative special operators

## If the expression is a call to the function s return its arguments
## Otherwise return the expression
function ex_or_args(ex::Expr,s::Symbol)
    if ex.head != :call error("Non-call expression encountered") end
    if ex.args[1] == s
        ## recurse in case there are more :calls of s below
        return vcat(map(x -> ex_or_args(x, s), ex.args[2:end])...)
    else
        ## not a :call to s, return condensed version of ex
        return condense(ex)
    end
end
ex_or_args(a,s::Symbol) = a

## Condense calls like :(+(a,+(b,c))) to :(+(a,b,c))
function condense(ex::Expr)
    if ex.head != :call error("Non-call expression encountered") end
    a1 = ex.args[1]
    if !(a1 in associative) return ex end
    excp = copy(ex)
    excp.args = vcat(a1, map(x->ex_or_args(x,a1), ex.args[2:end])...)
    excp
end
condense(a::Any) = a

## always return an ARRAY of terms
getterms(ex::Expr) = (ex.head == :call && ex.args[1] == :+) ? ex.args[2:end] : Expr[ex]
getterms(a::Any) = Any[a]

ord(ex::Expr) = (ex.head == :call && ex.args[1] == :&) ? length(ex.args)-1 : 1
ord(a::Any) = 1

const nonevaluation = Set([:&,:|])        # operators constructed from other evaluations
## evaluation terms - the (filtered) arguments for :& and :|, otherwise the term itself
function evt(ex::Expr)
    if ex.head != :call error("Non-call expression encountered") end
    if !(ex.args[1] in nonevaluation) return ex end
    filter(x->!isa(x,Number), vcat(map(getterms, ex.args[2:end])...))
end
evt(a) = Any[a]

function Terms(f::Formula)
    rhs = condense(distribute(dospecials(f.rhs)))
    tt = unique(getterms(rhs))
    tt = tt[!(tt .== 1)]             # drop any explicit 1's
    noint = (tt .== 0) | (tt .== -1) # should also handle :(-(expr,1))
    tt = tt[!noint]
    oo = Int[ord(t) for t in tt]     # orders of interaction terms
    if !issorted(oo)                 # sort terms by increasing order
        pp = sortperm(oo)
        tt = tt[pp]
        oo = oo[pp]
    end
    etrms = map(evt, tt)
    haslhs = f.lhs != nothing
    if haslhs
        unshift!(etrms, Any[f.lhs])
        unshift!(oo, 1)
    end
    ev = unique(vcat(etrms...))
    sets = [Set(x) for x in etrms]
    facs = Int8[t in s for t in ev, s in sets]
    Terms(tt, ev, facs, oo, haslhs, !any(noint))
end

function remove_response(t::Terms)
    # shallow copy original terms
    t = Terms(t.terms, t.eterms, t.factors, t.order, t.response, t.intercept)
    if t.response
        t.order = t.order[2:end]
        t.eterms = t.eterms[2:end]
        t.factors = t.factors[2:end, 2:end]
        t.response = false
    end
    return t
end

## Default NA handler.  Others can be added as keyword arguments
function na_omit(df::DataFrame)
    cc = complete_cases(df)
    df[cc,:], cc
end

## Trim the pool field of da to only those levels that occur in the refs
function dropUnusedLevels!(da::PooledDataArray)
    rr = da.refs
    uu = unique(rr)
    length(uu) == length(da.pool) && return da
    T = eltype(rr)
    su = sort!(uu)
    dict = Dict(zip(su, one(T):convert(T, length(uu))))
    da.refs = map(x -> dict[x], rr)
    da.pool = da.pool[uu]
    da
end
dropUnusedLevels!(x) = x

function ModelFrame(trms::Terms, d::AbstractDataFrame)
    df, msng = na_omit(DataFrame(map(x -> d[x], trms.eterms)))
    names!(df, convert(Vector{Symbol}, map(string, trms.eterms)))
    for c in eachcol(df) dropUnusedLevels!(c[2]) end
    ModelFrame(df, trms, msng)
end

ModelFrame(f::Formula, d::AbstractDataFrame) = ModelFrame(Terms(f), d)
ModelFrame(ex::Expr, d::AbstractDataFrame) = ModelFrame(Formula(ex), d)

function StatsBase.model_response(mf::ModelFrame)
    mf.terms.response || error("Model formula one-sided")
    convert(Array, mf.df[round(Bool, mf.terms.factors[:, 1])][:, 1])
end

function contr_treatment(n::Integer, contrasts::Bool, sparse::Bool, base::Integer)
    if n < 2 error("not enought degrees of freedom to define contrasts") end
    contr = sparse ? speye(n) : eye(n) .== 1.
    if !contrasts return contr end
    if !(1 <= base <= n) error("base = $base is not allowed for n = $n") end
    contr[:,vcat(1:(base-1),(base+1):end)]
end
contr_treatment(n::Integer,contrasts::Bool,sparse::Bool) = contr_treatment(n,contrasts,sparse,1)
contr_treatment(n::Integer,contrasts::Bool) = contr_treatment(n,contrasts,false,1)
contr_treatment(n::Integer) = contr_treatment(n,true,false,1)
cols(v::PooledDataVector) = contr_treatment(length(v.pool))[v.refs,:]
cols(v::DataVector) = convert(Vector{Float64}, v.data)
cols(v::Vector) = convert(Vector{Float64}, v)

function isfe(ex::Expr)                 # true for fixed-effects terms
    if ex.head != :call error("Non-call expression encountered") end
    ex.args[1] != :|
end
isfe(a) = true

function expandcols(trm::Vector)
    if length(trm) == 1
        return convert(Array{Float64}, trm[1])
    else
        a = convert(Array{Float64}, trm[1])
        b = expandcols(trm[2:end])
        nca = size(a, 2)
        ncb = size(b, 2)
        return hcat([a[:, i] .* b[:, j] for i in 1:nca, j in 1:ncb]...)
    end
end

function nc(trm::Vector)
    isempty(trm) && return 0
    n = 1
    for x in trm
        n *= size(x, 2)
    end
    n
end

function alignpool{T,Rx,Ry,N}(x::PooledDataArray{T, Rx, N}, y::PooledDataArray{T, Ry, N})
    if x.pool == y.pool
        return x
    end

    xi = DataFrame(pool=x.pool, xi=1:length(x.pool))
    yi = DataFrame(pool=y.pool, yi=1:length(y.pool))
    d = join(xi, yi, on=:pool, kind=:left)

    # Validate that x has support in y
    i = findfirst(isna(d[:yi]))
    if i>0
        if length(y.pool)<10
            error("Unknown level: ", d[i,:pool], ". Expected one of: ", y.pool)
        else
            error("Unknown level: ", d[i,:pool], ". Expected one of ", length(y.pool), " levels in reference dataframe.")
        end
    end

    newrefs = Array{Ry}(size(x.refs)...)
    for r in eachrow(d)
        newrefs[x.refs .== r[:xi]] = r[:yi]
    end

    PooledDataArray(DataArrays.RefArray(newrefs), y.pool)
end

function alignpool(x::DataArray, ::DataArray)
    return x
end

function ModelMatrix(mf::ModelFrame, referece_df = mf.df)
    trms = mf.terms
    aa = Any[Any[ones(size(mf.df,1), @compat(Int(trms.intercept)))]]
    asgn = zeros(Int, @compat(Int(trms.intercept)))
    fetrms = Bool[isfe(t) for t in trms.terms]
    if trms.response unshift!(fetrms, false) end
    ff = trms.factors[:, fetrms]

    # need to use the same levels in predictions as for regression
    for n in trms.eterms
        mf.df[n] = alignpool(mf.df[n], referece_df[n])
    end

    ## need to be cautious here to avoid evaluating cols for a factor with many levels
    ## if the factor doesn't occur in the fetrms
    rows = Bool[x != 0 for x in sum(ff, 2)]
    ff = ff[rows, :]
    cc = [cols(col) for col in columns(mf.df[:, rows])]
    for j in 1:size(ff,2)
        trm = cc[round(Bool, ff[:, j])]
        push!(aa, trm)
        asgn = vcat(asgn, fill(j, nc(trm)))
    end
    ModelMatrix{Float64}(hcat([expandcols(t) for t in aa]...), asgn)
end

termnames(term::Symbol, col) = [string(term)]
function termnames(term::Symbol, col::PooledDataArray)
    levs = levels(col)
    [string(term, " - ", levs[i]) for i in 2:length(levs)]
end

function coefnames(mf::ModelFrame)
    if mf.terms.intercept
        vnames = UTF8String["(Intercept)"]
    else
        vnames = UTF8String[]
    end
    # Need to only include active levels
    for term in mf.terms.terms
        if isa(term, Expr)
            if term.head == :call && term.args[1] == :|
                continue                # skip random-effects terms
            elseif term.head == :call && term.args[1] == :&
                ## for an interaction term, combine term names pairwise,
                ## starting with rightmost terms
                append!(vnames,
                        foldr((a,b) ->
                              vec([string(lev1, " & ", lev2) for
                                   lev1 in a,
                                   lev2 in b]),
                              map(x -> termnames(x, mf.df[x]), term.args[2:end])))
            else
                error("unrecognized term $term")
            end
        else
            append!(vnames, termnames(term, mf.df[term]))
        end
    end
    return vnames
end

function Formula(t::Terms)
    lhs = t.response ? t.eterms[1] : nothing
    rhs = Expr(:call,:+)
    if t.intercept
        push!(rhs.args,1)
    end
    append!(rhs.args,t.terms)
    Formula(lhs,rhs)
end
