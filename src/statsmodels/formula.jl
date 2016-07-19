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
    factors::Matrix{Bool} # maps terms to evaluation terms
# order can probably be dropped.  It is vec(sum(factors, 1))
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
    facs = Bool[t in s for t in ev, s in sets]
    Terms(tt, ev, facs, oo, haslhs, !any(noint))
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

asMatrix(a::AbstractMatrix) = a
asMatrix(v::AbstractVector) = reshape(v, (length(v), 1))

"""
    StatsBase.model_response(mf::ModelFrame)
Extract the response column, if present.  `DataVector` or
`PooledDataVector` columns are converted to `Array`s
"""
function StatsBase.model_response(mf::ModelFrame)
    if mf.terms.response
        convert(Array, mf.df[mf.terms.eterms[1]])
    else
        error("Model formula one-sided")
    end
end

"""
    contr_treatment(n::Integer, contrasts::Bool, sparse::Bool, base::Integer)
Create a sparse or dense identity of size `n`.  Return the identity if `contrasts`
is false.  Otherwise drop the `base` column.
"""
function contr_treatment(n::Integer, contrasts::Bool, sparse::Bool, base::Integer)
    if n < 2 error("not enought degrees of freedom to define contrasts") end
    contr = sparse ? speye(n) : eye(n) .== 1.
    if !contrasts
        contr
    elseif !(1 <= base <= n)
        error("base = $base is not allowed for n = $n")
    else
        contr[:, vcat(1 : (base-1), (base+1) : end)]
    end
end
contr_treatment(n::Integer,contrasts::Bool,sparse::Bool) = contr_treatment(n,contrasts,sparse,1)
contr_treatment(n::Integer,contrasts::Bool) = contr_treatment(n,contrasts,false,1)
contr_treatment(n::Integer) = contr_treatment(n,true,false,1)
cols(v::PooledDataVector) = contr_treatment(length(v.pool))[v.refs, :]
cols(v::DataVector) = asMatrix(convert(Vector{Float64}, v.data))
cols(v::Vector) = asMatrix(convert(Vector{Float64}, v))

"""
    expandcols(trm::Vector)
Create pairwise products of columns from a vector of matrices
"""
function expandcols(trm::Vector)
    if length(trm) == 1
        asMatrix(convert(Array{Float64}, trm[1]))
    else
        a = convert(Array{Float64}, trm[1])
        b = expandcols(trm[2 : end])
        reduce(hcat, [broadcast(*, a, view(b, :, j)) for j in 1 : size(b, 2)])
    end
end

"""
    dropRanefTerms(trms::Terms)
Expressions of the form `(a|b)` are "random-effects" terms and are not
incorporated in the ModelMatrix.  This function checks for such terms and,
if any are present, drops them from the `Terms` object.
"""
function dropRanefTerms(trms::Terms)
    retrms = Bool[Meta.isexpr(t, :call) && t.args[1] == :| for t in trms.terms]
    if !any(retrms)  # return trms unchanged
        trms
    elseif all(retrms) && !trms.response   # return an empty Terms object
        Terms(Any[],Any[],Array(Bool, (0,0)), Int[], false, trms.intercept)
    else
        # the rows of `trms.factors` correspond to `eterms`, the columns to `terms`
        # After dropping random-effects terms we drop any eterms whose rows are all false
        ckeep = !retrms                 # columns to retain
        facs = trms.factors[:, ckeep]
        rkeep = vec(sum(facs, 2) .> 0)
        Terms(trms.terms[ckeep], trms.eterms[rkeep], facs[rkeep, :],
            trms.order[ckeep], trms.response, trms.intercept)
    end
end

"""
    dropResponse(trms::Terms)
Drop the response term, `trms.eterms[1]` and the first row and column
of `trms.factors` if `trms.response` is true.
"""
dropResponse(trms::Terms) = !trms.response ? trms :
    Terms(trms.terms, trms.eterms[2 : end], trms.factors[2 : end, 2 : end],
        trms.order[2 : end], false, trms.intercept)


"""
    ModelMatrix(mf::ModelFrame)
Create a `ModelMatrix` from the `terms` and `df` members of `mf`

This is basically a map-reduce where terms are mapped to columns by `cols`
and reduced by `hcat`.  During the collection of the columns the `assign`
vector is created.  `assign` maps columns of the model matrix to terms in
the model frame.  It can also be considered as mapping coefficients to
terms and, hence, to names.

If there is an intercept in the model, that column occurs first and its
`assign` value is zero.

Mixed-effects models include "random-effects" terms which are ignored when
creating the model matrix.
"""
function ModelMatrix(mf::ModelFrame)
    dfrm = mf.df
    terms = dropRanefTerms(dropResponse(mf.terms))
    columns = [cols(dfrm[e]) for e in terms.eterms]
    blocks = Matrix{Float64}[]
    assign = Int[]
    if terms.intercept
        push!(blocks, ones(size(dfrm, 1), 1))  # columns of 1's is first block
        push!(assign, 0)                        # this block corresponds to term zero
    end
    factors = terms.factors
    for j in 1 : size(factors, 2)
        bb = expandcols(columns[view(factors, :, j)])
        push!(blocks, bb)
        append!(assign, fill(j, size(bb, 2)))
    end
    ModelMatrix{Float64}(reduce(hcat, blocks), assign)
end

"""
    termnames(term::Symbol, col)
Returns a vector of strings with the names of the coefficients
associated with a term.  If the column corresponding to the term
is not a `PooledDataArray` a one-element vector is returned.
"""
termnames(term::Symbol, col) = [string(term)]
function termnames(term::Symbol, col::PooledDataArray)
    levs = levels(col)
    [string(term, " - ", levs[i]) for i in 2:length(levs)]
end
# FIXME: Only handles treatment contrasts and PooledDataArray.

"""
    coefnames(mf::ModelFrame)
Returns a vector of coefficient names constructed from the Terms
member and the types of the evaluation columns.
"""
function coefnames(mf::ModelFrame)
    if mf.terms.intercept
        vnames = Compat.UTF8String["(Intercept)"]
    else
        vnames = Compat.UTF8String[]
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
