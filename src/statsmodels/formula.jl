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

#
# TODO: implement contrast types in Formula/Terms
#
type Terms
    terms::Vector
    eterms::Vector        # evaluation terms
    factors::Matrix{Bool} # maps terms to evaluation terms
    ## An eterms x terms matrix which is true for terms that need to be "promoted"
    ## to full rank in constructing a model matrx
    is_non_redundant::Matrix{Bool}
# order can probably be dropped.  It is vec(sum(factors, 1))
    order::Vector{Int}    # orders of rhs terms
    response::Bool        # indicator of a response, which is eterms[1] if present
    intercept::Bool       # is there an intercept column in the model matrix?
end

type ModelFrame
    df::AbstractDataFrame
    terms::Terms
    msng::BitArray
    ## mapping from df keys to contrasts matrices
    contrasts::Dict{Symbol, ContrastsMatrix}
end

typealias AbstractFloatMatrix{T<:AbstractFloat} AbstractMatrix{T}

type ModelMatrix{T <: AbstractFloatMatrix}
    m::T
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
    non_redundants = fill(false, size(facs)) # initialize to false
    Terms(tt, ev, facs, non_redundants, oo, haslhs, !any(noint))
end

## Default NA handler.  Others can be added as keyword arguments
function na_omit(df::DataFrame)
    cc = complete_cases(df)
    df[cc,:], cc
end

## Trim the pool field of da to only those levels that occur in the refs
function dropunusedlevels!(da::PooledDataArray)
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
dropunusedlevels!(x) = x

is_categorical(::PooledDataArray) = true
is_categorical(::Any) = false

## Check for non-redundancy of columns.  For instance, if x is a factor with two
## levels, it should be expanded into two columns in y~0+x but only one column
## in y~1+x.  The default is the rank-reduced form (contrasts for n levels only
## produce n-1 columns).  In general, an evaluation term x within a term
## x&y&... needs to be "promoted" to full rank if y&... hasn't already been
## included (either explicitly in the Terms or implicitly by promoting another
## term like z in z&y&...).
##
## This modifies the Terms, setting `trms.is_non_redundant = true` for all non-
## redundant evaluation terms.
function check_non_redundancy!(trms::Terms, df::AbstractDataFrame)

    (n_eterms, n_terms) = size(trms.factors)

    encountered_columns = Vector{eltype(trms.factors)}[]

    if trms.intercept
        push!(encountered_columns, zeros(eltype(trms.factors), n_eterms))
    end

    for i_term in 1:n_terms
        for i_eterm in 1:n_eterms
            ## only need to check eterms that are included and can be promoted
            ## (e.g., categorical variables that expand to multiple mm columns)
            if Bool(trms.factors[i_eterm, i_term]) && is_categorical(df[trms.eterms[i_eterm]])
                dropped = trms.factors[:,i_term]
                dropped[i_eterm] = 0

                if findfirst(encountered_columns, dropped) == 0
                    trms.is_non_redundant[i_eterm, i_term] = true
                    push!(encountered_columns, dropped)
                end

            end
        end
        ## once we've checked all the eterms in this term, add it to the list
        ## of encountered terms/columns
        push!(encountered_columns, Compat.view(trms.factors, :, i_term))
    end

    return trms.is_non_redundant
end


const DEFAULT_CONTRASTS = DummyCoding

## Set up contrasts:
## Combine actual DF columns and contrast types if necessary to compute the
## actual contrasts matrices, levels, and term names (using DummyCoding
## as the default)
function evalcontrasts(df::AbstractDataFrame, contrasts::Dict = Dict())
    evaledContrasts = Dict()
    for (term, col) in eachcol(df)
        is_categorical(col) || continue
        evaledContrasts[term] = ContrastsMatrix(haskey(contrasts, term) ?
                                                contrasts[term] :
                                                DEFAULT_CONTRASTS(),
                                                col)
    end
    return evaledContrasts
end

function ModelFrame(trms::Terms, d::AbstractDataFrame;
                    contrasts::Dict = Dict())
    df, msng = na_omit(DataFrame(map(x -> d[x], trms.eterms)))
    names!(df, convert(Vector{Symbol}, map(string, trms.eterms)))
    for c in eachcol(df) dropunusedlevels!(c[2]) end

    evaledContrasts = evalcontrasts(df, contrasts)

    ## Check for non-redundant terms, modifying terms in place
    check_non_redundancy!(trms, df)

    ModelFrame(df, trms, msng, evaledContrasts)
end

ModelFrame(df::AbstractDataFrame, term::Terms, msng::BitArray) = ModelFrame(df, term, msng, evalcontrasts(df))
ModelFrame(f::Formula, d::AbstractDataFrame; kwargs...) = ModelFrame(Terms(f), d; kwargs...)
ModelFrame(ex::Expr, d::AbstractDataFrame; kwargs...) = ModelFrame(Formula(ex), d; kwargs...)

## modify contrasts in place
function setcontrasts!(mf::ModelFrame, new_contrasts::Dict)
    new_contrasts = Dict([ Pair(col, ContrastsMatrix(contr, mf.df[col]))
                      for (col, contr) in filter((k,v)->haskey(mf.df, k), new_contrasts) ])

    mf.contrasts = merge(mf.contrasts, new_contrasts)
    return mf
end
setcontrasts!(mf::ModelFrame; kwargs...) = setcontrasts!(mf, Dict(kwargs))

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

## construct model matrix columns from model frame + name (checks for contrasts)
function modelmat_cols{T<:AbstractFloatMatrix}(::Type{T}, name::Symbol, mf::ModelFrame; non_redundant::Bool = false)
    if haskey(mf.contrasts, name)
        modelmat_cols(T, mf.df[name],
                      non_redundant ?
                      ContrastsMatrix{FullDummyCoding}(mf.contrasts[name]) :
                      mf.contrasts[name])
    else
        modelmat_cols(T, mf.df[name])
    end
end

modelmat_cols{T<:AbstractFloatMatrix}(::Type{T}, v::DataVector) = convert(T, reshape(v.data, length(v), 1))
modelmat_cols{T<:AbstractFloatMatrix}(::Type{T}, v::Vector) = convert(T, reshape(v, length(v), 1))

"""
    modelmat_cols{T<:AbstractFloatMatrix}(::Type{T}, v::PooledDataVector, contrast::ContrastsMatrix)

Construct `ModelMatrix` columns of type `T` based on specified contrasts, ensuring that
levels align properly.
"""
function modelmat_cols{T<:AbstractFloatMatrix}(::Type{T}, v::PooledDataVector, contrast::ContrastsMatrix)
    ## make sure the levels of the contrast matrix and the categorical data
    ## are the same by constructing a re-indexing vector. Indexing into
    ## reindex with v.refs will give the corresponding row number of the
    ## contrast matrix
    reindex = [findfirst(contrast.levels, l) for l in levels(v)]
    contrastmatrix = convert(T, contrast.matrix)
    return indexrows(contrastmatrix, reindex[v.refs])
end

indexrows(m::SparseMatrixCSC, ind::Vector{Int}) = m'[:, ind]'
indexrows(m::AbstractMatrix, ind::Vector{Int}) = m[ind, :]

"""
    expandcols{T<:AbstractFloatMatrix}(trm::Vector{T})
Create pairwise products of columns from a vector of matrices
"""
function expandcols{T<:AbstractFloatMatrix}(trm::Vector{T})
    if length(trm) == 1
        trm[1]
    else
        a = trm[1]
        b = expandcols(trm[2 : end])
        reduce(hcat, [broadcast(*, a, Compat.view(b, :, j)) for j in 1 : size(b, 2)])
    end
end

"""
    droprandomeffects(trms::Terms)
Expressions of the form `(a|b)` are "random-effects" terms and are not
incorporated in the ModelMatrix.  This function checks for such terms and,
if any are present, drops them from the `Terms` object.
"""
function droprandomeffects(trms::Terms)
    retrms = Bool[Meta.isexpr(t, :call) && t.args[1] == :| for t in trms.terms]
    if !any(retrms)  # return trms unchanged
        trms
    elseif all(retrms) && !trms.response   # return an empty Terms object
        Terms(Any[],Any[],Array(Bool, (0,0)),Array(Bool, (0,0)), Int[], false, trms.intercept)
    else
        # the rows of `trms.factors` correspond to `eterms`, the columns to `terms`
        # After dropping random-effects terms we drop any eterms whose rows are all false
        ckeep = !retrms                 # columns to retain
        facs = trms.factors[:, ckeep]
        rkeep = vec(sum(facs, 2) .> 0)
        Terms(trms.terms[ckeep], trms.eterms[rkeep], facs[rkeep, :],
              trms.is_non_redundant[rkeep, ckeep],
              trms.order[ckeep], trms.response, trms.intercept)
    end
end

"""
    dropresponse!(trms::Terms)
Drop the response term, `trms.eterms[1]` and the first row and column
of `trms.factors` if `trms.response` is true.
"""
function dropresponse!(trms::Terms)
    if trms.response
        ckeep = 2:size(trms.factors, 2)
        rkeep = vec(any(trms.factors[:, ckeep], 2))
        Terms(trms.terms, trms.eterms[rkeep], trms.factors[rkeep, ckeep],
              trms.is_non_redundant[rkeep, ckeep], trms.order[ckeep], false, trms.intercept)
    else
        trms
    end
end


"""
    ModelMatrix{T<:AbstractFloatMatrix}(mf::ModelFrame)
Create a `ModelMatrix` of type `T` (default `Matrix{Float64}`) from the
`terms` and `df` members of `mf`.

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
@compat function (::Type{ModelMatrix{T}}){T<:AbstractFloatMatrix}(mf::ModelFrame)
    dfrm = mf.df
    terms = droprandomeffects(dropresponse!(mf.terms))

    blocks = T[]
    assign = Int[]
    if terms.intercept
        push!(blocks, ones(size(dfrm, 1), 1))  # columns of 1's is first block
        push!(assign, 0)                       # this block corresponds to term zero
    end

    factors = terms.factors

    ## Map eval. term name + redundancy bool to cached model matrix columns
    eterm_cols = @compat Dict{Tuple{Symbol,Bool}, T}()
    ## Accumulator for each term's vector of eval. term columns.

    ## TODO: this method makes multiple copies of the data in the ModelFrame:
    ## first in term_cols (1-2x per evaluation term, depending on redundancy),
    ## second in constructing the matrix itself.

    ## turn each term into a vector of mm columns for its eval. terms, using
    ## "promoted" full-rank versions of categorical columns for non-redundant
    ## eval. terms:
    for (i_term, term) in enumerate(terms.terms)
        term_cols = T[]
        ## Pull out the eval terms, and the non-redundancy flags for this term
        ff = Compat.view(factors, :, i_term)
        eterms = Compat.view(terms.eterms, ff)
        non_redundants = Compat.view(terms.is_non_redundant, ff, i_term)
        ## Get cols for each eval term (either previously generated, or generating
        ## and storing as necessary)
        for (et, nr) in zip(eterms, non_redundants)
            if ! haskey(eterm_cols, (et, nr))
                eterm_cols[(et, nr)] = modelmat_cols(T, et, mf, non_redundant=nr)
            end
            push!(term_cols, eterm_cols[(et, nr)])
        end
        push!(blocks, expandcols(term_cols))
        append!(assign, fill(i_term, size(blocks[end], 2)))
    end

    if isempty(blocks)
        error("Could not construct model matrix. Resulting matrix has 0 columns.")
    end

    I = size(dfrm, 1)
    J = mapreduce(x -> size(x, 2), +, blocks)
    X = similar(blocks[1], I, J)
    i = 1
    for block in blocks
        len = size(block, 2)
        X[:, i:(i + len - 1)] = block
        i += len
    end
    ModelMatrix{T}(X, assign)
end
ModelMatrix(mf::ModelFrame) = ModelMatrix{Matrix{Float64}}(mf)


"""
    termnames(term::Symbol, col)
Returns a vector of strings with the names of the coefficients
associated with a term.  If the column corresponding to the term
is not a `PooledDataArray` a one-element vector is returned.
"""
termnames(term::Symbol, col) = [string(term)]
function termnames(term::Symbol, mf::ModelFrame; non_redundant::Bool = false)
    if haskey(mf.contrasts, term)
        termnames(term, mf.df[term],
                  non_redundant ?
                  ContrastsMatrix{FullDummyCoding}(mf.contrasts[term]) :
                  mf.contrasts[term])
    else
        termnames(term, mf.df[term])
    end
end

termnames(term::Symbol, col::Any, contrast::ContrastsMatrix) =
    ["$term: $name" for name in contrast.termnames]


function expandtermnames(term::Vector)
    if length(term) == 1
        return term[1]
    else
        return foldr((a,b) -> vec([string(lev1, " & ", lev2) for
                                   lev1 in a,
                                   lev2 in b]),
                     term)
    end
end


"""
    coefnames(mf::ModelFrame)
Returns a vector of coefficient names constructed from the Terms
member and the types of the evaluation columns.
"""
function coefnames(mf::ModelFrame)
    terms = droprandomeffects(dropresponse!(mf.terms))

    ## strategy mirrors ModelMatrx constructor:
    eterm_names = @compat Dict{Tuple{Symbol,Bool}, Vector{Compat.UTF8String}}()
    term_names = Vector{Compat.UTF8String}[]

    if terms.intercept
        push!(term_names, Compat.UTF8String["(Intercept)"])
    end

    factors = terms.factors

    for (i_term, term) in enumerate(terms.terms)

        ## names for columns for eval terms
        names = Vector{Compat.UTF8String}[]

        ff = Compat.view(factors, :, i_term)
        eterms = Compat.view(terms.eterms, ff)
        non_redundants = Compat.view(terms.is_non_redundant, ff, i_term)

        for (et, nr) in zip(eterms, non_redundants)
            if !haskey(eterm_names, (et, nr))
                eterm_names[(et, nr)] = termnames(et, mf, non_redundant=nr)
            end
            push!(names, eterm_names[(et, nr)])
        end
        push!(term_names, expandtermnames(names))
    end

    reduce(vcat, Vector{Compat.UTF8String}(), term_names)
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
