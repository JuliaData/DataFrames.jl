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
    ## mapping from df keys to contrasts.  Rather than specify types allowed,
    ## leave that to modelmat_cols() to check.  Allows more seamless later extension
    contrasts::Dict{Any, Any}
    ## An eterms x terms matrix which is true for terms that need to be "promoted"
    ## to full rank in constructing a model matrx
    non_redundant_terms::Matrix{Bool}
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

## whether or not a column of a particular type can be "promoted" to full rank
## (only true of factors)
can_promote(::PooledDataArray) = true
can_promote(::Any) = false

## Check for non-redundancy of columns.  For instance, if x is a factor with two
## levels, it should be expanded into two columns in y~0+x but only one column
## in y~1+x.  The default is the rank-reduced form (contrasts for n levels only
## produce n-1 columns).  In general, an evaluation term x within a term
## x&y&... needs to be "promoted" to full rank if y&... hasn't already been
## included (either explicitly in the Terms or implicitly by promoting another
## term like z in z&y&...).
##
## This function returns a boolean matrix that says whether each evaluation term
## of each term needs to be promoted.
function check_non_redundancy(trms::Terms, df::AbstractDataFrame)

    ## This can be checked using the .factors field of the terms, which is an
    ## evaluation terms x terms matrix.
    (n_eterms, n_terms) = size(trms.factors)
    to_promote = falses(n_eterms, n_terms)
    encountered_columns = Vector{eltype(trms.factors)}[]

    if trms.intercept
        push!(encountered_columns, zeros(eltype(trms.factors), n_eterms))
    end

    for i_term in 1:n_terms
        for i_eterm in 1:n_eterms
            ## only need to check eterms that are included and can be promoted
            ## (e.g., categorical variables that expand to multiple mm columns)
            if round(Bool, trms.factors[i_eterm, i_term]) && can_promote(df[trms.eterms[i_eterm]])
                dropped = trms.factors[:,i_term]
                dropped[i_eterm] = 0
                ## short circuiting check for whether the version of this term
                ## with the current eterm dropped has been encountered already
                ## (and hence does not need to be expanded
                is_redundant = false
                for enc in encountered_columns
                    if dropped == enc
                        is_redundant = true
                        break
                    end
                end
                ## more concisely, but with non-short-circuiting any:
                ##is_redundant = any([dropped == enc for enc in encountered_columns])

                if !is_redundant
                    to_promote[i_eterm, i_term] = true
                    push!(encountered_columns, dropped)
                end

            end
            ## once we've checked all the eterms in this term, add it to the list
            ## of encountered terms/columns
        end
        push!(encountered_columns, trms.factors[:, i_term])
    end

    return to_promote
    
end

## Goal here is to allow specification of _either_ a "naked" contrast type,
## or an instantiated contrast object itself.  This might be achieved in a more
## julian way by overloading call for c::AbstractContrasts to just return c.
evaluateContrasts(c::AbstractContrasts, col::AbstractDataVector) = ContrastsMatrix(c, col)
evaluateContrasts{C <: AbstractContrasts}(c::Type{C}, col::AbstractDataVector) = ContrastsMatrix(c(), col)
evaluateContrasts(c::ContrastsMatrix, col::AbstractDataVector) = c

needsContrasts(::PooledDataArray) = true
needsContrasts(::Any) = false

function ModelFrame(trms::Terms, d::AbstractDataFrame;
                    contrasts::Dict = Dict())
    df, msng = na_omit(DataFrame(map(x -> d[x], trms.eterms)))
    names!(df, convert(Vector{Symbol}, map(string, trms.eterms)))
    for c in eachcol(df) dropUnusedLevels!(c[2]) end

    ## Set up contrasts: 
    ## Combine actual DF columns and contrast types if necessary to compute the
    ## actual contrasts matrices, levels, and term names (using TreatmentContrasts
    ## as the default)
    evaledContrasts = Dict()
    for (term, col) in eachcol(df)
        needsContrasts(col) || continue
        evaledContrasts[term] = evaluateContrasts(haskey(contrasts, term) ?
                                                  contrasts[term] :
                                                  TreatmentContrasts,
                                                  col)
    end

    ## Check whether or not
    non_redundants = check_non_redundancy(trms, df)

    ModelFrame(df, trms, msng, evaledContrasts, non_redundants)
end

ModelFrame(f::Formula, d::AbstractDataFrame; kwargs...) = ModelFrame(Terms(f), d; kwargs...)
ModelFrame(ex::Expr, d::AbstractDataFrame; kwargs...) = ModelFrame(Formula(ex), d; kwargs...)

## modify contrasts in place
function setcontrasts!(mf::ModelFrame, new_contrasts::Dict)
    new_contrasts = [ col => evaluateContrasts(contr, mf.df[col])
                      for (col, contr) in filter((k,v)->haskey(mf.df, k), new_contrasts) ]
                      
    mf.contrasts = merge(mf.contrasts, new_contrasts)
    return mf
end
setcontrasts!(mf::ModelFrame; kwargs...) = setcontrasts!(mf, Dict(kwargs))

function StatsBase.model_response(mf::ModelFrame)
    mf.terms.response || error("Model formula one-sided")
    convert(Array, mf.df[round(Bool, mf.terms.factors[:, 1])][:, 1])
end

modelmat_cols(v::DataVector) = convert(Vector{Float64}, v.data)
modelmat_cols(v::Vector) = convert(Vector{Float64}, v)
## construct model matrix columns from model frame + name (checks for contrasts)
function modelmat_cols(name::Symbol, mf::ModelFrame; non_redundant::Bool = false)
    if haskey(mf.contrasts, name)
        modelmat_cols(mf.df[name],
             non_redundant ? promote_contrast(mf.contrasts[name]) : mf.contrasts[name])
    else
        modelmat_cols(mf.df[name])
    end
end

"""
    modelmat_cols(v::PooledDataVector, contrast::ContrastsMatrix)

Construct `ModelMatrix` columns based on specified contrasts, ensuring that
levels align properly.
"""
function modelmat_cols(v::PooledDataVector, contrast::ContrastsMatrix)
    ## make sure the levels of the contrast matrix and the categorical data
    ## are the same by constructing a re-indexing vector. Indexing into
    ## reindex with v.refs will give the corresponding row number of the
    ## contrast matrix
    reindex = [findfirst(contrast.levels, l) for l in levels(v)]
    return contrast.matrix[reindex[v.refs], :]
end


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

function ModelMatrix(mf::ModelFrame)
    ## TODO: this method makes multiple copies of the data in the ModelFrame:
    ## first in term_cols (1-2x per evaluation term, depending on redundancy),
    ## second in constructing the matrix itself.
    
    ## Map eval. term name + redundancy bool to cached model matrix columns
    eterm_cols = @compat Dict{Tuple{Symbol,Bool}, Array{Float64}}()
    ## Accumulator for each term's vector of eval. term columns.
    mm_cols = Vector{Array{Float64}}[]
    mf.terms.intercept && push!(mm_cols, Matrix{Float64}[ones(Float64, size(mf.df,1), 1)])
    factors = round(Bool, mf.terms.factors)

    ## turn each term into a vector of mm columns for its eval. terms, using
    ## "promoted" full-rank versions of categorical columns for non-redundant
    ## eval. terms:
    for (i_term, term) in enumerate(mf.terms.terms)
        ## Skip non-fixed-effect terms
        isfe(term) || continue
        term_cols = Array{Float64}[]
        ## adjust term index if there's a response in the formula (mf.terms.terms
        ## lists only non-response terms, but mf.terms.factors and .non_reundant_terms
        ## has first column for the response if it's present)
        i_term += mf.terms.response
        ## Pull out the eval terms, and the non-redundancy flags for this term
        eterms = mf.terms.eterms[factors[:, i_term]]
        non_redundant = mf.non_redundant_terms[factors[:, i_term], i_term]
        ## Get cols for each eval term (either previously generated, or generating
        ## and storing as necessary)
        for et_and_nr in zip(eterms, non_redundant)
            haskey(eterm_cols, et_and_nr) || 
                setindex!(eterm_cols,
                          modelmat_cols(et_and_nr[1], mf, non_redundant=et_and_nr[2]),
                          et_and_nr)
            push!(term_cols, eterm_cols[et_and_nr])
        end
        push!(mm_cols, term_cols)
    end

    ## TODO: could this be made more efficient by
    ## first computing mm_col_term_nums, initializing mm, and directly indexing?
    mm = hcat([expandcols(tc) for tc in mm_cols]...)
    mm_col_term_nums = vcat([fill(i_term, nc(tc)) for (i_term,tc) in enumerate(mm_cols)]...)

    ModelMatrix{Float64}(mm, mm_col_term_nums)

end



termnames(term::Symbol, col) = [string(term)]
function termnames(term::Symbol, mf::ModelFrame; non_redundant::Bool = false)
    if haskey(mf.contrasts, term)
        termnames(term, mf.df[term], 
             non_redundant ? promote_contrast(mf.contrasts[term]) : mf.contrasts[term])
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

function coefnames(mf::ModelFrame)
    ## strategy mirrors ModelMatrx constructor:
    eterm_names = @compat Dict{Tuple{Symbol,Bool}, Vector{Compat.UTF8String}}()
    term_names = Vector{Vector{Compat.UTF8String}}[]
    mf.terms.intercept && push!(term_names, Vector[Compat.UTF8String["(Intercept)"]])

    factors = round(Bool, mf.terms.factors)

    for (i_term, term) in enumerate(mf.terms.terms)
        isfe(term) || continue
        ## names for columns for eval terms
        names = Vector{Compat.UTF8String}[]

        i_term += mf.terms.response
        eterms = mf.terms.eterms[factors[:, i_term]]
        non_redundant = mf.non_redundant_terms[factors[:, i_term], i_term]

        for et_and_nr in zip(eterms, non_redundant)
            haskey(eterm_names, et_and_nr) ||
            setindex!(eterm_names,
                      termnames(et_and_nr[1], mf, non_redundant=et_and_nr[2]),
                      et_and_nr)
            push!(names, eterm_names[et_and_nr])
        end
        push!(term_names, names)
    end

    mapreduce(expandtermnames, vcat, term_names)
    
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
