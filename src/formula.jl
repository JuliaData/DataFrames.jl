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
    lhs::Union(Symbol, Expr,Nothing)
    rhs::Union(Symbol, Expr, Integer)
end

type Terms
    terms::Vector
    eterms::Vector                    # evaluation terms
    factors::Matrix{Int8}             # maps terms to evaluation terms
    order::Vector{Int}                # orders of rhs terms
    response::Bool       # indicator of a response, which is eterms[1] if present
    intercept::Bool      # is there an intercept column in the model matrix?
end

type ModelFrame
    df::AbstractDataFrame
    terms::Terms
    msng::BitArray
end
    
type ModelMatrix
    m::Matrix{Float64}
    assign::Vector{Int}
end

function Formula(ex::Expr) 
    aa = ex.args
    if aa[1] != :~
        error("Invalid formula, top-level argument must be '~'.  Check parentheses.")
    end
    if length(aa) == 2 return Formula(nothing, aa[2]) end
    Formula(aa[2], aa[3])
end

function show(io::IO, f::Formula)
    print(io, string("Formula: ", f.lhs == nothing ? "" : f.lhs, " ~ ", f.rhs))
end

## Return, as a vector of symbols, the names of all the variables in
## an expression or a formula
function allvars(ex::Expr)
    if ex.head != :call error("Non-call expression encountered") end
    [[allvars(a) for a in ex.args[2:]]...]
end
allvars(f::Formula) = unique(vcat(allvars(f.rhs), allvars(f.lhs)))
allvars(sym::Symbol) = [sym]
allvars(v) = Array(Symbol,0)

const specials = Set(:+,:-,:*,:/,:&,:|,:^) # special operators in formulas

function dospecials(ex::Expr)
    if ex.head != :call error("Non-call expression encountered") end
    a1 = ex.args[1]
    if !has(specials, a1) return ex end
    excp = copy(ex)
    excp.args = vcat(a1,map(dospecials, ex.args[2:]))
    if a1 != :* return excp end
    aa = excp.args
    a2 = aa[2]
    a3 = aa[3]
    if length(aa) > 3
        excp.args = vcat(a1, aa[3:])
        a3 = dospecials(excp)
    end
    :($a2 + $a3 + $a2 & $a3)
end
dospecials(a) = a

const associative = Set(:+,:*,:&)       # associative special operators

## If the expression is a call to the function s return its arguments
## Otherwise return the expression
function ex_or_args(ex::Expr,s::Symbol)
    if ex.head != :call error("Non-call expression encountered") end
    excp = copy(ex)
    a1 = ex.args[1]
    a2 = map(condense, ex.args[2:])
    if a1 == s return a2 end
    excp.args = vcat(a1, a2)
    excp
end
ex_or_args(a,s::Symbol) = a

## Condense calls like :(+(a,+(b,c))) to :(+(a,b,c))
## Also need to work out how to distribute & over +
function condense(ex::Expr)
    if ex.head != :call error("Non-call expression encountered") end
    a1 = ex.args[1]
    if !has(associative, a1) return ex end
    excp = copy(ex)
    excp.args = vcat(a1, map(x->ex_or_args(x,a1), ex.args[2:])...)
    excp
end    
condense(a) = a

getterms(ex::Expr) = (ex.head == :call && ex.args[1] == :+) ? ex.args[2:] : ex
getterms(a) = a

ord(ex::Expr) = (ex.head == :call && ex.args[1] == :&) ? length(ex.args)-1 : 1
ord(a) = 1

const nonevaluation = Set(:&,:|)        # operators constructed from other evaluations
## evaluation terms - the (filtered) arguments for :& and :|, otherwise the term itself
function evt(ex::Expr)
    if ex.head != :call error("Non-call expression encountered") end
    if !has(nonevaluation, ex.args[1]) return ex end
    filter(x->!isa(x,Number), map(getterms, ex.args[2:]))
end
evt(a) = {a}
    
function Terms(f::Formula)
    rhs = condense(dospecials(f.rhs))
    tt = getterms(rhs)
    if !isa(tt,AbstractArray) tt = [tt] end
    tt = tt[!(tt .== 1)]             # drop any explicit 1's
    noint = (tt .== 0) | (tt .== -1) # should also handle :(-(expr,1))
    tt = tt[!noint]
    oo = int(map(ord, tt))           # orders of interaction terms
    if !issorted(oo)                 # sort terms by increasing order
        pp = sortperm(oo)
        tt = tt[pp]
        oo = oo[pp]
    end
    etrms = map(evt, tt)
    haslhs = f.lhs != nothing
    if haslhs
        unshift!(etrms, {f.lhs})
        unshift!(oo, 1)
    end
    ev = unique(vcat(etrms...))
    facs = int8(hcat(map(x->(s=Set(x...);map(t->int8(has(s,t)), ev)),etrms)...))
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
    if length(uu) == length(da.pool) return da end
    T = eltype(rr)
    su = sort!(uu)
    dict = Dict(su, one(T):convert(T,length(uu)))
    da.refs = map(x->dict[x], rr)
    da.pool = da.pool[uu]
    da
end
dropUnusedLevels!(x) = x

function ModelFrame(f::Formula, d::AbstractDataFrame)
    trms = Terms(f)
    df,msng = na_omit(DataFrame(map(x->with(d,x),trms.eterms)))
    colnames!(df, map(string, trms.eterms))
    for c in df dropUnusedLevels!(c[2]) end
    ModelFrame(df, trms, msng)
end
ModelFrame(ex::Expr, d::AbstractDataFrame) = ModelFrame(Formula(ex), d)

function model_response(mf::ModelFrame)
    if !mf.terms.response
        error("Formula for the model frame was a one-sided formula")
    end
    mf.df[bool(mf.terms.factors[:,1])][:,1]
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
cols(v::DataVector) = reshape(float64(v.data), (length(v),1))

function isfe(ex::Expr)                 # true for fixed-effects terms
    if ex.head != :call error("Non-call expression encountered") end
    ex.args[1] != :|
end
isfe(a) = true

## Expand the columns in an interaction term
function expandcols(trm::Vector)
    if length(trm) == 1 return float64(trm[1]) end
    if length(trm) == 2
        a = float64(trm[1])
        b = float64(trm[2])
        nca = size(a,2)
        ncb = size(b,2)
        return hcat([a[:,i].*b[:,j] for i in 1:nca, j in 1:ncb]...)
    end
    error("code for 3rd and higher order interactions not yet written")
end

nc(trm::Vector) = *([size(x,2) for x in trm]...)

function ModelMatrix(mf::ModelFrame)
    trms = mf.terms
    aa = {{ones(size(mf.df,1),int(trms.intercept))}}
    asgn = zeros(Int, (int(trms.intercept)))
    fetrms = bool(map(isfe, trms.terms))
    if trms.response unshift!(fetrms,false) end
    ff = trms.factors[:,fetrms]
    ## need to be cautious here to avoid evaluating cols for a factor with many levels
    ## if the factor doesn't occur in the fetrms
    rows = vec(bool(sum(ff,[2])))
    ff = ff[rows,:]
    cc = [cols(x[2]) for x in mf.df[:,rows]]
    for j in 1:size(ff,2)
        trm = cc[bool(ff[:,j])]
        push!(aa, trm)
        asgn = vcat(asgn, fill(j, nc(trm)))
    end
    ModelMatrix(hcat([expandcols(t) for t in aa]...), asgn)
end

model_frame(f::Formula,d::AbstractDataFrame) = ModelFrame(f,d)
model_matrix(mf::ModelFrame) = ModelMatrix(mf)

# Expand dummy variables and equations
## function model_matrix(mf::ModelFrame)
##     ex = mf.formula.rhs[1]
##     # BUG: complete_cases doesn't preserve grouped columns
##     df = mf.df#[complete_cases(mf.df),1:ncol(mf.df)]  
##     rdf = df[mf.y_indexes]
##     mdf = expand(ex, df)

##     # TODO: Convert to Array{Float64} in a cleaner way
##     rnames = colnames(rdf)
##     mnames = colnames(mdf)
##     r = Array(Float64,nrow(rdf),ncol(rdf))
##     m = Array(Float64,nrow(mdf),ncol(mdf))
##     for i = 1:nrow(rdf)
##       for j = 1:ncol(rdf)
##         r[i,j] = float(rdf[i,j])
##       end
##       for j = 1:ncol(mdf)
##         m[i,j] = float(mdf[i,j])
##       end
##     end
    
##     include_intercept = true
##     if include_intercept
##       m = hcat(ones(nrow(mdf)), m)
##       unshift!(mnames, "(Intercept)")
##     end

##     ModelMatrix(m, r, mnames, rnames)
##     ## mnames = {}
##     ## rnames = {}
##     ## for c in 1:ncol(rdf)
##     ##   r = hcat(r, float(rdf[c]))
##     ##   push!(rnames, colnames(rdf)[c])
##     ## end
##     ## for c in 1:ncol(mdf)
##     ##   m = hcat(m, mdf[c])
##     ##   push!(mnames, colnames(mdf)[c])
##     ## end
## end

## model_matrix(f::Formula, d::AbstractDataFrame) = model_matrix(ModelFrame(f, d))
## model_matrix(ex::Expr, d::AbstractDataFrame) = model_matrix(ModelFrame(Formula(ex), d))

## # TODO: Make a more general version of these functions
## # TODO: Be able to extract information about each column name
## function interaction_design_matrix(a::AbstractDataFrame, b::AbstractDataFrame)
##    cols = {}
##    col_names = Array(ASCIIString,0)
##    for i in 1:ncol(a)
##        for j in 1:ncol(b)
##           push!(cols, DataArray(a[:,i] .* b[:,j]))
##           push!(col_names, string(colnames(a)[i],"&",colnames(b)[j]))
##        end
##    end
##    DataFrame(cols, col_names)
## end

## function interaction_design_matrix(a::AbstractDataFrame, b::AbstractDataFrame, c::AbstractDataFrame)
##    cols = {}
##    col_names = Array(ASCIIString,0)
##    for i in 1:ncol(a)
##        for j in 1:ncol(b)
##            for k in 1:ncol(b)
##               push!(cols, DataArray(a[:,i] .* b[:,j] .* c[:,k]))
##               push!(col_names, string(colnames(a)[i],"&",colnames(b)[j],"&",colnames(c)[k]))
##            end
##        end
##    end
##    DataFrame(cols, col_names)
## end

## # Temporary: Manually describe the interactions needed for DataFrame Array.
## function all_interactions(dfs::Array{Any,1})
##     d = DataFrame()
##     if length(dfs) == 2
##       combos = ([1,2],)
##     elseif length(dfs) == 3
##       combos = ([1,2], [1,3], [2,3], [1,2,3])
##     else
##       error("interactions with more than 3 terms not implemented (yet)")
##     end
##     for combo in combos
##        if length(combo) == 2
##          a = interaction_design_matrix(dfs[combo[1]],dfs[combo[2]])
##        elseif length(combo) == 3
##          a = interaction_design_matrix(dfs[combo[1]],dfs[combo[2]],dfs[combo[3]])
##        end
##        d = insert!(d, a)
##     end
##     return d
## end

## # string(Expr) now quotes, which we don't want. This hacks around that, stealing
## # from print_to_string
## function formula_string(ex::Expr)
##     s = memio(0, false)
##     Base.show_unquoted(s, ex)
##     takebuf_string(s)
## end

## #
## # The main expression to DataFrame expansion function.
## # Returns a DataFrame.
## #

## function expand(ex::Expr, df::AbstractDataFrame)
##     f = eval(ex.args[1])
##     if method_exists(f, (FormulaExpander, Vector{Any}, DataFrame))
##         # These are specialized expander functions (+, *, &, etc.)
##         f(FormulaExpander(), ex.args[2:end], df)
##     else
##         # Everything else is called recursively:
##         expand(with(df, ex), formula_string(ex), df)
##     end
## end

## function expand(s::Symbol, df::AbstractDataFrame)
##     expand(with(df, s), string(s), df)
## end

## # TODO: make this array{symbol}?
## function expand(args::Array{Any}, df::AbstractDataFrame)
##     [expand(x, df) for x in args]
## end

## function expand(x, name::ByteString, df::AbstractDataFrame)
##     # If this happens to be a column group, then expand each and concatenate
##     if is_group(df, name)
##       preds = get_groups(df)[name]
##       dfs = [expand(symbol(x), df) for x in preds]
##       return cbind(dfs...) 
##     end
##     # This is the default for expansion: put it right in to a DataFrame.
##     DataFrame({x}, [name])
## end

## #
## # Methods for expansion of specific data types
## #

## # Expand a PooledDataVector into a matrix of indicators for each dummy variable
## # TODO: account for NAs?
## function expand(poolcol::PooledDataVector, colname::ByteString, df::AbstractDataFrame)
##     newcol = {DataArray([convert(Float64,x)::Float64 for x in (poolcol.refs .== i)]) for i in 2:length(poolcol.pool)}
##     newcolname = [string(colname, ":", x) for x in poolcol.pool[2:length(poolcol.pool)]]
##     DataFrame(newcol, convert(Vector{ByteString}, newcolname))
## end


## #
## # Methods for Formula expansion
## #
## type FormulaExpander; end # This is an indictor type.

## function +(::FormulaExpander, args::Vector{Any}, df::AbstractDataFrame)
##     d = DataFrame()
##     for a in args
##         d = insert!(d, expand(a, df))
##     end
##     d
## end
## function (&)(::FormulaExpander, args::Vector{Any}, df::AbstractDataFrame)
##     interaction_design_matrix(expand(args[1], df), expand(args[2], df))
## end
## function *(::FormulaExpander, args::Vector{Any}, df::AbstractDataFrame)
##     d = +(FormulaExpander(), args, df)
##     d = insert!(d, all_interactions(expand(args, df)))
##     d
## end
