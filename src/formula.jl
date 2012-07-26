# Formulas for representing and working with linear-model-type expressions
# Harlan D. Harris

# we can use Julia's parser and just write them as expressions

type Formula
    lhs::Vector
    rhs::Vector
end

function _dashtree_to_list(ex::Expr)
    # if the head of the expression is a --, return a list of recursive calls to the LHS and RHS
    # otherwise, return what we got as a cell array
    if ex.args[1] == :(--)
        append(_dashtree_to_list(ex.args[2]), _dashtree_to_list(ex.args[3]))
    else
        [ ex ]
    end
end
_dashtree_to_list(ss::Symbol) = [ ss ]

function Formula(ex::Expr) 
    # confirm we've got a call to ~, then recursively build a list out of -- - separated subtrees
    if ex.args[1] != :(~)
        error("Formula must have a ~!")
    else
        lhs = _dashtree_to_list(ex.args[2])
        rhs = _dashtree_to_list(ex.args[3])
        Formula(lhs, rhs)
    end
end

# a ModelFrame is just a wrapper around a DataFrame
# construct with mf::ModelFrame = model_frame(f::Formula, d::DataFrame)
# this unpacks the formula, extracts the columns from the DataFrame, and
# applies the ^, &, *, / operators, and evaluates any other expressions 
# in the context of the df. Note that columns remain DataVecs, and NAs are 
# preserved through this step.

# minimal first version: support y ~ x1 + x2 + log(x3)
type ModelFrame
    df::DataFrame
    y_indexes::Vector{Int}
    formula::Formula 
end

function unique_symbols(ex::Expr)

end

# Obtain Array of Symbols used in an Expr 
function unique_symbols(ex::Expr)
    if length(ex.args) == 2
        return [unique_symbols(ex.args[2])]  # I() case
    else
        return [unique_symbols(ex.args[2]), unique_symbols(ex.args[3])]
    end
end
unique_symbols(ex::Array{Expr,1}) = [unique_symbols(ex[1])]
unique_symbols(ex::Symbol) = [ex]
unique_symbols(ex::Array{Symbol,1}) = [ex[1]]

# Create a DataFrame containing only variables required by the Formula
function model_frame(f::Formula, d::DataFrame)
    cols = {}
    col_names = Array(ASCIIString,0)

    lhs = unique_symbols(f.lhs)
    rhs = unique_symbols(f.rhs)
    
    # so, foreach element in the lhs, evaluate it in the context of the df
    for ll = 1:length(lhs)
        push(cols, with(d, lhs[ll]))
        push(col_names, string(lhs[ll]))
    end
    y_indexes = [1:length(lhs)]
    
    # and foreach unique symbol in the rhs, do the same
    for rr = 1:length(rhs)
       push(cols, with(d, rhs[rr]))
       push(col_names, string(rhs[rr]))
    end
    # bind together and return
    ModelFrame(DataFrame(cols, col_names), y_indexes, f)
end


# a ModelMatrix is a wrapper around a matrix, with column names.
# construct with mm::ModelMatrix = model_matrix(mf::ModelFrame, ...)
# this converts any non-numeric types to contrasts, deals with NAs, etc.

# minimal first version: allow numbers and strings only, complete cases only

_parallel_or(a,b) = [(x[1] || x[2])::Bool for x in zip(a, b)]
_parallel_and(a,b) = [(x[1] && x[2])::Bool for x in zip(a, b)]
complete_cases(df::AbstractDataFrame) = reduce(_parallel_and, colwise(x->!isna(x), df))

type ModelMatrix{T}
    model::Array{Float64}
    response::Array{Float64}
    model_colnames::Array{T}
    response_colnames::Array{T}
end

# Expand dummy variables and equations
function model_matrix(mf::ModelFrame)
    ex = mf.formula.rhs[1]
    df = mf.df[complete_cases(mf.df),1:ncol(mf.df)]
    rdf = df[mf.y_indexes]
    mdf = expand(ex, df)

    # TODO: Convert to Array{Float64} in a cleaner way
    rnames = colnames(rdf)
    mnames = colnames(mdf)
    r = Array(Float64,nrow(rdf),ncol(rdf))
    m = Array(Float64,nrow(mdf),ncol(mdf))
    for i = 1:nrow(rdf)
      for j = 1:ncol(rdf)
        r[i,j] = float(rdf[i,j])
      end
      for j = 1:ncol(mdf)
        m[i,j] = float(mdf[i,j])
      end
    end
    
    include_intercept = true
    if include_intercept
      m = hcat(ones(nrow(mdf)), m)
      unshift(mnames, "(Intercept)")
    end

    ModelMatrix(m, r, mnames, rnames)
    ## mnames = {}
    ## rnames = {}
    ## for c in 1:ncol(rdf)
    ##   r = hcat(r, float(rdf[c]))
    ##   push(rnames, colnames(rdf)[c])
    ## end
    ## for c in 1:ncol(mdf)
    ##   m = hcat(m, mdf[c])
    ##   push(mnames, colnames(mdf)[c])
    ## end
end



# TODO: Be able to extract information about each column name
function interaction_design_matrix(a::DataFrame, b::DataFrame)
   cols = {}
   col_names = Array(ASCIIString,0)
   for i in 1:ncol(a)
       for j in 1:ncol(b)
          push(cols, DataVec(a[:,i] .* b[:,j]))
          push(col_names, strcat(colnames(a)[i],"&",colnames(b)[j]))
       end
   end
   DataFrame(cols, col_names)
end


# Take an Expr/Symbol
function expand_helper(ex::Symbol, df::DataFrame)
    a = with(df, ex)
    if isa(a, PooledDataVec)
      r = expand(a, string(ex))
    elseif isa(a, DataVec)
      r = DataFrame()
      r[string(ex)] = a
    else
      error("could not expand symbol to a DataFrame")
    end
    return r
end

function expand_helper(ex::Expr, df::DataFrame)
    if length(ex.args) == 2  # e.g. log(x2)
      a = with(df, ex)
      r = DataFrame()
      r[string(ex)] = a
    else 
      r = expand(ex, df)
    end
    return r
end

# Expand an Expression (with +, &, or *) using the provided DataFrame
# + includes both columns
# & includes the elementwise product of every pair of columns
# * both of the above
function expand(ex, df::DataFrame)
  # Recurse on left and right children of provided Expression
  a = expand_helper(ex.args[2], df)
  b = expand_helper(ex.args[3], df)

  # Combine according to formula
  if ex.args[1] == :+
    return cbind(a,b)
  elseif ex.args[1] == :&
    return interaction_design_matrix(a,b)
  elseif ex.args[1] == :*
    return cbind(a, b, interaction_design_matrix(a,b))
  else
    error("Unknown operation in formula")
  end
end


# Expand a PooledDataVector into a matrix of indicators for each dummy variable
# TODO: account for NAs?
function expand(poolcol::PooledDataVec, colname::String)
    newcol = {DataVec([convert(Float64,x)::Float64 for x in (poolcol.refs .== i)]) for i in 2:length(poolcol.pool)}
    newcolname = [strcat(colname, ":", x) for x in poolcol.pool[2:length(poolcol.pool)]]
    DataFrame(newcol, convert(Vector{ByteString}, newcolname))
end
