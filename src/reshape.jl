##############################################################################
##
## Reshaping
##
##############################################################################

function stack(df::DataFrame, icols::Vector{Int})
    remainingcols = _setdiff([1:ncol(df)], icols)
    res = rbind([insert!(df[[i, remainingcols]], 1, colnames(df)[i], "key") for i in icols]...)
    replace_names!(res, colnames(res)[2], "value")
    res 
end
stack(df::DataFrame, icols) = stack(df, [df.colindex[icols]])

function unstack(df::DataFrame, ikey::Int, ivalue::Int, irefkey::Int)
    keycol = PooledDataArray(df[ikey])
    valuecol = df[ivalue]
    # TODO make a version with a default refkeycol
    refkeycol = PooledDataArray(df[irefkey])
    remainingcols = _setdiff([1:ncol(df)], [ikey, ivalue])
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    # TODO make fillNA(type, length) 
    payload = DataFrame({DataArray([fill(valuecol[1],Nrow)], fill(true, Nrow))  for i in 1:Ncol}, map(string, keycol.pool))
    nowarning = true 
    for k in 1:nrow(df)
        j = int(keycol.refs[k])
        i = int(refkeycol.refs[k])
        if i > 0 && j > 0
            if nowarning && !isna(payload[j][i]) 
                println("Warning: duplicate entries in unstack.")
                nowarning = false
            end
            payload[j][i]  = valuecol[k]
        end
    end
    insert!(payload, 1, refkeycol.pool, colnames(df)[irefkey])
end
unstack(df::DataFrame, ikey, ivalue, irefkey) =
    unstack(df, df.colindex[ikey], df.colindex[ivalue], df.colindex[irefkey])

    
##############################################################################
##
## Reshaping using referencing (issue #145)
## New AbstractVector types (all read only):
##     StackedVector
##     RepeatedVector
##     EachRepeatedVector
##
##############################################################################

import Base.ref
## StackedVector({[1,2], [9,10], [11,12]}) is equivalent to [1,2,9,10,11,12]
type StackedVector <: AbstractVector{Any}
    components::Vector{Any}
end

## RepeatedVector([1,2], 3) is equivalent to [1,2,1,2,1,2]
type RepeatedVector{T} <: AbstractVector{T}
    parent::AbstractVector{T}
    n::Int
end

## EachRepeatedVector([1,2], 3) is equivalent to [1,1,1,2,2,2]
type EachRepeatedVector{T} <: AbstractVector{T}
    parent::AbstractVector{T}
    n::Int
end

function ref(v::StackedVector,i::Real)
    lengths = [length(x)::Int for x in v.components]
    cumlengths = [0, cumsum(lengths)]
    j = search_sorted_last(cumlengths + 1, i)
    if j > length(cumlengths)
        error("indexing bounds error")
    end
    k = i - cumlengths[j]
    if k < 1 || k > length(v.components[j])
        error("indexing bounds error")
    end
    v.components[j][k]
end

function ref{I<:Real}(v::StackedVector,i::AbstractVector{I})
    result = similar(v.components[1], length(i))
    for idx in 1:length(i)
        result[idx] = v[i[idx]]
    end
    result
end
ref(v::StackedVector,i::Union(Ranges, Vector{Bool}, BitVector)) = ref(v, [i])

size(v::StackedVector) = (length(v),)
length(v::StackedVector) = sum(map(length, v.components))
ndims(v::StackedVector) = 1
eltype(v::StackedVector) = eltype(v.components[1])

show(io::IO, v::StackedVector) = internal_show_vector(io, v)
repl_show(io::IO, v::StackedVector) = internal_repl_show_vector(io, v)

function ref{T,I<:Real}(v::RepeatedVector{T},i::AbstractVector{I})
    j = mod(i - 1, length(v.parent)) + 1
    v.parent[j]
end
function ref{T}(v::RepeatedVector{T},i::Real)
    j = mod(i - 1, length(v.parent)) + 1
    v.parent[j]
end
ref(v::RepeatedVector,i::Union(Ranges, Vector{Bool}, BitVector)) = ref(v, [i])

size(v::RepeatedVector) = (length(v),)
length(v::RepeatedVector) = v.n * length(v.parent)
ndims(v::RepeatedVector) = 1
eltype{T}(v::RepeatedVector{T}) = T

show(io::IO, v::RepeatedVector) = internal_show_vector(io, v)
repl_show(io::IO, v::RepeatedVector) = internal_repl_show_vector(io, v)


function ref{T}(v::EachRepeatedVector{T},i::Real)
    j = div(i - 1, v.n) + 1
    v.parent[j]
end
function ref{T,I<:Real}(v::EachRepeatedVector{T},i::AbstractVector{I})
    j = div(i - 1, v.n) + 1
    v.parent[j]
end
ref(v::EachRepeatedVector,i::Union(Ranges, Vector{Bool}, BitVector)) = ref(v, [i])

size(v::EachRepeatedVector) = (length(v),)
length(v::EachRepeatedVector) = v.n * length(v.parent)
ndims(v::EachRepeatedVector) = 1
eltype{T}(v::EachRepeatedVector{T}) = T

show(io::IO, v::EachRepeatedVector) = internal_show_vector(io, v)
repl_show(io::IO, v::EachRepeatedVector) = internal_repl_show_vector(io, v)


# The default values of show and repl_show don't work because
# both try to reshape the vector into a matrix, and these
# types do not support that.

function internal_show_vector(io::IO, v::AbstractVector)
    Base.show_delim_array(io, v, '[', ',', ']', true)
end

function internal_repl_show_vector(io::IO, v::AbstractVector)
    print(io, summary(v))
    if length(v) < 21
        print_matrix(io, v[:]'')    # the double transpose ('') reshapes to a matrix
    else
        println(io)
        print_matrix(io, v[1:10]'')
        println(io)
        println(io, "  \u22ee")
        print_matrix(io, v[end - 9:end]'')
    end
end


##############################################################################
##
## stack_df()
## Reshaping using referencing (issue #145), using the above vector types
##
##############################################################################

# Same as `stack`, but uses references
# I'm not sure the name is very good
function stack_df(df::AbstractDataFrame, icols::Vector{Int})
    N = length(icols)
    remainingcols = _setdiff([1:ncol(df)], icols)
    names = colnames(df)[remainingcols]
    insert!(names, 1, "value")
    insert!(names, 1, "key")
    DataFrame({EachRepeatedVector(colnames(df)[icols], nrow(df)),       # key
               StackedVector({df[:,c] for c in 1:N}),                   # value
               ## RepeatedVector([1:nrow(df)], N),                      # idx - do we want this?
               [RepeatedVector(df[:,c], N) for c in remainingcols]...}, # remaining columns
              names)
end
stack_df(df::AbstractDataFrame, icols) = stack_df(df, [df.colindex[icols]])
