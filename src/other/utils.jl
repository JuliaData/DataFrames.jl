function make_unique!(names::Vector{Symbol}, src::AbstractVector{Symbol}; makeunique::Bool=false)
    if length(names) != length(src)
        throw(ArgumentError("Length of src doesn't match length of names."))
    end
    seen = Set{Symbol}()
    dups = Int[]
    for i in 1:length(names)
        name = src[i]
        if in(name, seen)
            push!(dups, i)
        else
            names[i] = src[i]
            push!(seen, name)
        end
    end

    if length(dups) > 0
        if !makeunique
            dupstr = join(string.(':', unique(src[dups])), ", ", " and ")
            msg = "Duplicate variable names: $dupstr. Pass makeunique=true " *
                  "to make them unique using a suffix automatically."
            throw(ArgumentError(msg))
        end
    end

    for i in dups
        nm = src[i]
        k = 1
        while true
            newnm = Symbol("$(nm)_$k")
            if !in(newnm, seen)
                names[i] = newnm
                push!(seen, newnm)
                break
            end
            k += 1
        end
    end

    return names
end

function make_unique(names::AbstractVector{Symbol}; makeunique::Bool=false)
    make_unique!(similar(names), names, makeunique=makeunique)
end

"""
    gennames(n::Integer)

Generate standardized names for columns of a DataFrame. The first name will be `:x1`, the
second `:x2`, etc.
"""
function gennames(n::Integer)
    res = Array{Symbol}(undef, n)
    for i in 1:n
        res[i] = Symbol(@sprintf "x%d" i)
    end
    return res
end

"""
    flatten(df::AbstractDataFrame, veccol::Union{Integer, Symbol})

When column `veccol` of `df` has iterable elements that define `length`, for example a `Vector` 
of `Vector`s. Returns a `DataFrame` where each element of `veccol` is flattened. 
Elements of row `i` of `df` other than `veccol` will be duplicated according to 
the length of `df[i, veccol]`

# Arguments

- `df`: an `AbstractDataFrame` to flatten
- `veccol`: a `Symbol` or `Integer` where `veccol` is a column of `df` whose 
   elements support iteration.
"""
function flatten(df::AbstractDataFrame, veccol::Union{Integer, Symbol})
    col_to_flatten = df[!, veccol]
    lengths = length.(col_to_flatten)    
    new_df = similar(df[!, Not(veccol)], sum(lengths))

    function copy_length!(longnew, shortold, lengths)
        counter = 1
        @inbounds for i in 1:length(shortold)
            for j in 1:lengths[i]
                longnew[counter] = shortold[i]
                counter += 1
            end
        end
    end

    for name in names(new_df)
        copy_length!(new_df[!, name], df[!, name], lengths)  
    end

    flattened_col = col_to_flatten isa AbstractVector{<:AbstractVector} ?
        reduce(vcat, col_to_flatten) :
        collect(Iterators.flatten(col_to_flatten))

    insertcols!(new_df, columnindex(df, veccol), veccol => flattened_col)

    return new_df
end


