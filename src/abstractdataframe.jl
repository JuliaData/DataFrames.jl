abstract AbstractDataFrame <: Associative{String, Any}

function colmissing(adf::AbstractDataFrame) # -> Vector{Int}
    nrows, ncols = size(adf)
    missing = zeros(Int, ncols)
    for j in 1:ncols
        missing[j] = countna(adf[j])
    end
    return missing
end
