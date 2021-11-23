"""
My `reverse` function.
"""
function Base.reverse(df::DataFrame)
    for column in 1:size(df, 2)
        df[!, column] = Base.reverse(df[!, column])
    end
    return df
end