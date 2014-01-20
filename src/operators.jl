import DataArrays.@swappable

function isequal(df1::AbstractDataFrame, df2::AbstractDataFrame)
    if size(df1, 2) != size(df2, 2)
        return false
    end
    for idx in 1:size(df1, 2)
        if !isequal(df1[idx], df2[idx])
            return false
        end
    end
    return true
end

for f in [:(.==), :(.!=), :(.>), :(.>=), :(.<), :(.<=)]
    @eval begin
        function $(f)(a::DataFrame, b::DataFrame)
            if size(a) != size(b); error("argument dimensions must match"); end
            DataFrame([$(f)(a[i], b[i]) for i=1:size(a, 2)], deepcopy(index(a)))
        end
        @swappable $(f)(a::DataFrame, b::Union(Number, String)) =
            DataFrame([$(f)(a[i], b) for i=1:size(a, 2)], deepcopy(index(a)))
        @swappable $(f)(a::DataFrame, b::NAtype) =
            DataFrame([$(f)(a[i], b) for i=1:size(a, 2)], deepcopy(index(a)))
    end
end
