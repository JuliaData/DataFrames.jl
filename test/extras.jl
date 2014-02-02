module TestDataFrames
    using Base.Test
    using DataArrays
    using DataFrames

    ##########
    ## rep
    ##########

    @assert rep(3, 2) == [3,3]
    @assert rep([3,4], 2) == [3,4,3,4]
    @assert rep([3,4], 1, 2) == [3,3,4,4]
    @assert rep([3,4], each = 2) == [3,3,4,4]
    @assert rep([3,4], times = 2) == [3,4,3,4]
    @assert rep([3,4], times = [2,3]) == [3,3,4,4,4]
    @assert rep([3,4], [2,3]) == [3,3,4,4,4]
    @assert isequal(rep(@data([NA, 3, 4]), 2),
                    @data([NA, 3, 4, NA, 3, 4]))
    @assert isequal(rep(@data([NA, 3, 4]), [2, 1, 2]),
                    @data([NA, NA, 3, 4, 4]))
    @assert isequal(rep(@data([NA, 3, 4]), [2, 1, 0]),
                    @data([NA, NA, 3]))
end
