module TestOperators
    using Base.Test
    using DataArrays
    using DataFrames
    using StatsBase

    #
    # Equality tests
    #
    dv = @data [false, NA]
    df = DataFrame({dv})
    alt_dv = @data [false, NA]
    alt_dv[1] = NA
    alt_df = DataFrame({alt_dv})
    # @assert isequal(DataFrame({dv}) .== DataFrame({dv}), DataFrame({DataVector[true, NA]}))

    @assert all(isna(NA .== df))
    @assert all(isna(df .== NA))
end
