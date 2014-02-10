module TestVcat
    using Base.Test
    using DataArrays
    using DataFrames

    null_df = DataFrame(0, 0)
    df = DataFrame(Int, 4, 3)

    # Assignment of rows
    df[1, :] = df[1, :]
    df[1:2, :] = df[1:2, :]

    # Broadcasting assignment of rows
    df[1, :] = 1

    # Assignment of columns
    df[1] = zeros(4)

    # Broadcasting assignment of columns
    df[:, 1] = 1
    df[1] = 3
    df[:x3] = 2

    vcat(null_df)
    vcat(null_df, null_df)
    vcat(null_df, df)
    vcat(df, null_df)
    vcat(df, df)
    vcat(df, df, df)

    alt_df = deepcopy(df)
    vcat(df, alt_df)
    df[1] = zeros(Int, nrow(df))
    # Fail on non-matching types
    vcat(df, alt_df)

    alt_df = deepcopy(df)
    names!(alt_df, [:A, :B, :C])
    # Fail on non-matching names
    vcat(df, alt_df)

    # df[:, 1] = dvzeros(Int, nrow(df))
end
