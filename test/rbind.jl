load("DataFrames")
using DataFrames

null_df = DataFrame(0, 0)
df = DataFrame(Int, 4, 3)

# Assignment of rows
df[1, :] = df[1, :]
df[1:2, :] = df[1:2, :]

# Broadcasting assignment of rows
df[1, :] = 1

# Assignment of columns
df[1] = dvzeros(4)

# Broadcasting assignment of columns
df[:, 1] = 1
df[1] = 3
df["x3"] = 2

rbind(null_df)
rbind(null_df, null_df)
rbind(null_df, df)
rbind(df, null_df)
rbind(df, df)
rbind(df, df, df)

alt_df = deepcopy(df)
rbind(df, alt_df)
df[1] = dvzeros(Int, nrow(df))
# Fail on non-matching types
rbind(df, alt_df)

alt_df = deepcopy(df)
colnames!(alt_df, ["A", "B", "C"])
# Fail on non-matching names
rbind(df, alt_df)

# df[:, 1] = dvzeros(Int, nrow(df))
