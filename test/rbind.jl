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
