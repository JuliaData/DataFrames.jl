# Load DataFrame package
require("DataFrames")
using DataFrames

# Load a CSV file into a DataFrame
df = read_table(Pkg.dir("DataFrames", "demo", "toy_example.csv"))

# Look at the data structure
# dump() is like R's str()
dump(df)

# Look at the internal data structure
idump(df)

# Print out a summary of each column
describe(df)

# head/tail of a DataFrame
head(df)
head(df, 3)
tail(df, 2)

# Select all rows where column A is greater than 4.0
# Element-wise operators in Julia usually have a leading "."
df[:( A .> 4.0 ), :]

# Make a new column using within
df2 = within(df, :( D = A + C ))

# This is similar, but now changes apply directly to df
within!(df, quote
    D = A + C
    E = A + sum(C)
end)

dump(df)

# Create a new DataFrame based on operations on another DataFrame
# This is similar to plyr's summarise()
df3 = based_on(df, quote
    ct = cut(replaceNA(A, 0.0), 3)
    sum_A = sum(A)
end)

# cut makes a PooledDataVector that is like R's factors, but
# PooledDataVectors can contain more than just strings. Here's
# the internal structure of a PooledDataVector:
idump(df3["ct"])

# In DataFrame, copies of data are minimized, especially for column
# operations
# These are both the same entity; change one, and you change the other:
df2 = df

colA = df2["A"]
colA[1] = 99.0   # This changes df and df2, too.
df
