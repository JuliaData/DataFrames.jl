# Load DataFrame package.
load("src/init.jl")

# Load a CSV file into a DataFrame.
df = csvDataFrame("demo/toy_example.csv")

# Look at data structure
# dump() is like R's str()
dump(df)

# Print out a summary of each column.
summary(df)

# head/tail of a DataFrame:
head(df)
tail(df)

# Select all rows where column A is greater than 4.0.
# Element-wise operators in Julia usually have a leading ".".
df[:( A .> 4.0 )]

# This is equivalent to:
df[df["A"] .> 4.0, :]

# Make a new column using within.
df2 = within(df, :( D = A + C ))

# This is similar, but now changes apply directly to df.
within!(df, quote
    D = A + C
    E = A + sum(C)
end)

dump(df)

# Create a new DataFrame based on operations on another DataFrame.
# This is similar to plyr's summarise().
based_on(df, quote
    AC = A + C
    sum_A = sum(A)
end)

# In DataFrame, copies of data are minimized, especially for column
# operations.
# These are both the same entity change one, and you change the other.
df2 = df 

colA = df2["A"]
colA[1] = 99.0   # This changes df and df2, too.
df