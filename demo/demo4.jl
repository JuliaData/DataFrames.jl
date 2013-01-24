# Load DataFrame package
require("DataFrames")
using DataFrames

# Set the seed
srand(1)

# DataFrames can also be created with an expression
# Columns are repeated to the longest column
df = DataFrame(quote
    a = shuffle(LETTERS[1:10])
    b = letters[rand(1:5,50)]
    x = randn(50)
end)

# Grouping by column b, find the sum and length of each group
by(df, "b", :( x_sum = sum(x); x_len = length(x)))

# Group by a and b:
by(df, ["a", "b"], :( x_sum = sum(x); x_len = length(x) ))

#
# Digging deeper
#

# by is a shortcut for the following:
based_on(groupby(df, "b"), :( x_sum = sum(x); x_len = length(x) ))

# You can also use the piping operator for the same thing.
df | groupby("b") | :( x_sum = sum(x); x_len = length(x) )

# groupby returns a GroupedDataFrame
gd = groupby(df, "b")

# Iterations or references to a GroupedDataFrame return a
# SubDataFrame, a very useful way to subset a DataFrame without
# copies
gd[1]

# Look at the structure of a GroupedDataFrame
dump(gd)

# Look at the structure and internals of the SubDataFrame for the
# first group
dump(gd[1])
idump(gd[1])

# You can iterate over a GroupedDataFrame or perform other operations
# Here's within:
within(gd, :( x_sum = sum(x) ))
