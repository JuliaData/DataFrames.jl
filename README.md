JuliaData
=========

Library for working with tabular data in Julia using `DataFrame`'s.

## Demo 1

Basic demo of `DataFrame` usage:

```julia
# Load DataFrame package.
load("src/init.jl")

# Load a CSV file into a DataFrame.
df = csvDataFrame("demo/toy_example.csv")

# Basic indexing.
df[1, :]
df["A"]
df[1, "A"]
df[1:2, "A"]
df[1:2, ["A", "B"]]

# Use the with() function to evaluate expressions relative to a DataFrame.
with(df, :(A + C))
```
[Output here.](https://github.com/HarlanH/JuliaData/blob/master/demo/demo1.out.jl)


## Demo 2

Demo of using `DataFrame`'s for regression modeling:

```julia
# Load DataFrame package.
load("src/init.jl")

# Load a very simple lm() function.
load("demo/lm.jl")

# Load a CSV file into a DataFrame.
df = csvDataFrame("demo/toy_example.csv")

# Run a simple linear model that predicts A using B and C.
lm_fit = lm(:(A ~ B + C), df)

# Print out a summary of the results.
print(lm_fit)

#
# Behind The Scenes
#

# Generate a Formula object.
f = Formula(:(A ~ B + C))

# Generate a ModelFrame object.
mf = model_frame(f, df)

# Generate a ModelMatrix object.
mm = model_matrix(mf)
```
[Output here.](https://github.com/HarlanH/JuliaData/blob/master/demo/demo2.out.jl)


## Demo 3

Various `DataFrame` manipulations.

```julia
# Load DataFrame package.
load("src/init.jl")

# Load a CSV file into a DataFrame.
df = csvDataFrame("demo/toy_example.csv")

# Look at the data structure.
# dump() is like R's str().
dump(df)

# Look at the internal data structure.
idump(df)

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
```
[Output here.](https://github.com/HarlanH/JuliaData/blob/master/demo/demo3.out.jl)


## Demo 4

Split-Apply-Combine

```julia
# Load DataFrame package.
load("src/init.jl")

srand(1) # Set the seed.

# DataFrames can also be created with an expression.
# Columns are repeated to the longest column.
df = DataFrame(quote
    a = shuffle(LETTERS[1:10])
    b = letters[randi(5,50)]
    x = randn(50)
end)

# Grouping by column b, find the sum and length of each group.
by(df, "b", :( x_sum = sum(x); x_len = length(x) )) 

# Group by a and b:
by(df, ["a", "b"], :( x_sum = sum(x); x_len = length(x) )) 

#
# Digging deeper.
#

# by is a shortcut for the following:
based_on(groupby(df, "b"), :( x_sum = sum(x); x_len = length(x) )) 

# You can also use the piping operator for the same thing.
df | groupby("b") | :( x_sum = sum(x); x_len = length(x) ) 

# groupby returns a GroupedDataFrame.
gd = groupby(df, "b")

# Iterations or references to a GroupedDataFrame return a
# SubDataFrame, a very useful way to subset a DataFrame without
# copies.
gd[1]

# Look at the structure of a GroupedDataFrame.
dump(gd)

# Look at the structure and internals of the SubDataFrame for the
# first group.
dump(gd[1])
idump(gd[1])

# You can iterate over a GroupedDataFrame or perform other operations.
# Here's within.

within(gd, :( x_sum = sum(x) ))

```
[Output here.](https://github.com/HarlanH/JuliaData/blob/master/demo/demo4.out.jl)



DataFrame Overview and Design Decisions
=======================================

Mainly Harlan here...


Next Steps
==========


