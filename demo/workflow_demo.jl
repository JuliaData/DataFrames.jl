# Load DataFrames.jl package.
load("DataFrames.jl")
using DataFrames

# Load a CSV file into a DataFrame.
df = csvDataFrame("demo/toy_example.csv")

# Basic ways to look at a DataFrame
print(df)
head(df,3)
tail(df)

# Print out a summary of each column.
summary(df)

# Look at the data structure.
# dump() is like R's str().
dump(df)

# Look at the internal data structure.
idump(df)

# Basic referencing and slicing
# singleton references return a basic element; slices are DataFrames
df[1, :]
df["A"]
df[1, "A"]
df[1:2, "A"]
df[1:2, ["A", "B"]]

# Element-wise operators in Julia usually have a leading "."
df["A"] .> 4.0
df[df["A"] .> 4.0, :]
# more compactly, referencing with an expression evaluated inside the DataFrame
# (more on that later)
df[:( A .> 4.0 )]

# basic in-place assignments
dfcopy = copy(df)
dfcopy[1,1] = 99.9
dfcopy[2:3, "B"] = "One and a half"

# Use the with() function to evaluate expressions relative to a DataFrame.
with(df, :(A + C))

# Make a copy of df with a new column using within().
df2 = within(df, :( D = A + C ))

# This is similar, but now changes apply directly to df.
within!(df, quote
    D = A + C
    E = A + sum(C)
end)

dump(df)

# Create a new DataFrame based on operations on another DataFrame.
# This is similar to plyr's summarise().
# This is similar to within(), but the result from based_on() does not include the
# original columns like within().
# The number or rows may or may not match that of the original.
df3 = based_on(df, quote
    ct = cut(nareplace(A,0.0), 3) # cut() doesn't operator on DataVecs yet; no NAs here
    sum_A = sum(A)
end)


# In DataFrame, copies of data are minimized, especially for column
# operations.
# These are both the same entity; change one, and you change the other:
df2 = df 

colA = df2["A"]
colA[1] = 99.0   # This changes df and df2, too.
df

# Make a new, bigger DataFrame

srand(1) # Set the seed.

# DataFrames can also be created with an expression.
# Columns are repeated to the longest column.
df = DataFrame(quote
    a = shuffle(LETTERS[1:10])
    b = letters[randi(5,50)]
    x = randn(50)
end)

# This is one way to do a split-apply-combine operation.
based_on(groupby(df, "b"), :( x_sum = sum(x); x_len = length(x) )) 
# groupby creates a Group of SubDataFrames, which is space-efficient
# based_on then evaluates the expression in the context of each and combines the results

# same thing, shorter syntax
by(df, "b", :( x_sum = sum(x); x_len = length(x) )) 

# Group by a and b:
by(df, ["a", "b"], :( x_sum = sum(x); x_len = length(x) )) 

# You can also use the piping operator
df | groupby("b") | :( x_sum = sum(x); x_len = length(x) ) 

# merging isn't complete, but simple cases work
srand(1)
df1 = DataFrame(quote
    a = shuffle([1:10])
    b = ["A","B"][randi(2,10)]
    v1 = randn(10)
end)

df2 = DataFrame(quote
    a = shuffle(reverse([1:5]))
    b2 = ["A","B","C"][randi(3,5)]
    v2 = randn(5)
end)

# inner join
m1 = merge(df1, df2, "a")

# visualization -- requires a Gaston installation. YMMV.
push(LOAD_PATH, "../gaston-0.5.4/")
load("gaston.jl")
set_terminal("x11")
with(df1, :(plot(nafilter(a), nafilter(v1)), "plotstyle", "points")))

# DataFrames and Formula can be used to do things like linear models

# Load a very simple lm() function.
load("demo/lm.jl")

# Load a CSV file into a DataFrame.
df = csvDataFrame("demo/toy_example.csv")

# Run a simple linear model that predicts A using B and C.
lm_fit = lm(:(A ~ B + C), df)

# Print out a summary of the results.
print(lm_fit)

