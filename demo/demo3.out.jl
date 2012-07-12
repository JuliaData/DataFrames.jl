julia> # Load a CSV file into a DataFrame.

julia> df = csvDataFrame("demo/toy_example.csv")
DataFrame  (6,3)
          A       B   C
[1,]    2.5   "One" 3.0
[2,]    3.6   "One" 5.0
[3,]    3.5   "Two" 3.0
[4,]    4.5   "Two" 5.0
[5,]    4.5 "Three" 3.0
[6,]    5.5 "Three" 5.0


julia> 

julia> # Look at data structure

julia> # dump() is like R's str()

julia> dump(df)
DataFrame  6 observations of 3 variables
  A: DataVec{Float64}(6) [2.5,3.6,3.5,4.5]
  B: PooledDataVec{ASCIIString}(6) ["One","One","Two","Two"]
  C: DataVec{Float64}(6) [3.0,5.0,3.0,5.0]

julia> 

julia> # Print out a summary of each column.

julia> summary(df)
A
Min      2.5
1st Qu.  3.525
Median   4.05
Mean     4.016666666666667
3rd Qu.  4.5
Max      5.5

B
Length: 6
Type  : Pooled ASCIIString
NAs   : 0

C
Min      3.0
1st Qu.  3.0
Median   4.0
Mean     4.0
3rd Qu.  5.0
Max      5.0


julia> 

julia> # head/tail of a DataFrame:

julia> head(df)
DataFrame  (6,3)
          A       B   C
[1,]    2.5   "One" 3.0
[2,]    3.6   "One" 5.0
[3,]    3.5   "Two" 3.0
[4,]    4.5   "Two" 5.0
[5,]    4.5 "Three" 3.0
[6,]    5.5 "Three" 5.0


julia> tail(df)
DataFrame  (6,3)
          A       B   C
[1,]    2.5   "One" 3.0
[2,]    3.6   "One" 5.0
[3,]    3.5   "Two" 3.0
[4,]    4.5   "Two" 5.0
[5,]    4.5 "Three" 3.0
[6,]    5.5 "Three" 5.0


julia> 

julia> # Select all rows where column A is greater than 4.0.

julia> # Element-wise operators in Julia usually have a leading ".".

julia> df[:( A .> 4.0 )]
no method isless(Float64,DataVec{Float64})
 in method_missing at base.jl:60
 in .> at operators.jl:66
 in anonymous at no file
 in with at /home/tshort/julia/JuliaData/src/data.jl:1640
 in ref at /home/tshort/julia/JuliaData/src/data.jl:800

julia> 

julia> # This is equivalent to:

julia> df[df["A"] .> 4.0, :]
no method isless(Float64,DataVec{Float64})
 in method_missing at base.jl:60
 in .> at operators.jl:66

julia> 

julia> # Make a new column using within.

julia> df2 = within(df, :( D = A + C ))
DataFrame  (6,4)
          A       B   C    D
[1,]    2.5   "One" 3.0  5.5
[2,]    3.6   "One" 5.0  8.6
[3,]    3.5   "Two" 3.0  6.5
[4,]    4.5   "Two" 5.0  9.5
[5,]    4.5 "Three" 3.0  7.5
[6,]    5.5 "Three" 5.0 10.5


julia> 

julia> # This is similar, but now changes apply directly to df.

julia> within!(df, quote
           D = A + C
           E = A + sum(C)
       end)
DataFrame  (6,5)
          A       B   C    D    E
[1,]    2.5   "One" 3.0  5.5 26.5
[2,]    3.6   "One" 5.0  8.6 27.6
[3,]    3.5   "Two" 3.0  6.5 27.5
[4,]    4.5   "Two" 5.0  9.5 28.5
[5,]    4.5 "Three" 3.0  7.5 28.5
[6,]    5.5 "Three" 5.0 10.5 29.5


julia> 

julia> dump(df)
DataFrame  6 observations of 5 variables
  A: DataVec{Float64}(6) [2.5,3.6,3.5,4.5]
  B: PooledDataVec{ASCIIString}(6) ["One","One","Two","Two"]
  C: DataVec{Float64}(6) [3.0,5.0,3.0,5.0]
  D: DataVec{Float64}(6) [5.5,8.6,6.5,9.5]
  E: DataVec{Float64}(6) [26.5,27.6,27.5,28.5]

julia> 

julia> # Create a new DataFrame based on operations on another DataFrame.

julia> # This is similar to plyr's summarise().

julia> based_on(df, quote
           AC = A + C
           sum_A = sum(A)
       end)
DataFrame  (6,2)
          AC sum_A
[1,]     5.5  24.1
[2,]     8.6  24.1
[3,]     6.5  24.1
[4,]     9.5  24.1
[5,]     7.5  24.1
[6,]    10.5  24.1


julia> 

julia> # In DataFrame, copies of data are minimized, especially for column

julia> # operations.

julia> # These are both the same entity change one, and you change the other.

julia> df2 = df 
DataFrame  (6,5)
          A       B   C    D    E
[1,]    2.5   "One" 3.0  5.5 26.5
[2,]    3.6   "One" 5.0  8.6 27.6
[3,]    3.5   "Two" 3.0  6.5 27.5
[4,]    4.5   "Two" 5.0  9.5 28.5
[5,]    4.5 "Three" 3.0  7.5 28.5
[6,]    5.5 "Three" 5.0 10.5 29.5


julia> 

julia> colA = df2["A"]
[2.5,3.6,3.5,4.5,4.5,5.5]

julia> colA[1] = 99.0   # This changes df and df2, too.
99.0

julia> df
DataFrame  (6,5)
           A       B   C    D    E
[1,]    99.0   "One" 3.0  5.5 26.5
[2,]     3.6   "One" 5.0  8.6 27.6
[3,]     3.5   "Two" 3.0  6.5 27.5
[4,]     4.5   "Two" 5.0  9.5 28.5
[5,]     4.5 "Three" 3.0  7.5 28.5
[6,]     5.5 "Three" 5.0 10.5 29.5

