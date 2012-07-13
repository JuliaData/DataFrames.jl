julia> # Load DataFrame package.

julia> load("src/init.jl")
Warning: New definition ==(NAtype,Any) is ambiguous with ==(Any,AbstractArray{T,N}).
         Make sure ==(NAtype,AbstractArray{T,N}) is defined first.
Warning: New definition ==(Any,NAtype) is ambiguous with ==(AbstractArray{T,N},Any).
         Make sure ==(AbstractArray{T,N},NAtype) is defined first.
Warning: New definition .==(AbstractDataVec{T},T) is ambiguous with .==(Any,AbstractArray{T,N}).
         Make sure .==(AbstractDataVec{AbstractArray{T,N}},AbstractArray{T,N}) is defined first.
Warning: New definition promote_rule(Type{AbstractDataVec{T}},Type{T}) is ambiguous with promote_rule(Type{AbstractDataVec{S}},Type{T}).
         Make sure promote_rule(Type{AbstractDataVec{T}},Type{T}) is defined first.

julia> 

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

julia> # Look at the data structure.

julia> # dump() is like R's str().

julia> dump(df)
DataFrame  6 observations of 3 variables
  A: DataVec{Float64}(6) [2.5,3.6,3.5,4.5]
  B: PooledDataVec{ASCIIString}(6) ["One","One","Two","Two"]
  C: DataVec{Float64}(6) [3.0,5.0,3.0,5.0]

julia> 

julia> # Look at the internal data structure.

julia> idump(df)
DataFrame 
  columns: Array(Any,(3,))
    1: DataVec{Float64} 
      data: Array(Float64,(6,)) [2.5, 3.6, 3.5, 4.5]
      na: Array(Bool,(6,)) [false, false, false, false]
      filter: Bool false
      replace: Bool false
      replaceVal: Float64 0.0
    2: PooledDataVec{ASCIIString} 
      refs: Array(Uint16,(6,)) [0x0001, 0x0001, 0x0003, 0x0003]
      pool: Array(ASCIIString,(3,)) ["One", "Three", "Two"]
      filter: Bool false
      replace: Bool false
      replaceVal: ASCIIString 
        data: Array(Uint8,(0,)) []
    3: DataVec{Float64} 
      data: Array(Float64,(6,)) [3.0, 5.0, 3.0, 5.0]
      na: Array(Bool,(6,)) [false, false, false, false]
      filter: Bool false
      replace: Bool false
      replaceVal: Float64 0.0
  colindex: Index 
    lookup: Dict{Union(UTF8String,ASCIIString),Int64} 
      keys: Array(Any,(16,))
        1: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        2: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        3: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        4: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        5: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        6: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        7: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        8: ASCIIString 
          data: Array(Uint8,(1,)) [0x42]
        9: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        10: ASCIIString 
          data: Array(Uint8,(1,)) [0x41]
      vals: Array(Any,(16,))
        1: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        2: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        3: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        4: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        5: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        6: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        7: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        8: Int64 2
        9: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
        10: Int64 1
      ndel: Int64 0
      deleter: identity
    names: Array(Union(UTF8String,ASCIIString),(3,)) ["A", "B", "C"]

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

julia> df3 = based_on(df, quote
           ct = cut(nafilter(A), 3)
           sum_A = sum(A)
       end)
DataFrame  (6,2)
                     ct sum_A
[1,]    "[2.5,3.56667]"  24.1
[2,]    "(3.56667,4.5]"  24.1
[3,]    "[2.5,3.56667]"  24.1
[4,]    "(3.56667,4.5]"  24.1
[5,]    "(3.56667,4.5]"  24.1
[6,]        "(4.5,5.5]"  24.1


julia> 

julia> # cut makes a PooledDataVec that is like R's factors, but

julia> # PooledDataVecs can contain more than just strings. Here's

julia> # the internal structure of a PooledDataVec:

julia> idump(df3["ct"])
PooledDataVec{ASCIIString} 
  refs: Array(Uint16,(6,)) [0x0001, 0x0002, 0x0001, 0x0002]
  pool: Array(ASCIIString,(3,)) ["[2.5,3.56667]", "(3.56667,4.5]", "(4.5,5.5]"]
  filter: Bool false
  replace: Bool false
  replaceVal: ASCIIString 
    data: Array(Uint8,(0,)) []

julia> 

julia> # In DataFrame, copies of data are minimized, especially for column

julia> # operations.

julia> # These are both the same entity; change one, and you change the other:

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

