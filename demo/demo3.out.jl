julia> # Load DataFrame package

julia> require("DataFrames")

julia> using DataFrames

julia> 

julia> # Load a CSV file into a DataFrame

julia> df = read_table(joinpath(julia_pkgdir(), "DataFrames", "demo", "toy_example.csv"))
6x3 DataFrame:
          A       B   C
[1,]    2.5   "One" 3.0
[2,]    3.6   "One" 5.0
[3,]    3.5   "Two" 3.0
[4,]    4.5   "Two" 5.0
[5,]    4.5 "Three" 3.0
[6,]    5.5 "Three" 5.0


julia> 

julia> # Look at the data structure

julia> # dump() is like R's str()

julia> dump(df)
DataFrame  6 observations of 3 variables
  A: DataArray{Float64,1}(6) [2.5,3.6,3.5,4.5]
  B: DataArray{UTF8String,1}(6) ["One","One","Two","Two"]
  C: DataArray{Float64,1}(6) [3.0,5.0,3.0,5.0]

julia> 

julia> # Look at the internal data structure

julia> idump(df)
DataFrame 
  columns: Array(Any,(3,))
    1: DataArray{Float64,1} 
      data: Array(Float64,(6,)) [2.5, 3.6, 3.5, 4.5, 4.5, 5.5]
      na: BitArray{1} 
        chunks: Array(Uint64,(1,)) [0x0000000000000000]
        dims: Array(Int64,(1,)) [6]
    2: DataArray{UTF8String,1} 
      data: Array(UTF8String,(6,)) ["One", "One", "Two", "Two", "Three", "Three"]
      na: BitArray{1} 
        chunks: Array(Uint64,(1,)) [0x0000000000000000]
        dims: Array(Int64,(1,)) [6]
    3: DataArray{Float64,1} 
      data: Array(Float64,(6,)) [3.0, 5.0, 3.0, 5.0, 3.0, 5.0]
      na: BitArray{1} 
        chunks: Array(Uint64,(1,)) [0x0000000000000000]
        dims: Array(Int64,(1,)) [6]
  colindex: Index 
    lookup: Dict{Union(ASCIIString,UTF8String),Union(AbstractArray{Real,1},Real)} 
      slots: Array(Uint8,(16,)) [0x00, 0x00, 0x00, 0x00, 0x00, 0x00  …  0x00, 0x01, 0x00, 0x00, 0x00, 0x00]
      keys: Array(Union(ASCIIString,UTF8String),(16,)) [#undef, #undef, #undef, #undef  …  "C", #undef, #undef, #undef, #undef]
      vals: Array(Union(AbstractArray{Real,1},Real),(16,)) [#undef, #undef, #undef, #undef  …  3, #undef, #undef, #undef, #undef]
      ndel: Int64 0
      count: Int64 3
      deleter: identity
    names: Array(Union(ASCIIString,UTF8String),(3,)) ["A", "B", "C"]

julia> 

julia> # Print out a summary of each column

julia> describe(df)
A
Min      2.5
1st Qu.  3.525
Median   4.05
Mean     4.016666666666667
3rd Qu.  4.5
Max      5.5

B
Length: 6
Type  : UTF8String
NAs   : 0

C
Min      3.0
1st Qu.  3.0
Median   4.0
Mean     4.0
3rd Qu.  5.0
Max      5.0


julia> 

julia> # head/tail of a DataFrame

julia> head(df)
6x3 DataFrame:
          A       B   C
[1,]    2.5   "One" 3.0
[2,]    3.6   "One" 5.0
[3,]    3.5   "Two" 3.0
[4,]    4.5   "Two" 5.0
[5,]    4.5 "Three" 3.0
[6,]    5.5 "Three" 5.0


julia> head(df, 3)
3x3 DataFrame:
          A     B   C
[1,]    2.5 "One" 3.0
[2,]    3.6 "One" 5.0
[3,]    3.5 "Two" 3.0


julia> tail(df, 2)
2x3 DataFrame:
          A       B   C
[1,]    4.5 "Three" 3.0
[2,]    5.5 "Three" 5.0


julia> 

julia> # Select all rows where column A is greater than 4.0

julia> # Element-wise operators in Julia usually have a leading "."

julia> df[:( A .> 4.0 ), :]
3x3 DataFrame:
          A       B   C
[1,]    4.5   "Two" 5.0
[2,]    4.5 "Three" 3.0
[3,]    5.5 "Three" 5.0


julia> 

julia> # Make a new column using within

julia> df2 = within(df, :( D = A + C ))
6x4 DataFrame:
          A       B   C    D
[1,]    2.5   "One" 3.0  5.5
[2,]    3.6   "One" 5.0  8.6
[3,]    3.5   "Two" 3.0  6.5
[4,]    4.5   "Two" 5.0  9.5
[5,]    4.5 "Three" 3.0  7.5
[6,]    5.5 "Three" 5.0 10.5


julia> 

julia> # This is similar, but now changes apply directly to df

julia> within!(df, quote
           D = A + C
           E = A + sum(C)
       end)
6x5 DataFrame:
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
  A: DataArray{Float64,1}(6) [2.5,3.6,3.5,4.5]
  B: DataArray{UTF8String,1}(6) ["One","One","Two","Two"]
  C: DataArray{Float64,1}(6) [3.0,5.0,3.0,5.0]
  D: DataArray{Float64,1}(6) [5.5,8.6,6.5,9.5]
  E: DataArray{Float64,1}(6) [26.5,27.6,27.5,28.5]

julia> 

julia> # Create a new DataFrame based on operations on another DataFrame

julia> # This is similar to plyr's summarise()

julia> df3 = based_on(df, quote
           ct = cut(replaceNA(A, 0.0), 3)
           sum_A = sum(A)
       end)
6x2 DataFrame:
                     ct sum_A
[1,]    "[2.5,3.56667]"  24.1
[2,]    "(3.56667,4.5]"  24.1
[3,]    "[2.5,3.56667]"  24.1
[4,]    "(3.56667,4.5]"  24.1
[5,]    "(3.56667,4.5]"  24.1
[6,]        "(4.5,5.5]"  24.1


julia> 

julia> # cut makes a PooledDataVector that is like R's factors, but

julia> # PooledDataVectors can contain more than just strings. Here's

julia> # the internal structure of a PooledDataVector:

julia> idump(df3["ct"])
PooledDataVector{ASCIIString} 
  refs: Array(Uint16,(6,)) [0x0001, 0x0002, 0x0001, 0x0002, 0x0002, 0x0003]
  pool: Array(ASCIIString,(3,)) ["[2.5,3.56667]", "(3.56667,4.5]", "(4.5,5.5]"]

julia> 

julia> # In DataFrame, copies of data are minimized, especially for column

julia> # operations

julia> # These are both the same entity; change one, and you change the other:

julia> df2 = df
6x5 DataFrame:
          A       B   C    D    E
[1,]    2.5   "One" 3.0  5.5 26.5
[2,]    3.6   "One" 5.0  8.6 27.6
[3,]    3.5   "Two" 3.0  6.5 27.5
[4,]    4.5   "Two" 5.0  9.5 28.5
[5,]    4.5 "Three" 3.0  7.5 28.5
[6,]    5.5 "Three" 5.0 10.5 29.5


julia> 

julia> colA = df2["A"]
6-element Float64 DataArray
 2.5
 3.6
 3.5
 4.5
 4.5
 5.5

julia> colA[1] = 99.0   # This changes df and df2, too.
99.0

julia> df
6x5 DataFrame:
           A       B   C    D    E
[1,]    99.0   "One" 3.0  5.5 26.5
[2,]     3.6   "One" 5.0  8.6 27.6
[3,]     3.5   "Two" 3.0  6.5 27.5
[4,]     4.5   "Two" 5.0  9.5 28.5
[5,]     4.5 "Three" 3.0  7.5 28.5
[6,]     5.5 "Three" 5.0 10.5 29.5


julia> 
