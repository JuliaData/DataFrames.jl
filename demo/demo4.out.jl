julia> # Load DataFrame package

julia> require("DataFrames")

julia> using DataFrames

julia> 

julia> # Set the seed

julia> srand(1)

julia> 

julia> # DataFrames can also be created with an expression

julia> # Columns are repeated to the longest column

julia> df = DataFrame(quote
           a = shuffle(LETTERS[1:10])
           b = letters[rand(1:5,50)]
           x = randn(50)
       end)
50x3 DataFrame:
           a   b          x
[1,]     "G" "a"   0.631291
[2,]     "I" "d"   -1.23373
[3,]     "D" "c"  -0.858585
[4,]     "A" "b"   0.258266
[5,]     "C" "a"   -1.45999
[6,]     "J" "d"   0.509527
[7,]     "B" "b"    -1.2459
[8,]     "F" "e"   -1.33165
[9,]     "H" "d"   -1.36073
[10,]    "E" "a"   0.542924
[11,]    "G" "e"   -2.63399
[12,]    "I" "a"   0.149901
[13,]    "D" "c"   -1.65878
[14,]    "A" "e"    -2.7486
[15,]    "C" "e"  -0.418066
[16,]    "J" "c"   -1.43133
[17,]    "B" "d"    1.15406
[18,]    "F" "a"  -0.485528
[19,]    "H" "a"   0.775438
[20,]    "E" "d"    0.75606
  :
[31,]    "G" "b"  -0.906487
[32,]    "I" "b"   -0.97022
[33,]    "D" "a"  -0.768929
[34,]    "A" "d"  -0.395993
[35,]    "C" "b"   0.475183
[36,]    "J" "c"   0.638632
[37,]    "B" "a"  -0.398748
[38,]    "F" "c" -0.0803466
[39,]    "H" "a"    1.00184
[40,]    "E" "b"  -0.704845
[41,]    "G" "a"   -1.43405
[42,]    "I" "a"   0.434751
[43,]    "D" "a" -0.0922058
[44,]    "A" "b"  -0.464083
[45,]    "C" "b"    2.18783
[46,]    "J" "b"   0.579141
[47,]    "B" "c"  -0.914724
[48,]    "F" "b"   -0.15506
[49,]    "H" "d"  0.0607178
[50,]    "E" "e"  -0.270859


julia> # Grouping by column b, find the sum and length of each group

julia> by(df, "b", :( x_sum = sum(x); x_len = length(x)))
5x3 DataFrame:
          b    x_sum x_len
[1,]    "a" -3.01026    14
[2,]    "b" 0.851169    11
[3,]    "c" -5.45193     8
[4,]    "d"  0.60101     9
[5,]    "e" -6.39996     8


julia> 

julia> # Group by a and b:

julia> by(df, ["a", "b"], :( x_sum = sum(x); x_len = length(x) ))
36x4 DataFrame:
           a   b     x_sum x_len
[1,]     "A" "a" -0.448951     1
[2,]     "B" "a" -0.398748     1
[3,]     "C" "a"  -1.45999     1
[4,]     "D" "a" -0.861135     2
[5,]     "E" "a"  0.542924     1
[6,]     "F" "a" -0.485528     1
[7,]     "G" "a" -0.802759     2
[8,]     "H" "a"  0.319276     3
[9,]     "I" "a"  0.584652     2
[10,]    "A" "b" -0.205817     2
[11,]    "B" "b"   -1.2459     1
[12,]    "C" "b"   2.66301     2
[13,]    "E" "b"    1.0925     2
[14,]    "F" "b"  -0.15506     1
[15,]    "G" "b" -0.906487     1
[16,]    "I" "b"  -0.97022     1
[17,]    "J" "b"  0.579141     1
[18,]    "B" "c" -0.914724     1
[19,]    "D" "c"  -2.51736     2
[20,]    "F" "c" -0.282469     2
[21,]    "I" "c" -0.944684     1
[22,]    "J" "c" -0.792694     2
[23,]    "A" "d" -0.395993     1
[24,]    "B" "d"   1.08004     2
[25,]    "C" "d"   1.18512     1
[26,]    "E" "d"   0.75606     1
[27,]    "H" "d"  -1.30002     2
[28,]    "I" "d"  -1.23373     1
[29,]    "J" "d"  0.509527     1
[30,]    "A" "e"   -2.7486     1
[31,]    "C" "e" -0.418066     1
[32,]    "D" "e"  0.658323     1
[33,]    "E" "e" -0.270859     1
[34,]    "F" "e"  -1.33165     1
[35,]    "G" "e"  -4.26072     2
[36,]    "J" "e"   1.97162     1


julia> 

julia> #

julia> # Digging deeper

julia> #

julia> 

julia> # by is a shortcut for the following:

julia> based_on(groupby(df, "b"), :( x_sum = sum(x); x_len = length(x) ))
5x3 DataFrame:
          b    x_sum x_len
[1,]    "a" -3.01026    14
[2,]    "b" 0.851169    11
[3,]    "c" -5.45193     8
[4,]    "d"  0.60101     9
[5,]    "e" -6.39996     8


julia> 

julia> # You can also use the piping operator for the same thing.

julia> df |> groupby("b") |> :( x_sum = sum(x); x_len = length(x) )
5x3 DataFrame:
          b    x_sum x_len
[1,]    "a" -3.01026    14
[2,]    "b" 0.851169    11
[3,]    "c" -5.45193     8
[4,]    "d"  0.60101     9
[5,]    "e" -6.39996     8


julia> 

julia> # groupby returns a GroupedDataFrame

julia> gd = groupby(df, "b")
GroupedDataFrame  5 groups with keys: ["b"]
First Group:
14x3 SubDataFrame:
           a   b          x
[1,]     "G" "a"   0.631291
[2,]     "C" "a"   -1.45999
[3,]     "E" "a"   0.542924
[4,]     "I" "a"   0.149901
[5,]     "F" "a"  -0.485528
[6,]     "H" "a"   0.775438
[7,]     "A" "a"  -0.448951
[8,]     "H" "a"     -1.458
[9,]     "D" "a"  -0.768929
[10,]    "B" "a"  -0.398748
[11,]    "H" "a"    1.00184
[12,]    "G" "a"   -1.43405
[13,]    "I" "a"   0.434751
[14,]    "D" "a" -0.0922058
       :
       :
Last Group:
8x3 SubDataFrame:
          a   b         x
[1,]    "F" "e"  -1.33165
[2,]    "G" "e"  -2.63399
[3,]    "A" "e"   -2.7486
[4,]    "C" "e" -0.418066
[5,]    "G" "e"  -1.62673
[6,]    "D" "e"  0.658323
[7,]    "J" "e"   1.97162
[8,]    "E" "e" -0.270859


julia> # Iterations or references to a GroupedDataFrame return a

julia> # SubDataFrame, a very useful way to subset a DataFrame without

julia> # copies

julia> gd[1]
14x3 SubDataFrame:
           a   b          x
[1,]     "G" "a"   0.631291
[2,]     "C" "a"   -1.45999
[3,]     "E" "a"   0.542924
[4,]     "I" "a"   0.149901
[5,]     "F" "a"  -0.485528
[6,]     "H" "a"   0.775438
[7,]     "A" "a"  -0.448951
[8,]     "H" "a"     -1.458
[9,]     "D" "a"  -0.768929
[10,]    "B" "a"  -0.398748
[11,]    "H" "a"    1.00184
[12,]    "G" "a"   -1.43405
[13,]    "I" "a"   0.434751
[14,]    "D" "a" -0.0922058


julia> 

julia> # Look at the structure of a GroupedDataFrame

julia> dump(gd)
GroupedDataFrame 
  parent: DataFrame  50 observations of 3 variables
    a: DataArray{ASCIIString,1}(50) ["G","I","D","A"]
    b: DataArray{ASCIIString,1}(50) ["a","d","c","b"]
    x: DataArray{Float64,1}(50) [0.6312912597615505,-1.233726144013923,-0.8585845030585529,0.25826607803995216]
  cols: Array(ASCIIString,(1,)) ["b"]
  idx: Array(Int64,(50,)) [1, 5, 10, 12, 18, 19, 24, 29, 33, 37  …  49, 8, 11, 14, 15, 21, 23, 26, 50]
  starts: Array(Int64,(5,)) [1, 15, 26, 34, 43]
  ends: Array(Int64,(5,)) [14, 25, 33, 42, 50]

julia> 

julia> # Look at the structure and internals of the SubDataFrame for the

julia> # first group

julia> dump(gd[1])
SubDataFrame  14 observations of 3 variables
  a: DataArray{ASCIIString,1}(14) ["G","C","E","I"]
  b: DataArray{ASCIIString,1}(14) ["a","a","a","a"]
  x: DataArray{Float64,1}(14) [0.6312912597615505,-1.459989295058019,0.5429237557634455,0.14990144064954639]

julia> idump(gd[1])
SubDataFrame 
  parent: DataFrame 
    columns: Array(Any,(3,))
      1: DataArray{ASCIIString,1} 
        data: Array(ASCIIString,(50,)) ["G", "I", "D", "A", "C", "J", "B"  …  "A", "C", "J", "B", "F", "H", "E"]
        na: BitArray{1} 
          chunks: Array(Uint64,(1,)) [0x0000000000000000]
          dims: Array(Int64,(1,)) [50]
      2: DataArray{ASCIIString,1} 
        data: Array(ASCIIString,(50,)) ["a", "d", "c", "b", "a", "d", "b"  …  "b", "b", "b", "c", "b", "d", "e"]
        na: BitArray{1} 
          chunks: Array(Uint64,(1,)) [0x0000000000000000]
          dims: Array(Int64,(1,)) [50]
      3: DataArray{Float64,1} 
        data: Array(Float64,(50,)) [0.631291, -1.23373, -0.858585, 0.258266  …  -0.15506, 0.0607178, -0.270859]
        na: BitArray{1} 
          chunks: Array(Uint64,(1,)) [0x0000000000000000]
          dims: Array(Int64,(1,)) [50]
    colindex: Index 
      lookup: Dict{Union(ASCIIString,UTF8String),Union(Real,AbstractArray{Real,1})} 
        slots: Array(Uint8,(16,)) [0x00, 0x00, 0x00, 0x00, 0x00, 0x00  …  0x01, 0x01, 0x00, 0x00, 0x00, 0x00]
        keys: Array(Union(ASCIIString,UTF8String),(16,)) [#undef, #undef, #undef, #undef  …  "x", #undef, #undef, #undef, #undef]
        vals: Array(Union(Real,AbstractArray{Real,1}),(16,)) [#undef, #undef, #undef, #undef  …  3, #undef, #undef, #undef, #undef]
        ndel: Int64 0
        count: Int64 3
        deleter: identity
      names: Array(Union(ASCIIString,UTF8String),(3,)) ["a", "b", "x"]
  rows: Array(Int64,(14,)) [1, 5, 10, 12, 18, 19, 24, 29, 33, 37, 39, 41, 42, 43]

julia> 

julia> # You can iterate over a GroupedDataFrame or perform other operations

julia> # Here's within:

julia> within(gd, :( x_sum = sum(x) ))
50x4 DataFrame:
           a   b          x    x_sum
[1,]     "G" "a"   0.631291 -3.01026
[2,]     "C" "a"   -1.45999 -3.01026
[3,]     "E" "a"   0.542924 -3.01026
[4,]     "I" "a"   0.149901 -3.01026
[5,]     "F" "a"  -0.485528 -3.01026
[6,]     "H" "a"   0.775438 -3.01026
[7,]     "A" "a"  -0.448951 -3.01026
[8,]     "H" "a"     -1.458 -3.01026
[9,]     "D" "a"  -0.768929 -3.01026
[10,]    "B" "a"  -0.398748 -3.01026
[11,]    "H" "a"    1.00184 -3.01026
[12,]    "G" "a"   -1.43405 -3.01026
[13,]    "I" "a"   0.434751 -3.01026
[14,]    "D" "a" -0.0922058 -3.01026
[15,]    "A" "b"   0.258266 0.851169
[16,]    "B" "b"    -1.2459 0.851169
[17,]    "E" "b"    1.79734 0.851169
[18,]    "G" "b"  -0.906487 0.851169
[19,]    "I" "b"   -0.97022 0.851169
[20,]    "C" "b"   0.475183 0.851169
  :
[31,]    "J" "c"   0.638632 -5.45193
[32,]    "F" "c" -0.0803466 -5.45193
[33,]    "B" "c"  -0.914724 -5.45193
[34,]    "I" "d"   -1.23373  0.60101
[35,]    "J" "d"   0.509527  0.60101
[36,]    "H" "d"   -1.36073  0.60101
[37,]    "B" "d"    1.15406  0.60101
[38,]    "E" "d"    0.75606  0.60101
[39,]    "C" "d"    1.18512  0.60101
[40,]    "B" "d" -0.0740195  0.60101
[41,]    "A" "d"  -0.395993  0.60101
[42,]    "H" "d"  0.0607178  0.60101
[43,]    "F" "e"   -1.33165 -6.39996
[44,]    "G" "e"   -2.63399 -6.39996
[45,]    "A" "e"    -2.7486 -6.39996
[46,]    "C" "e"  -0.418066 -6.39996
[47,]    "G" "e"   -1.62673 -6.39996
[48,]    "D" "e"   0.658323 -6.39996
[49,]    "J" "e"    1.97162 -6.39996
[50,]    "E" "e"  -0.270859 -6.39996


julia> 
