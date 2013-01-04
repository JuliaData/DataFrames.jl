julia> srand(1) # Set the seed.

julia> 

julia> # DataFrames can also be created with an expression.

julia> # Columns are repeated to the longest column.

julia> df = DataFrame(quote
           a = shuffle(LETTERS[1:10])
           b = letters[randi(5,50)]
           x = randn(50)
       end)
DataFrame  (50,3)
           a   b          x
[1,]     "B" "a"  -0.916177
[2,]     "C" "b"   0.483764
[3,]     "J" "b"   0.726738
[4,]     "I" "e"     1.0791
[5,]     "F" "b"   0.037687
[6,]     "G" "c"   -0.13161
[7,]     "H" "c"    1.49355
[8,]     "E" "b"   -1.09661
[9,]     "D" "e"  -0.953266
[10,]    "A" "c"   -2.61105
[11,]    "B" "b"     1.2224
[12,]    "C" "c"   0.571747
[13,]    "J" "e"  -0.906974
[14,]    "I" "a" -0.0275278
[15,]    "F" "a"   -2.58784
[16,]    "G" "a"  0.0317111
[17,]    "H" "d"   0.830985
[18,]    "E" "e"  -0.540067
[19,]    "D" "a"  -0.570632
[20,]    "A" "d"    1.14635
  :
[31,]    "B" "e"   0.323304
[32,]    "C" "a"    0.83458
[33,]    "J" "d"   -1.49795
[34,]    "I" "d"    1.31029
[35,]    "F" "e"  -0.772302
[36,]    "G" "c"    1.11484
[37,]    "H" "b"    1.14469
[38,]    "E" "a"  -0.364733
[39,]    "D" "e"  -0.564778
[40,]    "A" "e"  -0.124774
[41,]    "B" "a"  -0.722285
[42,]    "C" "d"    -1.0093
[43,]    "J" "b"  -0.856457
[44,]    "I" "d"   0.223723
[45,]    "F" "e" -0.0164157
[46,]    "G" "e"    0.28588
[47,]    "H" "a"  -0.366278
[48,]    "E" "e"    1.07446
[49,]    "D" "e"   0.465938
[50,]    "A" "d"   0.598922


julia> 

julia> # Grouping by column b, find the sum and length of each group.

julia> by(df, "b", :( x_sum = sum(x); x_len = length(x) )) 
DataFrame  (5,3)
          b    x_sum x_len
[1,]    "a" -6.47425    10
[2,]    "b"  2.42707     9
[3,]    "c"  1.24855     9
[4,]    "d"  2.75966     8
[5,]    "e" -1.00391    14


julia> 

julia> # Group by a and b:

julia> by(df, ["a", "b"], :( x_sum = sum(x); x_len = length(x) )) 
DataFrame  (34,4)
           a   b      x_sum x_len
[1,]     "B" "a"   -1.63846     2
[2,]     "C" "a"    0.83458     1
[3,]     "D" "a"  -0.570632     1
[4,]     "E" "a"   -2.14981     2
[5,]     "F" "a"   -2.58784     1
[6,]     "G" "a"  0.0317111     1
[7,]     "H" "a"  -0.366278     1
[8,]     "I" "a" -0.0275278     1
[9,]     "A" "b" -0.0360266     1
[10,]    "B" "b"     1.2224     1
[11,]    "C" "b"   0.483764     1
[12,]    "E" "b"   -1.09661     1
[13,]    "F" "b"   0.037687     1
[14,]    "H" "b"    1.14469     1
[15,]    "J" "b"   0.671162     3
[16,]    "A" "c"   -2.61105     1
[17,]    "C" "c"   0.255827     2
[18,]    "G" "c"   0.819058     3
[19,]    "H" "c"     1.7849     2
[20,]    "I" "c"   0.999815     1
[21,]    "A" "d"    1.74527     2
[22,]    "C" "d"    -1.0093     1
[23,]    "F" "d"    1.15665     1
[24,]    "H" "d"   0.830985     1
[25,]    "I" "d"    1.53401     2
[26,]    "J" "d"   -1.49795     1
[27,]    "A" "e"  -0.124774     1
[28,]    "B" "e" -0.0247522     2
[29,]    "D" "e"   -1.05807     4
[30,]    "E" "e"   0.534396     2
[31,]    "F" "e"  -0.788718     2
[32,]    "G" "e"    0.28588     1
[33,]    "I" "e"     1.0791     1
[34,]    "J" "e"  -0.906974     1


julia> 

julia> #

julia> # Digging deeper.

julia> #

julia> 

julia> # by is a shortcut for the following:

julia> based_on(groupby(df, "b"), :( x_sum = sum(x); x_len = length(x) )) 
DataFrame  (5,3)
          b    x_sum x_len
[1,]    "a" -6.47425    10
[2,]    "b"  2.42707     9
[3,]    "c"  1.24855     9
[4,]    "d"  2.75966     8
[5,]    "e" -1.00391    14


julia> 

julia> # You can also use the piping operator for the same thing.

julia> df | groupby("b") | :( x_sum = sum(x); x_len = length(x) ) 
DataFrame  (5,3)
          b    x_sum x_len
[1,]    "a" -6.47425    10
[2,]    "b"  2.42707     9
[3,]    "c"  1.24855     9
[4,]    "d"  2.75966     8
[5,]    "e" -1.00391    14


julia> 

julia> # groupby returns a GroupedDataFrame.

julia> gd = groupby(df, "b")
GroupedDataFrame  5 groups with keys: ["b"]
First Group:
SubDataFrame  (10,3)
           a   b          x
[1,]     "B" "a"  -0.916177
[2,]     "I" "a" -0.0275278
[3,]     "F" "a"   -2.58784
[4,]     "G" "a"  0.0317111
[5,]     "D" "a"  -0.570632
[6,]     "E" "a"   -1.78508
[7,]     "C" "a"    0.83458
[8,]     "E" "a"  -0.364733
[9,]     "B" "a"  -0.722285
[10,]    "H" "a"  -0.366278
       :
       :
Last Group:
SubDataFrame  (14,3)
           a   b           x
[1,]     "I" "e"      1.0791
[2,]     "D" "e"   -0.953266
[3,]     "J" "e"   -0.906974
[4,]     "E" "e"   -0.540067
[5,]     "B" "e"   -0.348056
[6,]     "D" "e" -0.00596024
[7,]     "B" "e"    0.323304
[8,]     "F" "e"   -0.772302
[9,]     "D" "e"   -0.564778
[10,]    "A" "e"   -0.124774
[11,]    "F" "e"  -0.0164157
[12,]    "G" "e"     0.28588
[13,]    "E" "e"     1.07446
[14,]    "D" "e"    0.465938


julia> 

julia> # Iterations or references to a GroupedDataFrame return a

julia> # SubDataFrame, a very useful way to subset a DataFrame without

julia> # copies.

julia> gd[1]
SubDataFrame  (10,3)
           a   b          x
[1,]     "B" "a"  -0.916177
[2,]     "I" "a" -0.0275278
[3,]     "F" "a"   -2.58784
[4,]     "G" "a"  0.0317111
[5,]     "D" "a"  -0.570632
[6,]     "E" "a"   -1.78508
[7,]     "C" "a"    0.83458
[8,]     "E" "a"  -0.364733
[9,]     "B" "a"  -0.722285
[10,]    "H" "a"  -0.366278


julia> 

julia> # Look at the structure of a GroupedDataFrame.

julia> dump(gd)
GroupedDataFrame 
  parent: DataFrame  50 observations of 3 variables
    a: DataVector{ASCIIString}(50) ["B","C","J","I"]
    b: DataVector{ASCIIString}(50) ["a","b","b","e"]
    x: DataVector{Float64}(50) [-0.9161768852275985,0.48376352282942453,0.7267382683529503,1.0790964781651111]
  cols: Array(ASCIIString,(1,)) ["b"]
  idx: Array(Int64,(50,)) [1, 14, 15, 16]
  starts: Array(Int64,(5,)) [1, 11, 20, 29]
  ends: Array(Int64,(5,)) [10, 19, 28, 36]

julia> 

julia> # Look at the structure and internals of the SubDataFrame for the

julia> # first group.

julia> dump(gd[1])
SubDataFrame  10 observations of 3 variables
  a: DataVector{ASCIIString}(10) ["B","I","F","G"]
  b: DataVector{ASCIIString}(10) ["a","a","a","a"]
  x: DataVector{Float64}(10) [-0.9161768852275985,-0.02752775061478325,-2.5878352798283024,0.03171114873790696]

julia> idump(gd[1])
SubDataFrame 
  parent: DataFrame 
    columns: Array(Any,(3,))
      1: DataVector{ASCIIString} 
        data: Array(ASCIIString,(50,)) ["B", "C", "J", "I"]
        na: Array(Bool,(50,)) [false, false, false, false]
        filter: Bool false
        replace: Bool false
        replaceVal: ASCIIString 
          data: Array(Uint8,(0,)) []
      2: DataVector{ASCIIString} 
        data: Array(ASCIIString,(50,)) ["a", "b", "b", "e"]
        na: Array(Bool,(50,)) [false, false, false, false]
        filter: Bool false
        replace: Bool false
        replaceVal: ASCIIString 
          data: Array(Uint8,(0,)) []
      3: DataVector{Float64} 
        data: Array(Float64,(50,)) [-0.916177, 0.483764, 0.726738, 1.0791]
        na: Array(Bool,(50,)) [false, false, false, false]
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
          8: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          9: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          10: ASCIIString 
        vals: Array(Any,(16,))
          1: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          2: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          3: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          4: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          5: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          6: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          7: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          8: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          9: Symbol __c782dbf1cf4d6a2e5e3865d7e95634f2e09b5902__
          10: Int64 1
        ndel: Int64 0
        deleter: identity
      names: Array(Union(UTF8String,ASCIIString),(3,)) {"a", "b", "x"}
  rows: Array(Int64,(10,)) [1, 14, 15, 16]

julia> 

julia> # You can iterate over a GroupedDataFrame or perform other operations.

julia> # Here's within.

julia> 

julia> within(gd, :( x_sum = sum(x) ))
DataFrame  (50,4)
           a   b           x    x_sum
[1,]     "B" "a"   -0.916177 -6.47425
[2,]     "I" "a"  -0.0275278 -6.47425
[3,]     "F" "a"    -2.58784 -6.47425
[4,]     "G" "a"   0.0317111 -6.47425
[5,]     "D" "a"   -0.570632 -6.47425
[6,]     "E" "a"    -1.78508 -6.47425
[7,]     "C" "a"     0.83458 -6.47425
[8,]     "E" "a"   -0.364733 -6.47425
[9,]     "B" "a"   -0.722285 -6.47425
[10,]    "H" "a"   -0.366278 -6.47425
[11,]    "C" "b"    0.483764  2.42707
[12,]    "J" "b"    0.726738  2.42707
[13,]    "F" "b"    0.037687  2.42707
[14,]    "E" "b"    -1.09661  2.42707
[15,]    "B" "b"      1.2224  2.42707
[16,]    "J" "b"     0.80088  2.42707
[17,]    "A" "b"  -0.0360266  2.42707
[18,]    "H" "b"     1.14469  2.42707
[19,]    "J" "b"   -0.856457  2.42707
[20,]    "G" "c"    -0.13161  1.24855
  :
[31,]    "F" "d"     1.15665  2.75966
[32,]    "J" "d"    -1.49795  2.75966
[33,]    "I" "d"     1.31029  2.75966
[34,]    "C" "d"     -1.0093  2.75966
[35,]    "I" "d"    0.223723  2.75966
[36,]    "A" "d"    0.598922  2.75966
[37,]    "I" "e"      1.0791 -1.00391
[38,]    "D" "e"   -0.953266 -1.00391
[39,]    "J" "e"   -0.906974 -1.00391
[40,]    "E" "e"   -0.540067 -1.00391
[41,]    "B" "e"   -0.348056 -1.00391
[42,]    "D" "e" -0.00596024 -1.00391
[43,]    "B" "e"    0.323304 -1.00391
[44,]    "F" "e"   -0.772302 -1.00391
[45,]    "D" "e"   -0.564778 -1.00391
[46,]    "A" "e"   -0.124774 -1.00391
[47,]    "F" "e"  -0.0164157 -1.00391
[48,]    "G" "e"     0.28588 -1.00391
[49,]    "E" "e"     1.07446 -1.00391
[50,]    "D" "e"    0.465938 -1.00391
