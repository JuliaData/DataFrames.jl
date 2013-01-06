julia> # Load DataFrame package

julia> require("DataFrames")

julia> using DataFrames

julia> 

julia> # Load a CSV file into a DataFrame

julia> df = read_table("demo/toy_example.csv")
6x3 DataFrame:
          A       B   C
[1,]    2.5   "One" 3.0
[2,]    3.6   "One" 5.0
[3,]    3.5   "Two" 3.0
[4,]    4.5   "Two" 5.0
[5,]    4.5 "Three" 3.0
[6,]    5.5 "Three" 5.0


julia> 

julia> # Basic indexing

julia> df[1, :]
1x3 DataFrame:
          A     B   C
[1,]    2.5 "One" 3.0


julia> df["A"]
6-element Float64 DataArray
 2.5
 3.6
 3.5
 4.5
 4.5
 5.5

julia> df[1, "A"]
2.5

julia> df[1:2, "A"]
2-element Float64 DataArray
 2.5
 3.6

julia> df[1:2, ["A", "B"]]
2x2 DataFrame:
          A     B
[1,]    2.5 "One"
[2,]    3.6 "One"


julia> 

julia> # Use the with() function to evaluate expressions relative to a DataFrame 

julia> with(df, :(A + C))
6-element Float64 DataArray
 5.5
 8.6
 6.5
 9.5
 7.5
 10.5

julia> 
