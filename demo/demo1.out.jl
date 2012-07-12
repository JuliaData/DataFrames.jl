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

julia> # Basic indexing.

julia> df[1, :]
DataFrame  (1,3)
          A     B   C
[1,]    2.5 "One" 3.0


julia> df["A"]
[2.5,3.6,3.5,4.5,4.5,5.5]

julia> df[1, "A"]
2.5

julia> df[1:2, "A"]
[2.5,3.6]

julia> df[1:2, ["A", "B"]]
DataFrame  (2,2)
          A     B
[1,]    2.5 "One"
[2,]    3.6 "One"


julia> 

julia> # Use the with() function to evaluate expressions relative to a DataFrame.

julia> with(df, :(A + C))
[5.5,8.6,6.5,9.5,7.5,10.5]
