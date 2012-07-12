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

julia> # Load a very simple lm() function.

julia> load("demo/lm.jl")

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

julia> # Run a simple linear model that predicts A using B and C.

julia> lm_fit = lm(:(A ~ B + C), df)
OLSResults(Formula([A],[+(B,C)]),6x4 Float64 Array:
 1.0  0.0  0.0  3.0
 1.0  0.0  0.0  5.0
 1.0  0.0  1.0  3.0
 1.0  0.0  1.0  5.0
 1.0  1.0  0.0  3.0
 1.0  1.0  0.0  5.0,{"(Intercept)", "B:Three", "B:Two", "C"},6x1 Float64 Array:
 2.5
 3.6
 3.5
 4.5
 4.5
 5.5,4x1 Float64 Array:
 0.983333
 1.95    
 0.95    
 0.516667,[0.00527778, 0.00166667, 0.00166667, 0.000277778],4x1 Float64 Array:
  456.379
 2865.9  
 1396.21 
 4556.05 ,4x1 Float64 Array:
  456.379
 2865.9  
 1396.21 
 4556.05 ,6x1 Float64 Array:
 2.53333
 3.56667
 3.48333
 4.51667
 4.48333
 5.51667,6x1 Float64 Array:
 -0.0333333
  0.0333333
  0.0166667
 -0.0166667
  0.0166667
 -0.0166667,0.9984591679506933)

julia> 

julia> # Print out a summary of the results.

julia> print(lm_fit)


Call to lm(): Formula([A],[+(B,C)])

Fitted Model:

             Term  Estimate  Std. Error         t   p-Value
      (Intercept)  0.9833333  0.0052778  456.3786152  456.3786152
          B:Three  1.9500000  0.0016667  2865.9029991  2865.9029991
            B:Two  0.9500000  0.0016667  1396.2091534  1396.2091534
                C  0.5166667  0.0002778  4556.0509216  4556.0509216

R-squared: 0.9985


julia> 

julia> #

julia> # Behind The Scenes

julia> #

julia> 

julia> # Generate a Formula object.

julia> f = Formula(:(A ~ B + C))
Formula([A],[+(B,C)])

julia> 

julia> # Generate a ModelFrame object.

julia> mf = model_frame(f, df)
ModelFrame(DataFrame  (6,3)
          A       B   C
[1,]    2.5   "One" 3.0
[2,]    3.6   "One" 5.0
[3,]    3.5   "Two" 3.0
[4,]    4.5   "Two" 5.0
[5,]    4.5 "Three" 3.0
[6,]    5.5 "Three" 5.0
,[1],Formula([A],[+(B,C)]))

julia> 

julia> # Generate a ModelMatrix object.

julia> mm = model_matrix(mf)
ModelMatrix(6x4 Float64 Array:
 1.0  0.0  0.0  3.0
 1.0  0.0  0.0  5.0
 1.0  0.0  1.0  3.0
 1.0  0.0  1.0  5.0
 1.0  1.0  0.0  3.0
 1.0  1.0  0.0  5.0,6x1 Float64 Array:
 2.5
 3.6
 3.5
 4.5
 4.5
 5.5,{"(Intercept)", "B:Three", "B:Two", "C"},{"A"})
