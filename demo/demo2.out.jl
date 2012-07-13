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

julia> # Load a very simple lm() function.

julia> load("demo/lm.jl")

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
 0.516667,[0.0726483, 0.0408248, 0.0408248, 0.0166667],4x1 Float64 Array:
 13.5355
 47.765 
 23.2702
 31.0   ,4x1 Float64 Array:
 0.00541392
 0.00043802
 0.00184162
 0.00103896,6x1 Float64 Array:
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

julia> # Print out a summary of the results.

julia> print(lm_fit)


Call:
lm(Formula([A],[+(B,C)]))

Coefficients:

        Term     Estimate   Std. Error      t value     Pr(>|t|)
 (Intercept)       0.983333       0.072648    13.535528        0.005414 **
     B:Three      1.950000       0.040825    47.765050         0.000438 ***
       B:Two       0.950000       0.040825    23.270153        0.001842 **
           C       0.516667       0.016667    31.000000        0.001039 **
---
Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1

Adjusted R-squared: 0.9985


julia> #

julia> # Behind The Scenes

julia> #

julia> 

julia> # Generate a Formula object.

julia> f = Formula(:(A ~ B + C))
Formula([A],[+(B,C)])

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
