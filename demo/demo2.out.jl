julia> # Load DataFrame package

julia> require("DataFrames")

julia> using DataFrames

julia> 

julia> # Load a very simple lm() function

julia> require(Pkg.dir("DataFrames", "demo", "lm.jl"))

julia> 

julia> # Load a CSV file into a DataFrame

julia> df = read_table(Pkg.dir("DataFrames", "demo", "toy_example.csv"))
6x3 DataFrame:
          A       B   C
[1,]    2.5   "One" 3.0
[2,]    3.6   "One" 5.0
[3,]    3.5   "Two" 3.0
[4,]    4.5   "Two" 5.0
[5,]    4.5 "Three" 3.0
[6,]    5.5 "Three" 5.0


julia> 

julia> # Run a simple linear model that predicts A using B and C

julia> lm_fit = lm(:(A ~ B + C), df)
ArgumentError("float64(String): invalid number format")
 in float64 at string.jl:1209
 in float at string.jl:1222
 in model_matrix at /Users/johnmyleswhite/.julia/DataFrames/src/formula.jl:141
 in lm at /Users/johnmyleswhite/.julia/DataFrames/demo/lm.jl:66

julia> 

julia> # Print out a summary of the results

julia> print(lm_fit)
lm_fit not defined

julia> 

julia> #

julia> # Behind The Scenes

julia> #

julia> 

julia> # Generate a Formula object

julia> f = Formula(:(A ~ B + C))
Formula([:A],[:(+(B,C))])

julia> 

julia> # Generate a ModelFrame object

julia> mf = model_frame(f, df)
ModelFrame(6x3 DataFrame:
          A       B   C
[1,]    2.5   "One" 3.0
[2,]    3.6   "One" 5.0
[3,]    3.5   "Two" 3.0
[4,]    4.5   "Two" 5.0
[5,]    4.5 "Three" 3.0
[6,]    5.5 "Three" 5.0
,[1],Formula([:A],[:(+(B,C))]))

julia> 

julia> # Generate a ModelMatrix object

julia> mm = model_matrix(mf)
ArgumentError("float64(String): invalid number format")
 in float64 at string.jl:1209
 in float at string.jl:1222
 in model_matrix at /Users/johnmyleswhite/.julia/DataFrames/src/formula.jl:141

julia> 
