# Load DataFrame package
require("DataFrames")
using DataFrames

# Load a very simple lm() function
require(joinpath(julia_pkgdir(), "DataFrames", "demo", "lm.jl"))

# Load a CSV file into a DataFrame
df = read_table(joinpath(julia_pkgdir(), "DataFrames", "demo", "toy_example.csv"))

# Run a simple linear model that predicts A using B and C
lm_fit = lm(:(A ~ B + C), df)

# Print out a summary of the results
print(lm_fit)

#
# Behind The Scenes
#

# Generate a Formula object
f = Formula(:(A ~ B + C))

# Generate a ModelFrame object
mf = model_frame(f, df)

# Generate a ModelMatrix object
mm = model_matrix(mf)
