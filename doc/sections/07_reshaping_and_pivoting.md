# Reshaping and Pivoting Data

Reshape data using the `stack` function:

    using DataFrames, RDatasets

    iris = data("datasets", "iris")

    stack(iris, "SepalLength")
