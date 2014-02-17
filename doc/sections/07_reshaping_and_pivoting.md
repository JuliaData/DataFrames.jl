# Reshaping and Pivoting Data

Reshape data using the `stack` function:

    using DataFrames, RDatasets

    iris = dataset("datasets", "iris")

    stack(iris, :SepalLength)
