# Reshaping and Pivoting Data

    require("DataFrames")
    using DataFrames

    require("RDatasets")
    using RDatasets

    iris = data("datasets", "iris")

    stack(iris, "Sepal.Length")
