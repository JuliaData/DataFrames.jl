# The Split-Apply-Combine Strategy

    require("DataFrames")
    using DataFrames

    require("RDatasets")
    using RDatasets

    iris = data("datasets", "iris")

    by(iris, "Species", nrow)
    by(iris, "Species", df -> mean(df["Petal.Length"]))
    by(iris, "Species", :(N = nrow(_DF)))
