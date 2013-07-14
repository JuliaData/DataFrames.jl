---

layout: minimal
title: Split-Apply-Combine Operations

---

# Reshaping and Pivoting Data

    require("DataFrames")
    using DataFrames

    require("RDatasets")
    using RDatasets

    iris = data("datasets", "iris")

    stack(iris, "Sepal.Length")
