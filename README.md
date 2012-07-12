JuliaData
=========

Library for working with tabular data in Julia using `DataFrame`'s.

## Demo 1

Basic demo of `DataFrame` usage:

```julia
    # Load DataFrame package.
    load("src/init.jl")

    # Load a CSV file into a DataFrame.
    df = csvDataFrame("demo/toy_example.csv")

    # Basic indexing.
    df[1, :]
    df["A"]
    df[1, "A"]
    df[1:2, "A"]
    df[1:2, ["A", "B"]]

    # Use the with() function to evaluate expressions relative to a DataFrame.
    with(df, :(A + C))
```

## Demo 2

Demo of using `DataFrame`'s for regression modeling:

```julia
    # Load DataFrame package.
    load("src/init.jl")

    # Load a very simple lm() function.
    load("demo/lm.jl")

    # Load a CSV file into a DataFrame.
    df = csvDataFrame("demo/toy_example.csv")

    # Run a simple linear model that predicts A using B and C.
    lm_fit = lm(:(A ~ B + C), df)

    # Print out a summary of the results.
    print(lm_fit)

    #
    # Behind The Scenes
    #

    # Generate a Formula object.
    f = Formula(:(A ~ B + C))

    # Generate a ModelFrame object.
    mf = model_frame(f, df)

    # Generate a ModelMatrix object.
    mm = model_matrix(mf)
```
