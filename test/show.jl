module TestShow
    using DataFrames
    using RDatasets
    movies = data("ggplot2", "movies")
    movies = movies[1:50, :]
    show(movies)
    show(movies, true)
    showall(movies)
    showall(movies, true)

    subdf = select(:(rating .> 6.4), movies)
    show(subdf)
    show(subdf, true)
    showall(subdf)
    showall(subdf, true)
end
