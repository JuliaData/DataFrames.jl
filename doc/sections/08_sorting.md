# Sorting

Sorting is a fundamental component of data analysis.  Basic sorting is
trivial: just calling `sort!` will sort all columns, in place.

    using DataFrames, RDatasets

    iris = dataset("datasets", "iris")
    sort!(iris)

In Sorting DataFrames, you may want to sort different columns with
different options.  Here are some examples showing most of the
possible options.

    sort!(iris, rev = true)

    sort!(iris, cols = [:SepalWidth, :SepalLength])

    sort!(iris, cols = [order(:Species, by = uppercase),
                        order(:SepalLength, rev = true)])

Keywords used above include `cols` (to specify columns), `rev` (to
sort a column or the whole DataFrame in reverse), and `by` (to apply a
function to a column/DataFrame).  Each keyword can either be a single
value, or can be a tuple or array, with values corresponding to
individual columns.

As an alternative to using array or tuple values, `order` to specify
an ordering for a particular column within a set of columns

The following two examples show two ways to sort the `iris` dataset
with the same result: `Species` will be ordered in reverse
lexicographic order, and within species, rows will be sorted by
increasing sepal length and width.

    sort!(iris, cols = (:Species, :SepalLength, :SepalWidth),
                rev = (true, false, false))

    sort!(iris,
          cols = (order(:Species, rev = true), :SepalLength, :SepalWidth))
