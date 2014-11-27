Reshaping and Pivoting Data
===========================

Reshape data from wide to long format using the ``stack`` function::

    using DataFrames, RDatasets
    iris = dataset("datasets", "iris")
    iris[:id] = 1:size(iris, 1)  # this makes it easier to unstack
    d = stack(iris, [1:4])
    
The second optional argument to ``stack`` indicates the columns to be
stacked. These are normally referred to as the measured variables.
Column names can also be given:

    d = stack(iris, [:SepalLength, :SepalWidth, :PetalLength, :PetalWidth])
    
Note that all columns must be of the same type. Currently, there is no
type coersion.

The stacked DataFrame that results includes all of the columns not
specified to be stacked. These are repeated for each stacked column.
These are normally refered to as identifier (id) columns. In addition
to the id columns, two additional columns labeled ``:variable`` and
``:values`` contain the column identifier and the stacked columns.

A third optional argument to ``stack`` represents the id columns that
are repeated. This makes it easier to specify which variables you want
included in the long format:

    d = stack(iris, [:SepalLength, :SepalWidth], :Species)

``melt`` is an alternative function to reshape from wide to long
format. It is based on ``stack``, but it prefers specification of the
id columns as:

    d = melt(iris, :Species)

All other columns are assumed to be measured variables (they are
stacked). 
    
You can also stack an entire DataFrame. Again, the columns must be the
same type.

    d = stack(iris[1:4])

``unstack`` converts from a long format to a wide format. The default
is requires specifying which columns are an id variable, column
variable names, and column values.

    longdf = melt(iris, :id)
    widedf = unstack(longdf, :id, :variable, :value)

If the remaining columns are unique, you can skip the id variable and
use:

    widedf = unstack(longdf, :variable, :value)

Still to be documented...

    meltdf(iris, :Species)
    pivottable(melt(iris, :Species), :variable, :Species, :value, x -> mean(x))


