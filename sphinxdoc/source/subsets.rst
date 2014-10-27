Data Structures
===============
In this section we will quickly go over datastructures in the DataFrames package. We will go over the two basic types of
data structures the DataArray and the DataFrame. To start you must import the DataFrames package::

using DataArrays, DataFrames

DataArrays
==========

The ``DataArray`` type is meant to behave like a standard Julia ``Array`` and
tries to implement identical indexing rules:

One dimensional ``DataArray``::

  julia> using DataArrays

  julia> dv = data([1, 2, 3])
  3-element DataArray{Int64,1}:
   1
   2
   3

  julia> dv[1]
  1

  julia> dv[2] = NA
  NA

  julia> dv[2]
  NA


Two dimensional ``DataArray``::

  julia> using DataArrays

  julia> dm = data([1 2; 3 4])
  2x2 DataArray{Int64,2}:
   1  2
   3  4

  julia> dm[1, 1]
  1

  julia> dm[2, 1] = NA
  NA

  julia> dm[2, 1]
  NA

DataFrames
==========

The ``DataFrame`` offers substantially more forms of indexing because columns can be referred to by name. The data frame can store integers, floats, and strings.
The can be created by the command `DataFrame`::

  df = DataFrame(A = 1:10, B = 2:2:20)

Output::
  
  10x2 DataFrame
  |-------|----|----|
  | Row # | A  | B  |
  | 1     | 1  | 2  |
  | 2     | 2  | 4  |
  | 3     | 3  | 6  |
  | 4     | 4  | 8  |
  | 5     | 5  | 10 |
  | 6     | 6  | 12 |
  | 7     | 7  | 14 |
  | 8     | 8  | 16 |
  | 9     | 9  | 18 |
  | 10    | 10 | 20 |


Empty DataFrames can also be created::

df = DataFrame()

Copying Dataframes
------------------

+------------+------------------------------------------------------------+
|copy(df)    | Creates a copy of the `df` where the columns are referenced|
+------------+------------------------------------------------------------+
|deepcopy(df)| Creates a physical copy of `df`.                           |
+------------+------------------------------------------------------------+

Determining Dimensions
----------------------
The following commands are used to determine the length of row and columns or the dimensions in a DataFrame

+------------+------------------------------------------------------------------------------------+
|ndims(df)   | Returns the dimensions of df                                                       |
+------------+------------------------------------------------------------------------------------+
|size(df)    | Returns the dimensions of df                                                       |
+------------+------------------------------------------------------------------------------------+
|ncol(df)    | Returns the number of columns in df                                                |
+------------+------------------------------------------------------------------------------------+
|length(df)  | Returns the number of columns in df                                                |
+------------+------------------------------------------------------------------------------------+
|nrow(df)    | Returns the number of rows in df                                                   |
+------------+------------------------------------------------------------------------------------+
|isempty(df) | Returns a boolean expression that checks if the number of columns is equal to zero.|
+------------+------------------------------------------------------------------------------------+

Refering to the first column by index or name::

  julia> df[1]
  10-element DataArray{Int64,1}:
    1
    2
    3
    4
    5
    6
    7
    8
    9
   10

  julia> df[:A]
  10-element DataArray{Int64,1}:
    1
    2
    3
    4
    5
    6
    7
    8
    9
   10

Refering to the first element of the first column::

  julia> df[1, 1]
  1

  julia> df[1, :A]
  1


Selecting a subset of rows by index and an (ordered) subset of columns by name::

  julia> df[1:3, [:A, :B]]
  3x2 DataFrame
  |-------|---|---|
  | Row # | A | B |
  | 1     | 1 | 2 |
  | 2     | 2 | 4 |
  | 3     | 3 | 6 |

  julia> df[1:3, [:B, :A]]
  3x2 DataFrame
  |-------|---|---|
  | Row # | B | A |
  | 1     | 2 | 1 |
  | 2     | 4 | 2 |
  | 3     | 6 | 3 |

Selecting a subset of rows by using a condition::

  julia> df[df[:A] % 2 .== 0, :]
  5x2 DataFrame
  |-------|----|----|
  | Row # | A  | B  |
  | 1     | 2  | 4  |
  | 2     | 4  | 8  |
  | 3     | 6  | 12 |
  | 4     | 8  | 16 |
  | 5     | 10 | 20 |

  julia> df[df[:B] % 2 .== 0, :]
  10x2 DataFrame
  |-------|----|----|
  | Row # | A  | B  |
  | 1     | 1  | 2  |
  | 2     | 2  | 4  |
  | 3     | 3  | 6  |
  | 4     | 4  | 8  |
  | 5     | 5  | 10 |
  | 6     | 6  | 12 |
  | 7     | 7  | 14 |
  | 8     | 8  | 16 |
  | 9     | 9  | 18 |
  | 10    | 10 | 20 |
