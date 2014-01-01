# AbstractDataFrame

An AbstractDataFrame is an in-memory database that consists of named columns, each of which can contain missing data. Every column has a well-defined type, but different columns can have different types. An AbstractDataFrame can be accessed using numeric indexing for both rows and columns and name-based
indexing for columns.

Current subtypes of AbstractDataFrame include DataFrame and SubDataFrame.

# DataFrame

A DataFrame is a vector of heterogeneous AbstractDataVector's that be accessed using numeric indexing for both rows and columns and name-based indexing for columns. The columns are stored in a vector, which means that operations that insert/delete columns are O(n).
