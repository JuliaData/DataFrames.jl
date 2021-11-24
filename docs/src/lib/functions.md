```@meta
CurrentModule = DataFrames
```

# Functions

## Multi-threading support

Selected operations in DataFrames.jl automatically use multiple threads when available.
It is task-based and implemented using the `@spawn` macro from Julia Base.

This is a list of operations that currently make use of multi-threading:
- `DataFrame` constructor with `copycols=true`; also recursively all functions
  that call this constructor, e.g. `copy`.
- `getindex` when multiple columns are selected.
- `groupby` (both when hashing is required and when fast path using `DataAPI.refpool`
  is used).
- `join*` functions for composing output data frame (but currently not for finding
  matching rows in joined data frames).
- `combine`, `select[!]`, and `transform[!]` on `GroupedDataFrame` when either of the conditions below is met:
  * multiple transformations are performed (each transformation is spawned in a separate task)
  * a transformation produces one row per group and the passed transformation
    is a custom function (i.e. not for standard reductions, which use
    optimized single-threaded methods).

In general at least Julia 1.4 is required to ensure that multi-threading is used
and the Julia process must be started with more than one thread. Some operations
turn on multi-threading only if enough rows in the processed data frame are present
(the exact threshold when multi-threading is enabled is considered to be undefined
and might change in the future).

Except for the list above, where multi-threading is used automatically,
all functions provided by DataFrames.jl that update a data frame are not thread safe.
This means that while they can be called from any thread, the caller is responsible
for ensuring that a given `DataFrame` object is never modified by one thread while
others are using it (either for reading or writing). Using the same `DataFrame`
at the same time from different threads is safe as long as it is not modified.

## Index
```@index
Pages = ["functions.md"]
```

## Constructing data frames
```@docs
copy
similar
```

## Summary information
```@docs
describe
length
ncol
ndims
nrow
rownumber
show
size
```

## Working with column names
```@docs
names
propertynames
rename
rename!
```

## Mutating and transforming data frames and grouped data frames
```@docs
append!
combine
flatten
hcat
insertcols!
mapcols
mapcols!
push!
reduce
repeat
repeat!
reverse
select
select!
transform
transform!
vcat
```

## Reshaping data frames between tall and wide formats
```@docs
stack
unstack
permutedims
```

## Sorting
```@docs
issorted
order
sort
sort!
sortperm
```

## Joining
```@docs
antijoin
crossjoin
innerjoin
leftjoin
leftjoin!
outerjoin
rightjoin
semijoin
```

## Grouping
```@docs
get
groupby
groupcols
groupindices
keys
parent
valuecols
```

## Filtering rows
```@docs
deleteat!
empty
empty!
filter
filter!
first
last
only
nonunique
subset
subset!
unique
unique!
```

## Working with missing values
```@docs
allowmissing
allowmissing!
completecases
disallowmissing
disallowmissing!
dropmissing
dropmissing!
```

## Iteration
```@docs
eachcol
eachrow
values
pairs
```

## Equality
```@docs
isapprox
```
