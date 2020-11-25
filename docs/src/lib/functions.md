```@meta
CurrentModule = DataFrames
```

# Functions

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
repeat
repeat!
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
delete!
empty
empty!
filter
filter!
first
last
only
nonunique
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

## Multithreading
```@docs
DataFrames.nthreads
DataFrames.nthreads!
```