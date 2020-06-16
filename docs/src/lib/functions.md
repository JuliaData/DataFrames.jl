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
completecases
delete!
dropmissing
dropmissing!
empty
empty!
filter
filter!
first
last
nonunique
unique
unique!
```

## Changing column types
```@docs
allowmissing
allowmissing!
categorical
categorical!
disallowmissing
disallowmissing!
```

## Iteration
```@docs
eachcol
eachrow
values
pairs
```
