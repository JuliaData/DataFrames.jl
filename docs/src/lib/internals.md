```@meta
CurrentModule = DataFrames
```

# Internals

!!! warning "Internal API"

    The functions, methods and types listed on this page are internal to DataFrames and are
    **not considered to be part of the public API**.

```@docs
compacttype
gennames
getmaxwidths
ourshow
ourstrwidth
@spawn_for_chunks
default_table_transformation
table_transformation
isreduction
```

When `AsTable` is used as source column selector in the
`source => function => target` mini-language supported by `select` and related
functions it is possible to override the default processing performed by
function `function` by adding a [`table_transformation`](@ref) method for this
function. This is most useful for custom reductions over columns of `NamedTuple`
created by `AsTable`, especially in cases when the user expects that very many
columns (over 1000 as a rule of thumb) would be selected by `AsTable` selector in which
case avoiding creation of `NamedTuple` object significantly reduces compilation
time (which is often longer than computation time in such cases).
