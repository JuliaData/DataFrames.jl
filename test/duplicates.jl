df = DataFrame({"a" => [1, 2, 3, 3, 4]})
@assert isequal(duplicated(df), [false, false, false, true, false])
drop_duplicates!(df)
@assert isequal(df, DataFrame({"a" => [1, 2, 3, 4]}))
