module TestMeta

using Compat, Compat.Test, DataFrames

df = DataFrame(:a=>1, :b=>2, :c=>[1,2])
showcols(df)

@test meta(df, :descr) == nothing
@test meta(df, :descr, default="test") == "test"
metaset!(df, :descr, "description")
@test meta(df, :descr) == "description"


@test meta(df, :a, :descr) == nothing
@test meta(df, :a, :descr, default="test") == "test"
metaset!(df, :a, :descr, "description")
@test meta(df, :a, :descr) == "description"

@test meta(df, :b, :unit) == nothing
@test meta(df, :b, :unit, default="test") == "test"
metaset!(df, :b, :unit, "unit")
@test meta(df, :b, :unit) == "unit"
showcols(df)
end
