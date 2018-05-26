module TestMeta

using Compat, Compat.Test, DataFrames

df = DataFrame(:a=>1, :b=>2, :c=>[1,2])
showcols(df)

@test metaget(df, :descr) == nothing
@test metaget(df, :descr, default="test") == "test"
metaset!(df, :descr, "description")
@test metaget(df, :descr) == "description"


@test metaget(df, :a, :descr) == nothing
@test metaget(df, :a, :descr, default="test") == "test"
metaset!(df, :a, :descr, "description")
@test metaget(df, :a, :descr) == "description"

@test metaget(df, :b, :unit) == nothing
@test metaget(df, :b, :unit, default="test") == "test"
metaset!(df, :b, :unit, "unit")
@test metaget(df, :b, :unit) == "unit"
showcols(df)
end
