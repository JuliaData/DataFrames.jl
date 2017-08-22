using DataFrames
using TableTraits
using NamedTuples
using GLM
using DataValues
using Base.Test

@testset "TableTraits" begin

source_df = DataFrame(a=[1,2,3], b=[1.,2.,3.], c=["A","B","C"])

@test isiterable(source_df) == true

target_array = collect(getiterator(source_df))

@test length(target_array) == 3
@test target_array[1] == @NT(a=DataValue(1), b=DataValue(1.), c=DataValue("A"))
@test target_array[2] == @NT(a=DataValue(2), b=DataValue(2.), c=DataValue("B"))
@test target_array[3] == @NT(a=DataValue(3), b=DataValue(3.), c=DataValue("C"))

data = []
push!(data, [1,2,3])
push!(data, [1.,2.,3.])
push!(data, ["A","B","C"])
source_df_non_nullable = DataFrame(data, [:a,:b,:c])
target_array2 = collect(getiterator(source_df_non_nullable))

@test length(target_array2) == 3
@test target_array2[1] == @NT(a=1, b=1., c="A")
@test target_array2[2] == @NT(a=2, b=2., c="B")
@test target_array2[3] == @NT(a=3, b=3., c="C")


source_array_non_nullable = [@NT(a=1,b=1.,c="A"), @NT(a=2,b=2.,c="B"), @NT(a=3,b=3.,c="C")]
df = DataFrame(source_array_non_nullable)

@test size(df) == (3,3)
@test isa(df[:a], Array)
@test isa(df[:b], Array)
@test isa(df[:c], Array)
@test df[:a] == [1,2,3]
@test df[:b] == [1.,2.,3.]
@test df[:c] == ["A","B","C"]

source_array = [@NT(a=DataValue(1),b=DataValue(1.),c=DataValue("A")), @NT(a=DataValue(2),b=DataValue(2.),c=DataValue("B")), @NT(a=DataValue(3),b=DataValue(3.),c=DataValue("C"))]
df = DataFrame(source_array)

@test size(df) == (3,3)
@test isa(df[:a], DataArray)
@test isa(df[:b], DataArray)
@test isa(df[:c], DataArray)
@test df[:a] == [1,2,3]
@test df[:b] == [1.,2.,3.]
@test df[:c] == ["A","B","C"]

mf_array = ModelFrame(@formula(a~b), source_array)

@test isa(mf_array, ModelFrame)

end
