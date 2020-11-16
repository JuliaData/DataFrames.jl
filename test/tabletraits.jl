module TestTableTraits

using Test, DataFrames, IteratorInterfaceExtensions, TableTraits, DataValues

struct ColumnSource
end

TableTraits.supports_get_columns_copy_using_missing(::ColumnSource) = true
TableTraits.isiterabletable(::ColumnSource) = true

function TableTraits.get_columns_copy_using_missing(x::ColumnSource)
    return (a=[1 ,2, 3], b=[4.0, 5.0, 6.0], c=["A", "B", "C"])
end

@testset "TableTraits" begin
    df = DataFrame(a=[1, 2 ,3], b=[1.0, missing, 3.0])
    @test IteratorInterfaceExtensions.isiterable(df)
    @test TableTraits.isiterabletable(df)
    @test collect(IteratorInterfaceExtensions.getiterator(df)) ==
        [(a=1, b=DataValue(1.0)), (a=2, b=DataValue{Float64}()), (a=3, b=DataValue(3.0))]
    sdf = view(df, 1:2, :)
    @test IteratorInterfaceExtensions.isiterable(sdf)
    @test TableTraits.isiterabletable(sdf)
    @test collect(IteratorInterfaceExtensions.getiterator(sdf)) ==
        [(a=1, b=DataValue(1.0)), (a=2, b=DataValue{Float64}())]

    df = DataFrame(ColumnSource())

    @test size(df)==(3, 3)
    @test df.a==[1, 2, 3]
    @test df.b==[4.0, 5.0, 6.0]
    @test df.c==["A", "B", "C"]
end

end # module
