module TestTableTraits
using Test, TableTraits, DataFrames

struct ColumnSource
end

TableTraits.supports_get_columns_copy_using_missing(::ColumnSource) = true
TableTraits.isiterabletable(::ColumnSource) = true

function TableTraits.get_columns_copy_using_missing(x::ColumnSource)
    return (a=[1,2,3], b=[4.,5.,6.], c=["A", "B", "C"])
end

@testset "TableTraits" begin

    df = DataFrame(ColumnSource())

    @test size(df)==(3,3)
    @test df.a==[1,2,3]
    @test df.b==[4.,5.,6.]
    @test df.c==["A", "B", "C"]

end

end
