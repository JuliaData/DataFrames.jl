module TestDataTableRow
    using Base.Test
    using DataTables, Compat

    df = DataTable(a=NullableArray([1,   2,   3,   1,   2,   2 ]),
                   b=NullableArray(Nullable{Float64}[2.0, Nullable(),
                                                     1.2, 2.0,
                                                     Nullable(), Nullable()]),
                   c=NullableArray(Nullable{String}["A", "B", "C", "A", "B", Nullable()]),
                   d=NullableCategoricalArray(Nullable{Symbol}[:A,  Nullable(),  :C,  :A,
                                                           Nullable(),  :C]))
    df2 = DataTable(a = NullableArray([1, 2, 3]))

    #
    # Equality
    #
    @test_throws ArgumentError isequal(DataTableRow(df, 1), DataTableRow(df2, 1))
    @test !isequal(DataTableRow(df, 1), DataTableRow(df, 2))
    @test !isequal(DataTableRow(df, 1), DataTableRow(df, 3))
    @test isequal(DataTableRow(df, 1), DataTableRow(df, 4))
    @test isequal(DataTableRow(df, 2), DataTableRow(df, 5))
    @test !isequal(DataTableRow(df, 2), DataTableRow(df, 6))

    # hashing
    @test !isequal(hash(DataTableRow(df, 1)), hash(DataTableRow(df, 2)))
    @test !isequal(hash(DataTableRow(df, 1)), hash(DataTableRow(df, 3)))
    @test isequal(hash(DataTableRow(df, 1)), hash(DataTableRow(df, 4)))
    @test isequal(hash(DataTableRow(df, 2)), hash(DataTableRow(df, 5)))
    @test !isequal(hash(DataTableRow(df, 2)), hash(DataTableRow(df, 6)))
end
