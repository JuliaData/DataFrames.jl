module TestDataTableRow
    using Base.Test
    using DataTables, Compat

    dt = DataTable(a=NullableArray([1,   2,   3,   1,   2,   2 ]),
                   b=NullableArray(Nullable{Float64}[2.0, Nullable(),
                                                     1.2, 2.0,
                                                     Nullable(), Nullable()]),
                   c=NullableArray(Nullable{String}["A", "B", "C", "A", "B", Nullable()]),
                   d=NullableCategoricalArray(Nullable{Symbol}[:A,  Nullable(),  :C,  :A,
                                                           Nullable(),  :C]))
    dt2 = DataTable(a = NullableArray([1, 2, 3]))

    #
    # Equality
    #
    @test_throws ArgumentError isequal(DataTableRow(dt, 1), DataTableRow(dt2, 1))
    @test !isequal(DataTableRow(dt, 1), DataTableRow(dt, 2))
    @test !isequal(DataTableRow(dt, 1), DataTableRow(dt, 3))
    @test isequal(DataTableRow(dt, 1), DataTableRow(dt, 4))
    @test isequal(DataTableRow(dt, 2), DataTableRow(dt, 5))
    @test !isequal(DataTableRow(dt, 2), DataTableRow(dt, 6))

    # hashing
    @test !isequal(hash(DataTableRow(dt, 1)), hash(DataTableRow(dt, 2)))
    @test !isequal(hash(DataTableRow(dt, 1)), hash(DataTableRow(dt, 3)))
    @test isequal(hash(DataTableRow(dt, 1)), hash(DataTableRow(dt, 4)))
    @test isequal(hash(DataTableRow(dt, 2)), hash(DataTableRow(dt, 5)))
    @test !isequal(hash(DataTableRow(dt, 2)), hash(DataTableRow(dt, 6)))
end
