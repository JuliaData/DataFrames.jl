module TestConstructors
    using Base.Test
    using DataTables, DataTables.Index

    #
    # DataTable
    #

    dt = DataTable()
    @test isequal(dt.columns, Any[])
    @test isequal(dt.colindex, Index())

    dt = DataTable(Any[NullableCategoricalVector(zeros(3)),
                       NullableCategoricalVector(ones(3))],
                   Index([:x1, :x2]))
    @test size(dt, 1) == 3
    @test size(dt, 2) == 2

    @test isequal(dt, DataTable(Any[NullableCategoricalVector(zeros(3)),
                                    NullableCategoricalVector(ones(3))]))
    @test isequal(dt, DataTable(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0]))

    dt2 = convert(DataTable, [0.0 1.0;
                              0.0 1.0;
                              0.0 1.0])
    names!(dt2, [:x1, :x2])
    @test isequal(dt[:x1], NullableArray(dt2[:x1]))
    @test isequal(dt[:x2], NullableArray(dt2[:x2]))

    @test isequal(dt, DataTable(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0]))
    @test isequal(dt, DataTable(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0],
                                x3 = [2.0, 2.0, 2.0])[[:x1, :x2]])

    dt = DataTable(Int, 2, 2)
    @test size(dt) == (2, 2)
    @test eltypes(dt) == [Nullable{Int}, Nullable{Int}]

    dt = DataTable([Int, Float64], [:x1, :x2], 2)
    @test size(dt) == (2, 2)
    @test eltypes(dt) == [Nullable{Int}, Nullable{Float64}]

    @test isequal(dt, DataTable([Int, Float64], 2))

end
