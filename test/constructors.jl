module TestConstructors
    using Base.Test
    using DataTables, DataTables.Index

    #
    # DataTable
    #

    df = DataTable()
    @test isequal(df.columns, Any[])
    @test isequal(df.colindex, Index())

    df = DataTable(Any[NullableCategoricalVector(zeros(3)),
                       NullableCategoricalVector(ones(3))],
                   Index([:x1, :x2]))
    @test size(df, 1) == 3
    @test size(df, 2) == 2

    @test isequal(df, DataTable(Any[NullableCategoricalVector(zeros(3)),
                                    NullableCategoricalVector(ones(3))]))
    @test isequal(df, DataTable(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0]))

    df2 = convert(DataTable, [0.0 1.0;
                              0.0 1.0;
                              0.0 1.0])
    names!(df2, [:x1, :x2])
    @test isequal(df[:x1], NullableArray(df2[:x1]))
    @test isequal(df[:x2], NullableArray(df2[:x2]))

    @test isequal(df, DataTable(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0]))
    @test isequal(df, DataTable(x1 = [0.0, 0.0, 0.0],
                                x2 = [1.0, 1.0, 1.0],
                                x3 = [2.0, 2.0, 2.0])[[:x1, :x2]])

    df = DataTable(Int, 2, 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Nullable{Int}, Nullable{Int}]

    df = DataTable([Int, Float64], [:x1, :x2], 2)
    @test size(df) == (2, 2)
    @test eltypes(df) == [Nullable{Int}, Nullable{Float64}]

    @test isequal(df, DataTable([Int, Float64], 2))

end
