module TestDuplicates
    using Base.Test, DataTables

    dt = DataTable(a = [1, 2, 3, 3, 4])
    udt = DataTable(a = [1, 2, 3, 4])
    @test nonunique(dt) == [false, false, false, true, false]
    @test udt == unique(dt)
    unique!(dt)
    @test dt == udt

    pdt = DataTable(a = CategoricalArray(["a", "a", null, null, "b", null, "a", null]),
                    b = CategoricalArray(["a", "b", null, null, "b", "a", "a", "a"]))
    updt = DataTable(a = CategoricalArray(["a", "a", null, "b", null]),
                     b = CategoricalArray(["a", "b", null, "b", "a"]))
    @test nonunique(pdt) == [false, false, false, true, false, false, true, true]
    @test nonunique(updt) == falses(5)
    @test updt == unique(pdt)
    unique!(pdt)
    @test pdt == updt

    @testset "missing" begin
        dt = DataTable(A = 1:12, B = repeat('A':'C', inner=4))
        @test DataTables.colmissing(dt) == [0, 0]
    end
end
