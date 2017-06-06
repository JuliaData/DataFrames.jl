module TestIO
    using Base.Test
    using DataTables
    using LaTeXStrings
    using NullableArrays
    using CategoricalArrays

    # Test LaTeX export
    dt = DataTable(A = 1:4,
                   B = ["\$10.0", "M&F", "A~B", "\\alpha"],
                   C = [L"\alpha", L"\beta", L"\gamma", L"\sum_{i=1}^n \delta_i"],
                   D = [1.0, 2.0, Nullable(), 3.0],
                   E = NullableCategoricalArray(["a", Nullable(), "c", "d"])
                   )
    str = """
        \\begin{tabular}{r|ccccc}
        \t& A & B & C & D & E\\\\
        \t\\hline
        \t1 & 1 & \\\$10.0 & \$\\alpha\$ & 1.0 & a \\\\
        \t2 & 2 & M\\&F & \$\\beta\$ & 2.0 &  \\\\
        \t3 & 3 & A\\textasciitilde{}B & \$\\gamma\$ &  & c \\\\
        \t4 & 4 & \\textbackslash{}alpha & \$\\sum_{i=1}^n \\delta_i\$ & 3.0 & d \\\\
        \\end{tabular}
        """
    @test reprmime(MIME("text/latex"), dt) == str

    #Test HTML output for IJulia and similar
    dt = DataTable(Fish = ["Suzy", "Amir"], Mass = [1.5, Nullable()],
                   E = NullableCategoricalArray(["a", Nullable()]))
    io = IOBuffer()
    show(io, "text/html", dt)
    str = String(take!(io))
    @test str ==
        "<table class=\"data-frame\"><tr><th></th><th>Fish</th><th>Mass</th><th>E</th></tr>" *
        "<tr><th>1</th><td>Suzy</td><td>1.5</td><td>a</td></tr>" *
        "<tr><th>2</th><td>Amir</td><td>#NULL</td><td>#NULL</td></tr></table>"


    # test limit attribute of IOContext is used
    dt = DataTable(a=collect(1:1000))
    ioc = IOContext(IOBuffer(), displaysize=(10, 10), limit=false)
    show(ioc, "text/html", dt)
    @test length(String(take!(ioc.io))) > 10000

    io = IOBuffer()
    show(io, "text/html", dt)
    @test length(String(take!(io))) < 10000

    dt = DataTable(A = 1:3,
                   B = 'a':'c',
                   C = ["A", "B", "C"],
                   D = CategoricalArray('a':'c'),
                   E = NullableCategoricalArray(["A", "B", "C"]),
                   F = NullableArray(1:3),
                   G = NullableArray(fill(Nullable(), 3)),
                   H = fill(Nullable(), 3))

    @test sprint(printtable, dt) == """
        \"A\",\"B\",\"C\",\"D\",\"E\",\"F\",\"G\",\"H\"
        1,\"'a'\",\"A\",\"'a'\",\"A\",\"1\",NULL,NULL
        2,\"'b'\",\"B\",\"'b'\",\"B\",\"2\",NULL,NULL
        3,\"'c'\",\"C\",\"'c'\",\"C\",\"3\",NULL,NULL
        """


    # DataStreams
    using CSV

    dt = CSV.read(joinpath(dirname(@__FILE__), "data/iris.csv"), DataTable)
    @test size(dt) == (150, 5)
end
