module TestIO
    using Test, DataFrames, CategoricalArrays, Dates
    using LaTeXStrings

    # Test LaTeX export
    @testset "LaTeX export" begin
        df = DataFrame(A = 1:4,
                    B = ["\$10.0", "M&F", "A~B", "\\alpha"],
                    C = [L"\alpha", L"\beta", L"\gamma", L"\sum_{i=1}^n \delta_i"],
                    D = [1.0, 2.0, missing, 3.0],
                    E = CategoricalArray(["a", missing, "c", "d"]),
                    F = Vector{String}(undef, 4)
                    )
        str = """
            \\begin{tabular}{r|cccccc}
            \t& A & B & C & D & E & F\\\\
            \t\\hline
            \t1 & 1 & \\\$10.0 & \$\\alpha\$ & 1.0 & a & \\#undef \\\\
            \t2 & 2 & M\\&F & \$\\beta\$ & 2.0 &  & \\#undef \\\\
            \t3 & 3 & A\\textasciitilde{}B & \$\\gamma\$ &  & c & \\#undef \\\\
            \t4 & 4 & \\textbackslash{}\\textbackslash{}alpha & \$\\sum_{i=1}^n \\delta_i\$ & 3.0 & d & \\#undef \\\\
            \\end{tabular}
            """
        @test repr(MIME("text/latex"), df) == str
    end

    @testset "Huge LaTeX export" begin
        df = DataFrame(a=1:1000)
        ioc = IOContext(IOBuffer(), :displaysize => (10, 10), :limit => false)
        show(ioc, "text/latex", df)
        @test length(String(take!(ioc.io))) > 10000

        io = IOBuffer()
        show(io, "text/latex", df)
        @test length(String(take!(io))) < 10000
    end

    #Test HTML output for IJulia and similar
    @testset "HTML output" begin
        df = DataFrame(Fish = ["Suzy", "Amir"], Mass = [1.5, missing])
        io = IOBuffer()
        show(io, "text/html", df)
        str = String(take!(io))
        @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                    "</th><th>Fish</th><th>Mass</th></tr>" *
                    "<tr><th></th><th>String</th><th>Float64⍰</th></tr></thead><tbody>" *
                    "<p>2 rows × 2 columns</p>" *
                    "<tr><th>1</th><td>Suzy</td><td>1.5</td></tr>" *
                    "<tr><th>2</th><td>Amir</td><td>missing</td></tr></tbody></table>"

        df = DataFrame(Fish = Vector{String}(undef, 2), Mass = [1.5, missing])
        io = IOBuffer()
        show(io, "text/html", df)
        str = String(take!(io))
        @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                    "</th><th>Fish</th><th>Mass</th></tr>" *
                    "<tr><th></th><th>String</th><th>Float64⍰</th></tr></thead><tbody>" *
                    "<p>2 rows × 2 columns</p>" *
                    "<tr><th>1</th><td>#undef</td><td>1.5</td></tr>" *
                    "<tr><th>2</th><td>#undef</td><td>missing</td></tr></tbody></table>"

        io = IOBuffer()
        show(io, MIME"text/html"(), df, summary=false)
        str = String(take!(io))
        @test str == "<table class=\"data-frame\"><thead><tr><th>" *
                    "</th><th>Fish</th><th>Mass</th></tr>" *
                    "<tr><th></th><th>String</th><th>Float64⍰</th></tr></thead><tbody>" *
                    "<tr><th>1</th><td>#undef</td><td>1.5</td></tr>" *
                    "<tr><th>2</th><td>#undef</td><td>missing</td></tr></tbody></table>"
    end

    # test limit attribute of IOContext is used
    @testset "limit attribute" begin
        df = DataFrame(a=1:1000)
        ioc = IOContext(IOBuffer(), :displaysize => (10, 10), :limit => false)
        show(ioc, "text/html", df)
        @test length(String(take!(ioc.io))) > 10000

        io = IOBuffer()
        show(io, "text/html", df)
        @test length(String(take!(io))) < 10000
    end

    @testset "printtable" begin
        df = DataFrame(A = 1:3,
                    B = 'a':'c',
                    C = ["A", "B", "C"],
                    D = CategoricalArray(string.('a':'c')),
                    E = CategoricalArray(["A", "B", missing]),
                    F = Vector{Union{Int, Missing}}(1:3),
                    G = missings(3),
                    H = fill(missing, 3))

        @test sprint(DataFrames.printtable, df) ==
            """
            "A","B","C","D","E","F","G","H"
            1,"'a'","A","a","A","1",missing,missing
            2,"'b'","B","b","B","2",missing,missing
            3,"'c'","C","c",missing,"3",missing,missing
            """
    end

    @testset "DataStreams" begin
        using DataStreams
        I = DataFrame(id = Int64[1, 2, 3, 4, 5],
            firstname = Union{String, Missing}["Benjamin", "Wayne", "Sean", "Charles", missing],
            lastname = String["Chavez", "Burke", "Richards", "Long", "Rose"],
            salary = Union{Float64, Missing}[missing, 46134.1, 45046.2, 30555.6, 88894.1],
            rate = Float64[39.44, 33.8, 15.64, 17.67, 34.6],
            hired = Union{Date, Missing}[Date("2011-07-07"), Date("2016-02-19"), missing,
                                         Date("2002-01-05"), Date("2008-05-15")],
            fired = DateTime[DateTime("2016-04-07T14:07:00"), DateTime("2015-03-19T15:01:00"),
                             DateTime("2006-11-18T05:07:00"), DateTime("2002-07-18T06:24:00"),
                             DateTime("2007-09-29T12:09:00")],
            reserved = missings(5)
        )
        sink = DataStreams.Data.close!(DataStreams.Data.stream!(I, deepcopy(I)))
        sch = DataStreams.Data.schema(sink)
        @test size(sch) == (5, 8)
        @test DataStreams.Data.header(sch) == ["id", "firstname", "lastname", "salary",
                                               "rate", "hired", "fired", "reserved"]
        @test DataStreams.Data.types(sch) == (Int64, Union{String, Missing}, String,
                                              Union{Float64, Missing}, Float64,
                                              Union{Date, Missing}, DateTime, Missing)
        @test sink[:id] == [1:5;]

        transforms = Dict(1=>x->x+1)
        sink = DataStreams.Data.close!(DataStreams.Data.stream!(I, deepcopy(I);
                                                                append=true, transforms=transforms))
        sch = DataStreams.Data.schema(sink)
        @test size(sch) == (10, 8)
        @test DataStreams.Data.header(sch) == ["id", "firstname", "lastname", "salary",
                                               "rate", "hired", "fired", "reserved"]
        @test DataStreams.Data.types(sch) == (Int64, Union{String, Missing}, String,
                                              Union{Float64, Missing}, Float64,
                                              Union{Date, Missing}, DateTime, Missing)
        @test sink[:id] == [1:5; 2:6]

        sink = DataStreams.Data.close!(Data.stream!(I, DataFrame, deepcopy(I)))
        sch = DataStreams.Data.schema(sink)
        @test size(sch) == (5, 8)
        @test DataStreams.Data.header(sch) == ["id", "firstname", "lastname", "salary",
                                               "rate", "hired", "fired", "reserved"]
        @test DataStreams.Data.types(sch) == (Int64, Union{String, Missing}, String,
                                              Union{Float64, Missing}, Float64,
                                              Union{Date, Missing}, DateTime, Missing)
        @test sink[:id] == [1:5;]

        # test DataFrameStream creation
        dfs = DataFrame(sch)
        DataStreams.Data.close!(dfs)
    end

    @testset "csv/tsv output" begin
        df = DataFrame(a = [1,2], b = [1.0, 2.0])
        @test sprint(show, "text/csv", df) == """
        "a","b"
        1,1.0
        2,2.0
        """
        @test sprint(show, "text/tab-separated-values", df) == """
        "a"\t"b"
        1\t1.0
        2\t2.0
        """
    end
end
