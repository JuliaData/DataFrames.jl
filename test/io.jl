module TestIO
    using Base.Test, DataFrames, CategoricalArrays
    using LaTeXStrings

    # Test LaTeX export
    df = DataFrame(A = 1:4,
                   B = ["\$10.0", "M&F", "A~B", "\\alpha"],
                   C = [L"\alpha", L"\beta", L"\gamma", L"\sum_{i=1}^n \delta_i"],
                   D = [1.0, 2.0, null, 3.0],
                   E = CategoricalArray(["a", null, "c", "d"])
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
    @test reprmime(MIME("text/latex"), df) == str

    #Test HTML output for IJulia and similar
    df = DataFrame(Fish = ["Suzy", "Amir"], Mass = [1.5, null])
    io = IOBuffer()
    show(io, "text/html", df)
    str = String(take!(io))
    @test str == "<table class=\"data-frame\">" *
                 "<tr><th></th><th>Fish</th><th>Mass</th></tr>" *
                 "<tr><th>1</th><td>Suzy</td><td>1.5</td></tr>" *
                 "<tr><th>2</th><td>Amir</td><td>null</td></tr></table>"

    # test limit attribute of IOContext is used
    df = DataFrame(a=collect(1:1000))
    ioc = IOContext(IOBuffer(), displaysize=(10, 10), limit=false)
    show(ioc, "text/html", df)
    @test length(String(take!(ioc.io))) > 10000

    io = IOBuffer()
    show(io, "text/html", df)
    @test length(String(take!(io))) < 10000

    df = DataFrame(A = 1:3,
                   B = 'a':'c',
                   C = ["A", "B", "C"],
                   D = CategoricalArray('a':'c'),
                   E = CategoricalArray(["A", "B", null]),
                   F = Vector{Union{Int, Null}}(1:3),
                   G = nulls(3),
                   H = fill(null, 3))

    @test sprint(printtable, df) ==
        """
        "A","B","C","D","E","F","G","H"
        1,"'a'","A","'a'","A","1",null,null
        2,"'b'","B","'b'","B","2",null,null
        3,"'c'","C","'c'",null,"3",null,null
        """

    # DataStreams
    using DataStreams
    I = DataFrames.DataFrame(id = Int64[1, 2, 3, 4, 5],
        firstname = Union{String, Null}["Benjamin", "Wayne", "Sean", "Charles", null],
        lastname = String["Chavez", "Burke", "Richards", "Long", "Rose"],
        salary = Union{Float64, Null}[null, 46134.1, 45046.2, 30555.6, 88894.1],
        rate = Float64[39.44, 33.8, 15.64, 17.67, 34.6],
        hired = Union{Date, Null}[Date("2011-07-07"), Date("2016-02-19"), null, Date("2002-01-05"), Date("2008-05-15")],
        fired = DateTime[DateTime("2016-04-07T14:07:00"), DateTime("2015-03-19T15:01:00"), DateTime("2006-11-18T05:07:00"), DateTime("2002-07-18T06:24:00"), DateTime("2007-09-29T12:09:00")]
    )
    sink = DataStreams.Data.close!(DataStreams.Data.stream!(I, deepcopy(I)))
    sch = DataStreams.Data.schema(sink)
    @test size(sch) == (5, 7)
    @test DataStreams.Data.header(sch) == ["id","firstname","lastname","salary","rate","hired","fired"]
    @test DataStreams.Data.types(sch) == (Int64, Union{String, Null}, String, Union{Float64, Null}, Float64, Union{Date, Null}, DateTime)
    @test sink[:id] == [1,2,3,4,5]

    transforms = Dict(1=>x->x+1)
    sink = DataStreams.Data.close!(DataStreams.Data.stream!(I, deepcopy(I); append=true, transforms=transforms))
    sch = DataStreams.Data.schema(sink)
    @test size(sch) == (10, 7)
    @test DataStreams.Data.header(sch) == ["id","firstname","lastname","salary","rate","hired","fired"]
    @test DataStreams.Data.types(sch) == (Int64, Union{String, Null}, String, Union{Float64, Null}, Float64, Union{Date, Null}, DateTime)
    @test sink[:id] == [1,2,3,4,5,2,3,4,5,6]

    sink = DataStreams.Data.close!(Data.stream!(I, DataFrame, deepcopy(I)))
    sch = DataStreams.Data.schema(sink)
    @test size(sch) == (5, 7)
    @test DataStreams.Data.header(sch) == ["id","firstname","lastname","salary","rate","hired","fired"]
    @test DataStreams.Data.types(sch) == (Int64, Union{String, Null}, String, Union{Float64, Null}, Float64, Union{Date, Null}, DateTime)
    @test sink[:id] == [1,2,3,4,5]
end
