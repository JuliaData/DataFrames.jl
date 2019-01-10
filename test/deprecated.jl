module TestDeprecated
    using Test, DataFrames
    import DataFrames: identifier

    # old sort(df; cols=...) syntax
    df = DataFrame(a=[1, 3, 2], b=[6, 5, 4])
    @test sort(df; cols=[:a, :b]) == DataFrame(a=[1, 2, 3], b=[6, 4, 5])
    sort!(df; cols=[:a, :b])
    @test df == DataFrame(a=[1, 2, 3], b=[6, 4, 5])

    df = DataFrame(a=[1, 3, 2], b=[6, 5, 4])
    @test sort(df; cols=[:b, :a]) == DataFrame(a=[2, 3, 1], b=[4, 5, 6])
    sort!(df; cols=[:b, :a])
    @test df == DataFrame(a=[2, 3, 1], b=[4, 5, 6])

    @test first.(collect(pairs(DataFrameRow(df, 1, :)))) == [:a, :b]
    @test last.(collect(pairs(DataFrameRow(df, 1, :)))) == [df[1, 1], df[1, 2]]

    @testset "identifier" begin
        @test identifier("%_B*_\tC*") == :_B_C_
        @test identifier("2a") == :x2a
        @test identifier("!") == :x!
        @test identifier("\t_*") == :_
        @test identifier("begin") == :_begin
        @test identifier("end") == :_end
    end

    # Check that reserved words are up to date

    @testset "reserved words" begin
        f = "$(Sys.BINDIR)/../../src/julia-parser.scm"
        if isfile(f)
            r1 = r"define initial-reserved-words '\(([^)]+)"
            r2 = r"define \(parse-block s(?: \([^)]+\))?\)\s+\(parse-Nary s (?:parse-eq '\([^(]*|down '\([^)]+\) '[^']+ ')\(([^)]+)"
            body = read(f, String)
            m1, m2 = match(r1, body), match(r2, body)
            if m1 == nothing || m2 == nothing
                error("Unable to extract keywords from 'julia-parser.scm'.")
            else
                s = replace(string(m1.captures[1]," ",m2.captures[1]), r";;.*?\n" => "")
                rw = Set(split(s, r"\W+"))
                @test rw == DataFrames.RESERVED_WORDS
            end
        else
            @warn("Unable to validate reserved words against parser. ",
                  "Expected if Julia was not built from source.")
        end
    end
end
