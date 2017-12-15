module TestUtils
    using Compat, Compat.Test, DataFrames, StatsBase
    import DataFrames: identifier

    @test identifier("%_B*_\tC*") == :_B_C_
    @test identifier("2a") == :x2a
    @test identifier("!") == :x!
    @test identifier("\t_*") == :_
    @test identifier("begin") == :_begin
    @test identifier("end") == :_end

    @test DataFrames._makeunique([:x, :x, :x_1, :x2]) == [:x, :x_2, :x_1, :x2]
    @test_throws ArgumentError DataFrames._makeunique([:x, :x, :x_1, :x2], makeunique=false)
    @test DataFrames._makeunique([:x, :x_1, :x2], makeunique=false) == [:x, :x_1, :x2]

    # Check that reserved words are up to date

    f = "$JULIA_HOME/../../src/julia-parser.scm"
    if isfile(f)
        r1 = r"define initial-reserved-words '\(([^)]+)"
        r2 = r"define \(parse-block s(?: \([^)]+\))?\)\s+\(parse-Nary s (?:parse-eq '\([^(]*|down '\([^)]+\) '[^']+ ')\(([^)]+)"
        body = readstring(f)
        m1, m2 = match(r1, body), match(r2, body)
        if m1 == nothing || m2 == nothing
            error("Unable to extract keywords from 'julia-parser.scm'.")
        else
            s = replace(m1.captures[1]*" "*m2.captures[1], r";;.*?\n", "")
            rw = Set(split(s, r"\W+"))
            @test rw == DataFrames.RESERVED_WORDS
        end
    else
        warn("Unable to validate reserved words against parser. ",
             "Expected if Julia was not built from source.")
    end

    @test DataFrames.countmissing([1:3;]) == 0

    data = Vector{Union{Float64, Missing}}(rand(20))
    @test DataFrames.countmissing(data) == 0
    data[sample(1:20, 11, replace=false)] = missing
    @test DataFrames.countmissing(data) == 11
    data[1:end] = missing
    @test DataFrames.countmissing(data) == 20

    pdata = Vector{Union{Int, Missing}}(sample(1:5, 20))
    @test DataFrames.countmissing(pdata) == 0
    pdata[sample(1:20, 11, replace=false)] = missing
    @test DataFrames.countmissing(pdata) == 11
    pdata[1:end] = missing
    @test DataFrames.countmissing(pdata) == 20

    funs = [mean, sum, var, x -> sum(x)]
    if string(funs[end]) == "(anonymous function)" # Julia < 0.5
        @test DataFrames._fnames(funs) == ["mean", "sum", "var", "λ1"]
    else
        @test DataFrames._fnames(funs) == ["mean", "sum", "var", string(funs[end])]
    end

    @testset "describe" begin
        io = IOBuffer()
        df = DataFrame(Any[collect(1:4), Vector{Union{Int, Missing}}(2:5),
                           CategoricalArray(3:6),
                           CategoricalArray{Union{Int, Missing}}(4:7)],
                       [:arr, :missingarr, :cat, :missingcat])
        describe(io, df)
        DRT = CategoricalArrays.DefaultRefType
        # Julia 0.7
        missingfirst =
            """
            arr
            Summary Stats:
            Mean:           2.500000
            Minimum:        1.000000
            1st Quartile:   1.750000
            Median:         2.500000
            3rd Quartile:   3.250000
            Maximum:        4.000000
            Length:         4
            Type:           $Int

            missingarr
            Summary Stats:
            Mean:           3.500000
            Minimum:        2.000000
            1st Quartile:   2.750000
            Median:         3.500000
            3rd Quartile:   4.250000
            Maximum:        5.000000
            Length:         4
            Type:           Union{Missings.Missing, $Int}
            Number Missing: 0
            % Missing:      0.000000

            cat
            Summary Stats:
            Length:         4
            Type:           CategoricalArrays.CategoricalValue{$Int,$DRT}
            Number Unique:  4

            missingcat
            Summary Stats:
            Length:         4
            Type:           Union{Missings.Missing, CategoricalArrays.CategoricalValue{$Int,$DRT}}
            Number Unique:  4
            Number Missing: 0
            % Missing:      0.000000

            """
        # Julia 0.6
        missingsecond =
            """
            arr
            Summary Stats:
            Mean:           2.500000
            Minimum:        1.000000
            1st Quartile:   1.750000
            Median:         2.500000
            3rd Quartile:   3.250000
            Maximum:        4.000000
            Length:         4
            Type:           $Int

            missingarr
            Summary Stats:
            Mean:           3.500000
            Minimum:        2.000000
            1st Quartile:   2.750000
            Median:         3.500000
            3rd Quartile:   4.250000
            Maximum:        5.000000
            Length:         4
            Type:           Union{$Int, Missings.Missing}
            Number Missing: 0
            % Missing:      0.000000

            cat
            Summary Stats:
            Length:         4
            Type:           CategoricalArrays.CategoricalValue{$Int,$DRT}
            Number Unique:  4

            missingcat
            Summary Stats:
            Length:         4
            Type:           Union{CategoricalArrays.CategoricalValue{$Int,$DRT}, Missings.Missing}
            Number Unique:  4
            Number Missing: 0
            % Missing:      0.000000

            """
            out = String(take!(io))
            @test (out == missingfirst || out == missingsecond)
    end

    @testset "describe" begin
        io = IOBuffer()
        df = DataFrame(Any[collect(1:4), collect(Union{Int, Missing}, 2:5),
                           CategoricalArray(3:6),
                           CategoricalArray{Union{Int, Missing}}(4:7)],
                       [:arr, :missingarr, :cat, :missingcat])
        describe(io, df)
        @test String(take!(io)) ==
            """
            arr
            Summary Stats:
            Mean:           2.500000
            Minimum:        1.000000
            1st Quartile:   1.750000
            Median:         2.500000
            3rd Quartile:   3.250000
            Maximum:        4.000000
            Length:         4
            Type:           $Int

            missingarr
            Summary Stats:
            Mean:           3.500000
            Minimum:        2.000000
            1st Quartile:   2.750000
            Median:         3.500000
            3rd Quartile:   4.250000
            Maximum:        5.000000
            Length:         4
            Type:           Union{$Int, Missings.Missing}
            Number Missing: 0
            % Missing:      0.000000

            cat
            Summary Stats:
            Length:         4
            Type:           CategoricalArrays.CategoricalValue{$Int,$(CategoricalArrays.DefaultRefType)}
            Number Unique:  4

            missingcat
            Summary Stats:
            Length:         4
            Type:           Union{CategoricalArrays.CategoricalValue{$Int,$(CategoricalArrays.DefaultRefType)}, Missings.Missing}
            Number Unique:  4
            Number Missing: 0
            % Missing:      0.000000

            """
    end
end
