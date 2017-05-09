module TestUtils
    using Base.Test
    using DataFrames
    using Compat

    import DataFrames: identifier

    @test identifier("%_B*_\tC*") == :_B_C_
    @test identifier("2a") == :x2a
    @test identifier("!") == :x!
    @test identifier("\t_*") == :_
    @test identifier("begin") == :_begin
    @test identifier("end") == :_end

    @test DataFrames.make_unique([:x, :x, :x_1, :x2]) == [:x, :x_2, :x_1, :x2]
    @test_throws ArgumentError DataFrames.make_unique([:x, :x, :x_1, :x2], allow_duplicates=false)
    @test DataFrames.make_unique([:x, :x_1, :x2], allow_duplicates=false) == [:x, :x_1, :x2]

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

    @test DataFrames.countna([1:3;]) == 0

    data = @data rand(20)
    @test DataFrames.countna(data) == 0
    data[sample(1:20, 11, replace=false)] = NA
    @test DataFrames.countna(data) == 11
    data[1:end] = NA
    @test DataFrames.countna(data) == 20

    pdata = @data sample(1:5, 20)
    @test DataFrames.countna(pdata) == 0
    pdata[sample(1:20, 11, replace=false)] = NA
    @test DataFrames.countna(pdata) == 11
    pdata[1:end] = NA
    @test DataFrames.countna(pdata) == 20

    funs = [mean, sum, var, x -> sum(x)]
    if string(funs[end]) == "(anonymous function)" # Julia < 0.5
        @test DataFrames._fnames(funs) == ["mean", "sum", "var", "λ1"]
    else
        @test DataFrames._fnames(funs) == ["mean", "sum", "var", string(funs[end])]
    end

    @testset "describe" begin
        io = IOBuffer()
        df = DataFrame(Any[collect(1:4), DataArray(Int, 4),
                           PooledDataArray(collect(3:6)),
                           PooledDataArray(Int, 4)],
                       [:array, :naarray, :pooledarray, :napooledarray])
        describe(io, df)
        @test String(take!(io)) ==
            """
            array
            Summary Stats:
            Mean:           2.500000
            Minimum:        1.000000
            1st Quartile:   1.750000
            Median:         2.500000
            3rd Quartile:   3.250000
            Maximum:        4.000000
            Length:         4
            Type:           $Int

            naarray
            Summary Stats:
            Type:           $Int
            Number Missing: 4
            % Missing:      100.000000

            pooledarray
            Summary Stats:
            Mean:           4.500000
            Minimum:        3.000000
            1st Quartile:   3.750000
            Median:         4.500000
            3rd Quartile:   5.250000
            Maximum:        6.000000
            Length:         4
            Type:           $Int
            Number Missing: 0
            % Missing:      0.000000

            napooledarray
            Summary Stats:
            Type:           $Int
            Number Missing: 4
            % Missing:      100.000000

            """
    end
end
