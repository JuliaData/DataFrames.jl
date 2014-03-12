module TestUtils
    using Base.Test
    using DataArrays
    using DataFrames

    macro testid(s::String, b::Bool)
        s = esc(s)
        b = esc(b)
        quote
            @test DataFrames.isidentifier($s) == $b
            @test DataFrames.is_valid_identifier(symbol($s)) == $b
        end
    end

    # 1-character

    @testid "a" true
    @testid "1" false
    @testid "!" false
    @testid "_" true

    # 2-characters, initial alphabetical character

    @testid "aa" true
    @testid "a1" true
    @testid "a!" true
    @testid "a_" true

    # 2-characters, initial underscore

    @testid "_a" true
    @testid "_1" true
    @testid "_!" true
    @testid "__" true

    # 3-characters, initial alphabetical character

    @testid "aaa" true
    @testid "aa1" true
    @testid "aa!" true
    @testid "aa_" true

    @testid "a1a" true
    @testid "a11" true
    @testid "a1!" true
    @testid "a1_" true

    @testid "a!a" true
    @testid "a!1" true
    @testid "a!!" true
    @testid "a!_" true

    @testid "a_a" true
    @testid "a_1" true
    @testid "a_!" true
    @testid "a__" true

    # 3-characters, initial underscore

    @testid "_aa" true
    @testid "_a1" true
    @testid "_a!" true
    @testid "_a_" true

    @testid "_1a" true
    @testid "_11" true
    @testid "_1!" true
    @testid "_1_" true

    @testid "_!a" true
    @testid "_!1" true
    @testid "_!!" true
    @testid "_!_" true

    @testid "__a" true
    @testid "__1" true
    @testid "__!" true
    @testid "___" true

    # False

    @testid "1a" false
    @testid "!a" false
    @testid "a*" false
    @testid "begin" false
    @testid "end" false


    @test DataFrames.is_valid_identifier(symbol("a\u212b")) == false

    @test DataFrames.makeidentifier("%_B*\tC*") == "B_C"
    @test DataFrames.makeidentifier("2a") == "x2a"
    @test DataFrames.makeidentifier("!") == "x!"
    @test DataFrames.makeidentifier("\t_*") == "x"
    @test DataFrames.makeidentifier("begin") == "_begin"
    @test DataFrames.makeidentifier("end") == "_end"

    @test DataFrames.make_unique([:x, :x, :x_1, :x2]) == [:x, :x_2, :x_1, :x2]

    # Check that reserved words are up to date
    f = "$JULIA_HOME/../../src/julia-parser.scm"
    if isfile(f)
        r1 = r"define reserved-words '\(([^)]+)"
        r2 = r"define \(parse-block s\) \(parse-Nary s parse-eq '\([^(]+\(([^)]+)"
        body = readall(f)
        m1, m2 = match(r1, body), match(r2, body)
        if m1 == nothing || m2 == nothing
            error("Unable to extract keywords from 'julia-parser.scm'.")
        else
            rw = split(m1.captures[1]*" "*m2.captures[1], r"\W+")
            @test rw == DataFrames.RESERVED_WORDS
        end
    else
        warn("Unable to find validate reserved words against parser. ",
             "Expected if Julia was not built from source.")
    end
end
