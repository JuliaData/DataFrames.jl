module TestEncoding
    using Base.Test
    using DataFrames, Compat

    #test_group("We can read various file types.")

    data = joinpath(dirname(@__FILE__), "data")

    # this fails for utf8 with exception
    df = readtable("$data/encoding/latin1.csv", encoding="LATIN1")
    @test df[1, 2] == "Sí"

end
