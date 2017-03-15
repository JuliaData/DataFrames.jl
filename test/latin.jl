module TestIO
    using Base.Test
    using DataFrames, Compat

    #test_group("We can read various file types.")

    data = joinpath(dirname(@__FILE__), "data")

    df = readtable("$data/latin1/latin1.csv", encoding=:latin1)
    @test df[1, 2] == "Sí"

    #fails for utf8
    df = readtable("$data/latin1/latin1.csv")
    @test df[1, 2] != "Sí"
    
end
