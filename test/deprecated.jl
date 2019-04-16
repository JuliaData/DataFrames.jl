module TestDeprecated

using Test, DataFrames
import DataFrames: identifier

const ≅ = isequal

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

# deprecated combine

df = DataFrame(a=[1, 1, 2, 2, 2], b=1:5)
gd = groupby(df, :a)
@test combine(gd) == combine(identity, gd)

@testset "categorical constructor" begin
    df = DataFrame([Int, String], [:a, :b], [false, true], 3)
    @test !(df[:a] isa CategoricalVector)
    @test df[:b] isa CategoricalVector
    @test_throws DimensionMismatch DataFrame([Int, String], [:a, :b], [true], 3)
end

@testset "DataFrame constructors" begin
    df = DataFrame(Union{Int, Missing}, 10, 3)
    @test size(df, 1) == 10
    @test size(df, 2) == 3
    @test typeof(df[1]) == Vector{Union{Int, Missing}}
    @test typeof(df[2]) == Vector{Union{Int, Missing}}
    @test typeof(df[3]) == Vector{Union{Int, Missing}}
    @test all(ismissing, df[1])
    @test all(ismissing, df[2])
    @test all(ismissing, df[3])
    @test typeof(df[:, 1]) == Vector{Union{Int, Missing}}
    @test typeof(df[:, 2]) == Vector{Union{Int, Missing}}
    @test typeof(df[:, 3]) == Vector{Union{Int, Missing}}
    @test all(ismissing, df[:, 1])
    @test all(ismissing, df[:, 2])
    @test all(ismissing, df[:, 3])

    df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}, Union{String, Missing}], 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[1]) == Vector{Union{Int, Missing}}
    @test typeof(df[2]) == Vector{Union{Float64, Missing}}
    @test typeof(df[3]) == Vector{Union{String, Missing}}
    @test all(ismissing, df[1])
    @test all(ismissing, df[2])
    @test all(ismissing, df[3])
    @test typeof(df[:, 1]) == Vector{Union{Int, Missing}}
    @test typeof(df[:, 2]) == Vector{Union{Float64, Missing}}
    @test typeof(df[:, 3]) == Vector{Union{String, Missing}}
    @test all(ismissing, df[:, 1])
    @test all(ismissing, df[:, 2])
    @test all(ismissing, df[:, 3])

    df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}, Union{String, Missing}],
                [:A, :B, :C], 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[1]) == Vector{Union{Int, Missing}}
    @test typeof(df[2]) == Vector{Union{Float64, Missing}}
    @test typeof(df[3]) == Vector{Union{String, Missing}}
    @test all(ismissing, df[1])
    @test all(ismissing, df[2])
    @test all(ismissing, df[3])
    @test typeof(df[:, 1]) == Vector{Union{Int, Missing}}
    @test typeof(df[:, 2]) == Vector{Union{Float64, Missing}}
    @test typeof(df[:, 3]) == Vector{Union{String, Missing}}
    @test all(ismissing, df[:, 1])
    @test all(ismissing, df[:, 2])
    @test all(ismissing, df[:, 3])

    df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}, Union{String, Missing}],
                [:A, :B, :C], [false, false, true], 100)
    @test size(df, 1) == 100
    @test size(df, 2) == 3
    @test typeof(df[1]) == Vector{Union{Int, Missing}}
    @test typeof(df[2]) == Vector{Union{Float64, Missing}}
    @test typeof(df[3]) <: CategoricalVector{Union{String, Missing}}
    @test all(ismissing, df[1])
    @test all(ismissing, df[2])
    @test all(ismissing, df[3])
    @test typeof(df[:, 1]) == Vector{Union{Int, Missing}}
    @test typeof(df[:, 2]) == Vector{Union{Float64, Missing}}
    @test typeof(df[:, 3]) <: CategoricalVector{Union{String, Missing}}
    @test all(ismissing, df[:, 1])
    @test all(ismissing, df[:, 2])
    @test all(ismissing, df[:, 3])
end

df = DataFrame(Union{Int, Missing}, 2, 2)
@test size(df) == (2, 2)
@test eltypes(df) == [Union{Int, Missing}, Union{Int, Missing}]

df = DataFrame([Union{Int, Missing}, Union{Float64, Missing}], [:x1, :x2], 2)
@test size(df) == (2, 2)
@test eltypes(df) == [Union{Int, Missing}, Union{Float64, Missing}]

@test df ≅ DataFrame([Union{Int, Missing}, Union{Float64, Missing}], 2)

end # module
