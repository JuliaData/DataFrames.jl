using Documenter, DataFrames

# Workaround for JuliaLang/julia/pull/28625
if Base.HOME_PROJECT[] !== nothing
    Base.HOME_PROJECT[] = abspath(Base.HOME_PROJECT[])
end

# Build documentation.
# ====================

makedocs(
    # options
    modules = [DataFrames],
    doctest = false,
    clean = false,
    sitename = "DataFrames.jl",
    format = :html,
    pages = Any[
        "Introduction" => "index.md",
        "User Guide" => Any[
            "Getting Started" => "man/getting_started.md",
            "Joins" => "man/joins.md",
            "Split-apply-combine" => "man/split_apply_combine.md",
            "Reshaping" => "man/reshaping_and_pivoting.md",
            "Sorting" => "man/sorting.md",
            "Categorical Data" => "man/categorical.md",
            "Missing Data" => "man/missing.md", 
            "Data manipulation frameworks" => "man/querying_frameworks.md"
        ],
        "API" => Any[
            "Types" => "lib/types.md",
            "Functions" => "lib/functions.md",
            "Indexing" => "lib/indexing.md"
        ]
    ]
)

# Deploy built documentation from Travis.
# =======================================

deploydocs(
    # options
    repo = "github.com/JuliaData/DataFrames.jl.git",
    target = "build",
    julia = "1.0",
    deps = nothing,
    make = nothing,
)
