using Documenter, DataFrames

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
            "Querying frameworks" => "man/querying_frameworks.md"
        ],
        "API" => Any[
            "Types" => "lib/types.md",
            "Functions" => "lib/functions.md"
        ]
    ]
)

# Deploy built documentation from Travis.
# =======================================

deploydocs(
    # options
    repo = "github.com/JuliaData/DataFrames.jl.git",
    target = "build",
    julia = "0.6",
    deps = nothing,
    make = nothing,
)
