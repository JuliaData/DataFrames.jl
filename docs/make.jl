using Documenter, DataTables

# Build documentation.
# ====================

makedocs(
    # options
    modules = [DataTables],
    doctest = true,
    clean = false,
    sitename = "DataTables.jl",
    format = Documenter.Formats.HTML,
    pages = Any[
        "Introduction" => "index.md",
        "User Guide" => Any[
            "Getting Started" => "man/getting_started.md",
            "Joins" => "man/joins.md",
            "Split-apply-combine" => "man/split_apply_combine.md",
            "Reshaping" => "man/reshaping_and_pivoting.md",
            "Sorting" => "man/sorting.md",
            "Categorical Data" => "man/categorical.md",
            "Querying frameworks" => "man/querying_frameworks.md",
        ],
        "API" => Any[
            "Main types" => "lib/maintypes.md",
            "Utilities" => "lib/utilities.md",
            "Data manipulation" => "lib/manipulation.md",
        ],
        "About" => Any[
            "Release Notes" => "NEWS.md",
            "License" => "LICENSE.md",
        ]
    ]
)

# Deploy built documentation from Travis.
# =======================================

deploydocs(
    # options
    repo = "github.com/JuliaData/DataTables.jl.git",
    target = "build",
    deps = nothing,
    make = nothing,
)
