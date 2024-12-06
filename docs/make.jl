using Documenter
using DataFrames
using CategoricalArrays
using StableRNGs

DocMeta.setdocmeta!(DataFrames, :DocTestSetup, :(using DataFrames); recursive=true)

# Build documentation.
# ====================

makedocs(
    # options
    modules = [DataFrames],
    doctest = true,
    clean = false,
    sitename = "DataFrames.jl",
    format = Documenter.HTML(
        canonical = "https://juliadata.github.io/DataFrames.jl/stable/",
        assets = ["assets/favicon.ico"],
        edit_link = "main",
        size_threshold_ignore = ["man/basics.md", "lib/functions.md"],
    ),
    pages = Any[
        "Introduction" => "index.md",
        "First Steps with DataFrames.jl" => "man/basics.md",
        "User Guide" => Any[
            "Getting Started" => "man/getting_started.md",
            "Working with DataFrames" => "man/working_with_dataframes.md",
            "Importing and Exporting Data (I/O)" => "man/importing_and_exporting.md",
            "Joins" => "man/joins.md",
            "Split-apply-combine" => "man/split_apply_combine.md",
            "Reshaping" => "man/reshaping_and_pivoting.md",
            "Sorting" => "man/sorting.md",
            "Categorical Data" => "man/categorical.md",
            "Missing Data" => "man/missing.md",
            "Data manipulation frameworks" => "man/querying_frameworks.md",
            "Comparison with Python/R/Stata" => "man/comparisons.md"
        ],
        "API" => Any[
            "Types" => "lib/types.md",
            "Functions" => "lib/functions.md",
            "Indexing" => "lib/indexing.md",
            "Metadata" => "lib/metadata.md",
            hide("Internals" => "lib/internals.md"),
        ]
    ],
)

# Deploy built documentation.
# ===========================

deploydocs(
    # options
    repo = "github.com/JuliaData/DataFrames.jl.git",
    target = "build",
    deps = nothing,
    make = nothing,
    devbranch = "main"
)
