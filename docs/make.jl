using Documenter, DocumenterMarkdown
using DataFrames
using CategoricalArrays

# Build documentation.
# ====================
makedocs(
    modules = [DataFrames],
    clean=true,
    doctest=false,
    sitename="DataFrames.jl",
    authors="Bogumił Kamiński et al.",
    strict=[
        :doctest,
        :linkcheck,
        :parse_error,
        :example_block,
        # Other available options are
        # :autodocs_block, :cross_references, :docs_block, :eval_block, :example_block,
        # :footnote, :meta_block, :missing_docs, :setup_block
    ], checkdocs=:all, format=Markdown(), draft=false,
    build=joinpath(@__DIR__, "docs")
)

# Deploy built documentation from Travis.
# =======================================

deploydocs(; repo="github.com/JuliaData/DataFrames.jl.git", push_preview=true,
    deps=Deps.pip("mkdocs", "pygments", "python-markdown-math", "mkdocs-material",
        "pymdown-extensions", "mkdocstrings", "mknotebooks",
        "pytkdocs_tweaks", "mkdocs_include_exclude_files", "jinja2", "mike"),
    make=() -> run(`mkdocs build`), target="site", devbranch="main")
