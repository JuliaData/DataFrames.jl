using DataFrames, FileIO, RDatasets, Compat
fn = tempname()
open(fn, "w") do f
    redirect_stderr(f) do
        dataset("datasets", "iris")
    end
end
@test contains(readstring(fn), "WARNING: read_rda(args...) is deprecated, use FileIO.load(args...) instead.")
