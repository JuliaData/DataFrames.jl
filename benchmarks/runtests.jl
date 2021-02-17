@assert length(ARGS) == 2
file_loc = joinpath(dirname(@__FILE__), "innerjoin_performance.jl")
llen = ARGS[1]
rlen = ARGS[2]

for a3 in ["str", "int", "pool", "cat"],
    a4 in ["uniq", "dup", "manydup"],
    a5 in ["sort", "rand"],
    a6 in ["1", "2"]
    a4 == "uniq" && a3 in ["pool", "cat"] && continue
    run(`julia $file_loc $llen $rlen $a3 $a4 $a5 $a6`)
end
