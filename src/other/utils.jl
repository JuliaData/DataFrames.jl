"""
    gennames(n::Integer)

Generate standardized names for columns of a DataFrame.
The first name will be `:x1`, the second `:x2`, etc.
"""
function gennames(n::Integer)
    res = Vector{Symbol}(undef, n)
    for i in 1:n
        res[i] = Symbol(@sprintf "x%d" i)
    end
    return res
end

# Compute chunks of indices, each with at least `basesize` entries
# This method ensures balanced sizes by avoiding a small last chunk
function split_indices(len::Integer, basesize::Integer)
    len′ = Int64(len) # Avoid overflow on 32-bit machines
    @assert len′ > 0
    @assert basesize > 0
    np = Int64(max(1, len ÷ basesize))
    return split_to_chunks(len′, np)
end

function split_to_chunks(len::Integer, np::Integer)
    len′ = Int64(len) # Avoid overflow on 32-bit machines
    np′ = Int64(np)
    @assert len′ > 0
    @assert 0 < np′ <= len′
    return (Int(1 + ((i - 1) * len′) ÷ np):Int((i * len′) ÷ np) for i in 1:np)
end

function _spawn_for_chunks_helper(iter, lbody, basesize)
    lidx = iter.args[1]
    range = iter.args[2]
    quote
        let x = $(esc(range)), basesize = $(esc(basesize))
            @assert firstindex(x) == 1

            nt = Threads.nthreads()
            len = length(x)
            if nt > 1 && len > basesize
                tasks = [Threads.@spawn begin
                                for i in p
                                    local $(esc(lidx)) = @inbounds x[i]
                                    $(esc(lbody))
                                end
                            end
                            for p in split_indices(len, basesize)]
                foreach(wait, tasks)
            else
                for i in eachindex(x)
                    local $(esc(lidx)) = @inbounds x[i]
                    $(esc(lbody))
                end
            end
        end
        nothing
    end
end

"""
    @spawn_for_chunks basesize for i in range ... end

Parallelize a `for` loop by spawning separate tasks
iterating each over a chunk of at least `basesize` elements
in `range`.

A number of tasks higher than `Threads.nthreads()` may be spawned,
since that can allow for a more efficient load balancing in case
some threads are busy (nested parallelism).
"""
macro spawn_for_chunks(basesize, ex)
    if !(isa(ex, Expr) && ex.head === :for)
        throw(ArgumentError("@spawn_for_chunks requires a `for` loop expression"))
    end
    if !(ex.args[1] isa Expr && ex.args[1].head === :(=))
        throw(ArgumentError("nested outer loops are not currently supported by @spawn_for_chunks"))
    end
    return _spawn_for_chunks_helper(ex.args[1], ex.args[2], basesize)
end

"""
    @spawn_or_run_task threads expr

Equivalent to `Threads.@spawn` if `threads === true`,
otherwise run `expr` and return a `Task` that returns its value.
"""
macro spawn_or_run_task(threads, expr)
    letargs = Base._lift_one_interp!(expr)

    thunk = esc(:(()->($expr)))
    var = esc(Base.sync_varname)
    quote
        let $(letargs...)
            if $(esc(threads))
                local task = Task($thunk)
                task.sticky = false
            else
                # Run expr immediately
                res = $thunk()
                # Return a Task that returns the value of expr
                local task = Task(() -> res)
                task.sticky = true
            end
            if $(Expr(:islocal, var))
                put!($var, task)
            end
            schedule(task)
            task
        end
    end
end

"""
    @spawn_or_run threads expr

Equivalent to `Threads.@spawn` if `threads === true`,
otherwise run `expr`.
"""
macro spawn_or_run(threads, expr)
    letargs = Base._lift_one_interp!(expr)

    thunk = esc(:(()->($expr)))
    var = esc(Base.sync_varname)
    quote
        let $(letargs...)
            if $(esc(threads))
                local task = Task($thunk)
                task.sticky = false
                if $(Expr(:islocal, var))
                    put!($var, task)
                end
                schedule(task)
            else
                $thunk()
            end
            nothing
        end
    end
end

function _nt_like_hash(v, h::UInt)
    length(v) == 0 && return hash(NamedTuple(), h)

    h = hash((), h)
    for i in length(v):-1:1
        h = hash(v[i], h)
    end

    return xor(objectid(Tuple(propertynames(v))), h)
end
