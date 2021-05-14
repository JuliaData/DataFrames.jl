"""
    AsTable(cols)

A type used for selection operations to signal that the columns selected by the
wrapped selector should be passed as a `NamedTuple` to the function.
"""
struct AsTable
    cols
end

Base.broadcastable(x::AsTable) = Ref(x)

function make_unique!(names::Vector{Symbol}, src::AbstractVector{Symbol};
                      makeunique::Bool=false)
    if length(names) != length(src)
        throw(DimensionMismatch("Length of src doesn't match length of names."))
    end
    seen = Set{Symbol}()
    dups = Int[]
    for i in 1:length(names)
        name = src[i]
        if in(name, seen)
            push!(dups, i)
        else
            names[i] = src[i]
            push!(seen, name)
        end
    end

    if length(dups) > 0
        if !makeunique
            dupstr = join(string.(':', unique(src[dups])), ", ", " and ")
            msg = "Duplicate variable names: $dupstr. Pass makeunique=true " *
                  "to make them unique using a suffix automatically."
            throw(ArgumentError(msg))
        end
    end

    for i in dups
        nm = src[i]
        k = 1
        while true
            newnm = Symbol("$(nm)_$k")
            if !in(newnm, seen)
                names[i] = newnm
                push!(seen, newnm)
                break
            end
            k += 1
        end
    end

    return names
end

function make_unique(names::AbstractVector{Symbol}; makeunique::Bool=false)
    make_unique!(similar(names), names, makeunique=makeunique)
end

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

function funname(f)
    n = nameof(f)
    String(n)[1] == '#' ? :function : n
end

if isdefined(Base, :ComposedFunction) # Julia >= 1.6.0-DEV.85
    using Base: ComposedFunction
else
    using Compat: ComposedFunction
end

funname(c::ComposedFunction) = Symbol(funname(c.outer), :_, funname(c.inner))

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

if VERSION >= v"1.4"
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
else
    function _spawn_for_chunks_helper(iter, lbody, basesize)
        lidx = iter.args[1]
        range = iter.args[2]
        quote
            let x = $(esc(range))
                for i in eachindex(x)
                    local $(esc(lidx)) = @inbounds x[i]
                    $(esc(lbody))
                end
            end
            nothing
        end
    end
end

"""
    @spawn_for_chunks basesize for i in range ... end

Parallelize a `for` loop by spawning separate tasks
iterating each over a chunk of at least `basesize` elements
in `range`.

A number of task higher than `Threads.nthreads()` may be spawned,
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

function _nt_like_hash(v, h::UInt)
    length(v) == 0 && return hash(NamedTuple(), h)

    h = hash((), h)
    for i in length(v):-1:1
        h = hash(v[i], h)
    end

    return xor(objectid(Tuple(propertynames(v))), h)
end

_findall(B) = findall(B)

@inline _blsr(x) = x & (x-1)

# findall returning a range when possible (all true indices are contiguous), and optimized for B::BitVector
# the main idea is taken from Base.findall(B::BitArray)
function _findall(B::BitVector)::Union{UnitRange{Int}, Vector{Int}}
    nnzB = count(B)
    nnzB == 0 && return 1:0
    nnzB == length(B) && return 1:length(B)
    local I
    Bc = B.chunks
    Bi = 1 # block index
    i1 = 1 # index of current block beginng in B
    i = 1  # index of the _next_ one in I
    c = Bc[1] # current block

    start = -1 # the begining of ones block
    stop = -1  # the end of ones block

    @inbounds while true # I not materialized
        if i > nnzB # all ones in B found
            Ir = start:start + i - 2
            @assert length(Ir) == nnzB
            return Ir
        end

        if c == 0
            if start != -1 && stop == -1
                stop = i1 - 1
            end
            while c == 0 # no need to return here as we returned above
                i1 += 64
                Bi += 1
                c = Bc[Bi]
            end
        end
        if c == ~UInt64(0)
            if stop != -1
                I = Vector{Int}(undef,nnzB)
                I[1:i-1] .= start:stop
                break
            end
            if start == -1
                start = i1
            end
            while c == ~UInt64(0)
                if Bi == length(Bc)
                    Ir = start:length(B)
                    @assert length(Ir) == nnzB
                    return Ir
                end

                i += 64
                i1 += 64
                Bi += 1
                c = Bc[Bi]
            end
        end
        if c != 0 # mixed ones and zeros in block
            tz = trailing_zeros(c)
            lz = leading_zeros(c)
            co = c >> tz == (one(UInt64) << (64 - lz - tz)) - 1 # block of countinous ones in c
            if stop != -1  # already found block of ones and zeros, just not materialized
                I = Vector{Int}(undef,nnzB)
                I[1:i-1] .= start:stop
                break
            elseif !co # not countinous ones
                I = Vector{Int}(undef,nnzB)
                if start != -1
                    I[1:i-1] .= start:start + i - 2
                end
                break
            else # countinous block of ones
                if start != -1
                    if tz > 0 # like __1111__ or 111111__
                        I = Vector{Int}(undef,nnzB)
                        I[1:i-1] .= start:start + i - 2
                        break
                    else # lz > 0, like __111111
                        stop = i1 + (64 - lz) - 1
                        i += 64 - lz

                        # return if last block
                        if Bi == length(Bc)
                            Ir = start:stop
                            @assert length(Ir) == nnzB
                            return Ir
                        end

                        i1 += 64
                        Bi += 1
                        c = Bc[Bi]
                    end
                else # start == -1
                    start = i1 + tz

                    if lz > 0 # like __111111 or like __1111__
                        stop = i1 + (64 - lz) - 1
                        i += stop - start + 1
                    else # like 111111__
                        i += 64 - tz
                    end

                    # return if last block
                    if Bi == length(Bc)
                        Ir = start:start + i - 2
                        @assert length(Ir) == nnzB
                        return Ir
                    end

                    i1 += 64
                    Bi += 1
                    c = Bc[Bi]
                end
            end
        end
    end
    @inbounds while true # I materialized, process like in Base.findall
        if i > nnzB # all ones in B found
            @assert nnzB == i - 1
            return I
        end

        while c == 0 # no need to return here as we returned above
            i1 += 64
            Bi += 1
            c = Bc[Bi]
        end

        while c == ~UInt64(0)
            I[i:i+64-1] .= i1:(i1+64-1)
            i += 64
            if Bi == length(Bc)
                @assert nnzB == i - 1
                return I
            end
            i1 += 64
            Bi += 1
            c = Bc[Bi]
        end

        while c != 0
            tz = trailing_zeros(c)
            c = _blsr(c) # zeros last nonzero bit
            I[i] = i1 + tz
            i += 1
        end
    end
    @assert false "should not be reached"
end
