using Blocks

importall Blocks

export Block, DDataFrame, as_dataframe, dreadtable, dwritetable, writetable, gather

type DDataFrame <: AbstractDataFrame
    rrefs::Vector
    procs::Vector
    nrows::Vector
    ncols::Int
    coltypes::Vector
    colindex::Index

    DDataFrame(rrefs::Vector, procs::Vector) = _dims(new(rrefs, procs))
end

Base.show(io::IO, dt::DDataFrame) = println("$(nrow(dt))x$(ncol(dt)) DDataFrame. $(length(dt.rrefs)) blocks over $(length(union(dt.procs))) processors")

gather(dt::DDataFrame) = reduce((x,y)->vcat(fetch(x), fetch(y)), dt.rrefs) 
#convert(::Type{DataFrame}, dt::DDataFrame) = reduce((x,y)->vcat(fetch(x), fetch(y)), dt.rrefs) 

# internal methods
function _dims(dt::DDataFrame, rows::Bool=true, cols::Bool=true)
    dt.nrows = pmap(x->nrow(fetch(x)), Block(dt))
    cnames = remotecall_fetch(dt.procs[1], (x)->colnames(fetch(x)), dt.rrefs[1])
    dt.ncols = length(cnames)
    dt.colindex = Index(cnames)
    dt.coltypes = remotecall_fetch(dt.procs[1], (x)->coltypes(fetch(x)), dt.rrefs[1])
    # propagate the column names
    for nidx in 2:length(dt.procs)
        remotecall(dt.procs[nidx], x->colnames!(fetch(x), cnames), dt.rrefs[nidx])
    end
    dt
end

function as_dataframe(bio::BlockableIO; kwargs...)
    kwargs = _check_readtable_kwargs(kwargs...)
    #tbl = readtable(bio; kwargs...)
    nbytes = -1
    if isa(bio, IOStream)
        p1 = position(bio)
        seekend(bio)
        nbytes = position(bio) - p1
        seek(bio, p1)
    elseif isa(bio, IOBuffer) || isa(bio, BlockIO)
        nbytes = nb_available(bio)
    else
        error("can not determine size of stream")
    end

    kwdict = { :header => false,
        :separator => ',',
        #:allowquotes => true,
        :quotemark => ['"'],
        :decimal => '.',
        :nastrings => ASCIIString["", "NA"],
        :truestrings => ASCIIString["T", "t", "TRUE", "true"],
        :falsestrings => ASCIIString["F", "f", "FALSE", "false"],
        :makefactors => false,
        :colnames => UTF8String[],
        :cleannames => false,
        :coltypes => Any[],
        :allowcomments => false,
        :commentmark => '#',
        :ignorepadding => true,
        :skipstart => 0,
        :skiprows => Int[],
        :skipblanks => true,
        :encoding => :utf8 }

    for kw in kwargs
        kwdict[kw[1]] = kw[2]
    end

    poargs = {}
    for argname in names(DataFrames.ParseOptions)
        push!(poargs, kwdict[argname])
    end

    po = DataFrames.ParseOptions(kwdict[:header], 
                kwdict[:separator], 
                kwdict[:quotemark],
                kwdict[:decimal],
                kwdict[:nastrings],
                kwdict[:truestrings],
                kwdict[:falsestrings],
                kwdict[:makefactors],
                kwdict[:colnames],
                kwdict[:cleannames],
                kwdict[:coltypes],
                kwdict[:allowcomments],
                kwdict[:commentmark],
                kwdict[:ignorepadding],
                kwdict[:skipstart],
                kwdict[:skiprows],
                kwdict[:skipblanks],
                kwdict[:encoding])

    p = DataFrames.ParsedCSV(Array(Uint8, nbytes), Array(Int, 1), Array(Int, 1), BitArray(1))

    tbl = DataFrames.readtable!(p, bio, nbytes, po)
    tbl
end

as_dataframe(A::Array) = DataFrame(A)

Block(dt::DDataFrame) = Block(dt, dt.rrefs, dt.procs, as_it_is, as_it_is)

function _check_readtable_kwargs(kwargs...)
    kwargs = {kwargs...}
    for kw in kwargs
        (kw[1] in [:skipstart, :skiprows]) && error("dreadtable does not support $(kw[1])")
    end
    for (idx,kw) in enumerate(kwargs)
        if (kw[1]==:header) 
            (kw[2] != false) && error("dreadtable does not support reading of headers")
            splice!(kwargs, idx)
            break
        end
    end
    push!(kwargs, (:header,false))
    kwargs
end

function dreadtable(b::Block; kwargs...)
    kwargs = _check_readtable_kwargs(kwargs...)
    if (b.affinity == Blocks.no_affinity) 
        b.affinity = [[w] for w in workers()]
    end
    rrefs = pmap(x->as_dataframe(x;kwargs...), b; fetch_results=false)
    procs = map(x->x.where, rrefs)
    DDataFrame(rrefs, procs)
end
dreadtable(fname::String; kwargs...) = dreadtable(Block(Base.FS.File(fname)) |> as_io |> as_recordio; kwargs...)
function dreadtable(io::Union(Base.AsyncStream,IOStream), chunk_sz::Int, merge_chunks::Bool=true; kwargs...)
    b = (Block(io, chunk_sz, '\n') .> as_recordio) .> as_bytearray
    rrefs = pmap(x->as_dataframe(PipeBuffer(x); kwargs...), b; fetch_results=false)
    procs = map(x->x.where, rrefs)

    if merge_chunks
        uniqprocs = unique(procs)
        collected_refs = map(proc->rrefs[find(x->(x==proc), procs)], uniqprocs)
        merging_block = Block(collected_refs, collected_refs, uniqprocs, as_it_is, as_it_is)

        vcat_refs = pmap(reflist->vcat([fetch(x) for x in reflist]...), merging_block; fetch_results=false)
        rrefs = vcat_refs
        procs = uniqprocs
    end
    
    DDataFrame(rrefs, procs)
end


##
# describe for ddataframe
# approximate median and quantile calculation
# not very efficient as it is an iterative process
function _randcolval(t, colname, minv, maxv)
    ex = :(minv .< colname .< maxv)
    ex.args[1] = minv
    ex.args[3] = symbol(colname)
    ex.args[5] = maxv
    md = t[ex,symbol(colname)]
    (length(md) == 0) && (return [])
    return md[rand(1:length(md))]
end

function _count_col_seps(t::DataFrame, colname, v)
    nlt = ngt = 0
    col = t[colname]
    for idx in 1:nrow(t)
        isna(col[idx]) && continue
        (col[idx] > v) && (ngt += 1)
        (col[idx] < v) && (nlt += 1)
    end
    nlt,ngt
end

function _count_col_seps(dt::DDataFrame, colname, v)
    f = let colname=colname,v=v
        (t)->_count_col_seps(fetch(t), colname, v)
    end
    nltgt = pmap(f, Block(dt))
    nlt = ngt = 0
    for (n1lt,n1gt) in nltgt
        nlt += n1lt
        ngt += n1gt
    end
    (nlt,ngt)
end

function _num_na(t, cnames)
    colnames = collect(cnames)
    nrows = nrow(t)
    cnts = zeros(Int,length(colnames))
    c = t[colnames]
    for cidx in 1:length(colnames)
        cc = c[cidx]
        cnt = 0
        for idx in 1:nrows
            isna(cc[idx]) && (cnt += 1)
        end
        cnts[cidx] = cnt
    end
    cnts
end

function _colranges(t::DataFrame, cnames)
    colnames = collect(cnames)
    nrows = nrow(t)
    ncols = length(colnames)
    mins = cell(ncols)
    maxs = cell(ncols)
    sums = zeros(ncols)
    numvalids = zeros(Int,ncols)
    for cidx in 1:ncols
        _min = _max = NA
        _sum = 0
        _numvalid = 0
        cc = t[colnames[cidx]]
        for idx in 1:nrows
            ccval = cc[idx]
            if !isna(ccval) 
                (isna(_min) || (_min > ccval)) && (_min = ccval)
                (isna(_max) || (_max < ccval)) && (_max = ccval)
                _sum += ccval
                _numvalid += 1
            end
        end
        mins[cidx] = _min
        maxs[cidx] = _max
        sums[cidx] = _sum
        numvalids[cidx] = _numvalid
    end
    mins,maxs,sums,numvalids
end

function _colranges(dt::DDataFrame, cnames)
    f = let cnames=cnames
            (t)->_colranges(fetch(t), cnames)
        end
    ret = pmap(f, Block(dt))
    allmins = map(x->x[1], ret)
    allmaxs = map(x->x[2], ret)
    allsums = map(x->x[3], ret)
    allnumvalids = map(x->x[4], ret)
    ncols = length(cnames)

    mins = {}
    maxs = {}
    sums = {}
    numvalids = {}
    for cidx in 1:ncols
        push!(mins, mapreduce(x->x[cidx], min, allmins))
        push!(maxs, mapreduce(x->x[cidx], max, allmaxs))
        push!(sums, mapreduce(x->x[cidx], +, allsums))
        push!(numvalids, mapreduce(x->x[cidx], +, allnumvalids))
    end
    mins,maxs,(sums./numvalids),numvalids
end

function _sorted_col_vals_at_pos(dt::DDataFrame, col, numvalid, minv, maxv, pos)
    (isna(minv) || isna(maxv)) && (return NA)
    posr = numvalid - pos

    while true
        # get a random value between min and max for col
        f = let col=col,minv=minv,maxv=maxv
            (t)->_randcolval(fetch(t), col, minv, maxv)
        end
        pivots = pmap(f, Block(dt))
        pivots = filter(x->(x != []), pivots)
        # there's no other value between minv and maxv. take pivot as one of min and max
        (length(pivots) == 0) && (pivots = [minv, maxv])
        pivot = pivots[rand(1:length(pivots))][1]

        #println("pivot $pivot chosen from: $pivots")

        nrowslt,nrowsgt = _count_col_seps(dt, col, pivot)

        #println("for $(col) => $(minv):$(maxv). rowdist: $(nrowslt) - $(pos) - $(nrowsgt)")
        if (nrowslt <= pos) && (nrowsgt <= posr)
            return pivot
        elseif (nrowsgt > posr)
            minv = pivot
        else  # (nrowslt > pos)
            maxv = pivot
        end
    end
end

function _dquantile(dt::DDataFrame, cname, numvalid, minv, maxv, q)
    qpos = quantile([1:numvalid], q)
    lo = ifloor(qpos)
    hi = iceil(qpos)

    local retvals::Array

    if lo == hi
        retval = _sorted_col_vals_at_pos(dt, cname, numvalid, minv, maxv, lo)
    else
        retval1 = _sorted_col_vals_at_pos(dt, cname, numvalid, minv, maxv, lo)
        retval2 = _sorted_col_vals_at_pos(dt, cname, numvalid, minv, maxv, hi)
        retval = (retval1 + (retval2-retval1)*(qpos-lo))
    end
    retval
end

describe(dt::DDataFrame) = describe(STDOUT, dt)
function describe(io, dt::DDataFrame)
    nrows = nrow(dt)
    cnames = colnames(dt)
    ctypes = coltypes(dt)
    qcolnames = String[]
    for idx in 1:length(cnames)
        ((ctypes[idx] <: Number)) && push!(qcolnames, cnames[idx])
    end

    numnas = pmapreduce(x->_num_na(fetch(x), cnames), +, Block(dt))
    qcols = Dict()
    if !isempty(qcolnames)
        mins,maxs,means,numvalids = _colranges(dt, qcolnames)

        for idx in 1:length(qcolnames)
            q1 = _dquantile(dt, qcolnames[idx], numvalids[idx], mins[idx], maxs[idx], 0.25)
            q2 = _dquantile(dt, qcolnames[idx], numvalids[idx], mins[idx], maxs[idx], 0.5)
            q3 = _dquantile(dt, qcolnames[idx], numvalids[idx], mins[idx], maxs[idx], 0.75)

            qcols[qcolnames[idx]] = {mins[idx], q1, q2, means[idx], q3, maxs[idx]}
        end
    end

    statNames = ["Min", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max"]
    for idx in 1:length(cnames)
        println(cnames[idx])
        if numnas[idx] == nrows
            println(io, " * All NA * ")
            continue
        end

        if (ctypes[idx] <: Number)
            statVals = qcols[cnames[idx]]
            for i = 1:6
                println(io, string(rpad(statNames[i], 8, " "), " ", string(statVals[i])))
            end
        else
            println(io, "Length  $(nrows)")
            println(io, "Type    $(ctypes[idx])")
        end

        println(io, "NAs     $(numnas[idx])")
        println(io, "NA%     $(round(numnas[idx]*100/nrows, 2))%")
        println(io, "")
    end
end


##
# indexing into DDataFrames
function Base.getindex(dt::DDataFrame, col_ind::DataFrames.ColumnIndex)
    rrefs = pmap(x->getindex(fetch(x), col_ind), Block(dt); fetch_results=false)
    DDataFrame(rrefs, dt.procs)
end
function Base.getindex{T <: DataFrames.ColumnIndex}(dt::DDataFrame, col_inds::AbstractVector{T})
    rrefs = pmap(x->getindex(fetch(x), col_inds), Block(dt); fetch_results=false)
    DDataFrame(rrefs, dt.procs)
end

# Operations on Distributed DataFrames
# TODO: colmedians, colstds, colvars, colffts, colnorms

for f in [DataFrames.elementary_functions, DataFrames.unary_operators, :copy, :deepcopy, :isfinite, :isnan]
    @eval begin
        function ($f)(dt::DDataFrame)
            rrefs = pmap(x->($f)(fetch(x)), Block(dt); fetch_results=false)
            DDataFrame(rrefs, dt.procs)
        end
    end
end

for f in [:without]
    @eval begin
        function ($f)(dt::DDataFrame, p1)
            rrefs = pmap(x->($f)(fetch(x), p1), Block(dt); fetch_results=false)
            DDataFrame(rrefs, dt.procs)
        end
    end
end

with(dt::DDataFrame, c::Expr) = vcat(pmap(x->with(fetch(x), c), Block(dt))...)
with(dt::DDataFrame, c::Symbol) = vcat(pmap(x->with(fetch(x), c), Block(dt))...)

function Base.delete!(dt::DDataFrame, c)
    pmap(x->begin delete!(fetch(x),c); nothing; end, Block(dt))
    _dims(dt, false, true)
end

function deleterows!(dt::DDataFrame, keep_inds::Vector{Int})
    # split keep_inds based on index ranges
    split_inds = {}
    beg_row = 1
    for idx in 1:length(dt.nrows)
        end_row = dt.nrows[idx]
        part_rows = filter(x->(beg_row <= x <= (beg_row+end_row-1)), keep_inds) .- (beg_row-1)
        push!(split_inds, remotecall_wait(dt.procs[idx], DataFrame, part_rows))
        beg_row = end_row+1
    end
    dt_keep_inds = DDataFrame(split_inds, dt.procs)
    
    pmap((x,y)->begin DataFrames.deleterows!(fetch(x),y[1].data); nothing; end, Block(dt), Block(dt_keep_inds))
    _dims(dt, true, false)
end

function within!(dt::DDataFrame, c::Expr)
    pmap(x->begin within!(fetch(x),c); nothing; end, Block(dt))
    _dims(dt, false, true)
end

for f in (:(DataArrays.isna), :complete_cases)
    @eval begin
        function ($f)(dt::DDataFrame)
            vcat(pmap(x->($f)(fetch(x)), Block(dt))...)
        end
    end
end    
function complete_cases!(dt::DDataFrame)
    pmap(x->begin complete_cases!(fetch(x)); nothing; end, Block(dt))
    _dims(dt, true, true)
end



for f in DataFrames.binary_operators
    @eval begin
        function ($f)(dt::DDataFrame, x::Union(Number, NAtype))
            rrefs = pmap(y->($f)(fetch(y),x), Block(dt); fetch_results=false)
            DDataFrame(rrefs, dt.procs)
        end
        function ($f)(x::Union(Number, NAtype), dt::DDataFrame)
            rrefs = pmap(y->($f)(x,fetch(y)), Block(dt); fetch_results=false)
            DDataFrame(rrefs, dt.procs)
        end
    end
end

for (f,_) in DataFrames.vectorized_comparison_operators
    for t in [:Number, :String, :NAtype]
        @eval begin
            function ($f){T <: ($t)}(dt::DDataFrame, x::T)
                rrefs = pmap(y->($f)(fetch(y),x), Block(dt); fetch_results=false)
                DDataFrame(rrefs, dt.procs)
            end
            function ($f){T <: ($t)}(x::T, dt::DDataFrame)
                rrefs = pmap(y->($f)(x,fetch(y)), Block(dt); fetch_results=false)
                DDataFrame(rrefs, dt.procs)
            end
        end
    end
    @eval begin
        function ($f)(a::DDataFrame, b::DDataFrame)
            rrefs = pmap((x,y)->($f)(fetch(x),fetch(y)), Block(a), Block(b); fetch_results=false)
            DDataFrame(rrefs, a.procs)
        end
    end
end

for f in (:colmins, :colmaxs, :colprods, :colsums, :colmeans)
    @eval begin
        function ($f)(dt::DDataFrame)
            ($f)(vcat(pmap(x->($f)(fetch(x)), Block(dt))...))
        end
    end
end    

for f in DataFrames.array_arithmetic_operators
    @eval begin
        function ($f)(a::DDataFrame, b::DDataFrame)
            # TODO: check dimensions
            rrefs = pmap((x,y)->($f)(fetch(x),fetch(y)), Block(a), Block(b); fetch_results=false)
            DDataFrame(rrefs, a.procs)
        end
    end
end

for f in [:(Base.all), :(Base.any)]
    @eval begin
        function ($f)(dt::DDataFrame)
            ($f)(pmap(x->($f)(fetch(x)), Block(dt)))
        end
    end
end

function Base.isequal(a::DDataFrame, b::DDataFrame)
    all(pmap((x,y)->isequal(fetch(x),fetch(y)), Block(a), Block(b)))
end

nrow(dt::DDataFrame) = sum(dt.nrows)
ncol(dt::DDataFrame) = dt.ncols
DataArrays.head(dt::DDataFrame) = remotecall_fetch(dt.procs[1], x->head(fetch(x)), dt.rrefs[1])
DataArrays.tail(dt::DDataFrame) = remotecall_fetch(dt.procs[end], x->tail(fetch(x)), dt.rrefs[end])
colnames(dt::DDataFrame) = dt.colindex.names
function colnames!(dt::DDataFrame, vals) 
    pmap(x->colnames!(fetch(x), vals), Block(dt))
    names!(dt.colindex, vals)
end
function clean_colnames!(dt::DDataFrame)
    new_names = map(n -> replace(n, r"\W", "_"), colnames(dt))
    colnames!(dt, new_names)
    return
end

for f in [:rename, :rename!]
    @eval begin
        function ($f)(dt::DDataFrame, from, to)
            pmap(x->($f)(fetch(x), from, to), Block(dt); fetch_results=false)
            ($f)(dt.colindex, from, to)
        end
    end
end

coltypes(dt::DDataFrame) = dt.coltypes
index(dt::DDataFrame) = dt.colindex

for f in [:vcat, :hcat, :rbind, :cbind]
    @eval begin
        function ($f)(dt::DDataFrame...)
            rrefs = pmap((x...)->($f)([fetch(y) for y in x]...), [Block(a) for a in dt]...; fetch_results=false)
            procs = dt[1].procs
            DDataFrame(rrefs, procs)   
        end
    end
end

function Base.merge(dt::DDataFrame, t::DataFrame, bycol, jointype)
    (jointype != "inner") && error("only inner joins are supported")
    
    rrefs = pmap((x)->merge(fetch(x),t), Block(dt); fetch_results=false)
    DDataFrame(rrefs, dt.procs)
end

function Base.merge(t::DataFrame, dt::DDataFrame, bycol, jointype)
    (jointype != "inner") && error("only inner joins are supported")
    
    rrefs = pmap((x)->merge(t,fetch(x)), Block(dt); fetch_results=false)
    DDataFrame(rrefs, dt.procs)
end

colwise(f::Function, dt::DDataFrame) = error("Not supported. Try colwise variant meant for DDataFrame instead.")
colwise(fns::Vector{Function}, dt::DDataFrame) = error("Not supported. Try colwise variant meant for DDataFrame instead.")
colwise(d::DDataFrame, s::Vector{Symbol}, cn::Vector) = error("Not supported. Try colwise variant meant for DDataFrame instead.")

function colwise(f::Function, r::Function, dt::DDataFrame)
    resarr = pmap((x)->colwise(f,fetch(x)), Block(dt))
    combined = hcat(resarr...)
    map(x->r([combined[x, :]...]), 1:size(combined,1))
end
function colwise(fns::Vector{Function}, rfns::Vector{Function}, dt::DDataFrame) 
    nfns = length(fns)
    (nfns != length(rfns)) && error("number of operations must match number of reduce operations")
    resarr = pmap((x)->colwise(fns,fetch(x)), Block(dt))
    combined = hcat(resarr...)
    map(x->(rfns[x%nfns=1])([combined[x, :]...]), 1:size(combined,1))
end
function colwise(dt::DDataFrame, s::Vector{Symbol}, reduces::Vector{Function}, cn::Vector)
    nfns = length(s)
    (nfns != length(reduces)) && error("number of operations must match number of reduce operations")
    resarr = pmap((x)->colwise(fetch(x), s, cn), Block(dt))
    combined = vcat(resarr...)
    resdf = DataFrame()
    
    for (idx,(colname,col)) in enumerate(combined)
        resdf[colname] = (reduces[idx%nfns+1])(col)
    end
    resdf
end

by(dt::DDataFrame, cols, f::Function) = error("Not supported. Try by variant meant for DDataFrame instead.")
by(dt::DDataFrame, cols, e::Expr) = error("Not supported. Try by variant meant for DDataFrame instead.")
by(dt::DDataFrame, cols, s::Vector{Symbol}) = error("Not supported. Try by variant meant for DDataFrame instead.")
by(dt::DDataFrame, cols, s::Symbol) = error("Not supported. Try by variant meant for DDataFrame instead.")

function by(dt::DDataFrame, cols, f, reducer::Function)
    resarr = pmap((x)->by(fetch(x), cols, f), Block(dt))
    combined = vcat(resarr...)
    by(combined, cols, x->reducer(x[end]))
end


dwritetable(path::String, suffix::String, dt::DDataFrame; kwargs...) = pmap(x->begin; fn=joinpath(path, string(myid())*"."*suffix); writetable(fn, fetch(x); kwargs...); fn; end, Block(dt))

function writetable(filename::String, dt::DDataFrame, do_gather::Bool=false; kwargs...)
    do_gather && (return writetable(filename, gather(dt); kwargs...))

    hdr = (:header,true)
    hdrnames = []
    for (idx,kw) in enumerate(kwargs)
        (kw[1]==:header) && (hdr=splice!(kwargs, idx); break)
    end
    push!(kwargs, (:header,false))

    basen = basename(filename)
    path = filename[1:(length(filename)-length(basename(filename)))]
    filenames = dwritetable(path, basen, dt, header=false)

    if hdr[2]
        h = DataFrame()
        for cns in colnames(dt) h[cns] = [] end
        writetable(filename, h)
    end
    f = open(filename, hdr[2] ? "a" : "w")

    const lb = 1024*16
    buff = Array(Uint8, lb)
    for fn in filenames
        fp = open(fn)
        while(!eof(fp))
            avlb = nb_available(fp)
            write(f, read(fp, (avlb < lb) ? Array(Uint8, avlb) : buff))
        end
        close(fp)
        rm(fn)
    end
    close(f)
end


