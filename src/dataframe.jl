
# Abstract DF includes DataFrame and SubDataFrame
abstract AbstractDataFrame <: Associative{Any,Any}

# ## DataFrame - a list of heterogeneous Data vectors with col and row indexs.
# Columns are a vector, which means that operations that insert/delete columns
# are O(n).
type DataFrame <: AbstractDataFrame
    columns::Vector{Any} 
    colindex::Index
    function DataFrame(cols::Vector, colindex::Index)
        # all columns have to be the same length
        if length(cols) > 1 && !all(map(length, cols) .== length(cols[1]))
            error("all columns in a DataFrame have to be the same length")
        end
        # colindex has to be the same length as columns vector
        if length(colindex) != length(cols)
            error("column names/index must be the same length as the number of columns")
        end
        new(cols, colindex)
    end
end

# constructors
DataFrame(cs::Vector) = DataFrame(cs, paste("x", map(string,[1:length(cs)])))
DataFrame(cs::Vector, cn::Vector) = DataFrame(cs, Index(cn))
# TODO expand the following to allow unequal lengths that are rep'd to the longest length.
DataFrame(ex::Expr) = based_on(DataFrame(), ex)
DataFrame{T}(x::Array{T,2}, cn::Vector) = DataFrame({x[:,i] for i in 1:length(cn)}, cn)
DataFrame{T}(x::Array{T,2}) = DataFrame(x, [strcat("x", i) for i in 1:size(x,2)])


colnames(df::DataFrame) = names(df.colindex)
names!(df::DataFrame, vals) = names!(df.colindex, vals)
colnames!(df::DataFrame, vals) = names!(df.colindex, vals)
replace_names!(df::DataFrame, from, to) = replace_names!(df.colindex, from, to)
replace_names(df::DataFrame, from, to) = replace_names(df.colindex, from, to)
ncol(df::DataFrame) = length(df.colindex)
nrow(df::DataFrame) = ncol(df) > 0 ? length(df.columns[1]) : 0
names(df::AbstractDataFrame) = colnames(df)
size(df::AbstractDataFrame) = (nrow(df), ncol(df))
size(df::AbstractDataFrame, i::Integer) = i==1 ? nrow(df) : (i==2 ? ncol(df) : error("DataFrames have two dimensions only"))
length(df::AbstractDataFrame) = ncol(df)
ndims(::AbstractDataFrame) = 2

ref(df::DataFrame, c) = df[df.colindex[c]]
ref(df::DataFrame, c::Integer) = df.columns[c]
ref(df::DataFrame, c::Vector{Int}) = DataFrame(df.columns[c], convert(Vector{ByteString}, colnames(df)[c]))

ref(df::DataFrame, r, c) = df[r, df.colindex[c]]
ref(df::DataFrame, r, c::Int) = df[c][r]
ref(df::DataFrame, r, c::Vector{Int}) =
    DataFrame({x[r] for x in df.columns[c]}, 
              convert(Vector{ByteString}, colnames(df)[c]))

# special cases
ref(df::DataFrame, r::Int, c::Int) = df[c][r]
ref(df::DataFrame, r::Int, c::Vector{Int}) = df[[r], c]
ref(df::DataFrame, r::Int, c) = df[r, df.colindex[c]]
ref(df::DataFrame, dv::AbstractDataVec) = df[with(df, ex), c]
ref(df::DataFrame, ex::Expr) = df[with(df, ex), :]  
ref(df::DataFrame, ex::Expr, c::Int) = df[with(df, ex), c]
ref(df::DataFrame, ex::Expr, c::Vector{Int}) = df[with(df, ex), c]
ref(df::DataFrame, ex::Expr, c) = df[with(df, ex), c]



# Associative methods:
has(df::DataFrame, key) = has(df.colindex, key)
get(df::DataFrame, key, default) = has(df, key) ? df[key] : default
keys(df::DataFrame) = keys(df.colindex)
values(df::DataFrame) = df.columns
del_all(df::DataFrame) = DataFrame()
# Collection methods:
start(df::AbstractDataFrame) = 1
done(df::AbstractDataFrame, i) = i > ncol(df)
next(df::AbstractDataFrame, i) = (df[i], i + 1)
## numel(df::AbstractDataFrame) = ncol(df)
isempty(df::AbstractDataFrame) = ncol(df) == 0
# Column groups
set_group(d::DataFrame, newgroup, names) = set_group(d.colindex, newgroup, names)
set_groups(d::DataFrame, gr::Dict{ByteString,Vector{ByteString}}) = set_groups(d.colindex, gr)
get_groups(d::DataFrame) = get_groups(d.colindex)

function insert(df::DataFrame, index::Integer, item, name)
    @assert 0 < index <= ncol(df) + 1
    df = shallowcopy(df)
    df[name] = item
    # rearrange:
    df[[1:index-1, end, index:end-1]]
end

function insert(df::DataFrame, df2::DataFrame)
    @assert nrow(df) == nrow(df2) || nrow(df) == 0
    for n in colnames(df2)
        df[n] = df2[n]
    end
    df
end

# if we have something else, convert each value in this tuple to a DataVec and pass it in, hoping for the best
DataFrame(vals...) = DataFrame([DataVec(x) for x = vals])
# if we have a matrix, create a tuple of columns and pass that in
DataFrame{T}(m::Array{T,2}) = DataFrame([DataVec(squeeze(m[:,i])) for i = 1:size(m)[2]])
# 

function DataFrame{K,V}(d::Associative{K,V})
    # Find the first position with maximum length in the Dict.
    # I couldn't get findmax to work here.
    ## (Nrow,maxpos) = findmax(map(length, values(d)))
    lengths = map(length, values(d))
    maxpos = find(lengths .== max(lengths))[1]
    keymaxlen = keys(d)[maxpos]
    Nrow = length(d[keymaxlen])
    # Start with a blank DataFrame
    df = DataFrame() 
    for (k,v) in d
        if length(v) == Nrow
            df[k] = v  
        elseif rem(Nrow, length(v)) == 0    # Nrow is a multiple of length(v)
            df[k] = vcat(fill(v, div(Nrow, length(v)))...)
        else
            vec = fill(v[1], Nrow)
            j = 1
            for i = 1:Nrow
                vec[i] = v[j]
                j += 1
                if j > length(v)
                    j = 1
                end
            end
            df[k] = vec
        end
    end
    df
end

# Blank DataFrame
DataFrame() = DataFrame({}, Index())

# copy of a data frame does a deep copy
copy(df::DataFrame) = DataFrame([copy(x) for x in df.columns], colnames(df))
shallowcopy(df::DataFrame) = DataFrame(df.columns, colnames(df))

# dimilar of a data frame creates new vectors, but with the same columns. Dangerous, as 
# changing the in one df can break the other.

# Equality
function ==(df1::AbstractDataFrame, df2::AbstractDataFrame)
    if ncol(df1) != ncol(df2)
        return false
    end
    for idx in 1:ncol(df1)
        if !(df1[idx] == df2[idx])
            return false
        end
    end
    return true
end

head(df::DataFrame, r::Int) = df[1:r, :]
head(df::DataFrame) = head(df, 6)
tail(df::DataFrame, r::Int) = df[(nrow(df)-r+1):nrow(df), :]
tail(df::DataFrame) = tail(df, 6)



# to print a DataFrame, find the max string length of each column
# then print the column names with an appropriate buffer
# then row-by-row print with an appropriate buffer
_string(x) = sprint(showcompact, x)
maxShowLength(v::Vector) = length(v) > 0 ? max([length(_string(x)) for x = v]) : 0
maxShowLength(dv::AbstractDataVec) = max([length(_string(x)) for x = dv])
function show(io, df::AbstractDataFrame)
    ## TODO use alignment() like print_matrix in show.jl.
    println(io, "$(typeof(df))  $(size(df))")
    gr = get_groups(df)
    if length(gr) > 0
        #print(io, "Column groups: ")
        pretty_show(io, gr)
        println(io)
    end
    N = nrow(df)
    Nmx = 20   # maximum head and tail lengths
    if N <= 2Nmx
        rowrng = 1:min(2Nmx,N)
    else
        rowrng = [1:Nmx, N-Nmx+1:N]
    end
    # we don't have row names -- use indexes
    rowNames = [sprintf("[%d,]", r) for r = rowrng]
    
    rownameWidth = maxShowLength(rowNames)
    
    # if we don't have columns names, use indexes
    # note that column names in R are obligatory
    if eltype(colnames(df)) == Nothing
        colNames = [sprintf("[,%d]", c) for c = 1:ncol(df)]
    else
        colNames = colnames(df)
    end
    
    colWidths = [max(length(string(colNames[c])), maxShowLength(df[rowrng,c])) for c = 1:ncol(df)]

    header = strcat(" " ^ (rownameWidth+1),
                    join([lpad(string(colNames[i]), colWidths[i]+1, " ") for i = 1:ncol(df)], ""))
    println(io, header)

    for i = 1:length(rowrng)
        rowname = rpad(string(rowNames[i]), rownameWidth+1, " ")
        line = strcat(rowname,
                      join([lpad(_string(df[rowrng[i],c]), colWidths[c]+1, " ") for c = 1:ncol(df)], ""))
        println(io, line)
        if i == Nmx && N > 2Nmx
            println(io, "  :")
        end
    end
end

# get the structure of a DF

function dump(io::IOStream, x::AbstractDataFrame, n::Int, indent)
    println(io, typeof(x), "  $(nrow(x)) observations of $(ncol(x)) variables")
    gr = get_groups(x)
    if length(gr) > 0
        pretty_show(io, gr)
        println(io)
    end
    if n > 0
        for col in names(x)[1:min(10,end)]
            print(io, indent, "  ", col, ": ")
            dump(io, x[col], n - 1, strcat(indent, "  "))
        end
    end
end
dump(io::IOStream, x::AbstractDataVec, n::Int, indent) =
    println(io, typeof(x), "(", length(x), ") ", x[1:min(4, end)])

# summarize the columns of a DF
# if the column's base type derives from Number, 
# compute min, 1st quantile, median, mean, 3rd quantile, and max
# filtering NAs, which are reported separately
# if boolean, report trues, falses, and NAs
# if anything else, punt.
# Note that R creates a summary object, which has a print method. That's
# a reasonable alternative to this. The summary() functions in show.jl
# return a string.
summary(dv::AbstractDataVec) = summary(OUTPUT_STREAM::IOStream, dv)
summary(df::DataFrame) = summary(OUTPUT_STREAM::IOStream, df)
function summary{T<:Number}(io, dv::AbstractDataVec{T})
    filtered = nafilter(dv)
    qs = quantile(filtered, [0, .25, .5, .75, 1])
    statNames = ["Min", "1st Qu.", "Median", "Mean", "3rd Qu.", "Max"]
    statVals = [qs[1:3], mean(filtered), qs[4:5]]
    for i = 1:6
        println(io, strcat(rpad(statNames[i], 8, " "), " ", string(statVals[i])))
    end
    nas = sum(isna(dv))
    if nas > 0
        println(io, "NAs      $nas")
    end
end
function summary{T}(io, dv::AbstractDataVec{T})
    ispooled = isa(dv, PooledDataVec) ? "Pooled " : ""
    # if nothing else, just give the length and element type and NA count
    println(io, "Length: $(length(dv))")
    println(io, "Type  : $(ispooled)$(string(eltype(dv)))")
    println(io, "NAs   : $(sum(isna(dv)))")
end

# TODO: clever layout in rows
# TODO: AbstractDataFrame
function summary(io, df::AbstractDataFrame)
    for c in 1:ncol(df)
        col = df[c]
        println(io, colnames(df)[c])
        summary(io, col)
        println(io, )
    end
end





# for now, use csvread to pull CSV data from disk

# colnames = "true", "false", "check" (default)
# poolstrings = "check" (default), "never" 
function csvDataFrame(filename, o::Options)
    @defaults o colnames="check" poolstrings="check"
    # TODO
    # for now, use the built-in csvread that creates a matrix of Anys, functionally numbers and strings. 
    # Ideally, we'd probably save RAM by doing a two-pass read over the file, once to determine types and
    # once to build the data structures.
    dat = csvread(filename)
    
    # if the first row looks like strings, chop it off and process it as the 
    # column names
    if colnames == "check"
        colnames = all([typeof(x)==ASCIIString for x = dat[1,:]]) ? "true" : "false"
    end
    if colnames == "true"
        columnNames = [_remove_quotes(x) for x = dat[1,:]]
        dat = dat[2:,:]
    else
        # null column names
        columnNames = []
    end
    
    # foreach column, if everything is either numeric or empty string, then build a numeric DataVec
    # otherwise build a string DataVec
    cols = Array(AbstractDataVec, size(dat,2)) # elements will be #undef initially
    for c = 1:size(dat,2)
        nas = [(x == "")::Bool for x = dat[:,c]]
        # iterate over the column, ignoring null strings, promoting through numeric types as we go, until we're done
        # simultaneously, collect a hash of up to 64K defined elements (which might look like
        # numbers, in case we have a heterogeneous column)
        # never short-circuit, as we need the full list of keys to test for booleans
        colType = None
        colIsNum = true
        colStrings = Dict{String,Bool}(0)
        colPoolStrings = poolstrings == "check" 
        for r = 1:size(dat,1)
            v = dat[r,c]
            if v != ""
                if isa(v,Number) && colIsNum
                    colType = promote_type(colType, typeof(v))
                else
                    colIsNum = false
                end
                # store that we saw this string
                colStrings[string(v)] = true # do we need a count here?
            end
        end
        if colPoolStrings && length(keys(colStrings)) > typemax(Uint16)
            # we've ran past the limit of pooled strings!
            colPoolStrings = false
        end
        
        # build DataVecs
        if _same_set(keys(colStrings), ["0", "1"])
            # boolean
            cols[c] = DataVec(dat[:,c] == "1", nas)
        elseif (colIsNum && colType != None)
            # this is annoying to have to pre-allocate the array, but comprehensions don't
            # seem to get the type right
            tmpcol = Array(colType, size(dat,1))
            for r = 1:length(tmpcol)
                tmpcol[r] = dat[r,c] == "" ? false : dat[r,c] # false is the smallest numeric 0
            end
            cols[c] = DataVec(tmpcol, nas)
        elseif _same_set(keys(colStrings), ["TRUE", "FALSE"])
            # boolean 
            cols[c] = DataVec(dat[:,c] == "TRUE", nas)
        elseif colPoolStrings
            # TODO: if we're trying to pool, build the underlying refs and pool as we check, rather
            # than throwing away eveything and starting over! we've got a perfectly nice constructor...
            cols[c] = PooledDataVec([string(_remove_quotes(x))::ASCIIString for x = dat[:,c]], nas)
        else
            cols[c] = DataVec([string(_remove_quotes(x))::ASCIIString for x = dat[:,c]], nas)
        end
    end
    
    @check_used o
    
    # combine the columns into a DataFrame and return
    if columnNames == []
        DataFrame(cols)
    else
        DataFrame(cols, columnNames)
    end
end
csvDataFrame(filename) = csvDataFrame(filename, Options())


# a SubDataFrame is a lightweight wrapper around a DataFrame used most frequently in
# split/apply sorts of operations.
type SubDataFrame <: AbstractDataFrame
    parent::DataFrame
    rows::Vector{Int} # maps from subdf row indexes to parent row indexes
    
    function SubDataFrame(parent::DataFrame, rows::Vector{Int})
        if any(rows .< 1)
            error("all SubDataFrame indices must be > 0")
        end
        if max(rows) > nrow(parent)
            error("all SubDataFrame indices must be <= the number of rows of the DataFrame")
        end
        new(parent, rows)
    end
end

sub(D::DataFrame, r, c) = sub(D[[c]], r)    # If columns are given, pass in a subsetted parent D.
                                            # Columns are not copies, so it's not expensive.
sub(D::DataFrame, r::Int) = sub(D, [r])
sub(D::DataFrame, rs::Vector{Int}) = SubDataFrame(D, rs)
sub(D::DataFrame, r) = sub(D, ref(SimpleIndex(nrow(D)), r)) # this is a wacky fall-through that uses light-weight fake indexes!
sub(D::DataFrame, ex::Expr) = sub(D, with(D, ex))

sub(D::SubDataFrame, r, c) = sub(D[[c]], r)
sub(D::SubDataFrame, r::Int) = sub(D, [r])
sub(D::SubDataFrame, rs::Vector{Int}) = SubDataFrame(D.parent, D.rows[rs])
sub(D::SubDataFrame, r) = sub(D, ref(SimpleIndex(nrow(D)), r)) # another wacky fall-through
sub(D::SubDataFrame, ex::Expr) = sub(D, with(D, ex))

ref(df::SubDataFrame, c) = df.parent[df.rows, c]
ref(df::SubDataFrame, r, c) = df.parent[df.rows[r], c]

nrow(df::SubDataFrame) = length(df.rows)
ncol(df::SubDataFrame) = ncol(df.parent)
colnames(df::SubDataFrame) = colnames(df.parent) 

head(df::AbstractDataFrame, r::Int) = df[1:min(r,nrow(df)), :]
head(df::AbstractDataFrame) = head(df, 6)
tail(df::AbstractDataFrame, r::Int) = df[max(1,nrow(df)-r+1):nrow(df), :]
tail(df::AbstractDataFrame) = tail(df, 6)

# Associative methods:
has(df::SubDataFrame, key) = has(df.colindex, key)
get(df::SubDataFrame, key, default) = has(df, key) ? df[key] : default
keys(df::SubDataFrame) = keys(df.colindex)
values(df::SubDataFrame, key) = keys(df.colindex)
del_all(df::SubDataFrame) = DataFrame()



# DF column operations
######################

# assignments return the complete object...

# df[1] = replace column
function assign(df::DataFrame, newcol::AbstractDataVec, icol::Integer)
    if icol > 0 && icol <= ncol(df)
        df.columns[icol] = newcol
    else
        throw(ArgumentError("Can't replace a non-existent DataFrame column"))
    end
    df
end
assign{T}(df::DataFrame, newcol::Vector{T}, icol::Integer) = assign(df, DataVec(newcol), icol)

# df["old"] = replace old columns
# df["new"] = append new column
function assign(df::DataFrame, newcol::AbstractDataVec, colname)
    icol = get(df.colindex.lookup, colname, 0)
    if length(newcol) != nrow(df) && nrow(df) != 0
        error("length of data doesn't match the number of rows.")
    end
    if icol > 0
        # existing
        assign(df, newcol, icol)
    else
        # new
        push(df.colindex, colname)
        push(df.columns, newcol)
    end
    df
end
assign{T}(df::DataFrame, newcol::Vector{T}, colname) = assign(df, DataVec(newcol), colname)

assign(df::DataFrame, newcol, colname) =
    nrow(df) > 0 ? assign(df, DataVec(fill(newcol, nrow(df))), colname) : assign(df, DataVec([newcol]), colname)

# do I care about vectorized assignment? maybe not...
# df[1:3] = (replace columns) eh...
# df[["new", "newer"]] = (new columns)

# df[1] = nothing
assign(df::DataFrame, x::Nothing, icol::Integer) = del!(df, icol)

# at least some of the elementwise assignments
assign(df::DataFrame, v, rows, cols) = assign(df[cols], v, rows)

# del!(df, 1)
# del!(df, "old")
function del!(df::DataFrame, icols::Vector{Int})
    for icol in icols 
        if icol > 0 && icol <= ncol(df)
            del(df.columns, icol)
            del(df.colindex, icol)
        else
            throw(ArgumentError("Can't delete a non-existent DataFrame column"))
        end
    end
    df
end
del!(df::DataFrame, c::Int) = del!(df, [c])
del!(df::DataFrame, c) = del!(df, df.colindex[c])

# df2 = del(df, 1) new DF, minus vectors
function del(df::DataFrame, icols::Vector{Int})
    newcols = _setdiff([1:ncol(df)], icols) 
    if length(newcols) == 0
        throw(ArgumentError("Can't delete a non-existent DataFrame column"))
    end
    # Note: this does not copy columns.
    df[newcols]
end
del(df::DataFrame, i::Int) = del(df, [i])
del(df::DataFrame, c) = del(df, df.colindex[c])
del(df::SubDataFrame, c) = SubDataFrame(del(df.parent, c), df.rows)


#### cbind, rbind, hcat, vcat
# hcat() is just cbind()
# rbind(df, ...) only accepts data frames. Finds union of columns, maintaining order
# of first df. Missing data becomes NAs.
# vcat() is just rbind()
 

# two-argument form, two dfs, references only
function cbind(df1::DataFrame, df2::DataFrame)
    # If df1 had metadata, we should copy that.
    colindex = Index(make_unique(concat(colnames(df1), colnames(df2))))
    columns = [df1.columns, df2.columns]
    DataFrame(columns, colindex)
end
    
# three-plus-argument form recurses
cbind(a, b, c...) = cbind(cbind(a, b), c...)
hcat(dfs::DataFrame...) = cbind(dfs...)


similar{T}(dv::DataVec{T}, dims) =
    DataVec(similar(dv.data, dims), fill(true, dims), dv.filter, dv.replace, dv.replaceVal)  

similar{T}(dv::PooledDataVec{T}, dims) =
    PooledDataVec(fill(uint16(1), dims), dv.pool, dv.filter, dv.replace, dv.replaceVal)  

similar(df::DataFrame, dims) = 
    DataFrame([similar(x, dims) for x in df.columns], colnames(df)) 

similar(df::SubDataFrame, dims) = 
    DataFrame([similar(df[x], dims) for x in colnames(df)], colnames(df)) 

function rbind(dfs::DataFrame...)
    Nrow = sum(nrow, dfs)
    Ncol = ncol(dfs[1])
    res = similar(dfs[1], Nrow)
    # TODO fix PooledDataVec columns with different pools.
    # for idx in 2:length(dfs)
    #     if colnames(dfs[1]) != colnames(dfs[idx])
    #         error("DataFrame column names must match.")
    #     end
    # end
    idx = 1
    for df in dfs
        for kdx in 1:nrow(df)
            for jdx in 1:Ncol
                res[jdx][idx] = df[kdx, jdx]
            end
            idx += 1
        end
    end
    res
end
vcat(dfs::DataFrame...) = rbind(dfs...)

function rbind(dfs::Vector)   # for a Vector of DataFrame's
    Nrow = sum(nrow, dfs)
    Ncol = ncol(dfs[1])
    res = similar(dfs[1], Nrow)
    # TODO fix PooledDataVec columns with different pools.
    # for idx in 2:length(dfs)
    #     if colnames(dfs[1]) != colnames(dfs[idx])
    #         error("DataFrame column names must match.")
    #     end
    # end
    idx = 1
    for df in dfs
        for kdx in 1:nrow(df)
            for jdx in 1:Ncol
                res[jdx][idx] = df[kdx, jdx]
            end
            idx += 1
        end
    end
    res
end


# DF row operations -- delete and append
# df[1] = nothing
# df[1:3] = nothing
# df3 = rbind(df1, df2...)
# rbind!(df1, df2...)


# split-apply-combine
# co(ap(myfun,
#    sp(df, ["region", "product"])))
# (|)(x, f::Function) = f(x)
# split(df, ["region", "product"]) | (apply(nrow)) | mean
# apply(f::function) = (x -> map(f, x))
# split(df, ["region", "product"]) | @@@)) | mean
# how do we add col names to the name space?
# transform(df, :(cat=dog*2, clean=proc(dirty)))
# summarise(df, :(cat=sum(dog), all=strcat(strs)))


function with(d::Associative, ex::Expr)
    # Note: keys must by symbols
    replace_symbols(x, d::Dict) = x
    replace_symbols(e::Expr, d::Dict) = Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, d), e.args), e.typ)
    function replace_symbols{K,V}(s::Symbol, d::Dict{K,V})
        if (K == Any || K == Symbol) && has(d, s)
            :(_D[$expr(:quote,s)])
        elseif (K == Any || K <: String) && has(d, string(s))
            :(_D[$string(s)])
        else
            s
        end
    end
    ex = replace_symbols(ex, d)
    global _ex = ex
    f = @eval (_D) -> $ex
    f(d)
end

function within!(d::Associative, ex::Expr)
    # Note: keys must by symbols
    replace_symbols(x, d::Associative) = x
    function replace_symbols{K,V}(e::Expr, d::Associative{K,V})
        if e.head == :(=) # replace left-hand side of assignments:
            if (K == Symbol || (K == Any && isa(keys(d)[1], Symbol)))
                exref = expr(:quote, e.args[1])
                if !has(d, e.args[1]) # Dummy assignment to reserve a slot.
                                      # I'm not sure how expensive this is.
                    d[e.args[1]] = values(d)[1]
                end
            else
                exref = string(e.args[1])
                if !has(d, exref) # dummy assignment to reserve a slot
                    d[exref] = values(d)[1]
                end
            end
            Expr(e.head,
                 vcat({:(_D[$exref])}, map(x -> replace_symbols(x, d), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, d), e.args), e.typ)
        end
    end
    function replace_symbols{K,V}(s::Symbol, d::Associative{K,V})
        if (K == Any || K == Symbol) && has(d, s)
            :(_D[$expr(:quote,s)])
        elseif (K == Any || K <: String) && has(d, string(s))
            :(_D[$string(s)])
        else
            s
        end
    end
    ex = replace_symbols(ex, d)
    f = @eval (_D) -> begin
        $ex
        _D
    end
    f(d)
end

function based_on(d::Associative, ex::Expr)
    # Note: keys must by symbols
    replace_symbols(x, d::Associative) = x
    function replace_symbols{K,V}(e::Expr, d::Associative{K,V})
        if e.head == :(=) # replace left-hand side of assignments:
            if (K == Symbol || (K == Any && isa(keys(d)[1], Symbol)))
                exref = expr(:quote, e.args[1])
                if !has(d, e.args[1]) # Dummy assignment to reserve a slot.
                                      # I'm not sure how expensive this is.
                    d[e.args[1]] = values(d)[1]
                end
            else
                exref = string(e.args[1])
                if !has(d, exref) # dummy assignment to reserve a slot
                    d[exref] = values(d)[1]
                end
            end
            Expr(e.head,
                 vcat({:(_ND[$exref])}, map(x -> replace_symbols(x, d), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, d), e.args), e.typ)
        end
    end
    function replace_symbols{K,V}(s::Symbol, d::Associative{K,V})
        if (K == Any || K == Symbol) && has(d, s)
            :(_D[$expr(:quote,s)])
        elseif (K == Any || K <: String) && has(d, string(s))
            :(_D[$string(s)])
        else
            s
        end
    end
    ex = replace_symbols(ex, d)
    f = @eval (_D) -> begin
        _ND = similar(_D)
        $ex
        _ND
    end
    f(d)
end


function within!(df::AbstractDataFrame, ex::Expr)
    # By-column operation within a DataFrame that allows replacing or adding columns.
    # Returns the transformed DataFrame.
    #   
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in df
    replace_symbols(x, syms::Dict) = x
    function replace_symbols(e::Expr, syms::Dict)
        if e.head == :(=) # replace left-hand side of assignments:
            if !has(syms, string(e.args[1]))
                syms[string(e.args[1])] = length(syms) + 1
            end
            Expr(e.head,
                 vcat({:(_DF[$(string(e.args[1]))])}, map(x -> replace_symbols(x, syms), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, syms), e.args), e.typ)
        end
    end
    function replace_symbols(s::Symbol, syms::Dict)
        if contains(keys(syms), string(s))
            :(_DF[$(syms[string(s)])])
        else
            s
        end
    end
    # Make a dict of colnames and column positions
    cn_dict = dict(tuple(colnames(df)...), tuple([1:ncol(df)]...))
    ex = replace_symbols(ex, cn_dict)
    f = @eval (_DF) -> begin
        $ex
        _DF
    end
    f(df)
end

within(x, args...) = within!(copy(x), args...)

function based_on_f(df::AbstractDataFrame, ex::Expr)
    # Returns a function for use on an AbstractDataFrame
    
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in a new df
    replace_symbols(x, syms::Dict) = x
    function replace_symbols(e::Expr, syms::Dict)
        if e.head == :(=) # replace left-hand side of assignments:
            if !has(syms, string(e.args[1]))
                syms[string(e.args[1])] = length(syms) + 1
            end
            Expr(e.head,
                 vcat({:(_col_dict[$(string(e.args[1]))])}, map(x -> replace_symbols(x, syms), e.args[2:end])),
                 e.typ)
        else
            Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, syms), e.args), e.typ)
        end
    end
    function replace_symbols(s::Symbol, syms::Dict)
        if contains(keys(syms), string(s))
            :(_DF[$(syms[string(s)])])
        else
            s
        end
    end
    # Make a dict of colnames and column positions
    cn_dict = dict(tuple(colnames(df)...), tuple([1:ncol(df)]...))
    ex = replace_symbols(ex, cn_dict)
    @eval (_DF) -> begin
        _col_dict = NamedArray()
        $ex
        DataFrame(_col_dict)
    end
end
function based_on(df::AbstractDataFrame, ex::Expr)
    # By-column operation within a DataFrame.
    # Returns a new DataFrame.
    f = based_on_f(df, ex)
    f(df)
end

function with(df::AbstractDataFrame, ex::Expr)
    # By-column operation with the columns of a DataFrame.
    # Returns the result of evaluating ex.
    
    # helper function to replace symbols in ex with a reference to the
    # appropriate column in df
    replace_symbols(x, syms::Dict) = x
    replace_symbols(e::Expr, syms::Dict) = Expr(e.head, isempty(e.args) ? e.args : map(x -> replace_symbols(x, syms), e.args), e.typ)
    function replace_symbols(s::Symbol, syms::Dict)
        if contains(keys(syms), string(s))
            :(_DF[$(syms[string(s)])])
        else
            s
        end
    end
    # Make a dict of colnames and column positions
    cn_dict = dict(tuple(colnames(df)...), tuple([1:ncol(df)]...))
    ex = replace_symbols(ex, cn_dict)
    f = @eval (_DF) -> $ex
    f(df)
end

with(df::AbstractDataFrame, s::Symbol) = df[string(s)]

# add function curries to ease pipelining:
with(e::Expr) = x -> with(x, e)
within(e::Expr) = x -> within(x, e)
within!(e::Expr) = x -> within!(x, e)
based_on(e::Expr) = x -> based_on(x, e)


# allow pipelining straight to an expression using within!:
(|)(x::AbstractDataFrame, e::Expr) = within!(x, e)


#
#  Split - Apply - Combine operations
#


function groupsort_indexer(x::Vector, ngroups::Integer)
    ## translated from Wes McKinney's groupsort_indexer in pandas (file: src/groupby.pyx).

    ## count group sizes, location 0 for NA
    n = length(x)
    ## counts = x.pool
    counts = fill(0, ngroups + 1)
    for i = 1:n
        counts[x[i] + 1] += 1
    end

    ## mark the start of each contiguous group of like-indexed data
    where = fill(1, ngroups + 1)
    for i = 2:ngroups+1
        where[i] = where[i - 1] + counts[i - 1]
    end
    
    ## this is our indexer
    result = fill(0, n)
    for i = 1:n
        label = x[i] + 1
        result[where[label]] = i
        where[label] += 1
    end
    result, where, counts
end
groupsort_indexer(pv::PooledDataVec) = groupsort_indexer(pv.refs, length(pv.pool))

type GroupedDataFrame
    parent::DataFrame
    cols::Vector         # columns used for sorting
    idx::Vector{Int}     # indexing vector when sorted by the given columns
    starts::Vector{Int}  # starts of groups
    ends::Vector{Int}    # ends of groups 
end

#
# Split
#
function groupby{T}(df::DataFrame, cols::Vector{T})
    ## a subset of Wes McKinney's algorithm here:
    ##     http://wesmckinney.com/blog/?p=489
    
    # use the pool trick to get a set of integer references for each unique item
    dv = PooledDataVec(df[cols[1]])
    # if there are NAs, add 1 to the refs to avoid underflows in x later
    dv_has_nas = (findfirst(dv.refs, 0) > 0 ? 1 : 0)
    x = copy(dv.refs) + dv_has_nas
    # also compute the number of groups, which is the product of the set lengths
    ngroups = length(dv.pool) + dv_has_nas
    # if there's more than 1 column, do roughly the same thing repeatedly
    for j = 2:length(cols)
        dv = PooledDataVec(df[cols[j]])
        dv_has_nas = (findfirst(dv.refs, 0) > 0 ? 1 : 0)
        for i = 1:nrow(df)
            x[i] += (dv.refs[i] + dv_has_nas- 1) * ngroups
        end
        ngroups = ngroups * (length(dv.pool) + dv_has_nas)
        # TODO if ngroups is really big, shrink it
    end
    (idx, starts) = groupsort_indexer(x, ngroups)
    # Remove zero-length groupings
    starts = _uniqueofsorted(starts) 
    ends = [starts[2:end] - 1]
    GroupedDataFrame(df, cols, idx, starts[1:end-1], ends)
end
groupby(d::DataFrame, cols) = groupby(d, [cols])

# add a function curry
groupby{T}(cols::Vector{T}) = x -> groupby(x, cols)
groupby(cols) = x -> groupby(x, cols)


unique(pd::PooledDataVec) = pd.pool
sort(pd::PooledDataVec) = pd[order(pd)]
order(pd::PooledDataVec) = groupsort_indexer(pd)[1]

start(gd::GroupedDataFrame) = 1
next(gd::GroupedDataFrame, state::Int) = 
    (sub(gd.parent, gd.idx[gd.starts[state]:gd.ends[state]]),
     state + 1)
done(gd::GroupedDataFrame, state::Int) = state > length(gd.starts)
length(gd::GroupedDataFrame) = length(gd.starts)
ref(gd::GroupedDataFrame, idx::Int) = sub(gd.parent, gd.idx[gd.starts[idx]:gd.ends[idx]]) 

function show(io, gd::GroupedDataFrame)
    N = length(gd)
    println(io, "$(typeof(gd))  $N groups with keys: $(gd.cols)")
    println(io, "First Group:")
    show(io, gd[1])
    if N > 1
        println(io, "       :")
        println(io, "       :")
        println(io, "Last Group:")
        show(io, gd[N])
    end
end

#
# Apply / map
#

# map() sweeps along groups
function map(f::Function, gd::GroupedDataFrame)
    [f(d) for d in gd]
end
## function map(f::Function, gd::GroupedDataFrame)
##     # preallocate based on the results on the first one
##     x = f(gd[1])
##     res = Array(typeof(x), length(gd))
##     res[1] = x
##     for idx in 2:length(gd)
##         res[idx] = f(gd[idx])
##     end
##     res
## end

# with() sweeps along groups and applies with to each group
function with(gd::GroupedDataFrame, e::Expr)
    [with(d, e) for d in gd]
end

# within() sweeps along groups and applies within to each group
function within!(gd::GroupedDataFrame, e::Expr)   
    x = [within!(d[:,:], e) for d in gd]
    rbind(x...)
end

within!(x::SubDataFrame, e::Expr) = within!(x[:,:], e)

function within(gd::GroupedDataFrame, e::Expr)  
    x = [within(d, e) for d in gd]
    rbind(x...)
end

within(x::SubDataFrame, e::Expr) = within(x[:,:], e)


# based_on() sweeps along groups and applies based_on to each group
function based_on(gd::GroupedDataFrame, ex::Expr)  
    f = based_on_f(gd.parent, ex)
    x = [f(d) for d in gd]
    idx = fill([1:length(x)], convert(Vector{Int}, map(nrow, x)))
    keydf = gd.parent[gd.idx[gd.starts[idx]], gd.cols]
    resdf = rbind(x)
    cbind(keydf, resdf)
end

# default pipelines:
map(f::Function, x::SubDataFrame) = f(x)
(|)(x::GroupedDataFrame, e::Expr) = based_on(x, e)   
## (|)(x::GroupedDataFrame, f::Function) = map(f, x)

# apply a function to each column in a DataFrame
colwise(f::Function, d::AbstractDataFrame) = [f(d[idx]) for idx in 1:ncol(d)]
colwise(f::Function, d::GroupedDataFrame) = map(colwise(f), d)
colwise(f::Function) = x -> colwise(f, x)
colwise(f) = x -> colwise(f, x)
# apply several functions to each column in a DataFrame
colwise(fns::Vector{Function}, d::AbstractDataFrame) = [f(d[idx]) for f in fns, idx in 1:ncol(d)][:]
colwise(fns::Vector{Function}, d::GroupedDataFrame) = map(colwise(fns), d)
colwise(fns::Vector{Function}, d::GroupedDataFrame, cn::Vector{String}) = map(colwise(fns), d)
colwise(fns::Vector{Function}) = x -> colwise(fns, x)

function colwise(d::AbstractDataFrame, s::Vector{Symbol}, cn::Vector)
    header = [s2 * "_" * string(s1) for s1 in s, s2 in cn][:]
    payload = colwise(map(eval, s), d)
    df = DataFrame()
    # TODO fix this to assign the longest column first or preallocate
    # based on the maximum length.
    for i in 1:length(header)
        df[header[i]] = payload[i]
    end
    df
end
## function colwise(d::AbstractDataFrame, s::Vector{Symbol}, cn::Vector)
##     header = [s2 * "_" * string(s1) for s1 in s, s2 in cn][:]
##     payload = colwise(map(eval, s), d)
##     DataFrame(payload, header)
## end
colwise(d::AbstractDataFrame, s::Symbol, x) = colwise(d, [s], x)
colwise(d::AbstractDataFrame, s::Vector{Symbol}, x::String) = colwise(d, s, [x])
colwise(d::AbstractDataFrame, s::Symbol) = colwise(d, [s], colnames(d))
colwise(d::AbstractDataFrame, s::Vector{Symbol}) = colwise(d, s, colnames(d))

# TODO make this faster by applying the header just once.
# BUG zero-rowed groupings cause problems here, because a sum of a zero-length
# DataVec is 0 (not 0.0).
colwise(d::GroupedDataFrame, s::Vector{Symbol}) = rbind(map(x -> colwise(del(x, d.cols),s), d)...)
function colwise(gd::GroupedDataFrame, s::Vector{Symbol})
    payload = rbind(map(x -> colwise(del(x, gd.cols),s), gd)...)
    keydf = rbind(with(gd, :( _DF[1,$(gd.cols)] )))
    cbind(keydf, payload)
end
colwise(d::GroupedDataFrame, s::Symbol, x) = colwise(d, [s], x)
colwise(d::GroupedDataFrame, s::Vector{Symbol}, x::String) = colwise(d, s, [x])
colwise(d::GroupedDataFrame, s::Symbol) = colwise(d, [s])
(|)(d::GroupedDataFrame, s::Vector{Symbol}) = colwise(d, s)
(|)(d::GroupedDataFrame, s::Symbol) = colwise(d, [s])
colnames(d::GroupedDataFrame) = colnames(d.parent)


# by() convenience function
by(d::AbstractDataFrame, cols, f::Function) = map(f, groupby(d, cols))
by(d::AbstractDataFrame, cols, e::Expr) = based_on(groupby(d, cols), e)
by(d::AbstractDataFrame, cols, s::Vector{Symbol}) = colwise(groupby(d, cols), s)
by(d::AbstractDataFrame, cols, s::Symbol) = colwise(groupby(d, cols), s)


##
## Reshaping
##




function stack(df::DataFrame, icols::Vector{Int})
    remainingcols = _setdiff([1:ncol(df)], icols)
    res = rbind([insert(df[[i, remainingcols]], 1, colnames(df)[i], "key") for i in icols]...)
    replace_names!(res, colnames(res)[2], "value")
    res 
end
stack(df::DataFrame, icols) = stack(df, [df.colindex[icols]])

function unstack(df::DataFrame, ikey::Int, ivalue::Int, irefkey::Int)
    keycol = PooledDataVec(df[ikey])
    valuecol = df[ivalue]
    # TODO make a version with a default refkeycol
    refkeycol = PooledDataVec(df[irefkey])
    remainingcols = _setdiff([1:ncol(df)], [ikey, ivalue])
    Nrow = length(refkeycol.pool)
    Ncol = length(keycol.pool)
    # TODO make fillNA(type, length) 
    payload = DataFrame({DataVec([fill(valuecol[1],Nrow)], fill(true, Nrow))  for i in 1:Ncol}, map(string, keycol.pool))
    nowarning = true 
    for k in 1:nrow(df)
        j = int(keycol.refs[k])
        i = int(refkeycol.refs[k])
        if i > 0 && j > 0
            if nowarning && !isna(payload[j][i]) 
                println("Warning: duplicate entries in unstack.")
                nowarning = false
            end
            payload[j][i]  = valuecol[k]
        end
    end
    insert(payload, 1, refkeycol.pool, colnames(df)[irefkey])
end
unstack(df::DataFrame, ikey, ivalue, irefkey) =
    unstack(df, df.colindex[ikey], df.colindex[ivalue], df.colindex[irefkey])

##
## Join / merge
##


function join_idx(left, right, max_groups)
    ## adapted from Wes McKinney's full_outer_join in pandas (file: src/join.pyx).

    # NA group in location 0

    left_sorter, where, left_count = groupsort_indexer(left, max_groups)
    right_sorter, where, right_count = groupsort_indexer(right, max_groups)

    # First pass, determine size of result set, do not use the NA group
    count = 0
    rcount = 0
    lcount = 0
    for i in 2 : max_groups + 1
        lc = left_count[i]
        rc = right_count[i]

        if rc > 0 && lc > 0
            count += lc * rc
        elseif rc > 0
            rcount += rc
        else
            lcount += lc
        end
    end
    
    # group 0 is the NA group
    position = 0
    lposition = 0
    rposition = 0

    # exclude the NA group
    left_pos = left_count[1]
    right_pos = right_count[1]

    left_indexer = Array(Int, count)
    right_indexer = Array(Int, count)
    leftonly_indexer = Array(Int, lcount)
    rightonly_indexer = Array(Int, rcount)
    for i in 1 : max_groups + 1
        lc = left_count[i]
        rc = right_count[i]

        if rc == 0
            for j in 1:lc
                leftonly_indexer[lposition + j] = left_pos + j
            end
            lposition += lc
        elseif lc == 0
            for j in 1:rc
                rightonly_indexer[rposition + j] = right_pos + j
            end
            rposition += rc
        else
            for j in 1:lc
                offset = position + (j-1) * rc
                for k in 1:rc
                    left_indexer[offset + k] = left_pos + j
                    right_indexer[offset + k] = right_pos + k
                end
            end
            position += lc * rc
        end
        left_pos += lc
        right_pos += rc
    end

    ## (left_sorter, left_indexer, leftonly_indexer,
    ##  right_sorter, right_indexer, rightonly_indexer)
    (left_sorter[left_indexer], left_sorter[leftonly_indexer],
     right_sorter[right_indexer], right_sorter[rightonly_indexer])
end

function merge(df1::AbstractDataFrame, df2::AbstractDataFrame, bycol)

    dv1, dv2 = PooledDataVecs(df1[bycol], df2[bycol])
    left_indexer, leftonly_indexer,
    right_indexer, rightonly_indexer =
        join_idx(dv1.refs, dv2.refs, length(dv1.pool))

    # inner join:
    cbind(df1[left_indexer,:], del(df2, bycol)[right_indexer,:])
    # TODO left/right join, outer join - needs better
    #      NA indexing or a way to create NA DataFrames.
    # TODO add support for multiple columns
end
