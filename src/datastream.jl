# TODO: NonSeekableDataStream
# TODO: Remove coltypes(ds::AbstractDataStream)
# TODO: Remove colnames(ds::AbstractDataStream)
# TODO: Implement MatrixStream, which reads CSV's into a Matrix of Float64's

export readstream

abstract AbstractDataStream

# Store DataFrame that will contain minibatches in stream
type SeekableDataStream <: AbstractDataStream
    io::IO
    p::ParsedCSV
    o::ParseOptions
    nrows::Int
    df::DataFrame

    function SeekableDataStream(io::IO,
                                p::ParsedCSV,
                                o::ParseOptions,
                                nrows::Integer,
                                df::DataFrame)
        r = new(io, p, o, int(nrows), df)
        finalizer(r, r -> close(r.io))
        return r
    end
end

# TODO: Use a custom prefix-checking tester for instrings.
function readstream(pathname::String;
                    nrows::Integer = 1,
                    header::Bool = true,
                    separator::Char = ',',
                    allowquotes::Bool = true,
                    quotemark::Vector{Char} = ['"'],
                    decimal::Char = '.',
                    nastrings::Vector = ASCIIString["", "NA"],
                    truestrings::Vector = ASCIIString["T", "t", "TRUE", "true"],
                    falsestrings::Vector = ASCIIString["F", "f", "FALSE", "false"],
                    makefactors::Bool = false,
                    colnames::Vector = UTF8String[],
                    cleannames::Bool = false,
                    coltypes::Vector{DataType} = DataType[],
                    allowcomments::Bool = false,
                    commentmark::Char = '#',
                    ignorepadding::Bool = true,
                    skipstart::Int = 0,
                    skiprows::Vector{Int} = Int[],
                    skipblanks::Bool = true,
                    encoding::Symbol = :utf8,
                    allowescapes::Bool = true)

    io = open(pathname, "r")
    nbytes = 2^20
    p = ParsedCSV(Array(Uint8, nbytes),
                  Array(Int, 1),
                  Array(Int, 1),
                  BitArray(1))
    o = ParseOptions(header,
                     separator,
                     #allowquotes,
                     quotemark,
                     decimal,
                     nastrings,
                     truestrings,
                     falsestrings,
                     makefactors,
                     colnames,
                     cleannames,
                     coltypes,
                     allowcomments,
                     commentmark,
                     ignorepadding,
                     skipstart,
                     skiprows,
                     skipblanks,
                     encoding,
                     allowescapes)
    return SeekableDataStream(io, p, o, nrows, DataFrame())
end

function Base.show(io::IO, ds::SeekableDataStream)
    @printf io "SeekableDataStream"
    @printf io "Minibatch Size: %d\n" ds.nrows
    return
end

# TODO: Return nothing here?
function Base.start(s::SeekableDataStream)
    seek(s.io, 0)
    return nothing
end

# TODO: Return df, nothing here?
function Base.next(s::SeekableDataStream, n::Nothing)
    if position(s.io) == 0
        s.df = readtable!(s.p, s.io, s.nrows, s.o)
        return s.df, nothing
    else
        bytes, fields, rows = readnrows!(s.p, s.io, s.nrows, s.o)
        cols = fld(fields, rows)
        filldf!(s.df, rows, cols, bytes, fields, s.p, s.o)
        return s.df, nothing
    end
end

Base.done(s::SeekableDataStream, n::Nothing) = eof(s.io)

##############################################################################
#
# Streaming data functions
#
##############################################################################

function Base.sum(ds::AbstractDataStream, dim::Integer)
    if dim != 2
        error("Only column sums available for AbstractDataStream")
    end

    df1 = start(ds)

    df1, df1 = next(ds, df1)

    cnames = names(df1)
    p = length(cnames)
    sums = zeros(p)
    counts = zeros(Int, p)

    n = nrow(df1)
    for j in 1:p
        c = df1[j]
        if eltype(c) <: Real
            for i in 1:n
                if !isna(c[i])
                    sums[j] += c[i]
                    counts[j] += 1
                end
            end
        end
    end

    while !done(ds, df1)
        df1, df1 = next(ds, df1)
        n = nrow(df1)
        for j in 1:p
            c = df1[j]
            if eltype(c) <: Real
                for i in 1:n
                    if !isna(c[i])
                        sums[j] += c[i]
                        counts[j] += 1
                    end
                end
            end
        end
    end

    res = DataFrame({Float64 for i in 1:p}, cnames, 1)

    for j in 1:p
        if counts[j] != 0
            res[1, j] = sums[j]
        end
    end

    return res
end

function Base.prod(ds::AbstractDataStream, dim::Integer)
    if dim != 2
        error("Only column sums available for AbstractDataStream")
    end

    df1 = start(ds)

    df1, df1 = next(ds, df1)

    cnames = names(df1)
    p = length(cnames)
    prods = ones(p)
    counts = zeros(Int, p)

    n = nrow(df1)
    for j in 1:p
        c = df1[j]
        if eltype(c) <: Real
            for i in 1:n
                if !isna(c[i])
                    prods[j] *= c[i]
                    counts[j] += 1
                end
            end
        end
    end

    while !done(ds, df1)
        df1, df1 = next(ds, df1)
        n = nrow(df1)
        for j in 1:p
            c = df1[j]
            if eltype(c) <: Real
                for i in 1:n
                    if !isna(c[i])
                        prods[j] *= c[i]
                        counts[j] += 1
                    end
                end
            end
        end
    end

    res = DataFrame({Float64 for i in 1:p}, cnames, 1)

    for j in 1:p
        if counts[j] != 0
            res[1, j] = prods[j]
        end
    end

    return res
end

function Base.mean(ds::AbstractDataStream, dim::Integer)
    if dim != 2
        error("Only column sums available for AbstractDataStream")
    end

    df1 = start(ds)

    df1, df1 = next(ds, df1)

    cnames = names(df1)
    p = length(cnames)
    sums = zeros(p)
    counts = zeros(Int, p)

    n = nrow(df1)
    for j in 1:p
        c = df1[j]
        if eltype(c) <: Real
            for i in 1:n
                if !isna(c[i])
                    sums[j] += c[i]
                    counts[j] += 1
                end
            end
        end
    end

    while !done(ds, df1)
        df1, df1 = next(ds, df1)
        n = nrow(df1)
        for j in 1:p
            c = df1[j]
            if eltype(c) <: Real
                for i in 1:n
                    if !isna(c[i])
                        sums[j] += c[i]
                        counts[j] += 1
                    end
                end
            end
        end
    end

    res = DataFrame({Float64 for i in 1:p}, cnames, 1)

    for j in 1:p
        if counts[j] != 0
            res[1, j] = sums[j] / counts[j]
        end
    end

    return res
end

# function colvars(ds::AbstractDataStream)
#   p = length(colnames(ds))
#   means = zeros(p)
#   deltas = zeros(p)
#   m2s = zeros(p)
#   vars = zeros(p)
#   ns = zeros(Int, p)

#   for minibatch in ds
#     for row_index in 1:nrow(minibatch)
#       for column_index in 1:p
#         if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
#           ns[column_index] += 1
#           deltas[column_index] = minibatch[row_index, column_index] - means[column_index]
#           means[column_index] += deltas[column_index] / ns[column_index]
#           m2s[column_index] = m2s[column_index] + deltas[column_index] * (minibatch[row_index, column_index] - means[column_index])
#           vars[column_index] = m2s[column_index] / (ns[column_index] - 1)
#         end
#       end
#     end
#   end

#   result_types = {Float64 for i in 1:p}
#   results = DataFrame(result_types, colnames(ds), 1)

#   for column_index in 1:p
#     if ns[column_index] != 0
#       results[1, column_index] = vars[column_index]
#     end
#   end

#   return results
# end

# function colstds(ds::AbstractDataStream)
#   vars = colvars(ds)
#   stds = deepcopy(vars)
#   column_types = coltypes(vars)
#   for j in 1:length(column_types)
#     if column_types[j] <: Real
#       stds[1, j] = sqrt(vars[1, j])
#     end
#   end
#   return stds
# end

# function colmins(ds::AbstractDataStream)
#   p = length(colnames(ds))
#   mins = [Inf for i in 1:p]
#   ns = zeros(Int, p)

#   for minibatch in ds
#     for row_index in 1:nrow(minibatch)
#       for column_index in 1:p
#         if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
#           if minibatch[row_index, column_index] < mins[column_index]
#             mins[column_index] = minibatch[row_index, column_index]
#             ns[column_index] += 1
#           end
#         end
#       end
#     end
#   end

#   result_types = {Float64 for i in 1:p}
#   df = DataFrame(result_types, colnames(ds), 1)

#   for column_index in 1:p
#     if ns[column_index] != 0
#       df[1, column_index] = mins[column_index]
#     end
#   end

#   return df
# end

# function colmaxs(ds::AbstractDataStream)
#   p = length(colnames(ds))
#   maxs = [-Inf for i in 1:p]
#   ns = zeros(Int, p)

#   for minibatch in ds
#     for row_index in 1:nrow(minibatch)
#       for column_index in 1:p
#         if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
#           if minibatch[row_index, column_index] > maxs[column_index]
#             maxs[column_index] = minibatch[row_index, column_index]
#             ns[column_index] += 1
#           end
#         end
#       end
#     end
#   end

#   result_types = {Float64 for i in 1:p}
#   df = DataFrame(result_types, colnames(ds), 1)

#   for column_index in 1:p
#     if ns[column_index] != 0
#       df[1, column_index] = maxs[column_index]
#     end
#   end

#   return df
# end

# function colranges(ds::AbstractDataStream)
#   p = length(colnames(ds))
#   mins = [Inf for i in 1:p]
#   maxs = [-Inf for i in 1:p]
#   ns = zeros(Int, p)

#   for minibatch in ds
#     for row_index in 1:nrow(minibatch)
#       for column_index in 1:p
#         if coltypes(minibatch)[column_index] <: Real && !isna(minibatch[row_index, column_index])
#           ns[column_index] += 1
#           if minibatch[row_index, column_index] < mins[column_index]
#             mins[column_index] = minibatch[row_index, column_index]
#           end
#           if minibatch[row_index, column_index] > maxs[column_index]
#             maxs[column_index] = minibatch[row_index, column_index]
#           end
#         end
#       end
#     end
#   end

#   result_types = {Float64 for i in 1:p}
#   df_mins = DataFrame(result_types, colnames(ds), 1)
#   df_maxs = DataFrame(result_types, colnames(ds), 1)

#   for column_index in 1:p
#     if ns[column_index] != 0
#       df_mins[1, column_index] = mins[column_index]
#       df_maxs[1, column_index] = maxs[column_index]
#     end
#   end

#   return (df_mins, df_maxs)
# end

# # Two-pass algorithm for covariance and correlation
# function cov(ds::AbstractDataStream)
#   p = length(colnames(ds))

#   # Make one pass to compute means
#   means = colmeans(ds)

#   # Now compute covariances during second pass
#   ns = zeros(Int, p, p)
#   covariances = datazeros(p, p)
 
#   for minibatch in ds
#     for row_index in 1:nrow(minibatch)
#       for column_index in 1:p
#         for alt_column_index in 1:p
#           if coltypes(minibatch)[column_index] <: Real &&
#                 !isna(minibatch[row_index, column_index]) &&
#                 coltypes(minibatch)[alt_column_index] <: Real &&
#                 !isna(minibatch[row_index, alt_column_index])
#             ns[column_index, alt_column_index] += 1
#             n = ns[column_index, alt_column_index]
#             a = minibatch[row_index, column_index] - means[1, column_index]
#             b = minibatch[row_index, alt_column_index] - means[1, alt_column_index]
#             covariances[column_index, alt_column_index] = ((n - 1) / n) * covariances[column_index, alt_column_index] + (a * b) / n
#           end
#         end
#       end
#     end
#   end

#   # Scale estimates by (n / (n - 1))
#   for i in 1:p
#     for j in 1:p
#       if ns[i, j] <= 2
#         covariances[i, j] = NA
#       else
#         n = ns[i, j]
#         covariances[i, j] *= (n / (n - 1))
#       end
#     end
#   end

#   return covariances
# end

# function cor(ds::AbstractDataStream)
#   covariances = cov(ds)
#   correlations = deepcopy(covariances)
#   p = nrow(correlations)
#   for i in 1:p
#     for j in 1:p
#       correlations[i, j] = covariances[i, j] / sqrt(covariances[i, i] * covariances[j, j])
#     end
#   end
#   return correlations
# end

function Base.select(ds::AbstractDataStream, query::Integer)
    i = 0
    for df in ds
        u = nrow(df)
        if i + u > query
            return df[query - i, :]
        end
        i += u
    end
    error("Did not find requested row")
end

# # TODO: Stop returning empty DataFrame at the end of a stream
# #       (NOTE: Probably not possible because we don't know nrows.)
# # TODO: Implement
# #        * colentropys
# #        * colcardinalities
# #        * colmedians
# #        * colffts
# #        * colnorms
