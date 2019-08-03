import Base: @deprecate

@deprecate DataFrame(t::Type, nrows::Integer, ncols::Integer) DataFrame([Vector{t}(undef, nrows) for i in 1:ncols])

@deprecate DataFrame(column_eltypes::AbstractVector{<:Type},
                     nrows::Integer) DataFrame(column_eltypes, Symbol.('x' .* string.(1:length(column_eltypes))), nrows)

function DataFrame(column_eltypes::AbstractVector{T}, cnames::AbstractVector{Symbol},
                   categorical::AbstractVector{Bool}, nrows::Integer;
                   makeunique::Bool=false)::DataFrame where T<:Type
    Base.depwarn("`DataFrame` constructor with `categorical` positional argument is deprecated. " *
                 "Instead use `DataFrame(columns, names)` constructor.",
                 :DataFrame)
    updated_types = convert(Vector{Type}, column_eltypes)
    if length(categorical) != length(column_eltypes)
        throw(DimensionMismatch("arguments column_eltypes and categorical must have the same length " *
                                "(got $(length(column_eltypes)) and $(length(categorical)))"))
    end
    for i in eachindex(categorical)
        categorical[i] || continue
        elty = CategoricalArrays.catvaluetype(Missings.T(updated_types[i]),
                                              CategoricalArrays.DefaultRefType)
        if updated_types[i] >: Missing
            updated_types[i] = Union{elty, Missing}
        else
            updated_types[i] = elty
        end
    end
    return DataFrame(updated_types, cnames, nrows, makeunique=makeunique)
end

import Base: insert!
@deprecate insert!(df::DataFrame, df2::AbstractDataFrame) (foreach(col -> df[!, col] = df2[!, col], names(df2)); df)

## write.table
export writetable
"""
Write data to a tabular-file format (CSV, TSV, ...)

```julia
writetable(filename, df, [keyword options])
```

### Arguments

* `filename::AbstractString` : the filename to be created
* `df::AbstractDataFrame` : the AbstractDataFrame to be written

### Keyword Arguments

* `separator::Char` -- The separator character that you would like to use. Defaults to the output of `getseparator(filename)`, which uses commas for files that end in `.csv`, tabs for files that end in `.tsv` and a single space for files that end in `.wsv`.
* `quotemark::Char` -- The character used to delimit string fields. Defaults to `'"'`.
* `header::Bool` -- Should the file contain a header that specifies the column names from `df`. Defaults to `true`.
* `nastring::AbstractString` -- What to write in place of missing data. Defaults to `"NA"`.

### Result

* `::DataFrame`

### Examples

```julia
df = DataFrame(A = 1:10)
writetable("output.csv", df)
writetable("output.dat", df, separator = ',', header = false)
writetable("output.dat", df, quotemark = '\'', separator = ',')
writetable("output.dat", df, header = false)
```
"""
function writetable(filename::AbstractString,
                    df::AbstractDataFrame;
                    header::Bool = true,
                    separator::Char = getseparator(filename),
                    quotemark::Char = '"',
                    nastring::AbstractString = "NA",
                    append::Bool = false)
    Base.depwarn("writetable is deprecated, use CSV.write from the CSV package instead",
                 :writetable)

    if endswith(filename, ".bz") || endswith(filename, ".bz2")
        throw(ArgumentError("BZip2 compression not yet implemented"))
    elseif endswith(filename, ".gz")
        throw(ArgumentError("GZip compression no longer implemented"))
    end

    if append && isfile(filename) && filesize(filename) > 0
        file_df = readtable(filename, header = false, nrows = 1)

        # Check if number of columns matches
        if size(file_df, 2) != size(df, 2)
            throw(DimensionMismatch("Number of columns differ between file and DataFrame"))
        end

        # When 'append'-ing to a nonempty file,
        # 'header' triggers a check for matching colnames
        if header
            if any(i -> Symbol(file_df[1, i]) != index(df)[i], 1:size(df, 2))
                throw(KeyError("Column names don't match names in file"))
            end

            header = false
        end
    end

    open(filename, append ? "a" : "w") do io
        printtable(io,
                   df,
                   header = header,
                   separator = separator,
                   quotemark = quotemark,
                   nastring = nastring)
    end

    return
end


## read.table

struct ParsedCSV
    bytes::Vector{UInt8} # Raw bytes from CSV file
    bounds::Vector{Int}  # Right field boundary indices
    lines::Vector{Int}   # Line break indices
    quoted::BitVector    # Was field quoted in text
end

struct ParseOptions{S <: String, T <: String}
    header::Bool
    separator::Char
    quotemarks::Vector{Char}
    decimal::Char
    nastrings::Vector{S}
    truestrings::Vector{T}
    falsestrings::Vector{T}
    makefactors::Bool
    names::Vector{Symbol}
    eltypes::Vector
    allowcomments::Bool
    commentmark::Char
    ignorepadding::Bool
    skipstart::Int
    skiprows::AbstractVector{Int}
    skipblanks::Bool
    encoding::Symbol
    allowescapes::Bool
    normalizenames::Bool
end

# Dispatch on values of ParseOptions to avoid running
#   unused checks for every byte read
struct ParseType{ALLOWCOMMENTS, SKIPBLANKS, ALLOWESCAPES, SPC_SEP} end
ParseType(o::ParseOptions) = ParseType{o.allowcomments, o.skipblanks, o.allowescapes, o.separator == ' '}()

macro read_peek_eof(io, nextchr)
    io = esc(io)
    nextchr = esc(nextchr)
    quote
        nextnext = eof($io) ? 0xff : read($io, UInt8)
        $nextchr, nextnext, nextnext == 0xff
    end
end

macro skip_within_eol(io, chr, nextchr, endf)
    io = esc(io)
    chr = esc(chr)
    nextchr = esc(nextchr)
    endf = esc(endf)
    quote
        if $chr == UInt32('\r') && $nextchr == UInt32('\n')
            $chr, $nextchr, $endf = @read_peek_eof($io, $nextchr)
        end
    end
end

macro skip_to_eol(io, chr, nextchr, endf)
    io = esc(io)
    chr = esc(chr)
    nextchr = esc(nextchr)
    endf = esc(endf)
    quote
        while !$endf && !@atnewline($chr, $nextchr)
            $chr, $nextchr, $endf = @read_peek_eof($io, $nextchr)
        end
        @skip_within_eol($io, $chr, $nextchr, $endf)
    end
end

macro atnewline(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        $chr == UInt32('\n') || $chr == UInt32('\r')
    end
end

macro atblankline(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        ($chr == UInt32('\n') || $chr == UInt32('\r')) &&
        ($nextchr == UInt32('\n') || $nextchr == UInt32('\r'))
    end
end

macro atescape(chr, nextchr, quotemarks)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quotemarks = esc(quotemarks)
    quote
        (UInt32($chr) == UInt32('\\') &&
            (UInt32($nextchr) == UInt32('\\') ||
                UInt32($nextchr) in $quotemarks)) ||
                    (UInt32($chr) == UInt32($nextchr) &&
                        UInt32($chr) in $quotemarks)
    end
end

macro atcescape(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        $chr == UInt32('\\') &&
        ($nextchr == UInt32('n') ||
         $nextchr == UInt32('t') ||
         $nextchr == UInt32('r') ||
         $nextchr == UInt32('a') ||
         $nextchr == UInt32('b') ||
         $nextchr == UInt32('f') ||
         $nextchr == UInt32('v') ||
         $nextchr == UInt32('\\'))
    end
end

macro mergechr(chr, nextchr)
    chr = esc(chr)
    nextchr = esc(nextchr)
    quote
        if $chr == UInt32('\\')
            if $nextchr == UInt32('n')
                '\n'
            elseif $nextchr == UInt32('t')
                '\t'
            elseif $nextchr == UInt32('r')
                '\r'
            elseif $nextchr == UInt32('a')
                '\a'
            elseif $nextchr == UInt32('b')
                '\b'
            elseif $nextchr == UInt32('f')
                '\f'
            elseif $nextchr == UInt32('v')
                '\v'
            elseif $nextchr == UInt32('\\')
                '\\'
            else
                msg = @sprintf("Invalid escape character '%s%s' encountered",
                               $chr,
                               $nextchr)
                error(msg)
            end
        else
            msg = @sprintf("Invalid escape character '%s%s' encountered",
                           $chr,
                           $nextchr)
            error(msg)
        end
    end
end

macro isspace(byte)
    byte = esc(byte)
    quote
        0x09 <= $byte <= 0x0d || $byte == 0x20
    end
end

# This trick is ugly, but is ~33% faster than push!() for large arrays
macro push(count, a, val, l)
    count = esc(count) # Number of items in array
    a = esc(a)         # Array to update
    val = esc(val)     # Value to insert
    l = esc(l)         # Length of array
    quote
        $count += 1
        if $l < $count
            $l *= 2
            resize!($a, $l)
        end
        $a[$count] = $val
    end
end

function getseparator(filename::AbstractString)
    m = match(r"\.(\w+)(\.(gz|bz|bz2))?$", filename)
    ext = isa(m, RegexMatch) ? m.captures[1] : ""
    if ext == "csv"
        return ','
    elseif ext == "tsv"
        return '\t'
    elseif ext == "wsv"
        return ' '
    else
        return ','
    end
end

tf = (true, false)
for allowcomments in tf, skipblanks in tf, allowescapes in tf, wsv in tf
    dtype = ParseType{allowcomments, skipblanks, allowescapes, wsv}
    @eval begin
        # Read CSV file's rows into buffer while storing field boundary information
        # TODO: Experiment with mmaping input
        function readnrows!(p::ParsedCSV,
                            io::IO,
                            nrows::Integer,
                            o::ParseOptions,
                            dispatcher::$(dtype),
                            firstchr::UInt8=0xff)
            # TODO: Use better variable names
            # Information about parse results
            n_bytes = 0
            n_bounds = 0
            n_lines = 0
            n_fields = 1
            l_bytes = length(p.bytes)
            l_lines = length(p.lines)
            l_bounds = length(p.bounds)
            l_quoted = length(p.quoted)

            # Current state of the parser
            in_quotes = false
            in_escape = false
            $(if allowcomments quote at_start = true end end)
            $(if wsv quote skip_white = true end end)
            chr = 0xff
            nextchr = (firstchr == 0xff && !eof(io)) ? read(io, UInt8) : firstchr
            endf = nextchr == 0xff

            # 'in' does not work if passed UInt8 and Vector{Char}
            quotemarks = convert(Vector{UInt8}, o.quotemarks)

            # Insert a dummy field bound at position 0
            @push(n_bounds, p.bounds, 0, l_bounds)
            @push(n_bytes, p.bytes, '\n', l_bytes)
            @push(n_lines, p.lines, 0, l_lines)

            # Loop over bytes from the input until we've read requested rows
            while !endf && ((nrows == -1) || (n_lines < nrows + 1))

                chr, nextchr, endf = @read_peek_eof(io, nextchr)

                # === Debugging ===
                # if in_quotes
                #     print_with_color(:red, string(char(chr)))
                # else
                #     print_with_color(:green, string(char(chr)))
                # end

                $(if allowcomments
                    quote
                        # Ignore text inside comments completely
                        if !in_quotes && chr == UInt32(o.commentmark)
                            @skip_to_eol(io, chr, nextchr, endf)

                            # Skip the linebreak if the comment began at the start of a line
                            if at_start
                                continue
                            end
                        end
                    end
                end)

                $(if skipblanks
                    quote
                        # Skip blank lines
                        if !in_quotes
                            while !endf && @atblankline(chr, nextchr)
                                chr, nextchr, endf = @read_peek_eof(io, nextchr)
                                @skip_within_eol(io, chr, nextchr, endf)
                            end
                        end
                    end
                end)

                $(if allowescapes
                    quote
                        # Merge chr and nextchr here if they're a c-style escape
                        if @atcescape(chr, nextchr) && !in_escape
                            chr = @mergechr(chr, nextchr)
                            nextchr = eof(io) ? 0xff : read(io, UInt8)
                            endf = nextchr == 0xff
                            in_escape = true
                        end
                    end
                end)

                # No longer at the start of a line that might be a pure comment
                $(if allowcomments quote at_start = false end end)

                # Processing is very different inside and outside of quotes
                if !in_quotes
                    # Entering a quoted region
                    if chr in quotemarks
                        in_quotes = true
                        p.quoted[n_fields] = true
                        $(if wsv quote skip_white = false end end)
                    # Finished reading a field
                    elseif $(if wsv
                                quote chr == UInt32(' ') || chr == UInt32('\t') end
                            else
                                quote chr == UInt32(o.separator) end
                            end)
                        $(if wsv
                            quote
                                if !(nextchr in UInt32[' ', '\t', '\n', '\r']) && !skip_white
                                    @push(n_bounds, p.bounds, n_bytes, l_bounds)
                                    @push(n_bytes, p.bytes, '\n', l_bytes)
                                    @push(n_fields, p.quoted, false, l_quoted)
                                    skip_white = false
                                end
                            end
                        else
                            quote
                                @push(n_bounds, p.bounds, n_bytes, l_bounds)
                                @push(n_bytes, p.bytes, '\n', l_bytes)
                                @push(n_fields, p.quoted, false, l_quoted)
                            end
                        end)
                    # Finished reading a row
                    elseif @atnewline(chr, nextchr)
                        @skip_within_eol(io, chr, nextchr, endf)
                        $(if allowcomments quote at_start = true end end)
                        @push(n_bounds, p.bounds, n_bytes, l_bounds)
                        @push(n_bytes, p.bytes, '\n', l_bytes)
                        @push(n_lines, p.lines, n_bytes, l_lines)
                        @push(n_fields, p.quoted, false, l_quoted)
                        $(if wsv quote skip_white = true end end)
                    # Store character in buffer
                    else
                        @push(n_bytes, p.bytes, chr, l_bytes)
                        $(if wsv quote skip_white = false end end)
                    end
                else
                    # Escape a quotemark inside quoted field
                    if @atescape(chr, nextchr, quotemarks) && !in_escape
                        in_escape = true
                    else
                        # Exit quoted field
                        if UInt32(chr) in quotemarks && !in_escape
                            in_quotes = false
                        # Store character in buffer
                        else
                            @push(n_bytes, p.bytes, chr, l_bytes)
                        end
                        # Escape mode only lasts for one byte
                        in_escape = false
                    end
                end
            end

            # Append a final EOL if it's missing in the raw input
            if endf && !@atnewline(chr, nextchr)
                @push(n_bounds, p.bounds, n_bytes, l_bounds)
                @push(n_bytes, p.bytes, '\n', l_bytes)
                @push(n_lines, p.lines, n_bytes, l_lines)
            end

            # Don't count the dummy boundaries in fields or rows
            return n_bytes, n_bounds - 1, n_lines - 1, nextchr
        end
    end
end

function bytematch(bytes::Vector{UInt8},
                   left::Integer,
                   right::Integer,
                   exemplars::Vector{T}) where T <: String
    l = right - left + 1
    for index in 1:length(exemplars)
        exemplar = exemplars[index]
        if length(exemplar) == l
            matched = true
            for i in 0:(l - 1)
                matched &= bytes[left + i] == UInt32(exemplar[1 + i])
            end
            if matched
                return true
            end
        end
    end
    return false
end

function bytestotype(::Type{N},
                     bytes::Vector{UInt8},
                     left::Integer,
                     right::Integer,
                     nastrings::Vector{T},
                     wasquoted::Bool = false,
                     truestrings::Vector{P} = P[],
                     falsestrings::Vector{P} = P[]) where {N <: Integer,
                                                           T <: String,
                                                           P <: String}
    if left > right
        return 0, true, true
    end

    if bytematch(bytes, left, right, nastrings)
        return 0, true, true
    end

    value = 0
    power = 1
    index = right
    byte = bytes[index]

    while index > left
        if UInt32('0') <= byte <= UInt32('9')
            value += (byte - UInt8('0')) * power
            power *= 10
        else
            return value, false, false
        end
        index -= 1
        byte = bytes[index]
    end

    if byte == UInt32('-')
        return -value, left < right, false
    elseif byte == UInt32('+')
        return value, left < right, false
    elseif UInt32('0') <= byte <= UInt32('9')
        value += (byte - UInt8('0')) * power
        return value, true, false
    else
        return value, false, false
    end
end

let out = Vector{Float64}(undef, 1)
    global bytestotype
    function bytestotype(::Type{N},
                         bytes::Vector{UInt8},
                         left::Integer,
                         right::Integer,
                         nastrings::Vector{T},
                         wasquoted::Bool = false,
                         truestrings::Vector{P} = P[],
                         falsestrings::Vector{P} = P[]) where {N <: AbstractFloat,
                                                               T <: String,
                                                               P <: String}
        if left > right
            return 0.0, true, true
        end

        if bytematch(bytes, left, right, nastrings)
            return 0.0, true, true
        end

        wasparsed = ccall(:jl_substrtod,
                          Int32,
                          (Ptr{UInt8}, Csize_t, Int, Ptr{Float64}),
                          bytes,
                          convert(Csize_t, left - 1),
                          right - left + 1,
                          out) == 0

        return out[1], wasparsed, false
    end
end

function bytestotype(::Type{N},
                     bytes::Vector{UInt8},
                     left::Integer,
                     right::Integer,
                     nastrings::Vector{T},
                     wasquoted::Bool = false,
                     truestrings::Vector{P} = P[],
                     falsestrings::Vector{P} = P[]) where {N <: Bool,
                                                           T <: String,
                                                           P <: String}
    if left > right
        return false, true, true
    end

    if bytematch(bytes, left, right, nastrings)
        return false, true, true
    end

    if bytematch(bytes, left, right, truestrings)
        return true, true, false
    elseif bytematch(bytes, left, right, falsestrings)
        return false, true, false
    else
        return false, false, false
    end
end

function bytestotype(::Type{N},
                     bytes::Vector{UInt8},
                     left::Integer,
                     right::Integer,
                     nastrings::Vector{T},
                     wasquoted::Bool = false,
                     truestrings::Vector{P} = P[],
                     falsestrings::Vector{P} = P[]) where {N <: AbstractString,
                                                           T <: String,
                                                           P <: String}
    if left > right
        if wasquoted
            return "", true, false
        else
            return "", true, true
        end
    end

    if bytematch(bytes, left, right, nastrings)
        return "", true, true
    end

    return String(bytes[left:right]), true, false
end

function builddf(rows::Integer,
                 cols::Integer,
                 bytes::Integer,
                 fields::Integer,
                 p::ParsedCSV,
                 o::ParseOptions)
    columns = Vector{Any}(undef, cols)

    for j in 1:cols
        if isempty(o.eltypes)
            values = Vector{Int}(undef, rows)
        else
            values = Vector{o.eltypes[j]}(undef, rows)
        end

        msng = falses(rows)
        is_int = true
        is_float = true
        is_bool = true

        i = 0
        while i < rows
            i += 1

            # Determine left and right boundaries of field
            left = p.bounds[(i - 1) * cols + j] + 2
            right = p.bounds[(i - 1) * cols + j + 1]
            wasquoted = p.quoted[(i - 1) * cols + j]

            # Ignore left-and-right whitespace padding
            # TODO: Debate moving this into readnrows()
            # TODO: Modify readnrows() so that '\r' and '\n'
            #       don't occur near edges
            if o.ignorepadding && !wasquoted
                while left < right && @isspace(p.bytes[left])
                    left += 1
                end
                while left <= right && @isspace(p.bytes[right])
                    right -= 1
                end
            end

            # If eltypes has been defined, use it
            if !isempty(o.eltypes)
                values[i], wasparsed, msng[i] =
                    bytestotype(o.eltypes[j],
                                p.bytes,
                                left,
                                right,
                                o.nastrings,
                                wasquoted,
                                o.truestrings,
                                o.falsestrings)

                # Don't go to guess type zone
                if wasparsed
                    continue
                else
                    error(@sprintf("Failed to parse '%s' using type '%s'",
                                   String(p.bytes[left:right]),
                                   o.eltypes[j]))
                end
            end

            # (1) Try to parse values as Int's
            if is_int
                values[i], wasparsed, msng[i] =
                  bytestotype(Int64,
                              p.bytes,
                              left,
                              right,
                              o.nastrings,
                              wasquoted,
                              o.truestrings,
                              o.falsestrings)
                if wasparsed
                    continue
                else
                    is_int = false
                    values = convert(Array{Float64}, values)
                end
            end

            # (2) Try to parse as Float64's
            if is_float
                values[i], wasparsed, msng[i] =
                  bytestotype(Float64,
                              p.bytes,
                              left,
                              right,
                              o.nastrings,
                              wasquoted,
                              o.truestrings,
                              o.falsestrings)
                if wasparsed
                    continue
                else
                    is_float = false
                    values = Vector{Bool}(undef, rows)
                    i = 0
                    continue
                end
            end

            # (3) Try to parse as Bool's
            if is_bool
                values[i], wasparsed, msng[i] =
                  bytestotype(Bool,
                              p.bytes,
                              left,
                              right,
                              o.nastrings,
                              wasquoted,
                              o.truestrings,
                              o.falsestrings)
                if wasparsed
                    continue
                else
                    is_bool = false
                    values = Vector{String}(undef, rows)
                    i = 0
                    continue
                end
            end

            # (4) Fallback to String
            values[i], wasparsed, msng[i] =
              bytestotype(String,
                          p.bytes,
                          left,
                          right,
                          o.nastrings,
                          wasquoted,
                          o.truestrings,
                          o.falsestrings)
        end

        vals = similar(values, Union{eltype(values), Missing})
        @inbounds for i in eachindex(vals)
            vals[i] = msng[i] ? missing : values[i]
        end
        if o.makefactors && !(is_int || is_float || is_bool)
            columns[j] = CategoricalArray{Union{eltype(values), Missing}}(vals)
        else
            columns[j] = vals
        end
    end

    if isempty(o.names)
        return DataFrame(columns, gennames(cols))
    else
        return DataFrame(columns, o.names)
    end
end

const RESERVED_WORDS = Set(["local", "global", "export", "let",
    "for", "struct", "while", "const", "continue", "import",
    "function", "if", "else", "try", "begin", "break", "catch",
    "return", "using", "baremodule", "macro", "finally",
    "module", "elseif", "end", "quote", "do"])

function identifier(s::AbstractString)
    s = Unicode.normalize(s)
    if !Base.isidentifier(s)
        s = makeidentifier(s)
    end
    Symbol(in(s, RESERVED_WORDS) ? "_"*s : s)
end

function makeidentifier(s::AbstractString)
    (iresult = iterate(s)) === nothing && return "x"

    res = IOBuffer(zeros(UInt8, sizeof(s)+1), write=true)

    (c, i) = iresult
    under = if Base.is_id_start_char(c)
        write(res, c)
        c == '_'
    elseif Base.is_id_char(c)
        write(res, 'x', c)
        false
    else
        write(res, '_')
        true
    end

    while (iresult = iterate(s, i)) !== nothing
        (c, i) = iresult
        if c != '_' && Base.is_id_char(c)
            write(res, c)
            under = false
        elseif !under
            write(res, '_')
            under = true
        end
    end

    return String(take!(res))
end

function parsenames!(names::Vector{Symbol},
                     ignorepadding::Bool,
                     bytes::Vector{UInt8},
                     bounds::Vector{Int},
                     quoted::BitVector,
                     fields::Int,
                     normalizenames::Bool)
    if fields == 0
        error("Header line was empty")
    end

    resize!(names, fields)

    for j in 1:fields
        left = bounds[j] + 2
        right = bounds[j + 1]

        if ignorepadding && !quoted[j]
            while left < right && @isspace(bytes[left])
                left += 1
            end
            while left <= right && @isspace(bytes[right])
                right -= 1
            end
        end

        name = String(bytes[left:right])
        if normalizenames
            name = identifier(name)
        end

        names[j] = name
    end

    return
end

function findcorruption(rows::Integer,
                        cols::Integer,
                        fields::Integer,
                        p::ParsedCSV)
    n = length(p.bounds)
    lengths = Vector{Int}(undef, rows)
    t = 1
    for i in 1:rows
        bound = p.lines[i + 1]
        f = 0
        while t <= n && p.bounds[t] < bound
            f += 1
            t += 1
        end
        lengths[i] = f
    end
    m = median(lengths)
    corruptrows = findall(lengths .!= m)
    l = corruptrows[1]
    error(@sprintf("Saw %d rows, %d columns and %d fields\n * Line %d has %d columns\n",
                   rows,
                   cols,
                   fields,
                   l,
                   lengths[l] + 1))
end

function readtable!(p::ParsedCSV,
                    io::IO,
                    nrows::Integer,
                    o::ParseOptions)

    chr, nextchr = 0xff, 0xff

    skipped_lines = 0

    # Skip lines at the start
    if o.skipstart != 0
        while skipped_lines < o.skipstart
            chr, nextchr, endf = @read_peek_eof(io, nextchr)
            @skip_to_eol(io, chr, nextchr, endf)
            skipped_lines += 1
        end
    else
        chr, nextchr, endf = @read_peek_eof(io, nextchr)
    end

    if o.allowcomments || o.skipblanks
        while true
            if o.allowcomments && nextchr == UInt32(o.commentmark)
                chr, nextchr, endf = @read_peek_eof(io, nextchr)
                @skip_to_eol(io, chr, nextchr, endf)
            elseif o.skipblanks && @atnewline(nextchr, nextchr)
                chr, nextchr, endf = @read_peek_eof(io, nextchr)
                @skip_within_eol(io, chr, nextchr, endf)
            else
                break
            end
            skipped_lines += 1
        end
    end

    # Use ParseOptions to pick the right method of readnrows!
    d = ParseType(o)

    # Extract the header
    if o.header
        bytes, fields, rows, nextchr = readnrows!(p, io, Int64(1), o, d, nextchr)

        # Insert column names from header if none present
        if isempty(o.names)
            parsenames!(o.names, o.ignorepadding, p.bytes, p.bounds, p.quoted, fields, o.normalizenames)
        end
    end

    # Parse main data set
    bytes, fields, rows, nextchr = readnrows!(p, io, Int64(nrows), o, d, nextchr)

    # Sanity checks
    bytes != 0 || error("Failed to read any bytes.")
    rows != 0 || error("Failed to read any rows.")
    fields != 0 || error("Failed to read any fields.")

    # Determine the number of columns
    cols = fld(fields, rows)

    # if the file is empty but has a header then fields, cols and rows will not be computed correctly
    if length(o.names) != cols && cols == 1 && rows == 1 && fields == 1 && bytes == 2
        fields = 0
        rows = 0
        cols = length(o.names)
    end

    # Confirm that the number of columns is consistent across rows
    if fields != rows * cols
        findcorruption(rows, cols, fields, p)
    end

    # Parse contents of a buffer into a DataFrame
    df = builddf(rows, cols, bytes, fields, p, o)

    # Return the final DataFrame
    return df
end

function readtable(io::IO,
                   nbytes::Integer = 1;
                   header::Bool = true,
                   separator::Char = ',',
                   quotemark::Vector{Char} = ['"'],
                   decimal::Char = '.',
                   nastrings::Vector = ["", "NA"],
                   truestrings::Vector = ["T", "t", "TRUE", "true"],
                   falsestrings::Vector = ["F", "f", "FALSE", "false"],
                   makefactors::Bool = false,
                   nrows::Integer = -1,
                   names::Vector = Symbol[],
                   eltypes::Vector = [],
                   allowcomments::Bool = false,
                   commentmark::Char = '#',
                   ignorepadding::Bool = true,
                   skipstart::Integer = 0,
                   skiprows::AbstractVector{Int} = Int[],
                   skipblanks::Bool = true,
                   encoding::Symbol = :utf8,
                   allowescapes::Bool = false,
                   normalizenames::Bool = true)
    if encoding != :utf8
        throw(ArgumentError("Argument 'encoding' only supports ':utf8' currently."))
    elseif !isempty(skiprows)
        throw(ArgumentError("Argument 'skiprows' is not yet supported."))
    elseif decimal != '.'
        throw(ArgumentError("Argument 'decimal' is not yet supported."))
    end

    if !isempty(eltypes)
        for j in 1:length(eltypes)
            if !(eltypes[j] in [String, Bool, Float64, Int64])
                throw(ArgumentError("Invalid eltype $(eltypes[j]) encountered.\n" *
                                    "Valid eltypes: $(String), Bool, Float64 or Int64"))
            end
        end
    end

    # Allocate buffers for storing metadata
    p = ParsedCSV(Vector{UInt8}(undef, nbytes),
                  Vector{Int}(undef, 1),
                  Vector{Int}(undef, 1),
                  BitArray(undef, 1))

    # Set parsing options
    o = ParseOptions(header, separator, quotemark, decimal,
                     nastrings, truestrings, falsestrings,
                     makefactors, names, eltypes,
                     allowcomments, commentmark, ignorepadding,
                     skipstart, skiprows, skipblanks, encoding,
                     allowescapes, normalizenames)

    # Use the IO stream method for readtable()
    df = readtable!(p, io, nrows, o)

    # Close the IO stream
    close(io)

    # Return the resulting DataFrame
    return df
end

export readtable

"""
Read data from a tabular-file format (CSV, TSV, ...)

```julia
readtable(filename, [keyword options])
```

### Arguments

* `filename::AbstractString` : the filename to be read

### Keyword Arguments

*   `header::Bool` -- Use the information from the file's header line to determine column names. Defaults to `true`.
*   `separator::Char` -- Assume that fields are split by the `separator` character. If not specified, it will be guessed from the filename: `.csv` defaults to `','`, `.tsv` defaults to `'\t'`, `.wsv` defaults to `' '`.
*   `quotemark::Vector{Char}` -- Assume that fields contained inside of two `quotemark` characters are quoted, which disables processing of separators and linebreaks. Set to `Char[]` to disable this feature and slightly improve performance. Defaults to `['"']`.
*   `decimal::Char` -- Assume that the decimal place in numbers is written using the `decimal` character. Defaults to `'.'`.
*   `nastrings::Vector{String}` -- Translate any of the strings into this vector into a `missing`. Defaults to `["", "NA"]`.
*   `truestrings::Vector{String}` -- Translate any of the strings into this vector into a Boolean `true`. Defaults to `["T", "t", "TRUE", "true"]`.
*   `falsestrings::Vector{String}` -- Translate any of the strings into this vector into a Boolean `false`. Defaults to `["F", "f", "FALSE", "false"]`.
*   `makefactors::Bool` -- Convert string columns into `PooledDataVector`'s for use as factors. Defaults to `false`.
*   `nrows::Int` -- Read only `nrows` from the file. Defaults to `-1`, which indicates that the entire file should be read.
*   `names::Vector{Symbol}` -- Use the values in this array as the names for all columns instead of or in lieu of the names in the file's header. Defaults to `[]`, which indicates that the header should be used if present or that numeric names should be invented if there is no header.
*   `eltypes::Vector` -- Specify the types of all columns. Defaults to `[]`.
*   `allowcomments::Bool` -- Ignore all text inside comments. Defaults to `false`.
*   `commentmark::Char` -- Specify the character that starts comments. Defaults to `'#'`.
*   `ignorepadding::Bool` -- Ignore all whitespace on left and right sides of a field. Defaults to `true`.
*   `skipstart::Int` -- Specify the number of initial rows to skip. Defaults to `0`.
*   `skiprows::Vector{Int}` -- Specify the indices of lines in the input to ignore. Defaults to `[]`.
*   `skipblanks::Bool` -- Skip any blank lines in input. Defaults to `true`.
*   `encoding::Symbol` -- Specify the file's encoding as either `:utf8` or `:latin1`. Defaults to `:utf8`.
*   `normalizenames::Bool` -- Ensure that column names are valid Julia identifiers. For instance this renames a column named `"a b"` to `"a_b"` which can then be accessed with `:a_b` instead of `Symbol("a b")`. Defaults to `true`.

### Result

* `::DataFrame`

### Examples

```julia
df = readtable("data.csv")
df = readtable("data.tsv")
df = readtable("data.wsv")
df = readtable("data.txt", separator = '\t')
df = readtable("data.txt", header = false)
```
"""
function readtable(pathname::AbstractString;
                   header::Bool = true,
                   separator::Char = getseparator(pathname),
                   quotemark::Vector{Char} = ['"'],
                   decimal::Char = '.',
                   nastrings::Vector = String["", "NA"],
                   truestrings::Vector = String["T", "t", "TRUE", "true"],
                   falsestrings::Vector = String["F", "f", "FALSE", "false"],
                   makefactors::Bool = false,
                   nrows::Integer = -1,
                   names::Vector = Symbol[],
                   eltypes::Vector = [],
                   allowcomments::Bool = false,
                   commentmark::Char = '#',
                   ignorepadding::Bool = true,
                   skipstart::Integer = 0,
                   skiprows::AbstractVector{Int} = Int[],
                   skipblanks::Bool = true,
                   encoding::Symbol = :utf8,
                   allowescapes::Bool = false,
                   normalizenames::Bool = true)
    Base.depwarn("readtable is deprecated, use CSV.read from the CSV package instead",
                 :readtable)

    _r(io) = readtable(io,
                       nbytes,
                       header = header,
                       separator = separator,
                       quotemark = quotemark,
                       decimal = decimal,
                       nastrings = nastrings,
                       truestrings = truestrings,
                       falsestrings = falsestrings,
                       makefactors = makefactors,
                       nrows = nrows,
                       names = names,
                       eltypes = eltypes,
                       allowcomments = allowcomments,
                       commentmark = commentmark,
                       ignorepadding = ignorepadding,
                       skipstart = skipstart,
                       skiprows = skiprows,
                       skipblanks = skipblanks,
                       encoding = encoding,
                       allowescapes = allowescapes,
                       normalizenames = normalizenames)

    # Open an IO stream based on pathname
    # (1) Path is an HTTP or FTP URL
    if startswith(pathname, "http://") || startswith(pathname, "ftp://")
        error("URL retrieval not yet implemented")
    # (2) Path is GZip file
    elseif endswith(pathname, ".gz")
        error("GZip decompression no longer implemented")
    # (3) Path is BZip2 file
    elseif endswith(pathname, ".bz") || endswith(pathname, ".bz2")
        error("BZip2 decompression not yet implemented")
    # (4) Path is an uncompressed file
    else
        nbytes = filesize(pathname)
        io = open(_r, pathname, "r")
    end
end

"""
    inlinetable(s[, flags]; args...)

A helper function to process strings as tabular data for non-standard string
literals. Parses the string `s` containing delimiter-separated tabular data
(by default, comma-separated values) using `readtable`. The optional `flags`
argument contains a list of flag characters, which, if present, are equivalent
to supplying named arguments to `readtable` as follows:

- `f`: `makefactors=true`, convert string columns to `PooledData` columns
- `c`: `allowcomments=true`, ignore lines beginning with `#`
- `H`: `header=false`, do not interpret the first line as column names
"""
inlinetable(s::AbstractString; args...) = readtable(IOBuffer(s); args...)
function inlinetable(s::AbstractString, flags::AbstractString; args...)
    flagbindings = Dict(
        'f' => (:makefactors, true),
        'c' => (:allowcomments, true),
        'H' => (:header, false) )
    for f in flags
        if haskey(flagbindings, f)
            push!(args, flagbindings[f])
        else
            throw(ArgumentError("Unknown inlinetable flag: $f"))
        end
    end
    readtable(IOBuffer(s); args...)
end

export @csv_str, @csv2_str, @tsv_str, @wsv_str

"""
    @csv_str(s[, flags])
    csv"[data]"fcH

Construct a `DataFrame` from a non-standard string literal containing comma-
separated values (CSV) using `readtable`, just as if it were being loaded from
an external file. The suffix flags `f`, `c`, and `H` are optional. If present,
they are equivalent to supplying named arguments to `readtable` as follows:

* `f`: `makefactors=true`, convert string columns to `CategoricalArray` columns
* `c`: `allowcomments=true`, ignore lines beginning with `#`
* `H`: `header=false`, do not interpret the first line as column names

# Example
```jldoctest
julia> df = csv\"""
           name,  age, squidPerWeek
           Alice,  36,         3.14
           Bob,    24,         0
           Carol,  58,         2.71
           Eve,    49,         7.77
           \"""
4×3 DataFrame
│ Row │ name    │ age │ squidPerWeek │
├─────┼─────────┼─────┼──────────────┤
│ 1   │ "Alice" │ 36  │ 3.14         │
│ 2   │ "Bob"   │ 24  │ 0.0          │
│ 3   │ "Carol" │ 58  │ 2.71         │
│ 4   │ "Eve"   │ 49  │ 7.77         │
```
"""
macro csv_str(s, flags...)
    Base.depwarn("@csv_str and the csv\"\"\" syntax are deprecated. " *
                 "Use CSV.read(IOBuffer(...)) from the CSV package instead.",
                 :csv_str)
    inlinetable(s, flags...; separator=',')
end

"""
    @csv2_str(s[, flags])
    csv2"[data]"fcH

Construct a `DataFrame` from a non-standard string literal containing
semicolon-separated values using `readtable`, with comma acting as the decimal
character, just as if it were being loaded from an external file. The suffix
flags `f`, `c`, and `H` are optional. If present, they are equivalent to
supplying named arguments to `readtable` as follows:

* `f`: `makefactors=true`, convert string columns to `CategoricalArray` columns
* `c`: `allowcomments=true`, ignore lines beginning with `#`
* `H`: `header=false`, do not interpret the first line as column names

# Example
```jldoctest
julia> df = csv2\"""
           name;  age; squidPerWeek
           Alice;  36;         3,14
           Bob;    24;         0
           Carol;  58;         2,71
           Eve;    49;         7,77
           \"""
4×3 DataFrame
│ Row │ name    │ age │ squidPerWeek │
├─────┼─────────┼─────┼──────────────┤
│ 1   │ "Alice" │ 36  │ 3.14         │
│ 2   │ "Bob"   │ 24  │ 0.0          │
│ 3   │ "Carol" │ 58  │ 2.71         │
│ 4   │ "Eve"   │ 49  │ 7.77         │
```
"""
macro csv2_str(s, flags...)
    Base.depwarn("@csv2_str and the csv2\"\"\" syntax are deprecated. " *
                 "Use CSV.read(IOBuffer(...)) from the CSV package instead.",
                 :csv2_str)
    inlinetable(s, flags...; separator=';', decimal=',')
end

"""
    @wsv_str(s[, flags])
    wsv"[data]"fcH

Construct a `DataFrame` from a non-standard string literal containing
whitespace-separated values (WSV) using `readtable`, just as if it were being
loaded from an external file. The suffix flags `f`, `c`, and `H` are optional.
If present, they are equivalent to supplying named arguments to `readtable` as
follows:

* `f`: `makefactors=true`, convert string columns to `CategoricalArray` columns
* `c`: `allowcomments=true`, ignore lines beginning with `#`
* `H`: `header=false`, do not interpret the first line as column names

# Example
```jldoctest
julia> df = wsv\"""
           name  age squidPerWeek
           Alice  36         3.14
           Bob    24         0
           Carol  58         2.71
           Eve    49         7.77
           \"""
4×3 DataFrame
│ Row │ name    │ age │ squidPerWeek │
├─────┼─────────┼─────┼──────────────┤
│ 1   │ "Alice" │ 36  │ 3.14         │
│ 2   │ "Bob"   │ 24  │ 0.0          │
│ 3   │ "Carol" │ 58  │ 2.71         │
│ 4   │ "Eve"   │ 49  │ 7.77         │
```
"""
macro wsv_str(s, flags...)
    Base.depwarn("@wsv_str and the wsv\"\"\" syntax are deprecated. " *
                 "Use CSV.read(IOBuffer(...)) from the CSV package instead.",
                 :wsv_str)
    inlinetable(s, flags...; separator=' ')
end

"""
    @tsv_str(s[, flags])
    tsv"[data]"fcH

Construct a `DataFrame` from a non-standard string literal containing tab-
separated values (TSV) using `readtable`, just as if it were being loaded from
an external file. The suffix flags `f`, `c`, and `H` are optional. If present,
they are equivalent to supplying named arguments to `readtable` as follows:

* `f`: `makefactors=true`, convert string columns to `CategoricalArray` columns
* `c`: `allowcomments=true`, ignore lines beginning with `#`
* `H`: `header=false`, do not interpret the first line as column names

# Example
```jldoctest
julia> df = tsv\"""
           name\tage\tsquidPerWeek
           Alice\t36\t3.14
           Bob\t24\t0
           Carol\t58\t2.71
           Eve\t49\t7.77
           \"""
4×3 DataFrame
│ Row │ name    │ age │ squidPerWeek │
├─────┼─────────┼─────┼──────────────┤
│ 1   │ "Alice" │ 36  │ 3.14         │
│ 2   │ "Bob"   │ 24  │ 0.0          │
│ 3   │ "Carol" │ 58  │ 2.71         │
│ 4   │ "Eve"   │ 49  │ 7.77         │
```
"""
macro tsv_str(s, flags...)
    Base.depwarn("@tsv_str and the tsv\"\"\" syntax are deprecated." *
                 "Use CSV.read(IOBuffer(...)) from the CSV package instead.",
                 :tsv_str)
    inlinetable(s, flags...; separator='\t')
end

import Base: show
@deprecate show(io::IO, df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol, summary::Bool) show(io, df, allcols=allcols, rowlabel=rowlabel, summary=summary)
@deprecate show(io::IO, df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol) show(io, df, allcols=allcols, rowlabel=rowlabel)
@deprecate show(io::IO, df::AbstractDataFrame, allcols::Bool) show(io, df, allcols=allcols)
@deprecate show(df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol, summary::Bool) show(df, allcols=allcols, rowlabel=rowlabel, summary=summary)
@deprecate show(df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol) show(df, allcols=allcols, rowlabel=rowlabel)
@deprecate show(df::AbstractDataFrame, allcols::Bool) show(df, allcols=allcols)

@deprecate showall(io::IO, df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol, summary::Bool) show(io, df, allrows=true, allcols=allcols, rowlabel=rowlabel, summary=summary)
@deprecate showall(io::IO, df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol) show(io, df, allrows=true, allcols=allcols, rowlabel=rowlabel)
@deprecate showall(io::IO, df::AbstractDataFrame, allcols::Bool = true) show(io, df, allrows=true, allcols=allcols)
@deprecate showall(df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol, summary::Bool) show(df, allrows=true, allcols=allcols, rowlabel=rowlabel, summary=summary)
@deprecate showall(df::AbstractDataFrame, allcols::Bool, rowlabel::Symbol) show(df, allrows=true, allcols=allcols, rowlabel=rowlabel)
@deprecate showall(df::AbstractDataFrame, allcols::Bool = true) show(df, allrows=true, allcols=allcols)

@deprecate showall(io::IO, dfvec::AbstractVector{T}) where {T <: AbstractDataFrame} foreach(df->show(io, df, allrows=true, allcols=true), dfvec)
@deprecate showall(dfvec::AbstractVector{T}) where {T <: AbstractDataFrame} foreach(df->show(df, allrows=true, allcols=true), dfvec)

@deprecate showall(io::IO, df::GroupedDataFrame) show(io, df, allgroups=true)
@deprecate showall(df::GroupedDataFrame) show(df, allgroups=true)

import Base: delete!, insert!, merge!

@deprecate delete!(df::AbstractDataFrame, cols::Any) select!(df, Not(cols))
@deprecate insert!(df::DataFrame, col_ind::Int, item, name::Symbol; makeunique::Bool=false) insertcols!(df, col_ind, name => item; makeunique=makeunique)
@deprecate merge!(df1::DataFrame, df2::AbstractDataFrame) (foreach(col -> df1[!, col] = df2[!, col], names(df2)); df1)

import Base: setindex!
@deprecate setindex!(df::DataFrame, x::Nothing, col_ind::Int) select!(df, Not(col_ind))

import Base: map
@deprecate map(f::Function, sdf::SubDataFrame) f(sdf)
@deprecate map(f::Union{Function,Type}, dfc::DataFrameColumns{<:AbstractDataFrame, Pair{Symbol, AbstractVector}}) mapcols(f, dfc.df)

import Base: length
@deprecate length(df::AbstractDataFrame) size(df, 2)

@deprecate head(df::AbstractDataFrame) first(df, 6)
@deprecate tail(df::AbstractDataFrame) last(df, 6)
@deprecate head(df::AbstractDataFrame, n::Integer) first(df, n)
@deprecate tail(df::AbstractDataFrame, n::Integer) last(df, n)

import Base: convert
@deprecate convert(::Type{Array}, df::AbstractDataFrame) convert(Matrix, df)
@deprecate convert(::Type{Array{T}}, df::AbstractDataFrame) where {T} convert(Matrix{T}, df)
@deprecate convert(::Type{Array}, dfr::DataFrameRow) permutedims(Vector(dfr))

@deprecate SubDataFrame(df::AbstractDataFrame, rows::AbstractVector{<:Integer}) SubDataFrame(df, rows, :)
@deprecate SubDataFrame(df::AbstractDataFrame, ::Colon) SubDataFrame(df, :, :)

@deprecate colwise(f, d::AbstractDataFrame) [f(col) for col in eachcol(d)]
@deprecate colwise(fns::Union{AbstractVector, Tuple}, d::AbstractDataFrame) [f(col) for f in fns, col in eachcol(d)]
@deprecate colwise(f, gd::GroupedDataFrame) [[f(col) for col in eachcol(d)] for d in gd]
@deprecate colwise(fns::Union{AbstractVector, Tuple}, gd::GroupedDataFrame) [[f(col) for f in fns, col in eachcol(d)] for d in gd]

import Base: get
@deprecate get(df::AbstractDataFrame, key::Any, default::Any) key in names(df) ? df[!, key] : default

import Base: haskey
@deprecate haskey(df::AbstractDataFrame, key::Symbol) hasproperty(df, key)
@deprecate haskey(df::AbstractDataFrame, key::Integer) key in 1:ncol(df)
@deprecate haskey(df::AbstractDataFrame, key::Any) key in 1:ncol(df) || key in names(df)

import Base: empty!
@deprecate empty!(df::DataFrame) select!(df, Int[])

@deprecate deletecols!(df::DataFrame, inds) select!(df, Not(inds))
@deprecate deletecols(df::DataFrame, inds; copycols::Bool=true) select(df, Not(inds), copycols=copycols)

import Base: getindex
@deprecate getindex(df::DataFrame, col_ind::ColumnIndex) df[!, col_ind]
@deprecate getindex(df::DataFrame, col_inds::Union{AbstractVector, Regex, Not}) df[:, col_inds]
@deprecate getindex(df::DataFrame, ::Colon) df[:, :]
@deprecate getindex(sdf::SubDataFrame, colind::ColumnIndex) sdf[!, colind]
@deprecate getindex(sdf::SubDataFrame, colinds::Union{AbstractVector, Regex, Not}) sdf[!, colinds]
@deprecate getindex(sdf::SubDataFrame, ::Colon) sdf[!, :]

import Base: view
@deprecate view(adf::AbstractDataFrame, colind::ColumnIndex) view(adf, :, colind)
@deprecate view(adf::AbstractDataFrame, colinds) view(adf, :, colinds)

import Base: setindex!
@deprecate setindex!(sdf::SubDataFrame, val::Any, colinds::Any) (sdf[:, colinds] = val; sdf)
@deprecate setindex!(df::DataFrame, v::AbstractVector, col_ind::ColumnIndex) (df[!, col_ind] = v; df)

# df[SingleColumnIndex] = Single Item (EXPANDS TO NROW(df) if NCOL(df) > 0)
function Base.setindex!(df::DataFrame, v, col_ind::ColumnIndex)
    if haskey(index(df), col_ind)
        Base.depwarn("Implicit broadcasting to an existing column in DataFrame assignment is deprecated." *
                     "Use an explicit broadcast with `df[:, col_ind] .= v`", :setindex!)
        df[:, col_ind] .= v
    else
        if ncol(df) == 0
            Base.depwarn("Implicit broadcasting to a new column in DataFrame assignment is deprecated." *
                         "Use `df[!, col_ind] = [v]` when `df` has zero columns", :setindex!)
            df[!, col_ind] = [v]
        else
            Base.depwarn("Implicit broadcasting to a new column in DataFrame assignment is deprecated." *
                         "Use `df[!, col_ind] .= v`  when `df` has some columns", :setindex!)
            df[!, col_ind] .= v
        end
    end
    return df
end

# df[MultiColumnIndex] = DataFrame
function Base.setindex!(df::DataFrame, new_df::DataFrame, col_inds::AbstractVector{Bool})
    setindex!(df, new_df, findall(col_inds))
end
@deprecate setindex!(df::DataFrame, new_df::DataFrame,
                     col_inds::AbstractVector{<:ColumnIndex}) foreach(((j, colind),) -> (df[!, colind] = new_df[!, j]), enumerate(col_inds))

# df[MultiColumnIndex] = AbstractVector (REPEATED FOR EACH COLUMN)
@deprecate setindex!(df::DataFrame, v::AbstractVector,
                     col_inds::AbstractVector{Bool}) (foreach(c -> (df[!, c] = copy(v)), findall(col_inds)); df)
@deprecate setindex!(df::DataFrame, v::AbstractVector,
                     col_inds::AbstractVector{<:ColumnIndex}) (foreach(c -> (df[!, c] = copy(v)), col_inds); df)

# df[MultiColumnIndex] = Single Item (REPEATED FOR EACH COLUMN; EXPANDS TO NROW(df) if NCOL(df) > 0)
function Base.setindex!(df::DataFrame,
                        val::Any,
                        col_inds::AbstractVector{Bool})
    setindex!(df, val, findall(col_inds))
end
function Base.setindex!(df::DataFrame, val::Any, col_inds::AbstractVector{<:ColumnIndex})
    Base.depwarn("implicit broadcasting in setindex! is deprecated; " *
                 "use `df[:, col_inds] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    for col_ind in col_inds
        df[col_ind] = val
    end
    return df
end

# df[:] = AbstractVector or Single Item
function Base.setindex!(df::DataFrame, v, ::Colon)
    Base.depwarn("`df[:] = v` syntax is deprecated; " *
                 "use `df[:, :] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    df[1:size(df, 2)] = v
    df
end

# df[SingleRowIndex, MultiColumnIndex] = 1-Row DataFrame
@deprecate setindex!(df::DataFrame, new_df::DataFrame, row_ind::Integer,
                     col_inds::AbstractVector{Bool}) (foreach(c -> (df[row_ind, c] = new_df[1, c]), findall(col_inds)); df)
@deprecate setindex!(df::DataFrame, new_df::DataFrame, row_ind::Integer,
                     col_inds::AbstractVector{<:ColumnIndex}) (foreach(c -> (df[row_ind, c] = new_df[1, c]), col_inds); df)

# df[SingleRowIndex, MultiColumnIndex] = Single Item
@deprecate setindex!(df::DataFrame, v::Any, row_ind::Integer,
                     col_inds::AbstractVector{<:ColumnIndex}) (df[row_ind, col_inds] .= Ref(v); df)

# df[:, SingleColumnIndex] = AbstractVector
@deprecate setindex!(df::DataFrame, v::AbstractVector, ::Colon,
                     col_ind::ColumnIndex) (df[!, col_ind] = v; df)

# df[MultiRowIndex, SingleColumnIndex] = Single Item
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_ind::ColumnIndex)
    setindex!(df, v, findall(row_inds), col_ind)
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Integer},
                        col_ind::ColumnIndex)
    Base.depwarn("implicit broadcasting in setindex! is deprecated; " *
                 "use `df[row_inds, col_ind] .= Ref(v)` broadcasting assignment to change the column in place", :setindex!)
    insert_multiple_entries!(df, v, row_inds, col_ind)
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = AbstractVector
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, findall(row_inds), findall(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(df, v, findall(row_inds), col_inds)
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, findall(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{<:ColumnIndex})
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
                 "use `df[row_inds, col_inds] .= v` broadcasting assignment to change the columns in place", :setindex!)
    for col_ind in col_inds
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    return df
end

# df[MultiRowIndex, MultiColumnIndex] = Single Item
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, findall(row_inds), findall(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    setindex!(df, v, findall(row_inds), col_inds)
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{Bool})
    setindex!(df, v, row_inds, findall(col_inds))
end
function Base.setindex!(df::DataFrame,
                        v::Any,
                        row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{<:ColumnIndex})
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
                 "use `df[row_inds, col_inds] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    for col_ind in col_inds
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    return df
end

# df[:, :] = ...
function Base.setindex!(df::DataFrame, v, ::Colon, ::Colon)
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
        "use `df[:, col_inds] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    df[1:size(df, 1), 1:size(df, 2)] = v
    df
end

# df[Any, :] = ...
function Base.setindex!(df::DataFrame, v, row_inds, ::Colon)
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
                 "use `df[row_inds, col_inds] .= Ref(v)` broadcasting assignment", :setindex!)
    df[row_inds, 1:size(df, 2)] = v
    df
end

# df[:, Any] = ...
function Base.setindex!(df::DataFrame, v, ::Colon, col_inds)
    Base.depwarn("implicit vector broadcasting in setindex! is deprecated; " *
                 "use `df[:, col_inds] .= Ref(v)` broadcasting assignment to change the columns in place", :setindex!)
    df[col_inds] = v
    df
end

import Base: setproperty!
@deprecate setproperty!(df::DataFrame, col_ind::Symbol, v) (df[!, col_ind] .= v)
@deprecate setproperty!(df::SubDataFrame, col_ind::Symbol, v) (df[:, col_ind] .= v)

# There methods duplicate functionality but are needed to resolve method call ambiuguities

function Base.setindex!(df::DataFrame,
                        new_df::AbstractDataFrame,
                        row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{<:ColumnIndex})
    idxs = index(df)[col_inds]
    if names(df)[idxs] != names(new_df)
        Base.depwarn("in the future column names in source and target will have to match", :setindex!)
    end
    for (j, col) in enumerate(idxs)
        df[row_inds, col] = new_df[!, j]
    end
    return df
end

function Base.setindex!(df::DataFrame,
                        new_df::AbstractDataFrame,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    idxs = index(df)[col_inds]
    if names(df)[idxs] != names(new_df)
        Base.depwarn("in the future column names in source and target will have to match", :setindex!)
    end
    for (j, col) in enumerate(idxs)
        df[row_inds, col] = new_df[!, j]
    end
    return df
end

function Base.setindex!(df::DataFrame,
                        mx::AbstractMatrix,
                        row_inds::AbstractVector{<:Integer},
                        col_inds::AbstractVector{<:ColumnIndex})
    idxs = index(df)[col_inds]
    if size(mx, 2) != length(idxs)
        throw(DimensionMismatch("number of selected columns ($(length(idxs))) and a" *
                                " matrix ($(size(mx, 2))) do not match"))
    end
    for (j, col) in enumerate(idxs)
        df[row_inds, col] = view(mx, :, j)
    end
    return df
end

function Base.setindex!(df::DataFrame,
                        mx::AbstractMatrix,
                        row_inds::AbstractVector{Bool},
                        col_inds::AbstractVector{<:ColumnIndex})
    idxs = index(df)[col_inds]
    if size(mx, 2) != length(idxs)
        throw(DimensionMismatch("number of selected columns ($(length(idxs))) and a" *
                                " matrix ($(size(mx, 2))) do not match"))
    end
    for (j, col) in enumerate(idxs)
        df[row_inds, col] = view(mx, :, j)
    end
    return df
end

function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{<:Integer},
                        col_ind::ColumnIndex)
    if length(v) == length(df[row_inds, col_ind])
        x = df[!, col_ind]
        x[row_inds] = v
    else
        Base.depwarn("implicit vector broadcasting in setindex! is deprecated", :setindex!)
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    return df
end

function Base.setindex!(df::DataFrame,
                        v::AbstractVector,
                        row_inds::AbstractVector{Bool},
                        col_ind::ColumnIndex)
    if length(v) == length(df[row_inds, col_ind])
        x = df[!, col_ind]
        x[row_inds] = v
    else
        Base.depwarn("implicit vector broadcasting in setindex! is deprecated", :setindex!)
        insert_multiple_entries!(df, v, row_inds, col_ind)
    end
    return df
end

####### END: duplicate methods
