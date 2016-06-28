# Read saved R datasets in the RDA2 or RDX2 format
# Files written in this format often have the extension .rda or .RData

##############################################################################
##
## A table of R's SEXPREC tags.  No longer used but good to have around.
##
##############################################################################

const SXPtab = @compat Dict(      # Defined in Rinternals.h
    0x00=>"NULL",
    0x01=>"Symbol",
    0x02=>"Pairlist",
    0x03=>"Closure",
    0x04=>"Environment",
    0x05=>"Promise",
    0x06=>"Lang",
    0x07=>"Special",
    0x08=>"Builtin",
    0x09=>"Char",          # "scalar" string type (internal only)
    0x0a=>"Logical",       # almost BitArray but allows NA's
    0x0d=>"Integer",       # Array{Int32, 1}
    0x0e=>"Real",          # Array{Float64, 1}
    0x0f=>"Complex",       # Array{Complex128, 1}
    0x10=>"String",        # Array{String, 1}
    0x11=>"Dot",           # dot-dot-dot object
    0x12=>"Any",           # make "any" args work
    0x13=>"List",          # generic vector, {} but with names
    0x14=>"Expr",          # expressions vectors
    0x15=>"ByteCode",
    0x16=>"XPtr",
    0x17=>"WeakRef",
    0x18=>"Raw",
    0x19=>"S4",
    0x1e=>"New",           # fresh node created in new page
    0x1f=>"Free",          # node released by GC
    0x63=>"Function",      # closure or builtin
                           # Defined in serialize.c
    0xf1=>"BaseEnv",
    0xf2=>"EmptyEnv",
    0xf3=>"BCREPREF",
    0xf4=>"BCREPDEF",
    0xf5=>"GenericRef",
    0xf6=>"ClassRef",
    0xf7=>"Persist",
    0xf8=>"Package",
    0xf9=>"Namespace",
    0xfa=>"BaseNamespace",
    0xfb=>"MissingArg",
    0xfc=>"UnboundValue",
    0xfd=>"GlobalEnv",
    0xfe=>"NilValue",      # terminates a pairs list?
    0xff=>"Ref"
)

##############################################################################
##
## Constants used as NA patterns in R.
## 0x000007a2 == UInt32(1954), stored as a NaN bit pattern
## (I assume 1954 is the year of Ross's birth or something like that.)
##
##############################################################################

# Need to work with UInt64 to work around corruption when reinterpreting as float:
# See JuliaLang/julia#17195
if ENDIAN_BOM == 0x01020304
    const R_NA_FLOAT64 = 0x000007a27ff00000
else
    const R_NA_FLOAT64 = 0x7ff00000000007a2
end
const R_NA_INT32 = typemin(Int32)
const R_NA_STRING = "NA"

##############################################################################
##
## Julia types encapsulating various types of R SEXPRECs
##
##############################################################################

typealias Hash Dict{String, Any}
const nullhash = Hash()

abstract RSEXPREC{S}             # Basic R object - symbolic expression

type RSymbol <: RSEXPREC{0x01}   # Not quite the same as a Julia symbol
    displayname::String
end

abstract ROBJ{S} <: RSEXPREC{S}  # R object that can have attributes

abstract RVEC{T, S} <: ROBJ{S}   # abstract R vector (actual storage implementation may differ)

type RVector{T, S} <: RVEC{T, S} # R vector object
    data::Array{T, 1}
    attr::Hash                   # collection of R object attributes
end

typealias RLogical RVector{Int32, 0x0a}
typealias RInteger RVector{Int32, 0x0d}
typealias RNumeric RVector{Float64, 0x0e}
typealias RComplex RVector{Complex128, 0x0f}

type RNullableVector{T, S} <: RVEC{T, S} # R vector object with explicit NA values
    data::Array{T, 1}
    na::BitArray{1}             # mask of NA elements
    attr::Hash                  # collection of R object attributes
end

typealias RString RNullableVector{String,0x10}
typealias RList RVector{Any, 0x13}  # "list" in R == Julia cell array

##############################################################################
##
## R objects in the file are preceded by a UInt32 giving the type and
## some flags.  These functiona unpack bits in the flags.  The isobj
## bit might be useful for distinguishing an RInteger from a factor or
## an RList from a data.frame.
##
##############################################################################

typealias RDATag UInt32

isobj(fl::RDATag) = (fl & 0x00000100) != 0
hasattr(fl::RDATag) = (fl & 0x00000200) != 0
hastag(fl::RDATag) = (fl & 0x00000400) != 0

if VERSION < v"0.4-"
    sxtype = uint8
else
    sxtype(fl::RDATag) = fl % UInt8
end

##############################################################################
##
## Utilities for reading a single data element.
## The read<type>orNA functions are needed because the ASCII format
## stores the NA as the string 'NA'.  Perhaps it would be easier to
## wrap the conversion in a try/catch block.
##
##############################################################################

LONG_VECTOR_SUPPORT = (WORD_SIZE > 32) # disable long vectors support on 32-bit machines

if LONG_VECTOR_SUPPORT
    typealias RVecLength Int64
else
    typealias RVecLength Int
end

# abstract RDA format IO stream wrapper
abstract RDAIO

type RDAXDRIO{T<:IO} <: RDAIO # RDA XDR(binary) format IO stream wrapper
    sub::T             # underlying IO stream
    buf::Vector{UInt8} # buffer for strings

    RDAXDRIO( io::T ) = new( io, Array(UInt8, 1024) )
end
RDAXDRIO{T <: IO}(io::T) = RDAXDRIO{T}(io)

readint32(io::RDAXDRIO) = ntoh(read(io.sub, Int32))
readuint32(io::RDAXDRIO) = ntoh(read(io.sub, UInt32))
readfloat64(io::RDAXDRIO) = ntoh(read(io.sub, Float64))

readintorNA(io::RDAXDRIO) = readint32(io)
readintorNA(io::RDAXDRIO, n::RVecLength) = map!(ntoh, read(io.sub, Int32, n))

readfloatorNA(io::RDAXDRIO) = readfloat64(io)
# Need to work with UInt64 to work around corruption when reinterpreting as float:
# See JuliaLang/julia#17195
readfloatorNA(io::RDAXDRIO, n::RVecLength) =
    reinterpret(Float64, map!(ntoh, read(io.sub, UInt64, n)))

function readnchars(io::RDAXDRIO, n::Int32)  # a single character string
    readbytes!(io.sub, io.buf, n)
    bytestring(pointer(io.buf), n)::String
end

type RDAASCIIIO{T<:IO} <: RDAIO # RDA ASCII format IO stream wrapper
    sub::T              # underlying IO stream

    RDAASCIIIO( io::T ) = new( io )
end
RDAASCIIIO{T <: IO}(io::T) = RDAASCIIIO{T}(io)

readint32(io::RDAASCIIIO) = parse(Int32, readline(io.sub))
readuint32(io::RDAASCIIIO) = parse(UInt32, readline(io.sub))
readfloat64(io::RDAASCIIIO) = parse(Float64, readline(io.sub))

function readintorNA(io::RDAASCIIIO)
    str = chomp(readline(io.sub));
    str == R_NA_STRING ? R_NA_INT32 : parse(Int32, str)
end
readintorNA(io::RDAASCIIIO, n::RVecLength) = Int32[readintorNA(io) for i in 1:n]

# Need to work with UInt64 to work around corruption when reinterpreting as float:
# See JuliaLang/julia#17195
#function readfloatorNA(io::RDAASCIIIO)
#    str = chomp(readline(io.sub));
#    str == R_NA_STRING ? R_NA_FLOAT64 : parse(Float64, str)
#end
function readfloatorNA(io::RDAASCIIIO, n::RVecLength)
    res = Vector{UInt64}(n)
    for i in 1:n
        str = chomp(readline(io.sub))
        res[i] = str == R_NA_STRING ? R_NA_FLOAT64 :
                                      reinterpret(UInt64, parse(Float64, str))
    end
    reinterpret(Float64, res)
end

function readnchars(io::RDAASCIIIO, n::Int32)  # reads N bytes-sized string
    if (n==-1) return "" end
    str = unescape_string(chomp(readline(io.sub)))
    length(str) == n || error("Character string length mismatch")
    str
end

type RDANativeIO{T<:IO} <: RDAIO # RDA native binary format IO stream wrapper (TODO)
    sub::T               # underlying IO stream

    RDANativeIO( io::T ) = new( io )
end
RDANativeIO{T <: IO}(io::T) = RDANativeIO{T}(io)

function rdaio(io::IO, formatcode::AbstractString)
    if formatcode == "X" RDAXDRIO(io)
    elseif formatcode == "A" RDAASCIIIO(io)
    elseif formatcode == "B" RDANativeIO(io)
    else error( "Unrecognized RDA format \"$formatcode\"" )
    end
end

if LONG_VECTOR_SUPPORT
    # reads the length of any data vector from a stream
    # from R's serialize.c
    function readlength(io::RDAIO)
        len = convert(RVecLength, readint32(io))
        if (len < -1) error("negative serialized length for vector")
        elseif (len >= 0)
            return len
        else # big vectors, the next 2 ints encode the length
            len1, len2 = convert(RVecLength, readint32(io)), convert(RVecLength, readint32(io))
            # sanity check for now
            if (len1 > 65536) error("invalid upper part of serialized vector length") end
            return (len1 << 32) + len2
        end
    end
else
    # reads the length of any data vector from a stream
    # fails when long (> 2^31-1) vector encountered
    # from R's serialize.c
    function readlength(io::RDAIO)
        len = convert(RVecLength, readint32(io))
        if (len >= 0)
            return len
        elseif (len < -1)
            error("negative serialized length for vector")
        else
            error("negative serialized vector length:\nperhaps long vector from 64-bit version of R?")
        end
    end
end

immutable CHARSXProps # RDA CHARSXP properties
  levs::UInt32       # level flags (encoding etc) TODO process
  nchar::Int32       # string length, -1 for NA strings
end

function readcharsxprops(io::RDAIO) # read character string encoding and length
    fl = readuint32(io)
    @assert sxtype(fl) == 0x09
### watch out for levs in here.  Generally it has the value 0x40 so that fl = 0x00040009 (262153)
### if levs == 0x00 then the next line should be -1 to indicate the NA_STRING
    CHARSXProps(fl >> 12, readint32(io))
end

function readcharacter(io::RDAIO)  # a single character string
    props = readcharsxprops(io)
    props.nchar==-1 ? "" : readnchars(io, props.nchar)
end

function readcharacter(io::RDAIO, n::RVecLength)  # a single character string
    res = fill("", n)
    na = falses(n)
    for i in 1:n
        props = readcharsxprops(io)
        if (props.nchar==-1) na[i] = true
        else res[i] = readnchars(io, props.nchar)
        end
    end
    return res, na
end

##############################################################################
##
## Utilities for reading compound RDA items: lists, arrays etc
##
##############################################################################

type RDAContext{T <: RDAIO}    # RDA reading context
    io::T                      # R input stream

    # RDA properties
    fmtver::UInt32             # RDA format version
    Rver::VersionNumber        # R version that has written RDA
    Rmin::VersionNumber        # R minimal version to read RDA

    # behaviour
    convertdataframes::Bool    # if R dataframe objects should be automatically converted into DataFrames

    # intermediate data
    symtab::Array{RSymbol,1}   # symbols array

    function RDAContext(io::T, kwoptions::Array{Any})
        fmtver = readint32(io)
        rver = readint32(io)
        rminver = readint32(io)
        kwdict = Dict{Symbol,Any}(kwoptions)
        new(io,
            fmtver,
            VersionNumber( div(rver,65536), div(rver%65536, 256), rver%256 ),
            VersionNumber( div(rminver,65536), div(rminver%65536, 256), rminver%256 ),
            get(kwdict,:convertdataframes,false),
            Array(RSymbol,0))
    end
end

RDAContext{T <: RDAIO}(io::T, kwoptions::Array{Any}) = RDAContext{T}(io, kwoptions)

function readnamedobjects(ctx::RDAContext, fl::RDATag)
    if !hasattr(fl) return nullhash end
    res = Hash()
    fl = readuint32(ctx.io)
    while sxtype(fl) != 0xfe
        ## need to call RSymbol here b/c of symbol reference table
        nm = readsymbol(ctx, readuint32(ctx.io)).displayname
        setindex!(res, readitem(ctx), nm)
        fl = readuint32(ctx.io)
    end
    res
end

readattrs(ctx::RDAContext, fl::RDATag) = readnamedobjects(ctx, fl)

function readnumeric(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x0e
    RNumeric(readfloatorNA(ctx.io, readlength(ctx.io)),
             readattrs(ctx, fl))
end

function readinteger(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x0d
    RInteger(readintorNA(ctx.io, readlength(ctx.io)),
             readattrs(ctx, fl))
end

function readlogical(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x0a # excluding this check, the method is the same as readinteger()
    RLogical(readintorNA(ctx.io, readlength(ctx.io)),
             readattrs(ctx, fl))
end

function readcomplex(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x0f
    n = readlength(ctx.io)
    data = readfloatorNA(ctx.io, 2n)
    RComplex(Complex128[@compat(Complex128(data[i],data[i+1])) for i in 2(1:n)-1],
             readattrs(ctx, fl))
end

function readstring(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x10
    RString(readcharacter(ctx.io, readlength(ctx.io))...,
            readattrs(ctx, fl))
end

function readlist(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x13
    n = readlength(ctx.io)
    res = RList([readitem(ctx) for i in 1:n],
                readattrs(ctx, fl))
    if ctx.convertdataframes && isdataframe(res)
        DataFrame(res)
    else
        res
    end
end

function readsymbol(ctx::RDAContext, fl::RDATag)
    # check if its a reference to an already defined string
    if sxtype(fl) == 0xff return ctx.symtab[fl >> 8] end
    # read the new strings and put it into symbols table
    @assert sxtype(fl) == 0x01
    res = RSymbol(readcharacter(ctx.io))
    push!(ctx.symtab, res)
    res
end

const SXReaders = @compat Dict( # maps RDA type to function that read it
    0x01 => readsymbol,
    0x0a => readlogical,
    0x0d => readinteger,
    0x0e => readnumeric,
    0x0f => readcomplex,
    0x10 => readstring,
    0x13 => readlist
);

function readitem(ctx::RDAContext)
    ff = readuint32(ctx.io)
    fl = sxtype(ff)
    if haskey(SXReaders, fl) return SXReaders[fl](ctx, ff) end
### Should not occur at the top level
###    if fl == 0xf3 return nothing end      # terminates dotted pair lists
###    if fl == 0x09 return readcharacter(ctx.io, ff) end
    error("Encountered flag $ff corresponding to type $(SXPtab[fl])")
end

function read_rda(io::IO, kwoptions::Array{Any})
    header = chomp(readline(io))
    @assert header[1] == 'R' # readable header (or RDX2)
    @assert header[2] == 'D'
    @assert header[4] == '2'
    ctx = RDAContext(rdaio(io, chomp(readline(io))), kwoptions)
    @assert ctx.fmtver == 2    # format version
#    println("Written by R version $(ctx.Rver)")
#    println("Minimal R version: $(ctx.Rmin)")
    return readnamedobjects(ctx, 0x00000200)
end

read_rda(io::IO; kwoptions...) = read_rda(io, kwoptions)

read_rda(fnm::AbstractString; kwoptions...) = gzopen(fnm) do io read_rda(io, kwoptions) end

##############################################################################
##
## Utilities for working with basic properties of R objects:
##    attributes, class inheritance, etc
##
##############################################################################

const emptystrvec = Array(String,0)

getattr{T}(ro::ROBJ, attrnm::String, default::T) = haskey(ro.attr, attrnm) ? ro.attr[attrnm].data : default;

Base.names(ro::ROBJ) = getattr(ro, "names", emptystrvec)

class(ro::ROBJ) = getattr(ro, "class", emptystrvec)
class(x) = emptystrvec
inherits(x, clnm::String) = any(class(x) .== clnm)

isdataframe(rl::RList) = inherits(rl, "data.frame")
isfactor(ri::RInteger) = inherits(ri, "factor")

Base.length(rl::RVEC) = length(rl.data)
Base.size(rv::RVEC) = length(rv.data)
Base.size(rl::RList) = isdataframe(rl) ? (length(rl.data[1]), length(rl.data)) : length(rl.data)

row_names(ro::ROBJ) = getattr(ro, "row.names", emptystrvec)

##############################################################################
##
## Conversion of intermediate R objects into DataArray and DataFrame objects
##
##############################################################################

namask(rl::RLogical) = BitArray(rl.data .== R_NA_INT32)
namask(ri::RInteger) = BitArray(ri.data .== R_NA_INT32)
# Need to work with UInt64 to work around corruption when reinterpreting as float:
# See JuliaLang/julia#17195
namask(rn::RNumeric) =
    BitArray([reinterpret(UInt64, rn.data)[i] == R_NA_FLOAT64 for i in 1:length(rn.data)])
function namask(rc::RComplex)
    data = reinterpret(Complex{UInt64}, rc.data)
    BitArray([data[i].re == R_NA_FLOAT64 || data[i].im == R_NA_FLOAT64
              for i in 1:length(rc.data)])
end
namask(rv::RNullableVector) = rv.na

DataArrays.data(rv::RVEC) = DataArray(rv.data, namask(rv))

function DataArrays.data(ri::RInteger)
    if !isfactor(ri) return DataArray(ri.data, namask(ri)) end
    # convert factor into PooledDataArray
    pool = getattr(ri, "levels", emptystrvec)
    sz = length(pool)
    REFTYPE = sz <= typemax(UInt8)  ? UInt8 :
              sz <= typemax(UInt16) ? UInt16 :
              sz <= typemax(UInt32) ? UInt32 :
                                      UInt64
    dd = ri.data
    dd[namask(ri)] = 0
    refs = convert(Vector{REFTYPE}, dd)
    return PooledDataArray(DataArrays.RefArray(refs), pool)
end

function DataFrame(rl::RList)
    DataFrame(map(data, rl.data),
              Symbol[identifier(x) for x in names(rl)])
end
