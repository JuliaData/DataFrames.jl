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
    0x10=>"String",        # Array{ASCIIString, 1}
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
## (I assume 1954 is the year of Ross's birth or something like that.)
##
##############################################################################

if ENDIAN_BOM == 0x01020304
    const R_NA_FLOAT64 = reinterpret(Float64, [0x7ff00000, uint32(1954)])[1]
else
    const R_NA_FLOAT64 = reinterpret(Float64, [uint32(1954), 0x7ff00000])[1]
end
const R_NA_INT32 = typemin(Int32)
const R_NA_STRING = "NA"

##############################################################################
##
## Julia types encapsulating various types of R SEXPRECs
##
##############################################################################

typealias Hash Dict{ASCIIString, Any}
const nullhash = Hash()

abstract RSEXPREC                # Basic R object - symbolic expression

type RSymbol <: RSEXPREC         # Not quite the same as a Julia symbol
    displayname::ASCIIString
end

abstract ROBJ <: RSEXPREC     # R object that can have attributes

abstract RVEC{T} <: ROBJ      # abstract R vector (actual storage implementation may differ)

type RVector{T} <: RVEC{T} # R vector object
    data::Array{T, 1}
    attr::Hash                  # collection of R object attributes
end

typealias RList RVector{Any}  # "list" in R == Julia cell array

typealias RNumeric RVector{Float64}

typealias RComplex RVector{Complex128}

typealias RInteger RVector{Int32}

type RLogical <: RVEC{Bool}
    data::Array{Bool, 1}
    missng::BitArray{1}
    attr::Hash                  # collection of R object attributes
end

typealias RString RVector{ASCIIString} # Vector of character strings

##############################################################################
##
## R objects in the file are preceded by a Uint32 giving the type and
## some flags.  These functiona unpack bits in the flags.  The isobj
## bit might be useful for distinguishing an RInteger from a factor or
## an RList from a data.frame.
##
##############################################################################

typealias RDATag Uint32

isobj(fl::RDATag) = bool(fl & 0x00000100)
hasattr(fl::RDATag) = bool(fl & 0x00000200)
hastag(fl::RDATag) = bool(fl & 0x00000400)

sxtype = uint8

##############################################################################
##
## Utilities for reading a single data element - ASCII format if A
## The read<type>orNA functions are needed because the ASCII format
## stores the NA as the string 'NA'.  Perhaps it would be easier to
## wrap the conversion in a try/catch block.
##
##############################################################################

type RDAIO # RDA IO stream
    sub::IO       # underlying IO stream
    ascii::Bool  # if the stream is in ASCII format
end

readint32(io::RDAIO) = io.ascii ? int32(readline(io.sub)) : hton(read(io.sub, Int32))
readuint32(io::RDAIO) = io.ascii ? uint32(readline(io.sub)) : hton(read(io.sub, Uint32))
readfloat64(io::RDAIO) = io.ascii ? float64(readline(io.sub)) : hton(read(io.sub, Float64))

function readintorNA(io::RDAIO)
    if io.ascii
        str = chomp(readline(io.sub));
        return str == R_NA_STRING ? R_NA_INT32 : int32(str)
    end
    hton(read(io.sub, Int32))
end

function readfloatorNA(io::RDAIO)
    if io.ascii
        str = chomp(readline(io.sub));
        return str == R_NA_STRING ? R_NA_FLOAT64 : float64(str)
    end
    hton(read(io.sub, Float64))
end

function readcharacter(io::RDAIO, fl::RDATag)  # a single character string
    @assert sxtype(fl) ==  0x09
    ## levs = uint16(fl >> 12)
### watch out for levs in here.  Generally it has the value 0x40 so that fl = 0x00040009 (262153)
### if levs == 0x00 then the next line should be -1 to indicate the NA_STRING
    nchar = readuint32(io)
    if io.ascii
        str = unescape_string(chomp(readline(io.sub)))
        return length(str) == nchar ? str : error("Character string length mismatch")
    end
    bytestring(read(io.sub, Array(Uint8, nchar)))
end

##############################################################################
##
## Utilities for reading compound RDA items: lists, arrays etc
##
##############################################################################

type RDAContext # RDA reading context
    io::RDAIO                  # R input stream
    fmtver::Uint32             # RDA format version
    Rver::VersionNumber        # R version that has written RDA
    Rmin::VersionNumber        # R minimal version to read RDA
    symtab::Array{RSymbol,1}   # symbols array

    function RDAContext(io::RDAIO)
        fmtver = readint32(io)
        rver = readint32(io)
        rminver = readint32(io)
        new(io,
            fmtver,
            VersionNumber( div(rver,65536), div(rver%65536, 256), rver%256 ),
            VersionNumber( div(rminver,65536), div(rminver%65536, 256), rminver%256 ),
            Array(RSymbol,0))
    end
end

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
    n = readuint32(ctx.io)
    RNumeric([readfloatorNA(ctx.io)::Float64 for i in 1:n],
             readattrs(ctx, fl))
end

function readinteger(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x0d
    n = readint32(ctx.io)
    RInteger([readintorNA(ctx.io)::Int32 for i in 1:n],
             readattrs(ctx, fl))
end

function readlogical(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x0a
    n = readuint32(ctx.io)
    data = [readintorNA(ctx.io)::Int32 for i in 1:n]
    RLogical(data,
             convert(BitArray{1}, data .== R_NA_INT32),
             readattrs(ctx, fl))
end

function readcomplex(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x0f
    n = readuint32(ctx.io)
    RComplex([complex128(readfloatorNA(ctx.io), readfloatorNA(ctx.io)) for i in 1:n],
             readattrs(ctx, fl))
end

function readstring(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x10
    n = readuint32(ctx.io)
    data = Array(ASCIIString, n)
    for i in 1:n
        fl1 = readuint32(ctx.io)
        data[i] = readcharacter(ctx.io, fl1)
    end
    RString(data, readattrs(ctx, fl))
end

function readlist(ctx::RDAContext, fl::RDATag)
    @assert sxtype(fl) == 0x13
    n = readuint32(ctx.io)
    RList([readitem(ctx) for i in 1:n],
          readattrs(ctx, fl))
end

function readsymbol(ctx::RDAContext, fl::RDATag)
    # check if its a reference to an already defined string
    if sxtype(fl) == 0xff return ctx.symtab[fl >> 8] end
    # read the new strings and put it into symbols table
    @assert sxtype(fl) == 0x01
    res = RSymbol(readcharacter(ctx.io, readuint32(ctx.io)))
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

function read_rda(io::IO)
    header = chomp(readline(io))
    @assert header[1] == 'R' # readable header (or RDX2)
    @assert header[2] == 'D'
    @assert header[4] == '2'
    ctx = RDAContext(RDAIO(io, chomp(readline(io)) == "A"))
    @assert ctx.fmtver == 2    # format version
#    println("Written by R version $(ctx.Rver)")
#    println("Minimal R version: $(ctx.Rmin)")
    return readnamedobjects(ctx, 0x00000200)
end

read_rda(fnm::ASCIIString) = gzopen(read_rda, fnm)

##############################################################################
##
## Utilities for working with basic properties of R objects:
##    attributes, class inheritance, etc
##
##############################################################################

const emptystrvec = Array(ASCIIString,0)

getattr{T}(ro::ROBJ, attrnm::ASCIIString, default::T) = haskey(ro.attr, attrnm) ? ro.attr[attrnm].data : default;

Base.names(ro::ROBJ) = getattr(ro, "names", emptystrvec)

class(ro::ROBJ) = getattr(ro, "class", emptystrvec)
class(x) = emptystrvec
inherits(x, clnm::ASCIIString) = any(class(x) .== clnm)

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

namask(rl::RLogical) = rl.missng
namask(ri::RInteger) = bitpack(ri.data .== R_NA_INT32)
namask(rn::RNumeric) = bitpack(isnan(rn.data))
namask(rs::RString) = falses(length(rs.data)) # FIXME use R_NA_STRING?
namask(rc::RComplex) = bitpack(real(rc.data) .== R_NA_FLOAT64) | BitArray(imag(rc.data) .== R_NA_FLOAT64)

DataArrays.data{T}(rv::RVEC{T}) = DataArray(rv.data, namask(rv))

function DataArrays.data(ri::RInteger)
    if !isfactor(ri) return DataArray(ri.data, namask(ri)) end
    # convert factor into PooledDataArray
    pool = getattr(ri, "levels", emptystrvec)
    sz = length(pool)
    REFTYPE = sz <= typemax(Uint8)  ? Uint8 :
              sz <= typemax(Uint16) ? Uint16 :
              sz <= typemax(Uint32) ? Uint32 :
                                      Uint64
    dd = ri.data
    dd[namask(ri)] = 0
    refs = convert(Vector{REFTYPE}, dd)
    return PooledDataArray(DataArrays.RefArray(refs), pool)
end

function DataFrame(rl::RList)
    DataFrame( map(data, rl.data),
               Symbol[identifier(x) for x in names(rl)] )
end
