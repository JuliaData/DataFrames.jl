# Read saved R datasets in the RDA2 or RDX2 format
# Files written in this format often have the extension .rda or .RData

##############################################################################
##
## A table of R's SEXPREC tags.  No longer used but good to have around.
##
##############################################################################

const SXPtab = [                        # Defined in Rinternals.h
                0x00=>"NULL",
                0x01=>"Symbol",
                0x02=>"Pairlist",
                0x03=>"Closure",
                0x04=>"Environment",
                0x05=>"Promise",
                0x06=>"Lang",
                0x07=>"Special",
                0x08=>"Builtin",
                0x09=>"Char",        # "scalar" string type (internal only)
                0x0a=>"Logical",     # almost BitArray but allows NA's
                0x0d=>"Integer",     # Array{Int32, 1}
                0x0e=>"Real",        # Array{Float64, 1}
                0x0f=>"Complex",     # Array{Complex128, 1}
                0x10=>"String",      # Array{ASCIIString, 1}
                0x11=>"Dot",         # dot-dot-dot object
                0x12=>"Any",         # make "any" args work
                0x13=>"List",        # generic vector, {} but with names
                0x14=>"Expr",        # expressions vectors
                0x15=>"ByteCode",
                0x16=>"XPtr",
                0x17=>"WeakRef",
                0x18=>"Raw",
                0x19=>"S4",
                0x1e=>"New",          # fresh node created in new page
                0x1f=>"Free",         # node released by GC
                0x63=>"Function",       # closure or builtin
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
                0xfe=>"NilValue", # terminates a pairs list?
                0xff=>"Ref"
                ]

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

##############################################################################
##
## Julia types encapsulating various types of R SEXPRECs
##
##############################################################################

typealias Hash Dict{ASCIIString, Any}
const nullhash = Hash()

abstract RSEXPREC               # Basic R object - symbolic expression

abstract RVEC <: RSEXPREC               # Vector R object

type RSymbol <: RSEXPREC        # Not quite the same as a Julia symbol
    displayname::ASCIIString
end

type RList <: RVEC                   # "list" in R == Julia cell array
    data::Array{Any, 1}
    attr::Hash
end

type RNumeric <: RVEC
    data::Array{Float64, 1}
    attr::Hash
end

type RComplex <: RVEC
    data::Array{Complex128, 1}
    attr::Hash
end

type RInteger <: RVEC
    data::Array{Int32, 1}
    attr::Hash
end

type RLogical <: RVEC
    data::BitArray{1}
    missng::BitArray{1}
    attr::Hash
end

type RString <: RVEC                    # Vector of character strings
    data::Array{ASCIIString, 1}
    missng::BitArray{1}
    attr::Hash
end

##############################################################################
##
## R objects in the file are preceded by a Uint32 giving the type and
## some flags.  These functiona unpack bits in the flags.  The isobj
## bit might be useful for distinguishing an RInteger from a factor or
## an RList from a data.frame.
##
##############################################################################

isobj(fl::Uint32) = bool(fl & 0x00000100)
hasattr(fl::Uint32) = bool(fl & 0x00000200)
hastag(fl::Uint32) = bool(fl & 0x00000400)

##############################################################################
##
## Utilities for reading a single data element - ASCII format if A
## The read<type>orNA functions are needed because the ASCII format
## stores the NA as the string 'NA'.  Perhaps it would be easier to
## wrap the conversion in a try/catch block.
##
##############################################################################

readint32(io::IO, A::Bool) = A ? int32(readline(io)) : hton(read(io, Int32))
readuint32(io::IO, A::Bool) = A ? uint32(readline(io)) : hton(read(io, Uint32))
readfloat64(io::IO, A::Bool) = A ? float64(readline(io)) : hton(read(io, Float64))

function readintorNA(io::IO, A::Bool)
    if A 
        str = chomp(readline(io));
        return str == "NA" ? R_NA_INT32 : int32(str)
    end
    hton(read(io, Int32))
end

function readfloatorNA(io::IO, A::Bool)
    if A 
        str = chomp(readline(io));
        return str == "NA" ? R_NA_FLOAT64 : float64(str)
    end
    hton(read(io, Float64))
end

function InputChar(io::IO, fl::Uint32, A::Bool)  # a single character string
    @assert uint8(fl) ==  0x09
    ## levs = uint16(fl >> 12)
### watch out for levs in here.  Generally it has the value 0x40 so that fl = 0x00040009 (262153)
### if levs == 0x00 then the next line should be -1 to indicate the NA_STRING
    nchar = readuint32(io, A)
    if A
        str = unescape_string(chomp(readline(io)))
        return length(str) == nchar ? str : error("Character string length mismatch")
    end
    bytestring(read(io, Array(Uint8, nchar)))
end

function namedobjects(io::IO, fl::Uint32, A::Bool, symtab::Array{RSymbol,1})
    if !hasattr(fl) return nullhash end
    res = Hash()
    fl = readuint32(io,A)
    while uint8(fl) != 0xfe
        ## need to call RSymbol here b/c of symbol reference table
        nm = RSymbol(io, readuint32(io, A), A, symtab).displayname
        setindex!(res, readitem(io, A, symtab), nm)
        fl = readuint32(io, A)
    end
    res
end

function RNumeric(io::IO, fl::Uint32, A::Bool, symtab::Array{RSymbol,1})
    @assert uint8(fl) == 0x0e
    n = readuint32(io, A)
    RNumeric([readfloatorNA(io, A)::Float64 for i in 1:n],
             namedobjects(io, fl, A, symtab))
end

function RInteger(io::IO, fl::Uint32, A::Bool, symtab::Array{RSymbol,1})
    @assert uint8(fl) == 0x0d
    n = readint32(io, A)
    RInteger([readintorNA(io, A)::Int32 for i in 1:n],
             namedobjects(io, fl, A, symtab))
end

function RLogical(io::IO, fl::Uint32, A::Bool, symtab::Array{RSymbol,1})
    @assert uint8(fl) == 0x0a
    n = readuint32(io, A)
    rr = [readintorNA(io, A)::Int32 for i in 1:n]
    RLogical(convert(BitArray{1}, rr), convert(BitArray{1}, rr .== R_NA_INT32),
             namedobjects(io, fl, A, symtab))
end

function RComplex(io::IO, fl::Uint32, A::Bool, symtab::Array{RSymbol,1})
    @assert uint8(fl) == 0x0f
    n = readuint32(io, A)
    RComplex([(readfloatorNA(io,A) + readfloatorNA(io, A)*im)::Complex128 for i in 1:n],
             namedobjects(io, fl, A, symtab))
end

function RString(io::IO, fl::Uint32, A::Bool, symtab::Array{RSymbol,1})
    @assert uint8(fl) == 0x10
    n = readuint32(io, A)
    data = Array(ASCIIString, n)
    for i in 1:n
        fl1 = readuint32(io, A)
        data[i] = InputChar(io, fl1, A)
    end
    RString(data, falses(int(n)), namedobjects(io, fl, A, symtab))
end

function RList(io::IO, fl::Uint32, A::Bool, symtab::Array{RSymbol,1})
    @assert uint8(fl) == 0x13
    n = readuint32(io, A)
    RList([readitem(io, A, symtab) for i in 1:n], namedobjects(io, fl, A, symtab))
end

function RSymbol(io::IO, fl::Uint32, A::Bool, symtab::Array{RSymbol,1})
    if uint8(fl) == 0xff return symtab[fl >> 8] end
    @assert uint8(fl) == 0x01
    res = RSymbol(InputChar(io, readuint32(io, A), A))
    push!(symtab, res)
    res
end

function readitem(io::IO, A::Bool, symtab::Array{RSymbol,1})
    ff = readuint32(io, A)
    fl = uint8(ff)
    if fl == 0x01 return RSymbol(io, ff, A, symtab) end
    if fl == 0x0a return RLogical(io, ff, A, symtab) end
    if fl == 0x0d return RInteger(io, ff, A, symtab) end
    if fl == 0x0e return RNumeric(io, ff, A, symtab) end
    if fl == 0x0f return RComplex(io, ff, A, symtab) end
    if fl == 0x10 return RString(io, ff, A, symtab) end
    if fl == 0x13 return RList(io, ff, A, symtab) end
### Should not occur at the top level
###    if fl == 0xf3 return nothing end      # terminates dotted pair lists
###    if fl == 0x09 return InputChar(io, ff, A) end
    error("Encountered flag $ff corresponding to type $(SXPtab[fl])")
end

function read_rda(io::IO)
    header = chomp(readline(io))
    @assert header[1] == 'R' # readable header (or RDX2)
    @assert header[2] == 'D'
    @assert header[4] == '2'
    A = chomp(readline(io)) == "A"
    @assert readint32(io, A) == 2    # format version
    symtab = Array(RSymbol,0)
    Rver = readint32(io, A)
#    println("Written by version $(div(Rver,65536)).$(div(Rver%65536, 256)).$(Rver%256)")
    Rmin = readint32(io, A)
#    println("Minimal R version: $(div(Rmin,65536)).$(div(Rmin%65536, 256)).$(Rmin%256)")
    namedobjects(io, 0x00000200, A, symtab)
end

read_rda(fnm::ASCIIString) = gzopen(read_rda, fnm)

Base.size(rv::RVEC) = size(rv.data)
Base.size(rl::RList) = inherits(rl, "dataframe") ? (length(rl.data[1]), length(rl.data)) : length(rl.data)
Base.length(rl::RList) = length(rl.data)
Base.length(ri::RInteger) = length(ri.data)
Base.length(rn::RNumeric) = length(rn.data)
class(v::RVEC) = haskey(v.attr, "class") ? v.attr["class"].data : Array(ASCIIString,0)
class(x) = Array(ASCIIString, 0)
inherits(x, clnm::ASCIIString) = any(class(x) .== clnm)

names(v::RVEC) = has(v.attr, "names") ? v.attr["names"].data : Array(ASCIIString,0)
row_names(v::RVEC) = has(v.attr, "row.names") ? v.attr["row.names"].data : Array(ASCIIString,0)

data(rl::RLogical) = DataArray(rl.data, rl.missng)
data(rn::RNumeric) = DataArray(rn.data, convert(BitArray,isnan(rn.data)))
function data(ri::RInteger)
    dd = ri.data
    msng = dd .== R_NA_INT32
    if !inherits(ri, "factor") return DataArray(dd, msng) end
    pool = ri.attr["levels"].data
    sz = length(pool)
    REFTYPE = sz <= typemax(Uint8)  ? Uint8 :
              sz <= typemax(Uint16) ? Uint16 :
              sz <= typemax(Uint32) ? Uint32 :
                                      Uint64
    refs = convert(Vector{REFTYPE}, dd)
    refs[msng] = zero(REFTYPE)
    PooledDataArray(DataArrays.RefArray(refs), pool)
end

data(rs::RString) = DataArray(rs.data, falses(length(rs.data)))
function data(rc::RComplex)
    DataArray(rc.data, BitArray(real(rc.data) .== R_NA_FLOAT64) |
              BitArray(imag(rc.data) .== R_NA_FLOAT64))
end

DataFrame(rl::RList) = DataFrame(map(x->data(x), rl.data), rl.attr["names"].data)
