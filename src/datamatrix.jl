##############################################################################
##
## Matrix constructors
##
##############################################################################

for (f, basef) in ((:dataeye, :eye), )
    @eval begin
        ($f)(n::Int) = DataArray(($basef)(n), falses(n, n))
        ($f)(n::Int, p::Int) = DataArray(($basef)(n, p), falses(n, p))
    end
end
for (f, basef) in ((:datadiagm, :diagm), )
    @eval begin
        ($f)(vals::Vector) = DataArray(($basef)(vals), falses(length(vals), length(vals)))
    end
end

##############################################################################
##
## Extract the matrix diagonal
##
##############################################################################

function diag{T}(dm::DataMatrix{T})
    return DataArray(diag(dm.data), diag(dm.na))
end

##############################################################################
##
## Size information
##
##############################################################################

nrow(dm::DataMatrix) = size(dm, 1)
ncol(dm::DataMatrix) = size(dm, 2)
