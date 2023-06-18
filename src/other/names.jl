# common utilities for getting and comparing column names

_getnames(x::NamedTuple) = propertynames(x)
_getnames(x::AbstractDataFrame) = _names(x)
_getnames(x::DataFrameRow) = _names(x)
_getnames(x::Tables.AbstractRow) = Tables.columnnames(x)
_getnames(x::GroupKey) = parent(x).cols

# this function is needed as == does not allow for comparison between tuples and vectors
function _equal_names(r1, r2)
    n1 = _getnames(r1)
    n2 = _getnames(r2)
    length(n1) == length(n2) || return false
    for (a, b) in zip(n1, n2)
        a == b || return false
    end
    return true
end
