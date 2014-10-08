module Compatibility
    export @AnyDict, @Dict

    if VERSION < v"0.4.0-dev+980"
        macro Dict(pairs...)
            Expr(:dict, pairs...)
        end
        macro AnyDict(pairs...)
            Expr(:typed_dict, :(Any=>Any), pairs...)
        end
    else
        macro Dict(pairs...)
            Expr(:call, :Dict, pairs...)
        end
        macro AnyDict(pairs...)
            Expr(:call, :(Base.AnyDict), pairs...)
        end
    end
end # module Compatibility

using .Compatibility
