"""
@alias df.col = v
@alias df[!, :col] = v

df::DataFrame
v::AbstractVector

Assigns v to the column `col` without copying. Such that `df.col === v === df]!, :col]```.
Any AbstractVector is permissable, this may limit what operations are possible to do on `df` afterwards.
For instance after `@alias df.col = 1:3` it won't be possible to change the number of rows because UnitRange does not support resizing.
"""
macro alias(ex)
    if !Meta.isexpr(ex, :(=))
        throw(ArgumentError("Invalid use of @alias macro: argument must be a column assignment `df.col = v` or `df[!, col] = v`"))
    end

    lhs, rhs = ex.args

    if Meta.isexpr(lhs, :ref)
        ex = :(setindex!($(lhs.args[1]), $rhs, $(lhs.args[2:end]...); copycols = false); $rhs)

    elseif Meta.isexpr(lhs, :.)
        ex = :(setproperty!($(lhs.args...), $rhs; copycols = false); $rhs)

    else
        throw(ArgumentError("Invalid use of @alias macro: argument must be a column assignment `df.col = v` or `df[!, col] = v`"))

    end

    esc(ex)
end
