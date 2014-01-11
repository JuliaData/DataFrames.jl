rewrite(s::Any, dfname::Symbol) = s

function rewrite(ex::Expr, dfname::Symbol)
	if ex.head == :quote
		return Expr(:ref, dfname, string(ex.args[1]))
	else
		newargs = Array(Any, length(ex.args))
		for i in 1:length(ex.args)
			newargs[i] = rewrite(ex.args[i], dfname)
		end
		return Expr(ex.head, newargs...)
	end
end

function rewrite(n::QuoteNode, dfname::Symbol)
	return Expr(:ref, dfname, string(n.value))
end

macro select(dfname, constraints)
	cleanconstraints = rewrite(constraints, dfname)
	return Expr(:ref,
		        esc(dfname),
		        esc(cleanconstraints),
		        Expr(:(:),
		        	 1,
                     Expr(:call, :size, esc(dfname), 2)))
end
