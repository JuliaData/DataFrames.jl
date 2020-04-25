function gen(df::AbstractDataFrame, args...; kwargs...)
	transform(df, args...; kwargs...)
end

function gen(gd::GroupedDataFrame, args...; keepgroup = false, kwargs...)
	
	out = combine(gd, :, args...; kwargs...)
	if keepgroup == true
		return groupby(out, groupcols(gd))
	else
		return out
	end
end

function keep(df::AbstractDataFrame, args...; kwargs...)
	t = gensym()
	df[!, t] = df[!, 1]
	ndf = select(df, t, args...; kwargs...)
	return select(ndf, Not(t))
end

function keep(gd::GroupedDataFrame, id, args...; keepgroup = false, kwargs...)
	t = gensym()
	df = parent(gd)
	df[!, t] = df[!, 1]
	ndf = combine(gd, t, args...; kwargs...)
	out = select(ndf, Not(t))
	if keepgroup == true
		return groupby(out, groupcols(gd))
	else
		return out
	end	
end

function collapse(df::AbstractDataFrame, args...; kwargs...)
	select(df, args...; kwargs...)
end

function collapse(gd::GroupedDataFrame, id, args...; keepgroup = false, kwargs...)
	out = combine(gd, args...; kwargs...)
	if keepgroup == true
		return groupby(out, groupcols(gd))
	else
		return out
	end	
end



