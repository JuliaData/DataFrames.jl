function gen(df::AbstractDataFrame, args...; kwargs...)
	transform(df, args...; kwargs...)
end

function genby(df::AbstractDataFrame, id, args...; kwargs...)
	by(df, id, :, args...; kwargs...)
end

function keep(df::AbstractDataFrame, args...; kwargs...)
		t = gensym()
	df[!, t] = df[!, 1]
	ndf = select(df, t, args...; kwargs...)
	return select(ndf, Not(t))
end

function keepby(df::AbstractDataFrame, id, args...; kwargs...)
	t = gensym()
	df[!, t] = df[!, 1]
	ndf = by(df, id, t, args...; kwargs...)
	select(ndf, Not(t))
end

function collapse(df::AbstractDataFrame, args...; kwargs...)
	select(df, args...; kwargs...)
end

function collapseby(df::AbstractDataFrame, id, args...; kwargs...)
	by(df, id, args...; kwargs...)
end

function agggen(gd::GroupedDataFrame, args...; kwargs...)
	combine(gd, :, args...; kwargs...)
end

function aggkeep(gd::GroupedDataFrame, args...; kwargs...)
	df = parent(gd)
	t = gensym()
	df[!, t] = df[!, 1]
	ndf = combine(gd, t, args...; kwargs...)
	select(ndf, Not(t))
end

function aggcollapse(gd::GroupedDataFrame, args...; kwargs...)
	combine(gd, args...; kwargs...)
end

