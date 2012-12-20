function autocor{T}(v::Vector{T})
	cor(v[1:(end-1)], v[2:end])
end

function autocor{T}(dv::DataVec{T})
	cor(dv[1:(end-1)], dv[2:end])
end

# Sample estimates
# Produce unbiased estimators instead?
function skewness{T}(v::Vector{T})
	n = length(v)
	sample_mean = mean(v)
	sample_third_centered_moment = 0.0
	sample_variance = 0.0
	for x_i in v
		sample_third_centered_moment += (x_i - sample_mean)^3
		sample_variance += (x_i - sample_mean)^2
	end
	sample_third_centered_moment /= n
	sample_variance /= n
	return sample_third_centered_moment / (sample_variance^1.5)
end

# Sample estimates
# Produce unbiased estimators instead?
function kurtosis{T}(v::Vector{T})
	n = length(v)
	sample_mean = mean(v)
	sample_fourth_centered_moment = 0.0
	sample_variance = 0.0
	for x_i in v
		sample_fourth_centered_moment += (x_i - sample_mean)^4
		sample_variance += (x_i - sample_mean)^2
	end
	sample_fourth_centered_moment /= n
	sample_variance /= n
	return (sample_fourth_centered_moment / (sample_variance^2)) - 3.0
end

# The median absolute deviation adjusted to be consistent
function mad{T}(v::Vector{T})
	center = median(v)
	adjustment = 1.4826
	absolute_deviations = 0.0
	return adjustment * median(abs(v .- center))
end

function iqr{T}(v::Vector{T})
	return quantile(v, [0.25, 0.75])
end

function rle{T}(v::Vector{T})
	n = length(v)
	current_value = v[1]
	current_length = 1
	values = Array(T, 0)
	lengths = Array(Int, 0)
	for i in 2:n
		if v[i] == current_value
			current_length += 1
		else
			push(values, current_value)
			push(lengths, current_length)
			current_value = v[i]
			current_length = 1
		end
	end
	push(values, current_value)
	push(lengths, current_length)
	return (values, lengths)
end

function inverse_rle{T}(values::Vector{T}, lengths::Vector{Int})
	total_n = sum(lengths)
	pos = 0
	res = Array(T, total_n)
	n = length(values)
	for i in 1:n
		v = values[i]
		l = lengths[i]
		for j in 1:l
			pos += 1
			res[pos] = v
		end
	end
	return res
end

function dist{T}(m::Matrix{T})
	n = size(m, 1)
	d = Array(Float64, n, n)
	for i in 1:n
		d[i, i] = 0.0
		for j in (i + 1):n
			x = norm(m[i, :] - m[j, :])
			d[i, j] = x
			d[j, i] = x
		end
	end
	return d
end
