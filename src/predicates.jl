numeric_predicates = (:iseven, :ispow2, :isfinite, :isprime, :isinf, :isodd)

type_predicates = (:isbool, :isinteger, :isreal, :iscomplex, :islogical)

container_predicates = (:isempty,)

for p in numeric_predicates
	@eval begin
		($p)(v::NAtype) = NA
	end
end

for p in type_predicates
	@eval begin
		($p)(v::NAtype) = NA
	end
end
