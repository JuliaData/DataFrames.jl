# Initialized constructors with 0's, 1's
for (f, basef) in ((:dmzeros, :zeros), (:dmones, :ones), (:dmeye, :eye))
    @eval begin
        ($f)(n::Int64, p::Int64) = DataArray(($basef)(n, p), bitfalses(n, p))
        ($f)(t::Type, n::Int64, p::Int64) = DataArray(($basef)(t, n, p), bitfalses(n, p))
    end
end

# Initialized constructors with false's or true's
for (f, basef) in ((:dmfalses, :falses), (:dmtrues, :trues))
    @eval begin
        ($f)(n::Int64, p::Int64) = DataVec(($basef)(n, p), bitfalses(n, p))
    end
end

# dmdiag, dmdiagm
