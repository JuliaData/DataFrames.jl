# Formal Specification of DataFrames Data Structures

* Type Definitions and Type Hierarchy
* Constructors
* Indexing (Refs / Assigns)
* Operators
	* Unary Operators:
		* `+`, `-`, `!`, `'`
	* Elementary Unary Functions
		* `abs`, ...
	* Binary Operators:
		* Arithmetic Operators:
			* Scalar Arithmetic: `+`, `-`, `*`, `/`, `^`,
			* Array Arithmetic: `+`, `.+`, `-`, `.-`, `.*`, `./`, `.^`
		* Bit Operators: `&`, `|`, `$`
		* Comparison Operators:
			* Scalar Comparisons: `==`, `!=`, `<`, `<=`, `>`, `>=`
			* Array Comparisons: `.==`, `.!=`, `.<`, `.<=`, `.>`, `.>=`
* Container Operations
* Broadcasting / Recycling
* Type Promotion and Conversion
* String Representations
* IO
* Copying
* Properties
	* size
	* length
	* ndims
	* numel
	* eltype
* Predicates
* Handling NA's
* Iteration
* Miscellaneous

## The NAtype

### Behavior under Unary Operators

The unary operators

### Behavior under Unary Operators

The unary operators

### Behavior under Arithmetic Operators

# Constructors

* NA's
	* Constructor: `NAtype()`
	* Const alias: `NA`
* DataVector's
	* From (Vector, BitVector): `DataArray([1, 2, 3], falses(3))`
	* From (Vector, Vector{Bool}): `DataArray([1, 2, 3], [false, false, false])`
	* From (Vector): `DataArray([1, 2, 3])`
	* From (BitVector, BitVector): `DataArray(trues(3), falses(3))`
	* From (BitVector): `DataArray(trues(3))`
	* From (Range1): `DataArray(1:3)`
	* From (DataVector): `DataArray(DataArray([1, 2, 3]))`
	* From (Type, Int): `DataArray(Int64, 3)`
	* From (Int): `DataArray(3)` (Type defaults to Float64)
	* From (): `DataArray()` (Type defaults to Float64, length defaults to 0)
	* Initialized with Float64 zeros: `datazeros(3)`
	* Initialized with typed zeros: `datazeros(Int64, 3)`
	* Initialized with Float64 ones: `dataones(3)`
	* Initialized with typed ones: `dataones(Int64, 3)`
	* Initialized with falses: `datafalses(3)`
	* Initialized with trues: `datatrues(3)`
	* Literal syntax: `DataVector[1, 2, NA]`
* PooledDataVector's
	* From (Vector, BitVector): `PooledDataVector([1, 2, 3], falses(3))`
	* From (Vector, Vector{Bool}): `PooledDataVector([1, 2, 3], [false, false, false])`
	* From (Vector): `PooledDataVector([1, 2, 3])`
	* From (BitVector, BitVector): `PooledDataVector(trues(3), falses(3))`
	* From (BitVector, Vector{Bool}): `PooledDataVector(trues(3), [false, false, false])`
	* From (BitVector): `PooledDataVector(trues(3))`
	* From (Range1): `PooledDataVector(1:3)`
	* From (DataVector): `PooledDataVector(DataArray([1, 2, 3]))`
	* From (Type, Int): `PooledDataVector(Int64, 3)`
	* From (Int): `PooledDataVector(3)` (Type defaults to Float64)
	* From (): `PooledDataVector()` (Type defaults to Float64, length defaults to 0)
	* Initialized with Float64 zeros: `pdatazeros(3)`
	* Initialized with typed zeros: `pdatazeros(Int64, 3)`
	* Initialized with Float64 ones: `pdataones(3)`
	* Initialized with typed ones: `pdataones(Int64, 3)`
	* Initialized with falses: `pdatafalses(3)`
	* Initialized with trues: `pdatatrues(3)`
	* Literal syntax: `PooledDataVector[1, 2, NA]`
* DataMatrix
	* From (Array, BitArray): `DataMatrix([1 2; 3 4], falses(2, 2))`
	* From (Array, Array{Bool}): `DataMatrix([1 2; 3 4], [false false; false false])`
	* From (Array): `DataMatrix([1 2; 3 4])`
	* From (BitArray, BitArray): `DataMatrix(trues(2, 2), falses(2, 2))`
	* From (BitArray): `DataMatrix(trues(2, 2))`
	* From (DataVector...): `DataMatrix(DataVector[1, NA], DataVector[NA, 2])`
	* From (Range1...): `DataMatrix(1:3, 1:3)`
	* From (DataMatrix): `DataMatrix(DataArray([1 2; 3 4]))`
	* From (Type, Int, Int): `DataMatrix(Int64, 2, 2)`
	* From (Int, Int): `DataMatrix(2, 2)` (Type defaults to Float64)
	* From (): `DataMatrix()` (Type defaults to Float64, length defaults to (0, 0))
	* Initialized with Float64 zeros: `dmzeros(2, 2)`
	* Initialized with typed zeros: `dmzeros(Int64, 2, 2)`
	* Initialized with Float64 ones: `dmones(2, 2)`
	* Initialized with typed ones: `dmones(Int64, 2, 2)`
	* Initialized with falses: `dmfalses(2, 2)`
	* Initialized with trues: `dmtrues(2, 2)`
	* Initialized identity matrix: `dmeye(2, 2)`
	* Initialized identity matrix: `dmeye(2)`
	* Initialized diagonal matrix: `dmdiagm([2, 1])`
	* Literal syntax: `DataMatrix[1 2; NA 2]`
* DataFrame
	* From (): `DataFrame()`
	* From (Vector{Any}, Index): `DataFrame({datazeros(3), dataones(3)}, Index(["A", "B"]))`
	* From (Vector{Any}): `DataFrame({datazeros(3), dataones(3)})
	* From (Expr): `DataFrame(quote A = [1, 2, 3, 4] end)`
	* From (Matrix, Vector{String}): `DataFrame([1 2; 3 4], ["A", "B"])`
	* From (Matrix): `DataFrame([1 2; 3 4])`
	* From (Tuple): `DataFrame(dataones(2), datafalses(2))`
	* From (Associative): ???
	* From (Vector, Vector, Groupings): ???
	* From (Dict of Vectors): `DataFrame({"A" => [1, 3], "B" => [2, 4]})`
	* From (Dict of Vectors, Vector{String}): `DataFrame({"A" => [1, 3], "B" => [2, 4]}, ["A"])`
	* From (Type, Int, Int): `DataFrame(Int64, 2, 2)`
	* From (Int, Int): `DataFrame(2, 2)`
	* From (Vector{Types}, Vector{String}, Int): `DataFrame({Int64, Float64}, ["A", "B"], 2)`
	* From (Vector{Types}, Int): `DataFrame({Int64, Float64}, 2)`

# Indexing

NA

dv = datazeros(10)

dv[1]

dv[1:2]

dv[:]

dv[[1, 2 3]]

dv[[false, false, true, false, false]]

dmzeros(10)

Indexers: Int, Range, Colon, Vector{Int}, Vector{Bool}, String, Vector{String}

DataVector's and PooledDataVector's implement:

* Int
* Range
* Colon
* Vector{Int}
* Vector{Bool}

DataMatrix's implement the Cartesian product:

* Int, Int
* Int, Range
* Int, Colon
* Int, Vector{Int}
* Int, Vector{Bool}
...
* Vector{Bool}, Int
* Vector{Bool}, Range
* Vector{Bool}, Colon
* Vector{Bool}, Vector{Int}
* Vector{Bool}, Vector{Bool}

Single Int access?

DataFrame's add two new indexer types:

* String
* Vector{String}

These can only occur as (a) the only indexer or (b) in the second slot of a paired indexer

Anything that can be ref()'d can also be assign()'d

Where do we allow Expr indexing?
