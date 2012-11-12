require("enum.jl")
require("bitarray.jl")

using Base
import Base.start
import Base.next
import Base.done
import Base.isempty
import Base.map
import Base.string

require("options.jl")   ## to load the extras version
## require("Options.jl")   ## to load the package version
using OptionsMod

load("src/index.jl")
load("src/datavec.jl")
load("src/namedarray.jl")
load("src/dataframe.jl")
load("src/formula.jl")
load("src/utils.jl")
