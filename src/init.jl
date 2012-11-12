require("enum.jl")
require("bitarray.jl")

import Base.*
import Base.start
import Base.next
import Base.done
import Base.map
import Base.string

require("options.jl")
import OptionsMod.*

load("src/index.jl")
load("src/datavec.jl")
load("src/namedarray.jl")
load("src/dataframe.jl")
load("src/formula.jl")
load("src/utils.jl")
