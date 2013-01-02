#
# IndexedVector tests
#

srand(1)
a = randi(5,20)
ia = IndexedVector(a)
ia2 = IndexedVector(randi(4,20))

ia .== 4
v = [1:20]
@assert v[ia .== 4] == v[a .== 4]
@assert sort(v[(ia .== 4) | (ia .== 5)]) == v[(a .== 4) | (a .== 5)]
@assert sort(v[(ia .>= 4) & (ia .== 5)]) == v[(a .>= 4) & (a .== 5)]
@assert sort(v[!(ia .== 4)]) == v[!(a .== 4)]

df = DataFrame(quote
         x1 = IndexedVector(vcat(fill([1:5],4)...))
         x2 = IndexedVector(vcat(fill(letters[1:10],2)...))
     end)

df[:(x2 .== "a"), :] 
df[:( (x2 .== "a") | (x1 .== 2) ), :] 
df[:( ("b" .<= x2 .<= "c") | (x1 .== 5) ), :]
df[:( (x1 .== 1) & (x2 .== "a") ), :]

df[:( in(x2, ["c","e"]) ), :]
 
