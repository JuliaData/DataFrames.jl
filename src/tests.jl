bytestofloat("1.2345abc".data, 3, 3)
 => (2345.0,false,false)

bytestofloat("1.2345abc".data, 3, 6)
 => (2345.0,true,false)

bytestofloat("1.2345abc".data, 3, 7)
 => (2345.0,false,false)
