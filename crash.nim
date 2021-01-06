
proc inspect(a: sink openarray[byte]) =
  echo "a.len = ", a.len
  echo "a[0] at ", cast[uint](unsafeAddr a[0])
  echo "a[0] is ", a[0]
  echo "a[1] is ", a[1]
  var s: seq[byte]
  s.add(a)
  
inspect(toOpenArrayByte("hi", 0, 1))
