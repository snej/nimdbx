# vals.nim

import libmdbx
import strformat


## Helper functions that make `MDBX_val` act more like a Nim sequence.
## Not intended to be public, since `MDBX_val` is not public; `Data` is the public abstraction.


#%%%%%%%% FACTORIES


proc mkVal*[A](a: openarray[A]): MDBX_val {.inline.} =
    if a.len > 0:
        result.iov_base = unsafeAddr a[0]
        result.iov_len = csize_t(a.len * sizeof a[0])

proc mkVal*[T](p: ptr T): MDBX_val {.inline.} =
    result.iov_base = p
    result.iov_len = csize_t(sizeof T)


#%%%%%%% ACCESSORS


proc exists*(val: MDBX_val): bool {.inline.} =
    val.iov_base != nil

proc len*(val: MDBX_val): int {.inline.} =
    int(val.iov_len)

proc unsafeArray*[T](val: MDBX_val): ptr UncheckedArray[T] {.inline.} =
    cast[ptr UncheckedArray[T]](val.iov_base)

proc unsafeBytes*(val: MDBX_val): ptr UncheckedArray[byte] {.inline.} =
    cast[ptr UncheckedArray[byte]](val.iov_base)

proc asValue*[T](val: MDBX_val): T =
    if val.len != sizeof(T):
        raise newException(RangeDefect, &"Data is wrong size ({val.iov_len}; expected {sizeof(T)})")
    return cast[ptr T](val.iov_base)[]

proc asValue*[T](val: var MDBX_val): var T =
    if val.len != sizeof(T):
        raise newException(RangeDefect, &"Data is wrong size ({val.len}; expected {sizeof(T)})")
    return cast[ptr T](val.iov_base)[]

proc `[]`*(val: MDBX_val, i: int): var byte =
    rangeCheck i < int(val.len)
    return val.unsafeBytes[i]

proc `[]`*(val: MDBX_val, range: Slice[int]): seq[byte] =
    rangeCheck range.a >= 0
    rangeCheck range.b < int(val.len)
    if range.len == 0:
        return @[]
    return @( toOpenArray(unsafeBytes(val), range.a, range.b) )

proc asSeq*[T](val: MDBX_val): seq[T] =
    if val.len < sizeof(T):
        return @[]
    return @( toOpenArray(unsafeArray[T](val), 0, (val.len div sizeof(T)) - 1) )

proc asString*(val: MDBX_val): string =
    result = newString(val.len)
    if val.len > 0:
        copyMem(addr result[0], val.iov_base, val.len)

template asOpenArray*(val: MDBX_val): openarray[byte] =
    toOpenArray(unsafeBytes(val), 0, val.len - 1)
