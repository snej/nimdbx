# Data.nim

import Error, private/libmdbx


#%%%%%%% DATA IN:


type DataKind = enum
    stringData,
    int32Data,
    int64Data

type Data* = object
    ## A wrapper around a libmdbx key or value, which is just a pointer and length.
    ## Data is automatically convertible to and from string and integer types, so you normally
    ## won't use it directly.
    case kind: DataKind:
    of stringData:
        m_val: MDBX_val
    of int32Data:
        m_i32: int32
    of int64Data:
        m_i64: int64

# Disallow copying `Data`, to discourage keeping it around, since it often contains a pointer to
# ephemeral local data provided by the caller.
proc `=`(dst: var Data, src: Data) {.error.}

proc clear*(d: var Data) =
    d.kind = stringData
    d.m_val = MDBX_val(iov_base: nil, iov_len: 0)

converter exists*(d: Data): bool =
    case d.kind:
    of stringData: return d.m_val.iov_base != nil
    else:          return true

proc `not`*(d: Data): bool = not d.exists

template raw*(d: Data): MDBX_val =
    ## Returns an ``MDBX_val`` that points to the value in a ``Data``.
    # (This has to be a template. If it's a proc, ``d.m_i32`` would be a copied local variable
    # and wouldn't exist after the proc returns, so its address would be a dangling pointer.)
    case d.kind:
    of stringData: d.m_val
    of int32Data:  MDBX_val(iov_base: unsafeAddr d.m_i32, iov_len: 4)
    of int64Data:  MDBX_val(iov_base: unsafeAddr d.m_i64, iov_len: 8)

proc mkData[A](a: A): Data {.inline.} =
    # Creates a Data that points at the contents of an array/seq/string.
    result.kind = stringData
    if a.len > 0:
        result.m_val = MDBX_val(iov_base: unsafeAddr a[0], iov_len: csize_t(a.len * sizeof(a[0])))

converter asData*(a: string): Data = mkData(a)
converter asData*(a: seq[char]): Data = mkData(a)
converter asData*(a: openarray[char]): Data = mkData(a)
converter asData*(a: seq[byte]): Data = mkData(a)
converter asData*(a: openarray[byte]): Data = mkData(a)

converter asData*(i: int32): Data =
    return Data(kind: int32Data, m_i32: i)
converter asData*(i: int64): Data =
    return Data(kind: int64Data, m_i64: i)

type NoData_t* = distinct int
const NoData* = NoData_t(0)
    # A special constant that denotes a nil Data value.

converter asData*(n: NoData_t): Data =
    return

converter asData*(mdbx: MDBX_val): Data =
    return Data(kind: stringData, m_val: mdbx)

proc asSeq[T](base: pointer, len: int): seq[T] =
    result = newSeq[T](len div sizeof(T))
    if len > 0:
        copyMem(addr result[0], base, len)

proc asSeq*[T](val: MDBX_val): seq[T] = asSeq[T](val.iov_base, int(val.iov_len))

converter asByteSeq*(d: Data): seq[byte] =
    case d.kind:
    of stringData: return asSeq[byte](d.m_val)
    of int32Data: return asSeq[byte](unsafeAddr d.m_i32, sizeof(d.m_i32))
    of int64Data: return asSeq[byte](unsafeAddr d.m_i64, sizeof(d.m_i64))


#%%%%%%% DATA OUT:


type DataOut* = object
    ## A wrapper around a *returned* libmdbx key or value, which is just a pointer and length.
    ## DataOut is automatically convertible to and from string and integer types, so you normally
    ## won't use it directly.
    ##
    ## IMPORTANT: A DataOut value is valid only until the end of the Snapshot or Transaction
    ## within which is was created. After that, the data it points to may be overwritten.
    val*: MDBX_val


# Disallow copying `DataOut`, to discourage keeping it around. A `DataOut`'s pointer becomes invalid
# when the Snapshot or Transaction used to get it ends, because it points to an address inside the
# memory-mapped database.
proc `=`(dst: var DataOut, src: DataOut) {.error.}

converter exists*(d: DataOut): bool = d.val.iov_base != nil
proc `not`*(d: DataOut): bool       = not d.exists

proc clear*(d: var DataOut) =
    d.val = MDBX_val(iov_base: nil, iov_len: 0)

converter asString*(d: DataOut): string =
    result = newString(d.val.iov_len)
    if d.val.iov_len > 0:
        copyMem(addr result[0], d.val.iov_base, d.val.iov_len)

proc `$`*(d: DataOut): string = d.asString()

converter asCharSeq*(d: DataOut): seq[char] = asSeq[char](d.val)
converter asByteSeq*(d: DataOut): seq[byte] = asSeq[byte](d.val)

converter asInt32*(d: DataOut): int32 =
    if d.val.iov_len != 4: throw(MDBX_BAD_VALSIZE)
    return cast[ptr int32](d.val.iov_base)[]

converter asInt64*(d: DataOut): int64 =
    if d.val.iov_len == 4:
        return cast[ptr int32](d.val.iov_base)[]
    elif d.val.iov_len == 8:
        return cast[ptr int64](d.val.iov_base)[]
    else:
        throw(MDBX_BAD_VALSIZE)

converter asInt*(d: DataOut): int =
    when sizeof(int) >= 8:
        int(asInt64(d))
    else:
        int(asInt32(d))

converter asDataOut*(a: seq[byte]): DataOut =
    if a.len > 0:
        result.val = MDBX_val(iov_base: unsafeAddr a[0], iov_len: csize_t(a.len))
