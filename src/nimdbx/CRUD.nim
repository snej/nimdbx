# CRUD.nim

import Collection, Transaction, private/libmdbx, private/utils


######## DATA IN:


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
    # (This has to be a template because if it's a proc, ``d.m_i32`` is a copied local variable
    # and doesn't exist after the proc returns, meaning it returns a dangling pointer.)
    case d.kind:
    of stringData: d.m_val
    of int32Data:  MDBX_val(iov_base: unsafeAddr d.m_i32, iov_len: 4)
    of int64Data:  MDBX_val(iov_base: unsafeAddr d.m_i64, iov_len: 8)

proc mkData[A](a: A): Data {.inline.} =
    result.kind = stringData
    if a.len > 0:
        result.m_val = MDBX_val(iov_base: unsafeAddr a[0], iov_len: csize_t(a.len))

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


######## DATA OUT:


type DataOut* = object
    val*: MDBX_val


# Disallow copying `DataOut`, to discourage keeping it around. A `Data` value becomes invalid when
# the Snapshot or Transaction used to get it ends, because it points to an address inside the
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

converter asDataOut*(a: seq[byte]): DataOut =
    if a.len > 0:
        result.val = MDBX_val(iov_base: unsafeAddr a[0], iov_len: csize_t(a.len))


######## COLLECTION VALUE GETTERS


proc get*(snap: CollectionSnapshot, key: Data): DataOut =
    ## Looks up the value of a key in a Collection. Returns the value, or nil Data if not found.
    ## As with all "get" operations, the value is valid (the memory it points to will be unchanged)
    ## until the enclosing Snapshot finishes. It points into the memory-mapped database, not a copy.
    var rawKey = key.raw
    if not checkOptional mdbx_get(snap.txn, snap.collection.dbi,
                                  addr rawKey, addr result.val):
        result.clear()

proc `[]`*(snap: CollectionSnapshot, key: Data): DataOut = snap.get(key)
    ## Syntactic sugar for ``get``.


proc getGreaterOrEqual*(snap: CollectionSnapshot, key: Data): (DataOut, DataOut) =
    ## Finds the first key _greater than or equal to_ ``key``.
    ## If found, returns its value and updates ``key`` to the actual key.
    ## If not found, returns nil Data and sets ``key`` to nil.
    var rawKey = key.raw
    var value: DataOut
    if checkOptional mdbx_get_equal_or_great(snap.txn, snap.collection.dbi,
                                             addr rawKey, addr value.val):
        return (DataOut(val: rawKey), DataOut(val: value.val))


proc get*(snap: CollectionSnapshot,
          key: Data,
          fn: proc(val:openarray[char])): bool {.discardable.} =
    ## Looks up the value of a key in a Collection; if found, it passes it to the callback
    ## function as an ``openarray``, _without copying_, then returns true.
    ## If not found, the callback is not called, and the result is false.
    var rawKey = key.raw
    var mdbVal: MDBX_val
    result = checkOptional mdbx_get(snap.txn, snap.collection.dbi,
                                    addr rawKey, addr mdbVal)
    if result:
        let valPtr = cast[ptr UncheckedArray[char]](mdbVal.iov_base)
        fn(valPtr.toOpenArray(0, int(mdbVal.iov_len) - 1))


######## COLLECTION "PUT" OPERATIONS


type
    PutFlag* = enum
        Insert,         # Don't replace existing entry with same key
        Update,         # Don't add a new entry, only replace existing one
        Append,         # Optimized write where key must be the last in the collection
        AllDups,        # Remove any duplicate keys (can combine with ``Update``)
        NoDupData,      # Don't create a duplicate key/value pair
        AppendDup       # Same as Append, but for ``DuplicateKeys`` collections
    PutFlags* = set[PutFlag]
        ## Options for ``put`` operations.


# MDBX flags corresponding to PutFlag items:
const kPutFlags = [MDBX_NOOVERWRITE, MDBX_CURRENT, MDBX_APPEND,
                   MDBX_ALLDUPS, MDBX_NODUPDATA, MDBX_APPENDDUP]

proc convertFlags(flags: PutFlags): MDBX_put_flags_t =
    result = MDBX_put_flags_t(0)
    for bit in 0..5:
        if (cast[uint](flags) and uint(1 shl bit)) != 0:
            result = result or kPutFlags[bit]

proc i_put(t: CollectionTransaction, key: Data, value: Data, mdbxFlags: MDBX_put_flags_t): MDBX_error_t =
    var rawKey = key.raw
    var rawVal = value.raw
    let err = MDBX_error_t(mdbx_put(t.txn, t.collection.dbi, addr rawKey, addr rawVal, mdbxFlags))
    case err:
        of MDBX_SUCCESS, MDBX_KEYEXIST, MDBX_NOTFOUND, MDBX_EMULTIVAL:
            return err
        else: throw err


proc put*(t: CollectionTransaction, key: Data, value: Data) =
    ## Stores a value for a key in a Collection. If a value existed, it will be replaced.
    ## If ``value`` points to nil, the key/value is deleted.
    if value:
        check t.i_put(key, value, MDBX_UPSERT)
    else:
        var rawKey = key.raw
        discard checkOptional mdbx_del(t.txn, t.collection.dbi,
                                       addr rawKey, nil)

proc `[]=`*(t: CollectionTransaction, key: Data, value: Data) = t.put(key, value)
    ## Syntactic sugar for a simple ``put``.


proc insert*(t: CollectionTransaction, key: Data, val: Data): bool =
    ## Adds a new key and its value; if the key exists, does nothing and returns false.
    ## (Same as ``put`` with the ``Insert`` flag.)
    return t.i_put(key, val, MDBX_NOOVERWRITE) == MDBX_SUCCESS


proc update*(t: CollectionTransaction, key: Data, val: Data): bool =
    ## Replaces an existing value for a key in a Collection;
    ## If the key doesn't already exist, does nothing and returns false.
    ## (Same as ``put`` with the ``Update`` flag.)
    return t.i_put(key, val, MDBX_CURRENT) == MDBX_SUCCESS


proc append*(t: CollectionTransaction, key: Data, val: Data) =
    ## Adds a key and value to the end of the collection. This is faster than ``put``, and is
    ## useful when populating a Collection with already-sorted data.
    ## The key must be greater than any existing key, or ``MDBX_EKEYMISMATCH`` will be raised.
    ## (Same as ``put`` with the ``Append`` flag.)
    check t.i_put(key, val, MDBX_APPEND)


proc put*(t: CollectionTransaction, key: Data, value: Data, flags: PutFlags): bool =
    ## Stores a value for a key in a Collection, according to the flags given.
    ## If the write was prevented because of a flag (for example, if ``Insert`` given but a value
    ## already exists) the function returns ``false``.
    ## Other errors are raised as exceptions.
    return t.i_put(key, value, convertFlags(flags)) == MDBX_SUCCESS


proc put*(t: CollectionTransaction, key: Data, valueLen: int, flags: PutFlags,
          fn: proc(val:openarray[char])): bool =
    ## Stores a value for a key in a Collection. The value is filled in by a callback function.
    ## This eliminates a memory-copy inside libmdbx, and might save you some allocation.
    ## If the write was prevented because of a flag (for example, if ``Insert`` given but a value
    ## already exists) the function returns ``false`` instead of calling the callback.
    var rawKey = key.raw
    var rawVal = MDBX_val(iov_base: nil, iov_len: csize_t(valueLen))
    let err = mdbx_put(t.txn, t.collection.dbi, addr rawKey, addr rawVal,
                       convertFlags(flags) or MDBX_RESERVE)
    if err==MDBX_KEYEXIST or err==MDBX_NOTFOUND or err==MDBX_EMULTIVAL:
        return false
    check err
    # Now pass the value pointer/size to the caller to fill in:
    let valPtr = cast[ptr UncheckedArray[char]](rawVal.iov_base)
    fn(valPtr.toOpenArray(0, valueLen - 1))


proc putDuplicates*(t: CollectionTransaction, key: Data,
                    values: openarray[byte], valueCount: int,
                    flags: PutFlags) =
    ## Stores multiple values for a single key.
    ## The collection must use ``DupFixed``, i.e. have multiple fixed-size values.
    ## ``values`` must contain all the values in contiguous memory.
    assert values.len mod valueCount == 0
    # The way the values are passed with MDBX_MULTIPLE is *really* weird; see the libmdbx docs.
    var rawKey = key.raw
    var vals: array[2, MDBX_val]
    vals[0].iov_len = csizet(values.len div valueCount)
    vals[0].iov_base = unsafeAddr values[0]
    vals[1].iov_len = csizet(valueCount)
    check mdbx_put(t.txn, t.collection.dbi, addr rawKey, addr vals[0],
                       convertFlags(flags) or MDBX_MULTIPLE)
    # Note: `mdbx_put_PTR` is actually the same C function as `mdbx_put`, just declared in
    # libmdbx.nim as a proc that takes key/value as `ptr` instead of `var`, so that 2 MDBX_vals
    # can be passed.


# TODO: Add mdbx_replace


######## COLLECTION "DELETE" OPERATIONS


proc del*(t: CollectionTransaction, key: Data): bool {.discardable.} =
    ## Removes a key and its value from a Collection.
    ## Returns true if the key existed, false if it doesn't exist.
    var rawKey = key.raw
    return checkOptional mdbx_del(t.txn, t.collection.dbi, addr rawKey, nil)


proc delAll*(t: CollectionTransaction) =
    ## Removes **all** keys and values from a Collection, but not the Collection itself.
    check mdbx_drop(t.txn, t.collection.dbi, false)

proc deleteCollection*(t: CollectionTransaction) =
    ## Deletes the Collection itself, including all its keys and values.
    check mdbx_drop(t.txn, t.collection.dbi, true)
