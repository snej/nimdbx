# CRUD.nim

import Collection, private/libmdbx, private/utils

######## KEY/VALUE DATA


type Data* = object
    ## A wrapper around a libmdbx key or value, which is just a pointer and length.
    ## Data is automatically convertible to and from string and integer types, so you normally
    ## won't use it directly.
    val*: MDBX_val
    i: int64   # unfortunately-necessary buffer for storing int values

# Disallow copying `Data`, to discourage keeping it around. A `Data` value becomes invalid when
# the Snapshot or Transaction used to get it ends, because it points to an address inside the
# memory-mapped database.
proc `=`(dst: var Data, src: Data) {.error.} = echo "(can't copy a Data)"


proc clear*(d: var Data) =
    d.val = MDBX_val(base: nil, len: 0)

converter exists*(d: Data): bool = d.val.base != nil
proc `not`*(d: Data): bool         = d.val.base == nil

converter asData*(a: string): Data =
    result.val = MDBX_val(base: unsafeAddr a[0], len: csize_t(a.len))
converter asData*(a: openarray[char]): Data =
    result.val = MDBX_val(base: unsafeAddr a[0], len: csize_t(a.len))
converter asData*(a: openarray[byte]): Data =
    result.val = MDBX_val(base: unsafeAddr a[0], len: csize_t(a.len))

converter asData*(i: int32): Data =
    result.i = i
    result.val = MDBX_val(base: addr result.i, len: 4) #FIX: Endian dependent
converter asData*(i: int64): Data =
    result.i = i
    result.val = MDBX_val(base: addr result.i, len: 8)

converter asString*(d: Data): string =
    result = newString(d.val.len)
    if d.val.len > 0:
        copyMem(addr result[0], d.val.base, d.val.len)

proc `$`*(d: Data): string = d.asString()

converter asByteSeq*(d: Data): seq[byte] =
    result = newSeq[byte](d.val.len)
    if d.val.len > 0:
        copyMem(addr result[0], d.val.base, d.val.len)

converter asInt32*(d: Data): int32 =
    if d.val.len != 4: throw(MDBX_BAD_VALSIZE)
    return cast[ptr int32](d.val.base)[]
converter asInt64*(d: Data): int64 =
    if d.val.len == 4:
        return cast[ptr int32](d.val.base)[]
    elif d.val.len == 8:
        return cast[ptr int64](d.val.base)[]
    else:
        throw(MDBX_BAD_VALSIZE)


######## COLLECTION VALUE GETTERS


proc get*(snap: CollectionSnapshot, key: Data): Data =
    ## Looks up the value of a key in a Collection. Returns the value, or nil Data if not found.
    ## As with all "get" operations, the value is valid (the memory it points to will be unchanged)
    ## until the enclosing Snapshot finishes. It points into the memory-mapped database, not a copy.
    if not checkOptional mdbx_get(snap.txn, snap.collection.dbi, key.val, result.val):
        result.clear()

proc `[]`*(snap: CollectionSnapshot, key: Data): Data = snap.get(key)
    ## Syntactic sugar for ``get``.


proc getGreaterOrEqual*(snap: CollectionSnapshot, key: var Data): Data =
    ## Finds the first key _greater than or equal to_ ``key``.
    ## If found, returns its value and updates ``key`` to the actual key.
    ## If not found, returns nil Data and sets ``key`` to nil.
    var value: Data
    if checkOptional mdbx_get_equal_or_great(snap.txn, snap.collection.dbi, key.val, value.val):
        return value
    else:
        key.clear()
        value.clear()


proc get*(snap: CollectionSnapshot,
          key: Data,
          fn: proc(val:openarray[char])): bool {.discardable.} =
    ## Looks up the value of a key in a Collection; if found, it passes it to the callback
    ## function as an ``openarray``, _without copying_, then returns true.
    ## If not found, the callback is not called, and the result is false.
    var mdbVal: MDBX_val
    result = checkOptional mdbx_get(snap.txn, snap.collection.dbi, key.val, mdbVal)
    if result:
        let valPtr = cast[ptr UncheckedArray[char]](mdbVal.base)
        fn(valPtr.toOpenArray(0, int(mdbVal.len) - 1))


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

proc convertFlags(flags: PutFlags): MDBXPutFlags =
    result = MDBXPutFlags(0)
    for bit in 0..5:
        if (cast[uint](flags) and uint(1 shl bit)) != 0:
            result = result or kPutFlags[bit]

proc i_put(t: CollectionTransaction, key: Data, value: Data, mdbxFlags: MDBXPutFlags): MDBXErrorCode =
    var rawVal = value.val
    let err = mdbx_put(t.txn, t.collection.dbi, key.val, rawVal, mdbxFlags)
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
        discard checkOptional mdbx_del(t.txn, t.collection.dbi, key.val, nil)

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
    var mdbVal = MDBX_val(base: nil, len: csize_t(valueLen))
    let err = mdbx_put(t.txn, t.collection.dbi, key.val, mdbVal,
                       convertFlags(flags) or MDBX_RESERVE)
    if err==MDBX_KEYEXIST or err==MDBX_NOTFOUND or err==MDBX_EMULTIVAL:
        return false
    check err
    # Now pass the value pointer/size to the caller to fill in:
    let valPtr = cast[ptr UncheckedArray[char]](mdbVal.base)
    fn(valPtr.toOpenArray(0, valueLen - 1))


proc putDuplicates*(t: CollectionTransaction, key: Data,
                    values: openarray[byte], valueCount: int,
                    flags: PutFlags) =
    ## Stores multiple values for a single key.
    ## The collection must use ``DupFixed``, i.e. have multiple fixed-size values.
    ## ``values`` must contain all the values in contiguous memory.
    assert values.len mod valueCount == 0
    # The way the values are passed with MDBX_MULTIPLE is *really* weird; see the libmdbx docs.
    var vals: array[2, MDBX_val]
    vals[0].len = csizet(values.len div valueCount)
    vals[0].base = unsafeAddr values[0]
    vals[1].len = csizet(valueCount)
    check mdbx_put_PTR(t.txn, t.collection.dbi, unsafeAddr key.val, addr vals[0],
                       convertFlags(flags) or MDBX_MULTIPLE)
    # Note: `mdbx_put_PTR` is actually the same C function as `mdbx_put`, just declared in
    # libmdbx.nim as a proc that takes key/value as `ptr` instead of `var`, so that 2 MDBX_vals
    # can be passed.


# TODO: Add mdbx_replace


######## COLLECTION "DELETE" OPERATIONS


proc del*(t: CollectionTransaction, key: Data): bool {.discardable.} =
    ## Removes a key and its value from a Collection.
    ## Returns true if the key existed, false if it doesn't exist.
    return checkOptional mdbx_del(t.txn, t.collection.dbi, key.val, nil)


proc delAll*(t: CollectionTransaction) =
    ## Removes **all** keys and values from a Collection, but not the Collection itself.
    check mdbx_drop(t.txn, t.collection.dbi, false)

proc deleteCollection*(t: CollectionTransaction) =
    ## Deletes the Collection itself, including all its keys and values.
    check mdbx_drop(t.txn, t.collection.dbi, true)
