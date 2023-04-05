# CRUD.nim

{.experimental: "notnil".}
{.experimental: "strictFuncs".}

import Collection, Data, Error, Transaction, private/[libmdbx, vals]


#%%%%%%% GETTERS


proc get*(snap: CollectionSnapshot, key: Data): DataOut =
    ## Looks up the value of a key in a Collection. Returns the value, or nil Data if not found.
    ## As with all "get" operations, the value is valid (the memory it points to will be unchanged)
    ## until the enclosing Snapshot finishes. It points into the memory-mapped database, not a copy.
    var rawKey = key.raw
    if not checkOptional mdbx_get(snap.i_txn, snap.collection.i_dbi,
                                  addr rawKey, addr result.val):
        result.clear()

proc `[]`*(snap: CollectionSnapshot, key: Data): DataOut = snap.get(key)
    ## Syntactic sugar for ``get``.


proc getGreaterOrEqual*(snap: CollectionSnapshot, key: Data): (DataOut, DataOut) =
    ## Finds the first key *greater than or equal to* ``key``.
    ## If found, returns the actual key and its value as a tuple.
    ## If not found, returns an empty/nil key and value.
    var rawKey = key.raw
    var value: DataOut
    if checkOptional mdbx_get_equal_or_great(snap.i_txn, snap.collection.i_dbi,
                                             addr rawKey, addr value.val):
        return (DataOut(val: rawKey), DataOut(val: value.val))


proc get*(snap: CollectionSnapshot,
          key: Data,
          fn: proc(val:openarray[char]) not nil): bool {.discardable.} =
    ## Looks up the value of a key in a Collection; if found, it passes it to the callback
    ## function as an ``openarray``, *without copying*, then returns true.
    ## If not found, the callback is not called, and the result is false.
    var rawKey = key.raw
    var mdbVal: MDBX_val
    result = checkOptional mdbx_get(snap.i_txn, snap.collection.i_dbi,
                                    addr rawKey, addr mdbVal)
    if result:
        let valPtr = cast[ptr UncheckedArray[char]](mdbVal.iov_base)
        fn(valPtr.toOpenArray(0, int(mdbVal.iov_len) - 1))


#%%%%%%% SETTERS


type
    PutFlag* = enum
        Insert,         ## Don't replace existing entry[ies] with same key
        Update,         ## Don't add a new entry, only replace existing one
        Append,         ## Optimized write where key must be the last in the collection
        AllDups,        ## Remove any duplicate keys (can combine with ``Update``)
        NoDupData,      ## Don't create a duplicate key/value pair
        AppendDup       ## Same as Append, but for ``DuplicateKeys`` collections
    PutFlags* = set[PutFlag]
        ## Options for ``put`` operations.


# MDBX flags corresponding to PutFlag items:
const kPutFlags = [MDBX_NOOVERWRITE, MDBX_CURRENT, MDBX_APPEND,
                   MDBX_ALLDUPS, MDBX_NODUPDATA, MDBX_APPENDDUP]

func convertFlags(flags: PutFlags): MDBX_put_flags_t =
    result = MDBX_put_flags_t(0)
    for bit in 0..5:
        if (cast[uint](flags) and uint(1 shl bit)) != 0:
            result = result or kPutFlags[bit]

func convertFlags(flag: PutFlag): MDBX_put_flags_t =
    return kPutFlags[int(flag)]


proc callChangeHook(t: CollectionTransaction;
                    key, oldVal, newVal: MDBX_val,
                    flags: MDBX_put_flags_t) {.inline.} =
    let hook = t.collection.i_changeHook
    if hook != nil:
        hook(t.i_txn, key, oldVal, newVal, flags)


proc i_replace(t: CollectionTransaction,
               rawKey: ptr MDBX_val not nil,
               rawVal: ptr MDBX_val,
               mdbxFlags: MDBX_put_flags_t,
               outOldValue: ptr string = nil): MDBX_error_t =
    var rawOldVal: MDBX_val
    var freeOldVal = false

    proc preserveFunc(context: pointer, target: ptr MDBX_val,
                      src: pointer, len: csize_t): cint {.cdecl.} =
        target.iov_base = alloc(len)
        if target.iov_base == nil:
            return cint(MDBX_ENOMEM)
        target.iov_len = len
        copymem(target.iov_base, src, len)
        cast[ptr bool](context)[] = true
        return cint(MDBX_SUCCESS)

    result = MDBX_error_t(mdbx_replace_ex(t.i_txn, t.collection.i_dbi, rawKey, rawVal,
                                          addr rawOldVal, mdbxFlags, preserveFunc, addr freeOldVal))
    if result == MDBX_SUCCESS:
        var newVal: MDBX_val
        if rawVal != nil:
            newVal = rawVal[]
        if t.collection.i_changeHook != nil:
            t.collection.i_changeHook(t.i_txn, rawKey[], rawOldVal, newVal, mdbxFlags)
        if outOldValue != nil:
            outOldValue[] = rawOldVal.asString

    if freeOldVal:
        dealloc(rawOldVal.iov_base)


proc i_put(t: CollectionTransaction, key: Data, value: Data, mdbxFlags: MDBX_put_flags_t): MDBX_error_t =
    var rawKey = key.raw
    var rawVal = value.raw

    let hook = t.collection.i_changeHook
    if hook == nil or (mdbxFlags and MDBX_NOOVERWRITE) != 0:
        result = MDBX_error_t(mdbx_put(t.i_txn, t.collection.i_dbi, addr rawKey, addr rawVal, mdbxFlags))
        if result == MDBX_SUCCESS and hook != nil:
            # (We know there is no oldVal because MDBX_NOOVERWRITE was used)
            hook(t.i_txn, rawKey, MDBX_val(), rawVal, mdbxFlags)
    else:
        result = i_replace(t, addr rawKey, addr rawVal, mdbxFlags)

    case result:
        of MDBX_SUCCESS, MDBX_KEYEXIST, MDBX_NOTFOUND, MDBX_EMULTIVAL:
            return
        else:
            throw result


proc del*(t: CollectionTransaction, key: Data): bool {.discardable.}


proc put*(t: CollectionTransaction, key: Data, value: Data) =
    ## Stores a value for a key in a Collection. If a value existed, it will be replaced.
    ## If ``value`` points to nil, the key/value is deleted.
    if value:
        check t.i_put(key, value, MDBX_UPSERT)
    else:
        t.del(key)

proc `[]=`*(t: CollectionTransaction, key: Data, value: Data) =
    ## Syntactic sugar for a simple ``put``.
    t.put(key, value)


proc insert*(t: CollectionTransaction, key: Data, val: Data): bool =
    ## Adds a new key and its value; if it already exists, does nothing and returns false.
    ## (Same as ``put`` with the ``Insert`` flag.)
    let flag = if t.collection.duplicateKeys: MDBX_NODUPDATA else: MDBX_NOOVERWRITE
    return t.i_put(key, val, flag) == MDBX_SUCCESS


proc update*(t: CollectionTransaction, key: Data, val: Data): bool =
    ## Replaces an existing value for a key in a Collection;
    ## If the key doesn't already exist, does nothing and returns false.
    ## (Same as ``put`` with the ``Update`` flag.)
    return t.i_put(key, val, MDBX_CURRENT) == MDBX_SUCCESS


proc updateAndGet*(t: CollectionTransaction, key: Data, val: Data): string =
    ## Replaces an existing value for a key in a Collection, like `update`,
    ## and returns the old value.
    ## If the key doesn't already exist, does nothing and returns an empty string.
    var rawKey = key.raw
    var rawVal = val.raw

    discard checkOptional t.i_replace(addr rawKey, addr rawVal, MDBX_CURRENT, addr result)


proc append*(t: CollectionTransaction, key: Data, val: Data) =
    ## Adds a key and value to the end of the collection. This is faster than ``put``, and is
    ## useful when populating a Collection with already-sorted data.
    ## The key must be greater than any existing key, or ``MDBX_EKEYMISMATCH`` will be raised.
    ## (Same as ``put`` with the ``Append`` flag.)
    check t.i_put(key, val, MDBX_APPEND)


proc put*(t: CollectionTransaction, key: Data, value: Data, flags: PutFlags | PutFlag): bool =
    ## Stores a value for a key in a Collection, according to the flags given.
    ## If the write was prevented because of a flag (for example, if ``Insert`` given but a value
    ## already exists) the function returns ``false``.
    ## Other errors are raised as exceptions.
    return t.i_put(key, value, convertFlags(flags)) == MDBX_SUCCESS


proc put*(t: CollectionTransaction, key: Data, valueLen: int, flags: PutFlags | PutFlag,
          fn: proc(val:openarray[char])): bool =
    ## Stores a value for a key in a Collection. The value is filled in by a callback function.
    ## This eliminates a memory-copy inside libmdbx, and might save you some allocation.
    ## If the write was prevented because of a flag (for example, if ``Insert`` given but a value
    ## already exists) the function returns ``false`` instead of calling the callback.

    # With MDBX_RESERVE, we don't give a pointer to the data. Instead, `mdbx_put` reserves space,
    # then sets the value pointer to the address where the value should be written.
    var rawKey = key.raw
    var rawVal = MDBX_val(iov_base: nil, iov_len: csize_t(valueLen))
    let mdbxFlags = convertFlags(flags) or MDBX_RESERVE
    let err = mdbx_put(t.i_txn, t.collection.i_dbi, addr rawKey, addr rawVal, mdbxFlags)
    if err==MDBX_KEYEXIST or err==MDBX_NOTFOUND or err==MDBX_EMULTIVAL:
        return false
    check err
    # Now pass the value pointer/size to the caller to fill in:
    fn( rawVal.asOpenArray )
    callChangeHook(t, rawKey, rawVal, mdbxFlags)



proc putDuplicates*(t: CollectionTransaction, key: Data,
                    values: openarray[byte], valueCount: int,
                    flags: PutFlags | PutFlag) =
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
    let mdbxFlags = convertFlags(flags) or MDBX_MULTIPLE
    check mdbx_put(t.i_txn, t.collection.i_dbi, addr rawKey, addr vals[0], mdbxFlags)
    # Note: Does not call changeHook!


#%%%%%%% COLLECTION "DELETE" OPERATIONS


proc del*(t: CollectionTransaction, key: Data): bool =
    ## Removes a key and its value from a Collection.
    ## Returns true if the key existed, false if it doesn't exist.
    var rawKey = key.raw
    if t.collection.i_changeHook == nil:
        result = checkOptional mdbx_del(t.i_txn, t.collection.i_dbi, addr rawKey, nil)
    else:
        result = checkOptional i_replace(t, addr rawKey, nil, MDBX_CURRENT)

proc del*(t: CollectionTransaction, key: Data, val: Data): bool =
    ## Removes matching key/value pair from a collection.
    ## Returns true if the key and value existed, false if it doesn't exist.
    var rawKey = key.raw
    var rawVal = val.raw
    result = checkOptional mdbx_del(t.i_txn, t.collection.i_dbi, addr rawKey, addr rawVal)
    if result:
        callChangeHook(t, rawKey, rawVal, MDBX_val(), MDBX_CURRENT)

proc delAndGet*(t: CollectionTransaction, key: Data): string =
    ## Removes a key and its value from a Collection.
    ## Returns the old value, or an empty string if there was none to delete.
    var rawKey = key.raw
    discard checkOptional i_replace(t, addr rawKey, nil, MDBX_CURRENT, addr result)
