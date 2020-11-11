# Cursor.nim

import Collection, private/libmdbx, private/utils


type
    Cursor* = object of RootObj
        ## A read-only iterator over the keys/values of a Collection.
        curs {.requiresInit.}: MDBX_cursor
        owner: Snapshot
        mdbKey, mdbVal: MDBX_val
        positioned: bool


proc `=`(dst: var Cursor, src: Cursor) {.error.} = echo "(can't copy a Cursor)"

proc `=destroy`(curs: var Cursor) =
    if curs.curs != nil:
        mdbx_cursor_close(curs.curs)


proc makeCursor*(coll: Collection, snap: var Snapshot): Cursor =
    ## Creates a Cursor on a Collection in a Snapshot.
    ## The Cursor starts out at an undefined position and must be positioned before its key or value
    ## are accessed. Since the ``next`` function will move an unpositioned cursor to the first key,
    ## a typical way to iterate a collection by ascending key is:
    ## ```
    ##     var curs = coll.makeCursor(snap)
    ##     while curs.next():
    ##         doSomethingWith(curs.key, curs.value)
    ## ```
    var curs: MDBX_cursor
    check mdbx_cursor_open(snap.txn, coll.dbi, curs)
    return Cursor(curs: curs, owner: snap)

proc makeCursor*(snap: var CollectionSnapshot): Cursor =
    ## Creates a Cursor on a Collection in a Snapshot.
    return makeCursor(snap.collection, snap.snapshot)


proc close*(curs: var Cursor) =
    ## Closes a Cursor, freeing its underlying resources. The Cursor may not be used again.
    ## (This happens automatically when a Cursor is destroyed, but you may want to do it earlier.)
    if curs.curs != nil:
        mdbx_cursor_close(curs.curs)
        curs.curs = nil
        curs.owner = nil
        curs.positioned = false


######## CURSOR POSITIONING:


proc op(curs: var Cursor, op: CursorOp): bool =
    ## Lowest level cursor operation.
    assert curs.curs != nil
    result = checkOptional mdbx_cursor_get(curs.curs, curs.mdbKey, curs.mdbVal, op)
    if not result:
        curs.mdbKey = MDBX_val(base: nil, len: 0)
        curs.mdbVal = MDBX_val(base: nil, len: 0)
    curs.positioned = true

proc first*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the first key in the Collection; returns false if there is none.
    curs.op(MDBX_FIRST)

proc last*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the last key in the Collection; returns false if there is none.
    curs.op(MDBX_LAST)

proc next*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the next key; returns false if there is none.
    ## If this is the first movement of this cursor (i.e. the cursor is not yet at a defined
    ## position), it moves to the first key, if there is one.
    curs.op(if curs.positioned: MDBX_NEXT else: MDBX_FIRST)

proc prev*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the previous key; returns false if there is none.
    ## If this is the first movement of this cursor (i.e. the cursor is not yet at a defined
    ## position), it moves to the last key, if there is one.
    curs.op(if curs.positioned: MDBX_PREV else: MDBX_LAST)

proc seek*(curs: var Cursor, key: openarray[char]): bool {.discardable.} =
    ## Moves to the first key greater than or equal to the given key;
    ## returns false if there is none.
    curs.mdbKey = key
    curs.op(MDBX_SET_RANGE)

proc seekExact*(curs: var Cursor, key: openarray[char]): bool {.discardable.} =
    ## Moves to the _exact_ key given; returns false if it isn't found.
    curs.mdbKey = key
    curs.op(MDBX_SET_KEY)


######## CURSOR ATTRIBUTES


proc asInt64(val: MDBX_val): int64 =
    if val.len != 8: throw(MDBX_BAD_VALSIZE)
    return cast[ptr int64](val.base)[]
proc asInt(val: MDBX_val): int =
    if val.len == 4:
        return cast[ptr int32](val.base)[]
    elif val.len == 8 and sizeof(int) >= 8:
        return int(cast[ptr int64](val.base)[])
    else:
        throw(MDBX_BAD_VALSIZE)

proc key*(curs: var Cursor): string =
    ## Returns the current key. If there is none, returns an empty string.
    assert curs.positioned
    return curs.mdbKey

proc intKey*(curs: var Cursor): int =
    assert curs.positioned
    return asInt(curs.mdbKey)

proc int64Key*(curs: var Cursor): int64 =
    assert curs.positioned
    return asInt64(curs.mdbKey)

proc value*(curs: var Cursor): string =
    ## Returns the current value as a string. If there is none, returns an empty string.
    assert curs.positioned
    return curs.mdbVal

proc valueSeq*(curs: var Cursor): seq[uint8] =
    ## Returns the current value as a ``seq[uint8]``. If there is none, returns an empty sequence.
    assert curs.positioned
    return curs.mdbVal

proc valueLen*(curs: var Cursor): int =
    ## Returns the length of the current value, in bytes.
    assert curs.positioned
    int(curs.mdbVal.len)

proc hasValue*(curs: var Cursor): bool =
    ## Returns true if the Cursor is at a valid key & value (i.e. is not past the end.)
    assert curs.positioned
    return curs.mdbVal.base != nil

proc onFirst*(curs: Cursor): bool =
    ## Returns true if the cursor is positioned at the first key.
    curs.positioned and mdbx_cursor_on_first(curs.curs) == MDBX_RESULT_TRUE

proc onLast* (curs: Cursor): bool =
    ## Returns true if the cursor is positioned at the last key.
    curs.positioned and mdbx_cursor_on_last(curs.curs) == MDBX_RESULT_TRUE

converter toBool*(curs: var Cursor): bool = curs.hasValue
