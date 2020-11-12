# Cursor.nim

import Collection, private/libmdbx, private/utils


type
    Cursor* = object of RootObj
        ## A read-only iterator over the keys/values of a Collection.
        curs {.requiresInit.}: MDBX_cursor
        owner: Snapshot
        mdbKey, mdbVal: Data
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
    # Lowest level cursor operation.
    assert curs.curs != nil
    result = checkOptional mdbx_cursor_get(curs.curs, curs.mdbKey.val, curs.mdbVal.val, op)
    if not result:
        curs.mdbKey.clear()
        curs.mdbVal.clear()
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
    ## Moves to the first key _greater than or equal to_ the given key;
    ## returns false if there is none.
    curs.mdbKey = key
    curs.op(MDBX_SET_RANGE)

proc seekExact*(curs: var Cursor, key: openarray[char]): bool {.discardable.} =
    ## Moves to the _exact_ key given; returns false if it isn't found.
    curs.mdbKey = key
    curs.op(MDBX_SET_KEY)


######## CURSOR ATTRIBUTES


proc key*(curs: var Cursor): lent Data =
    ## Returns the current key. If there is none, returns an empty string.
    assert curs.positioned
    return curs.mdbKey

proc value*(curs: var Cursor): lent Data =
    ## Returns the current value as a string. If there is none, returns an empty string.
    assert curs.positioned
    return curs.mdbVal

proc valueLen*(curs: var Cursor): int =
    ## Returns the length of the current value, in bytes.
    assert curs.positioned
    return int(curs.mdbVal.val.len)

proc hasValue*(curs: var Cursor): bool =
    ## Returns true if the Cursor is at a valid key & value (i.e. is not past the end.)
    assert curs.positioned
    return curs.mdbVal.exists

proc onFirst*(curs: Cursor): bool =
    ## Returns true if the cursor is positioned at the first key.
    curs.positioned and mdbx_cursor_on_first(curs.curs) == MDBX_RESULT_TRUE

proc onLast* (curs: Cursor): bool =
    ## Returns true if the cursor is positioned at the last key.
    curs.positioned and mdbx_cursor_on_last(curs.curs) == MDBX_RESULT_TRUE

proc valueCount*(curs: Cursor): int =
    ## Returns the number of values for the current key.
    ## (This is always 1 unless the Collection supports ``DuplicateKeys``)
    var count: csize_t
    check mdbx_cursor_count(curs.curs, count)
    return int(count)

converter toBool*(curs: var Cursor): bool = curs.hasValue
