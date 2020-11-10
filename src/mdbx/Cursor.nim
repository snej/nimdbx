# Cursor.nim

import Collection, private/libmdbx, private/utils


type
    Cursor* = object of RootObj
        ## A read-only iterator over the keys/values of a Collection.
        curs {.requiresInit.}: MDBX_cursor
        owner: Snapshot
        mdbKey, mdbVal: MDBX_val
        hasVal: bool


proc `=`(dst: var Cursor, src: Cursor) {.error.} = echo "(can't copy a Cursor)"

proc `=destroy`(curs: var Cursor) =
    discard mdbx_cursor_close(curs.curs)


proc makeCursor*(coll: Collection, snap: var Snapshot): Cursor =
    var curs: MDBX_cursor
    check mdbx_cursor_open(snap.txn, coll.dbi, curs)
    return Cursor(curs: curs, owner: snap)

proc makeCursor*(snap: var CollectionSnapshot): Cursor =
    var curs: MDBX_cursor
    check mdbx_cursor_open(snap.txn, snap.collection.dbi, curs)
    return Cursor(curs: curs, owner: snap.snapshot)


######## CURSOR ATTRIBUTES


proc key*(curs: var Cursor): string =
    ## Returns the current key. If there is none, returns an empty string.
    curs.mdbKey

proc value*(curs: var Cursor): seq[uint8] =
    ## Returns the current value. If there is none, returns an empty sequence.
    curs.mdbVal

proc hasValue*(curs: var Cursor): bool =
    ## Returns true if the Cursor is at a valid key & value (i.e. is not past the end.)
    curs.hasVal


######## CURSOR POSITIONING:


proc op(curs: var Cursor, op: CursorOp): bool =
    ## Lowest level cursor operation.
    curs.hasVal = checkOptional mdbx_cursor_get(curs.curs, curs.mdbKey, curs.mdbVal, op)
    return curs.hasVal


proc first*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the first key in the Collection.
    curs.op(MDBX_FIRST) and curs.op(MDBX_GET_CURRENT)

proc last*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the last key in the Collection.
    curs.op(MDBX_LAST) and curs.op(MDBX_GET_CURRENT)

proc next*(curs: var Cursor): bool =
    ## Moves to the next key; returns false if there is none.
    curs.op(MDBX_NEXT)

proc prev*(curs: var Cursor): bool =
    ## Moves to the previous key; returns false if there is none.
    curs.op(MDBX_PREV)

proc seek*(curs: var Cursor, key: openarray[char]): bool =
    ## Moves to the first key greater than or equal to the given key;
    ## returns false if there is no such key.
    curs.op(MDBX_SET_RANGE)

proc seekExact*(curs: var Cursor, key: openarray[char]): bool =
    ## Moves to the _exact_ key given; returns false if it isn't found.
    curs.op(MDBX_SET_KEY)
