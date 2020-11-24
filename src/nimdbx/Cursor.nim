# Cursor.nim

import Collection, CRUD, Transaction, private/libmdbx, private/utils


type
    Cursor* = object of RootObj
        ## A read-only iterator over the keys/values of a Collection.
        ##
        ## NOTE: Writeable Cursors are not implemented yet.
        curs {.requiresInit.}: ptr MDBX_cursor
        owner: Snapshot
        mdbKey, mdbVal: DataOut
        minKey, maxKey: seq[byte]
        minKeyCmp, maxKeyCmp: int
        positioned: bool


proc `=`(dst: var Cursor, src: Cursor) {.error.}

proc `=destroy`(curs: var Cursor) =
    if curs.curs != nil:
        mdbx_cursor_close(curs.curs)


proc makeCursor*(coll: Collection, snap: Snapshot): Cursor =
    ## Creates a Cursor on a Collection in a Snapshot.
    ## The Cursor starts out at an undefined position and must be positioned before its key or value
    ## are accessed. Since the ``next`` function will move an unpositioned cursor to the first key,
    ## a typical way to iterate a collection by ascending key is:
    ## ```
    ##     var curs = coll.makeCursor(snap)
    ##     while curs.next():
    ##         doSomethingWith(curs.key, curs.value)
    ## ```
    var curs: ptr MDBX_cursor
    check mdbx_cursor_open(snap.txn, coll.dbi, addr curs)
    return Cursor(curs: curs, owner: snap)

proc makeCursor*(snap: CollectionSnapshot): Cursor =
    ## Creates a Cursor on a Collection in a Snapshot.
    return makeCursor(snap.collection, snap.snapshot)


const NoKey* = NoData
    ## Use this to denote a missing end of a key range, e.g. ``["a"..NoKey]`` or ``[NoKey.."z"]``.

proc `[]`*[T,U](snap: CollectionSnapshot, range: HSlice[T,U]): Cursor =
    ## Convenience to create a cursor by subscripting a CollectionSnapshot with a key range:
    ## ```
    ## for (key,value) in cs["a" .. "z"]:
    ## ```
    ## Use ``NoKey`` to denote no limit, so for 'everthing starting from "a"' use
    ## ``["a" .. NoKey]``.
    result = makeCursor(snap)
    result.minKey = Data(range.a)
    result.maxKey = Data(range.b)


proc close*(curs: var Cursor) =
    ## Closes a Cursor, freeing its underlying resources. The Cursor may not be used again.
    ## (This happens automatically when a Cursor is destroyed, but you may want to do it earlier.)
    if curs.curs != nil:
        mdbx_cursor_close(curs.curs)
        curs.curs = nil
        curs.owner = nil
        curs.positioned = false


proc minKey*(curs: Cursor): DataOut =  curs.minKey
    ## The minimum key to iterate over, if any.

proc `minKey=`*(curs: var Cursor, key: Data) =  curs.minKey = key
    ## Sets minimum key to iterate over, if any.

proc maxKey*(curs: Cursor): DataOut =  curs.maxKey
    ## The maximum key to iterate over, if any.

proc `maxKey=`*(curs: var Cursor, key: Data) =  curs.maxKey = key
    ## Sets the maximum key to iterate over, if any.

proc `skipMinKey=`*(curs: var Cursor, skip: bool) =  curs.minKeyCmp = (if skip:  1 else: 0)
proc `skipMaxKey=`*(curs: var Cursor, skip: bool) =  curs.maxKeyCmp = (if skip: -1 else: 0)

proc compareKey*(curs: Cursor, withKey: Data): int =
    ## Compares the Cursor's current key with the given value, according to the Collection's
    ## sort order.
    ## Returns 1 if the cursor's key is greater, 0 if equal, -1 if ``withKey`` is greater.
    assert curs.positioned
    var rawKey = withKey.raw
    return mdbx_cmp(curs.owner.txn, mdbx_cursor_dbi(curs.curs),
                    unsafeAddr curs.mdbKey.val, addr rawKey)

#%%%%%%% CURSOR POSITIONING:


proc clr(curs: var Cursor): bool    = curs.mdbKey.clear(); curs.mdbVal.clear(); return false
proc pastMinKey(curs: Cursor): bool = curs.compareKey(curs.minKey) < curs.minKeyCmp
proc pastMaxKey(curs: Cursor): bool = curs.compareKey(curs.maxKey) > curs.maxKeyCmp

proc op(curs: var Cursor, op: MDBX_cursor_op): bool =
    # Lowest level cursor operation.
    assert curs.curs != nil
    result = checkOptional mdbx_cursor_get(curs.curs, addr curs.mdbKey.val, addr curs.mdbVal.val, op)
    if not result:
        result = curs.clr()
    curs.positioned = true


proc seek*(curs: var Cursor, key: Data): bool {.discardable.} =
    ## Moves to the first key _greater than or equal to_ the given key;
    ## returns false if there is none.
    curs.mdbKey.val = key.raw
    curs.op(MDBX_SET_RANGE)


proc seekExact*(curs: var Cursor, key: Data): bool {.discardable.} =
    ## Moves to the _exact_ key given; returns false if it isn't found.
    curs.mdbKey.val = key.raw
    curs.op(MDBX_SET_KEY)


proc first*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the first key in range; returns false if there is none.
    if curs.minKey.len == 0:
        return curs.op(MDBX_FIRST)
    result = curs.seek(curs.minKey)
    if result and curs.minKeyCmp != 0 and curs.pastMinKey():
        # Skip first key
        result = curs.op(MDBX_NEXT)
    if result and curs.maxKey.len > 0 and curs.pastMaxKey():
        # First key is after maxKey, so don't include it
        result = curs.clr()


proc last*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the last key in range; returns false if there is none.
    if curs.maxKey.len == 0:
        return curs.op(MDBX_LAST)
    result = curs.seek(curs.maxKey) or curs.op(MDBX_LAST)
    if result and curs.pastMaxKey():
        # The seek overshot, so I need to go back one (or else I'm skipping the exact max key)
        result = curs.op(MDBX_PREV)
    if result and curs.minKey.len > 0 and curs.pastMinKey():
        # Last key is before minKey, so don't include it
        result = curs.clr()


proc next(curs: var Cursor, op: MDBX_cursor_op): bool {.discardable.} =
    if not curs.positioned:
        return curs.first()
    result = curs.op(op)
    if result and curs.maxKey.len > 0 and curs.pastMaxKey():
        result = curs.clr()

proc prev(curs: var Cursor, op: MDBX_cursor_op): bool {.discardable.} =
    if not curs.positioned:
        return curs.last()
    result = curs.op(MDBX_PREV)
    if result and curs.minKey.len > 0 and curs.pastMinKey():
        result = curs.clr()


proc next*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the next value; returns false if there is none.
    ## If this is the first movement of this cursor (i.e. the cursor is not yet at a defined
    ## position), it moves to the first key, if there is one.
    curs.next(MDBX_NEXT)

proc prev*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the previous value; returns false if there is none.
    ## If this is the first movement of this cursor (i.e. the cursor is not yet at a defined
    ## position), it moves to the last value of the last key, if there is one.
    curs.prev(MDBX_PREV)


proc nextKey*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the next key's first value; returns false if there is none.
    ## (This is the same as ``next`` in Collections without ``DuplicateKeys``.)
    curs.next(MDBX_NEXT_NODUP)

proc prevKey*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the previous key's last value; returns false if there is none.
    ## (This is the same as ``prev`` in Collections without ``DuplicateKeys``.)
    curs.prev(MDBX_PREV_NODUP)


proc nextDup*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the next value of the same key; returns false if there is none.
    ## (This only makes sense in Collections with ``DuplicateKeys``.)
    assert curs.positioned
    result = curs.op(MDBX_NEXT_DUP)

proc prevDup*(curs: var Cursor): bool {.discardable.} =
    ## Moves to the previous value of the same key; returns false if there is none.
    ## (This only makes sense in Collections with ``DuplicateKeys``.)
    assert curs.positioned
    result = curs.op(MDBX_PREV_DUP)


#%%%%%%% CURSOR ATTRIBUTES


proc key*(curs: var Cursor): lent DataOut =
    ## Returns the current key, if any.
    assert curs.positioned
    return curs.mdbKey

proc value*(curs: var Cursor): lent DataOut =
    ## Returns the current value, if any.
    assert curs.positioned
    return curs.mdbVal

proc valueLen*(curs: var Cursor): int =
    ## Returns the length of the current value, in bytes.
    assert curs.positioned
    return int(curs.mdbVal.val.iov_len)

proc hasValue*(curs: var Cursor): bool =
    ## Returns true if the Cursor is at a valid key & value (i.e. is not past the end.)
    assert curs.positioned
    return curs.mdbVal.exists

proc onFirst*(curs: Cursor): bool =
    ## Returns true if the cursor is positioned at the first key of the collection.
    curs.positioned and mdbx_cursor_on_first(curs.curs) == MDBX_RESULT_TRUE

proc onLast* (curs: Cursor): bool =
    ## Returns true if the cursor is positioned at the last key of the collection.
    curs.positioned and mdbx_cursor_on_last(curs.curs) == MDBX_RESULT_TRUE

proc valueCount*(curs: Cursor): int =
    ## Returns the number of values for the current key.
    ## (This is always 1 unless the Collection supports ``DuplicateKeys``)
    var count: csize_t
    check mdbx_cursor_count(curs.curs, addr count)
    return int(count)

converter toBool*(curs: var Cursor): bool = curs.hasValue
    ## A Cursor can be tested as a bool, to check if it has a value.


#%%%%%%% ITERATORS


iterator pairs*(curs: var Cursor): (DataOut, DataOut) {.inline.} =
    ## This iterator lets you write a ``for`` loop over a Cursor:
    ## ``for key, value in cursor: ...``
    defer: curs.close()
    while curs.next():
        # Construct new `Data`s as a workaround since `key` and `val` cannot be copied
        yield (DataOut(val: curs.key.val), DataOut(val: curs.value.val))


iterator reversed*(curs: var Cursor): (DataOut, DataOut) {.inline.} =
    ## This iterator lets you write a reverse-order ``for`` loop over a Cursor:
    ## ``for key, value in cursor.reversed: ...``
    defer: curs.close()
    while curs.prev():
        # Construct new `DataOut`s as a workaround since `key` and `val` cannot be copied
        yield (DataOut(val: curs.key.val), DataOut(val: curs.value.val))


iterator pairs*(snap: CollectionSnapshot): (DataOut, DataOut) {.inline.} =
    ## This iterator lets you write a ``for`` loop over a CollectionSnapshot:
    ## ``for key, value in coll.with(snap): ...``
    var curs = makeCursor(snap)
    defer: curs.close()
    while curs.next():
        # Construct new `DataOut`s as a workaround since `key` and `val` cannot be copied
        yield (DataOut(val: curs.key.val), DataOut(val: curs.value.val))


iterator reversed*(snap: CollectionSnapshot): (DataOut, DataOut) {.inline.} =
    ## This iterator lets you write a reverse-order ``for`` loop over a CollectionSnapshot:
    ## ``for key, value in coll.with(snap).reversed: ...``
    var curs = makeCursor(snap)
    defer: curs.close()
    while curs.prev():
        # Construct new `DataOut`s as a workaround since `key` and `val` cannot be copied
        yield (DataOut(val: curs.key.val), DataOut(val: curs.value.val))
