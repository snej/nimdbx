# Collection.nim

import Database, private/libmdbx, private/utils

let nil_DBI = MDBX_dbi(0xFFFFFFFF)

type
    CollectionObj = object
        m_dbi {.requiresInit.}  : MDBX_dbi
        db* {.requiresInit.}    : Database
        name* {.requiresInit.}  : string
        initialized*            : bool

    Collection* = ref CollectionObj

    CollectionFlag* = enum
        ## Flags that describe properties of a Collection when opening or creating it.
        Create,             # Create Collection if it doesn't exist
        ReverseKeys,        # Compare key strings back-to-front
        DuplicateKeys,      # Allow duplicate keys
        IntegerKeys,        # Keys are interpreted as native ints; must be 4 or 8 bytes long
        DupFixed,           # With DuplicateKeys, all values of a key must have the same size
        IntegerDup,         # With DuplicateKeys, values are native ints, 4 or 8 bytes long
        ReverseDup          # With DuplicateKeys, values are compared back-to-front
    CollectionFlags* = set[CollectionFlag]
        ## Flags that describe properties of a Collection when opening or creating it.


proc openDBI(db: Database, name: string, flags: DBIFlags): (MDBX_dbi, bool) =
    #db.mutex.lock() # FIX
    var envFlags: EnvFlags
    check mdbx_env_get_flags(db.env, envFlags);
    let readOnly = cuint(envFlags and MDBX_RDONLY) != 0
    var txnFlags: TxnFlags
    if readOnly:
        txnFlags = MDBX_TXN_RDONLY

    var txn: MDBX_txn
    check mdbx_txn_begin(db.env, nil, txnFlags, txn)

    var dbi: MDBX_dbi
    let err = mdbx_dbi_open(txn, name, flags, dbi)

    if err != MDBX_SUCCESS or readOnly:
        discard mdbx_txn_abort(txn);
        discard checkOptional(err)
        return (nil_DBI, false)
    else:
        var newFlags: DBIFlags
        var state: DBIStateFlags
        check(mdbx_dbi_flags_ex(txn, dbi, newFlags, state));
        let isNew = (cuint(state and MDBX_TBL_CREAT) != 0)
        check mdbx_txn_commit(txn)
        return (dbi, isNew);


proc openRequiredDBI(db: Database, name: string, flags: DBIFlags): (MDBX_dbi, bool) =
    result = openDBI(db, name, flags)
    if result[0] == nil_DBI:
        check MDBX_NOTFOUND


proc openCollection*(db: Database, name: string, flags: CollectionFlags): Collection =
    ## Creates a Collection object giving access to a named collection in a Database.
    ## If the collection does not exist, and the ``Create`` flag is not set, raises MDBX_NOTFOUND.

    # WARNING: This assumes set[CollectionFlags] matches DBIFlags, except for MDBX_CREATE.
    #          If you reorder CollectionFlags, this will break!
    var dbiflags = DBIFlags(cast[uint](flags - {Create}))
    if Create in flags:
        dbiflags = dbiflags or MDBX_CREATE
    let (dbi, isNew) = db.openRequiredDBI(name, dbiflags)
    return Collection(name: name, db: db, m_dbi: dbi, initialized: not isNew)

proc createCollection*(db: Database, name: string, flags: CollectionFlags = {}): Collection =
    ## A convenience that calls ``openCollection`` with the ``Create`` flag set.
    openCollection(db, name, flags + {Create})


proc dbi*(coll: Collection): MDBX_dbi =
    coll.db.mustBeOpen()
    return coll.m_dbi


######## SNAPSHOTS & TRANSACTIONS


type
    SnapshotObj = object of RootObj
        m_txn {.requiresInit.}: MDBX_txn
        m_db  {.requiresInit.}: Database

    Snapshot* = ref SnapshotObj
        ## A read-only view of the database. The contents of this view are fixed at the moment the
        ## Snapshot was created and will not change, even if a concurrent Transaction commits
        ## changes.

    Transaction* = ref object of SnapshotObj
        ## A writeable view of the database. Changes are saved permanently when ``commit`` is
        ## called, or abandoned if ``abort`` is called or the Transaction is destroyed without
        ## committing.
        ##
        ## Changes made in a Transaction are not visible outside it (i.e. in Snapshots) until/unless
        ## it's committed. Even after a commit, a Snapshot made before the commit will not see the
        ## changes; you need to create a new Snapshot.
        ##
        ## A database may have only one Transaction open at a time. Opening a transaction will
        ## block if necessary until a prior transaction finishes. This applies even across OS
        ## processes.


proc `=`(dst: var SnapshotObj, src: SnapshotObj) {.error.} = echo "(can't copy a SnapshotObj)"
proc `=sink`(dst: var SnapshotObj, src: SnapshotObj) {.error.} = echo "(can't copy a SnapshotObj)"
proc `=destroy`(snap: var SnapshotObj) =
    if snap.m_txn != nil and snap.m_db.isOpen:
        discard mdbx_txn_abort(snap.m_txn)


proc makeTransaction(db: Database, flags: TxnFlags): MDBX_txn =
    var txn: MDBX_txn
    check mdbx_txn_begin(db.env, nil, flags, txn)
    return txn


proc beginSnapshot*(db: Database): Snapshot =
    ## Creates a read-only transaction on the database, that lasts until the returned
    ## Snapshot exits scope.
    result = Snapshot(m_txn: makeTransaction(db, MDBX_TXN_RDONLY), m_db: db)
    discard mdbx_txn_set_userctx(result.m_txn, cast[pointer](result))

proc beginTransaction*(db: Database): Transaction =
    ## Creates a writeable transaction on the database, that lasts until the returned
    ## Transaction exits scope.
    ##
    ## This call will block until any existing Transaction (in any OS process) finishes.
    result = Transaction(m_txn: makeTransaction(db, MDBX_TXN_READWRITE), m_db: db)
    discard mdbx_txn_set_userctx(result.m_txn, cast[pointer](result))


proc txn*(s: Snapshot): MDBX_txn =
    s.m_db.mustBeOpen
    let txn = s.m_txn
    if txn == nil: raise newException(CatchableError, "Using already-completed transaction")
    return txn


proc finish*(s: Snapshot) =
    ## Ends the snapshot's underlying MDBX transaction, if it's still active.
    ## This will happen automatically when a Snapshot (or Transaction) object is destroyed,
    ## but it can be useful to clean it up earlier.
    let txn = s.m_txn
    if txn != nil:
        check mdbx_txn_abort(txn)
        s.m_txn = nil
        s.m_db = nil

proc commit*(t: Transaction) =
    ## Commits changes made in a Transaction, and finishes the transaction.
    ## Raises an exception if there's a database error, or if the Transaction was already
    ## finished.
    check mdbx_txn_commit(t.txn)
    t.m_txn = nil
    t.m_db = nil

proc abort*(t: Transaction) =
    ## Throws away all changes made in a Transaction, and finishes the transaction.
    ## Raises an exception if there's a database error, or if the Transaction was already
    ## finished.
    check mdbx_txn_abort(t.txn)
    t.m_txn = nil
    t.m_db = nil


######## COLLECTION SNAPSHOTS & COLLECTION TRANSACTIONS


type
    CollectionSnapshot* = object of RootObj
        ## A reference to a Collection, as viewed through a Snapshot.
        collection*: Collection
        snapshot* {.requiresInit.}: Snapshot

    CollectionTransaction* = object of CollectionSnapshot
        ## A reference to a Collection, in a Transaction.


proc transaction*(t: CollectionTransaction): Transaction =
    return cast[Transaction](t.snapshot)

proc txn*(snap: CollectionSnapshot): MDBX_txn = snap.snapshot.txn


proc with*(coll: Collection, snap: Snapshot): CollectionSnapshot =
    ## Creates a CollectionSnapshot, a combination of a Collection and a Snapshot.
    CollectionSnapshot(collection: coll, snapshot: snap)

proc with*(coll: Collection, t: Transaction): CollectionTransaction =
    ## Creates a CollectionTransaction, a combination of a Collection and a Transaction.
    CollectionTransaction(collection: coll, snapshot: t)


proc beginSnapshot*(coll: Collection): CollectionSnapshot =
    ## A convenience function that creates a new Snapshot and returns it scoped to a Collection.
    return CollectionSnapshot(collection: coll, snapshot: coll.db.beginSnapshot())
proc beginTransaction*(coll: Collection): CollectionTransaction =
    ## A convenience function that creates a new Transaction and returns it scoped to a Collection.
    return CollectionTransaction(collection: coll, snapshot: coll.db.beginTransaction())

proc finish*(s: CollectionSnapshot)    = s.snapshot.finish()
    ## Calls ``finish`` on the underlying Snapshot.
proc commit*(t: CollectionTransaction) = t.transaction.commit()
    ## Calls ``commit`` on the underlying Transaction.
proc abort*(t: CollectionTransaction)  = t.transaction.abort()
    ## Calls ``abort`` on the underlying Transaction.


proc inSnapshot*(coll: Collection, fn: proc(t:CollectionSnapshot)) =
    ## Runs a callback within a new CollectionSnapshot.
    ## The snapshot is automatically finished after the callback returns.
    let s = coll.beginSnapshot()
    defer: s.finish()
    fn(s)

proc inTransaction*(coll: Collection, fn: proc(t:CollectionTransaction)) =
    ## Runs a callback within a new CollectionTransaction.
    ## The callback is responsible for committing the transaction, otherwise it will be
    ## aborted when the callback returns (or raises an exception.)
    let t = coll.beginTransaction()
    defer: t.finish()
    fn(t)


######## COLLECTION ACCESSORS


proc i_sequence(s: CollectionSnapshot, thenAdd: uint64): uint64 =
    check mdbx_dbi_sequence(s.txn, s.collection.dbi, result, thenAdd)

proc sequence*(s: CollectionSnapshot): uint64 =
    ## Returns the current value of the Collection's persistent sequence counter.
    ## This is initially zero but can be incremented by calling ``CollectionTransaction.sequence()``
    s.i_sequence(0)

proc sequence*(t: CollectionTransaction, thenAdd: uint64): uint64 =
    ## Returns the current value of the Collection's persistent sequence counter (initially 0),
    ## then adds ``thenAdd`` to the stored value.
    ## Note: As with all other changes in a transaction, the new value of the counter is not
    ## visible outside the Transaction until the Transaction is committed.
    t.i_sequence(thenAdd)


proc stats*(s: CollectionSnapshot): MDBX_stat =
    ## Returns low-level information about a Collection.
    var stat: MDBX_stat
    check mdbx_dbi_stat(s.txn, s.collection.dbi, stat, csize_t(sizeof(stat)))
    return stat

proc stats*(coll: Collection): MDBX_stat =
    ## Returns low-level information about a Collection.
    return coll.beginSnapshot().stats()

proc entryCount*(coll: Collection | CollectionSnapshot): int = int(coll.stats.ms_entries)
    ## The number of key/value pairs in the collection.


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
