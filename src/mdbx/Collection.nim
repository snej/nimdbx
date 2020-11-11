# Collection.nim

import FlatDB, private/libmdbx, private/utils

let nil_DBI = MDBX_dbi(0xFFFFFFFF)

type
    CollectionObj = object
        name* {.requiresInit.}  : string
        db* {.requiresInit.}    : FlatDB
        dbi {.requiresInit.}    : MDBX_dbi
        initialized*            : bool

    Collection* = ref CollectionObj

    CollectionFlag* = enum
        Create,             # Create Collection if it doesn't exist
        ReverseKeys,        # Compare key strings back-to-front
        DuplicateKeys,      # Allow duplicate keys
        IntegerKeys,        # Keys are interpreted as native ints; must be 4 or 8 bytes long
        DupFixed,           # With DuplicateKeys, all values of a key must have the same size
        IntegerDup,         # With DuplicateKeys, values are native ints, 4 or 8 bytes long
        ReverseDup          # With DuplicateKeys, values are compared back-to-front
    CollectionFlags* = set[CollectionFlag]


proc openDBI(db: FlatDB, name: string, flags: DBIFlags): (MDBX_dbi, bool) =
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


proc openRequiredDBI(db: FlatDB, name: string, flags: DBIFlags): (MDBX_dbi, bool) =
    result = openDBI(db, name, flags)
    if result[0] == nil_DBI:
        check MDBX_NOTFOUND


proc openCollection*(db: FlatDB, name: string, flags: CollectionFlags): Collection =
    ## Creates a Collection object giving access to a named collection in a FlatDB.
    ## If the collection does not exist, and the ``Create`` flag is not set, raises MDBX_NOTFOUND.

    # WARNING: This assumes set[CollectionFlags] matches DBIFlags, except for MDBX_CREATE.
    #          If you reorder CollectionFlags, this will break!
    var dbiflags = DBIFlags(cast[uint](flags - {Create}))
    if Create in flags:
        dbiflags = dbiflags or MDBX_CREATE
    let (dbi, isNew) = db.openRequiredDBI(name, dbiflags)
    return Collection(name: name, db: db, dbi: dbi, initialized: not isNew)

proc createCollection*(db: FlatDB, name: string, flags: CollectionFlags = {}): Collection =
    ## A convenience that calls ``openCollection`` with the ``Create`` flag set.
    openCollection(db, name, flags + {Create})


proc dbi*(coll: Collection): MDBX_dbi = coll.dbi


######## SNAPSHOTS & TRANSACTIONS


type
    SnapshotObj = object of RootObj
        m_txn {.requiresInit.}: MDBX_txn

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
    if snap.m_txn != nil:
        discard mdbx_txn_abort(snap.m_txn)


proc makeTransaction(db: FlatDB, flags: TxnFlags): MDBX_txn =
    var txn: MDBX_txn
    check mdbx_txn_begin(db.env, nil, flags, txn)
    return txn


proc beginSnapshot*(db: FlatDB): Snapshot =
    ## Creates a read-only transaction on the database, that lasts until the returned
    ## Snapshot exits scope.
    return Snapshot(m_txn: makeTransaction(db, MDBX_TXN_RDONLY))

proc beginTransaction*(db: FlatDB): Transaction =
    ## Creates a writeable transaction on the database, that lasts until the returned
    ## Transaction exits scope.
    ##
    ## This call will block until any existing Transaction (in any OS process) finishes.
    return Transaction(m_txn: makeTransaction(db, MDBX_TXN_DEFAULT))


proc txn*(s: Snapshot): MDBX_txn =
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

proc commit*(t: Transaction) =
    ## Commits changes made in a Transaction, and finishes the transaction.
    ## Raises an exception if there's a database error, or if the Transaction was already
    ## finished.
    check mdbx_txn_commit(t.txn)
    t.m_txn = nil

proc abort*(t: Transaction) =
    ## Throws away all changes made in a Transaction, and finishes the transaction.
    ## Raises an exception if there's a database error, or if the Transaction was already
    ## finished.
    check mdbx_txn_abort(t.txn)
    t.m_txn = nil


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


######## COLLECTION VALUE ACCESS


proc asMDBX_val(a: openarray[char]): MDBX_val = MDBX_val(base: unsafeAddr a[0], len: csize_t(a.len))
template asMDBX_val(i: int32): MDBX_val = MDBX_val(base: unsafeAddr i, len: 4)
template asMDBX_val(i: int64): MDBX_val = MDBX_val(base: unsafeAddr i, len: 8)


proc get*[K](snap: CollectionSnapshot, key: K): string =
    ## Looks up the value of a key in a Collection, and returns it as a (copied) string.
    ## If the key is not found, returns an empty string.
    var mdbKey: MDBX_val = asMDBX_val(key)
    var mdbVal: MDBX_Val
    if checkOptional mdbx_get(snap.txn, snap.collection.dbi, mdbKey, mdbVal):
        return toString(mdbVal)
    else:
        return ""     # FIX: Should be something nil-like


proc getNearest*(snap: CollectionSnapshot, key: openarray[char]): (string, string) =
    ## Finds the first key _greater than or equal to_ ``key``, and returns it and its value.
    ## Else returns a pair of empty strings.
    var mdbKey: MDBX_val = key
    var mdbVal: MDBX_Val
    if checkOptional mdbx_get_nearest(snap.txn, snap.collection.dbi, mdbKey, mdbVal):
        return (toString(mdbKey), toString(mdbVal))
    else:
        return ("", "")     # FIX: Should be something nil-like


proc get*(snap: CollectionSnapshot,
          key: openarray[char],
          fn: proc(val:openarray[char])): bool {.discardable.} =
    ## Looks up the value of a key in a Collection; if found, it passes it to the callback
    ## function as an ``openarray``, _without copying_, then returns true.
    ## If not found, the callback is not called, and the result is false.
    var mdbKey: MDBX_val = key
    var mdbVal: MDBX_Val
    result = checkOptional mdbx_get(snap.txn, snap.collection.dbi, mdbKey, mdbVal)
    if result:
        let val = cast[ptr UncheckedArray[char]](mdbVal.base)
        fn(val.toOpenArray(0, int(mdbVal.len) - 1))


proc i_put[K,V](t: CollectionTransaction, key: K, val: V, flags: UpdateFlags): MDBXErrorCode =
    var mdbKey: MDBX_val = asMDBX_val(key)
    var mdbVal: MDBX_val = asMDBX_val(val)
    return mdbx_put(t.txn, t.collection.dbi, mdbKey, mdbVal, flags)

proc put*[K,V](t: CollectionTransaction, key: K, val: V) =
    ## Stores a value for a key in a Collection.
    check i_put(t, key, val, UpdateFlags(0))


proc put*(t: CollectionTransaction, key: openarray[char], valueLen: int,
          fn: proc(val:openarray[char])) =
    ## Stores a value for a key in a Collection. The value is filled in by a callback function.
    ## This eliminates a memory-copy inside libmdbx, and might save the caller some allocation.
    var mdbKey: MDBX_val = key
    var mdbVal = MDBX_val(base: nil, len: csize_t(valueLen))
    check mdbx_put(t.txn, t.collection.dbi, mdbKey, mdbVal, MDBX_RESERVE)
    # Now pass the value pointer/size to the caller to fill in:
    let valPtr = cast[ptr UncheckedArray[char]](mdbVal.base)
    fn(valPtr.toOpenArray(0, valueLen - 1))


proc insert*(t: CollectionTransaction, key: openarray[char], val: openarray[char]): bool =
    ## Adds a new key and its value; if the key exists, does nothing and returns false.
    let err = i_put(t, key, val, MDBX_NOOVERWRITE)
    if err == MDBX_KEYEXIST:
        return false
    else:
        check err
        return true

proc update*(t: CollectionTransaction, key: openarray[char], val: openarray[char]): bool =
    ## Replaces an existing value for a key in a Collection;
    ## If the key doesn't already exist, does nothing and returns false.
    let err = i_put(t, key, val, MDBX_CURRENT)
    if err == MDBX_NOTFOUND or err == MDBX_EMULTIVAL:
        return false
    else:
        check err
        return true

proc append*(t: CollectionTransaction, key: openarray[char], val: openarray[char]) =
    ## Adds a key and value to the end of the collection. This is faster than ``put``, and is
    ## useful when populating a Collection with already-sorted data.
    ## The key must be greater than any existing key, or MDBX_EKEYMISMATCH will be raised.
    check i_put(t, key, val, MDBX_APPEND)

# TODO: Allow combinations of flags, by exposing a higher level flag set(?)
# TODO: Add mdbx_replace

proc del*(t: CollectionTransaction, key: openarray[char]): bool {.discardable.} =
    ## Removes a key and its value from a Collection.
    ## Returns true if the key existed, false if it doesn't exist.
    var mdbKey: MDBX_val = key
    return checkOptional mdbx_del(t.txn, t.collection.dbi, mdbKey, nil)


######## COLLECTION METADATA


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
