# Collection.nim

import FlatDB, private/libmdbx, private/utils

let nil_DBI = MDBX_dbi(0xFFFFFFFF)

type
    CollectionObj = object
        name* {.requiresInit.}  : string
        db* {.requiresInit.}    : FlatDB
        dbi {.requiresInit.}    : MDBX_dbi
        initialized             : bool

    Collection* = ref CollectionObj

    CollectionFlag* = enum
        Create,
        ReverseKeys,
        DuplicateKeys,
        IntegerKeys,
        DupFixed,
        IntegerDup,
        ReverseDup
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
    var dbiflags = DBIFlags(cast[uint](flags - {Create}))
    if Create in flags:
        dbiflags = dbiflags or MDBX_CREATE
    let (dbi, isNew) = db.openRequiredDBI(name, dbiflags)
    return Collection(name: name, db: db, dbi: dbi, initialized: not isNew)

proc createCollection*(db: FlatDB, name: string, flags: CollectionFlags = {}): Collection =
    openCollection(db, name, flags + {Create})


proc dbi*(coll: Collection): MDBX_dbi = coll.dbi


######## SNAPSHOTS & TRANSACTIONS


type
    SnapshotObj = object of RootObj
        m_txn {.requiresInit.}: MDBX_txn
    Snapshot*    = ref SnapshotObj
    Transaction* = ref object of SnapshotObj


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


######## COLLECTIONSNAPSHOTS & COLLECTIONTRANSACTIONS


type
    CollectionSnapshot* = object of RootObj
        collection*: Collection
        snapshot* {.requiresInit.}: Snapshot

    CollectionTransaction* = object of CollectionSnapshot


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
    return CollectionSnapshot(collection: coll, snapshot: coll.db.beginSnapshot())
proc beginTransaction*(coll: Collection): CollectionTransaction =
    return CollectionTransaction(collection: coll, snapshot: coll.db.beginTransaction())

proc finish*(s: CollectionSnapshot)    = s.snapshot.finish()
proc commit*(t: CollectionTransaction) = t.transaction.commit()
proc abort*(t: CollectionTransaction)  = t.transaction.abort()


proc inSnapshot*(coll: Collection, fn: proc(t:CollectionSnapshot)) =
    ## Runs a callback within a new Snapshot.
    ## The snapshot is automatically cleaned up after the callback returns.
    let s = coll.beginSnapshot()
    defer: s.finish()
    fn(s)

proc inTransaction*(coll: Collection, fn: proc(t:CollectionTransaction)) =
    ## Runs a callback within a new Transaction.
    ## The callback is responsible for committing the transaction, otherwise it will be
    ## aborted.
    let t = coll.beginTransaction()
    defer: t.finish()
    fn(t)

######## COLLECTION VALUE ACCESS


proc get*(snap: CollectionSnapshot, key: openarray[char]): seq[char] =
    ## Looks up the value of a key in a Collection, and returns it as a (copied) ``seq``.
    ## If the key is not found, returns an empty ``seq``.
    # FIX: Return val as some sort of 'lent' memory
    var mdbKey: MDBX_val = key
    var mdbVal: MDBX_Val
    if checkOptional mdbx_get(snap.txn, snap.collection.dbi, mdbKey, mdbVal):
        return mdbVal  # see converter above
    else:
        return newSeq[char](0)     # FIX: Should be something nil-like


proc get*(snap: CollectionSnapshot, key: openarray[char], fn: proc(val:openarray[char])) =
    ## Looks up the value of a key in a Collection; if found, it passes it to the callback
    ## function as an ``openarray``, without copying.
    ## If not found, the callback is not called.
    var mdbKey: MDBX_val = key
    var mdbVal: MDBX_Val
    if checkOptional mdbx_get(snap.txn, snap.collection.dbi, mdbKey, mdbVal):
        let val = cast[ptr UncheckedArray[char]](mdbVal.base)
        fn(val.toOpenArray(0, int(mdbVal.len) - 1))


proc put(t: CollectionTransaction, key: openarray[char], val: openarray[char], flags: UpdateFlags) =
    ## Stores a value for a key in a Collection.
    var mdbKey: MDBX_val = key
    var mdbVal: MDBX_val = val
    check mdbx_put(t.txn, t.collection.dbi, mdbKey, mdbVal, flags)

proc put*(t: CollectionTransaction, key: openarray[char], val: openarray[char]) =
    put(t, key, val, UpdateFlags(0))


proc del*(t: CollectionTransaction, key: openarray[char]) =
    ## Removes a key/value from a Collection.
    var mdbKey: MDBX_val = key
    check mdbx_del(t.txn, t.collection.dbi, mdbKey, nil)


proc stats*(s: CollectionSnapshot): MDBX_stat =
    ## Returns low-level information about a Collection.
    var stat: MDBX_stat
    check mdbx_dbi_stat(s.txn, s.collection.dbi, stat, csize_t(sizeof(stat)))
    return stat

proc stats*(coll: Collection): MDBX_stat =
    ## Returns low-level information about a Collection.
    return coll.beginSnapshot().stats()
