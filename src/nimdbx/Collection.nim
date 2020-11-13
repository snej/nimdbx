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
