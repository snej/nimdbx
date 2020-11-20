# Transaction.nim

import Database, Collection, private/libmdbx, private/utils


type
    SnapshotObj = object of RootObj
        m_txn {.requiresInit.}: ptr MDBX_txn
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


proc `=`(dst: var SnapshotObj, src: SnapshotObj) {.error.}
proc `=sink`(dst: var SnapshotObj, src: SnapshotObj) {.error.}
proc `=destroy`(snap: var SnapshotObj) =
    if snap.m_txn != nil and snap.m_db.isOpen:
        discard mdbx_txn_abort(snap.m_txn)


proc makeTransaction(db: Database, flags: MDBX_txn_flags_t): ptr MDBX_txn =
    var txn: ptr MDBX_txn
    check mdbx_txn_begin(db.env, nil, flags, addr txn)
    return txn


proc beginSnapshot*(db: Database): Snapshot =
    ## Creates a read-only transaction on the database, that lasts until the returned Snapshot
    ## object's ``finish`` function is called, or the object is destroyed.
    ##
    ## NOTE: If not using ARC or ORC, the object will probably not be destroyed for a while after
    ## you stop using it, so explicit calls to ``finish`` are recommended.
    result = Snapshot(m_txn: makeTransaction(db, MDBX_TXN_RDONLY), m_db: db)
    discard mdbx_txn_set_userctx(result.m_txn, cast[pointer](result))

proc beginTransaction*(db: Database): Transaction =
    ## Creates a writeable transaction on the database, that lasts until the returned Transaction
    ## object's ``commit`` or ``abort`` functions are called, or the object is destroyed.
    ##
    ## This call will block until any existing Transaction (in any OS process) finishes.
    ##
    ## NOTE: If not using ARC or ORC, the object will probably not be destroyed for a while after
    ## you stop using it, so explicit calls are recommended.
    result = Transaction(m_txn: makeTransaction(db, MDBX_TXN_READWRITE), m_db: db)
    discard mdbx_txn_set_userctx(result.m_txn, cast[pointer](result))


proc txn*(s: Snapshot): ptr MDBX_txn =
    s.m_db.mustBeOpen
    let txn = s.m_txn
    if txn == nil: raise newException(CatchableError, "Using already-completed transaction")
    return txn


proc finish*(s: Snapshot) =
    ## Ends the snapshot's underlying MDBX transaction, if it's still active.
    ## If the Snapshot is a Transaction, it will be aborted.
    ##
    ## This will happen automatically when a Snapshot (or Transaction) object is destroyed,
    ## but it can be useful to clean it up earlier, since the old garbage collector can leave the
    ## object around for quite a while before destroying it.
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


proc inSnapshot*(db: Database, fn: proc(t:Snapshot)) =
    ## Runs a callback within a new Snapshot.
    ## The snapshot is automatically finished after the callback returns.
    let s = db.beginSnapshot()
    defer: s.finish()
    fn(s)

proc inTransaction*(db: Database, fn: proc(t:Transaction)) =
    ## Runs a callback within a new Transaction.
    ##
    ## NOTE: The callback is responsible for committing the transaction, otherwise it will be
    ## aborted when the callback returns (or if it raises an exception.)
    let t = db.beginTransaction()
    defer: t.finish()
    fn(t)


#%%%%%%% COLLECTION SNAPSHOTS & COLLECTION TRANSACTIONS


type
    CollectionSnapshot* = object of RootObj
        ## A reference to a Collection, as viewed through a Snapshot.
        collection* {.requiresInit.}: Collection
        snapshot* {.requiresInit.}: Snapshot

    CollectionTransaction* = object of CollectionSnapshot
        ## A reference to a Collection, in a Transaction.


proc transaction*(t: CollectionTransaction): Transaction =
    return cast[Transaction](t.snapshot)

proc txn*(snap: CollectionSnapshot): ptr MDBX_txn = snap.snapshot.txn


proc with*(coll: Collection, snap: Snapshot): CollectionSnapshot {.inline.} =
    ## Creates a CollectionSnapshot, a combination of a Collection and a Snapshot.
    CollectionSnapshot(collection: coll, snapshot: snap)

proc with*(coll: Collection, t: Transaction): CollectionTransaction {.inline.} =
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
    ##
    ## NOTE: The callback is responsible for committing the transaction, otherwise it will be
    ## aborted when the callback returns (or if it raises an exception.)
    let t = coll.beginTransaction()
    defer: t.finish()
    fn(t)


#%%%%%%% COLLECTION ACCESSORS


proc i_sequence(s: CollectionSnapshot, thenAdd: uint64): uint64 =
    check mdbx_dbi_sequence(s.txn, s.collection.dbi, addr result, thenAdd)

proc lastSequence*(s: CollectionSnapshot): uint64 =
    ## Returns the last sequence number generated by the Collection's persistent sequence counter.
    ## This is initially zero.
    ##
    ## Note: Calls to ``nextSequence`` in another Transaction do not affect this value, until
    ## that Transaction is committed.
    s.i_sequence(0)

proc nextSequence*(t: CollectionTransaction, count: int = 1): uint64 =
    ## Assigns a new sequence number by incrementing the Collection's sequence counter.
    ## If you need multiple new sequences, set ``count`` to the number you need. The first one
    ## will be returned.
    ##
    ## You can use sequences for whatever you want. They're useful for generating unique consecutive
    ## keys for integer-keyed tables.
    assert count > 0
    t.i_sequence(uint64(count)) + 1


proc stats*(s: CollectionSnapshot): MDBX_stat =
    ## Returns low-level information about a Collection.
    var stat: MDBX_stat
    check mdbx_dbi_stat(s.txn, s.collection.dbi, addr stat, csize_t(sizeof(stat)))
    return stat

proc stats*(coll: Collection): MDBX_stat =
    ## Returns low-level information about a Collection.
    return coll.beginSnapshot().stats()

proc entryCount*(coll: Collection | CollectionSnapshot): int = int(coll.stats.ms_entries)
    ## The number of key/value pairs in the collection.
