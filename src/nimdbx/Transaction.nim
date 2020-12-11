# Transaction.nim

import Database, Collection, Error, private/libmdbx


type
    SnapshotObj = object of RootObj
        m_txn {.requiresInit.}: ptr MDBX_txn
        database {.requiresInit.}: Database

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
    if snap.m_txn != nil and snap.database.isOpen:
        discard mdbx_txn_abort(snap.m_txn)


proc makeTransaction(db: Database, flags: MDBX_txn_flags_t): ptr MDBX_txn =
    var txn: ptr MDBX_txn
    check mdbx_txn_begin(db.i_env, nil, flags, addr txn)
    return txn


proc beginSnapshot*(db: Database): Snapshot =
    ## Creates a read-only transaction on the database, that lasts until the returned Snapshot
    ## object's ``finish`` function is called, or the object is destroyed.
    ##
    ## NOTE: If not using ARC or ORC, the object will probably not be destroyed for a while after
    ## you stop using it, so explicit calls to ``finish`` are recommended.
    result = Snapshot(m_txn: makeTransaction(db, MDBX_TXN_RDONLY), database: db)
    discard mdbx_txn_set_userctx(result.m_txn, cast[pointer](result))

proc beginTransaction*(db: Database): Transaction =
    ## Creates a writeable transaction on the database, that lasts until the returned Transaction
    ## object's ``commit`` or ``abort`` functions are called, or the object is destroyed.
    ##
    ## This call will block until any existing Transaction (in any OS process) finishes.
    ##
    ## NOTE: If not using ARC or ORC, the object will probably not be destroyed for a while after
    ## you stop using it, so explicit calls are recommended.
    result = Transaction(m_txn: makeTransaction(db, MDBX_TXN_READWRITE), database: db)
    discard mdbx_txn_set_userctx(result.m_txn, cast[pointer](result))


proc i_txn*(s: Snapshot): ptr MDBX_txn =
    s.database.mustBeOpen
    let txn = s.m_txn
    if txn == nil: raise newException(CatchableError, "Using already-completed transaction")
    return txn


func i_recoverTransaction*(txn: ptr MDBX_txn): Transaction =
    return cast[Transaction](mdbx_txn_get_userctx(txn))


func database*(s: Snapshot): Database = s.database


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
        s.database = nil


proc commit*(t: Transaction) =
    ## Commits changes made in a Transaction, and finishes the transaction.
    ## Raises an exception if there's a database error, or if the Transaction was already
    ## finished.
    check mdbx_txn_commit(t.i_txn)
    t.m_txn = nil
    t.database = nil


proc abort*(t: Transaction) =
    ## Throws away all changes made in a Transaction, and finishes the transaction.
    ## Raises an exception if there's a database error, or if the Transaction was already
    ## finished.
    check mdbx_txn_abort(t.i_txn)
    t.m_txn = nil
    t.database = nil


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
        collection {.requiresInit.}: Collection
        snapshot {.requiresInit.}: Snapshot

    CollectionTransaction* = object of CollectionSnapshot
        ## A reference to a Collection, in a Transaction.

proc i_clear*(s: var CollectionSnapshot) {.inline.} =
    s.collection = nil
    s.snapshot = nil

func i_txn*(snap: CollectionSnapshot): ptr MDBX_txn = snap.snapshot.i_txn


func collection*(s: CollectionSnapshot) : Collection {.inline.} = s.collection
func snapshot*(s: CollectionSnapshot) : Snapshot {.inline.} = s.snapshot
func transaction*(t: CollectionTransaction) : Transaction {.inline.} = cast[Transaction](t.snapshot)


func with*(coll: Collection, snap: Snapshot): CollectionSnapshot {.inline.} =
    ## Creates a CollectionSnapshot, a combination of a Collection and a Snapshot.
    CollectionSnapshot(collection: coll, snapshot: snap)

func with*(coll: Collection, t: Transaction): CollectionTransaction {.inline.} =
    ## Creates a CollectionTransaction, a combination of a Collection and a Transaction.
    CollectionTransaction(collection: coll, snapshot: t)

func i_with*(coll: Collection, txn: ptr MDBX_txn): CollectionTransaction =
    let transaction = cast[Transaction](mdbx_txn_get_userctx(txn))
    assert transaction != nil
    return coll.with(transaction)


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
    check mdbx_dbi_sequence(s.i_txn, s.collection.i_dbi, addr result, thenAdd)


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
    check mdbx_dbi_stat(s.i_txn, s.collection.i_dbi, addr stat, csize_t(sizeof(stat)))
    return stat


proc stats*(coll: Collection): MDBX_stat =
    ## Returns low-level information about a Collection.
    return coll.beginSnapshot().stats()


proc entryCount*(coll: Collection | CollectionSnapshot): int =
    ## The number of key/value pairs in the collection.
    return int(coll.stats.ms_entries)


#%%%%%%% COLLECTION BULK DELETION


proc delAll*(t: CollectionTransaction) =
    ## Removes **all** keys and values from a Collection, but does not delete the Collection itself.
    check mdbx_drop(t.i_txn, t.collection.i_dbi, false)

proc deleteCollection*(t: CollectionTransaction) =
    ## Deletes the Collection itself, including all its keys and values.
    check mdbx_drop(t.i_txn, t.collection.i_dbi, true)
