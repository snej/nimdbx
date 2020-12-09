# Index.nim

import Collatable, Collection, CRUD, Cursor, Data, Transaction
import private/libmdbx
import strformat, strutils


type
    IndexObj = object
        source: Collection
        index: Collection
        name: string
        indexer: IndexFunc

    Index* = ref IndexObj
        ## An object that maintains an index of a Collection.
        ## The index is stored as a separate Collection, that can be queried using key-value
        ## getters or Cursors.

    IndexFunc* = proc(value: DataOut, outColumns: var Collatable) {.nosideeffect.}
        ## A user-supplied function that's given a value from the source Collection
        ## and writes the column value(s) to be indexed into a Collatable.
        ##
        ## NOTE: It's very important that this function be repeatable: given the same `value` it
        ## should always write the same value(s) to `outColumns`. Otherwise the index will be
        ## corrupted.


proc update(ind: Index; t: ptr MDBX_txn; key, oldValue, newValue: MDBX_val; flags: MDBX_put_flags_t) =
    try:
        echo "INDEX: ", ($DataOut(val: key)).escape, " -> old ", ($DataOut(val: oldValue)).escape, " / new ", ($DataOut(val: newValue)).escape
        # Get the Collatable entries for the old & new values:
        var oldEntry, newEntry: Collatable
        if oldValue.exists:
            ind.indexer(DataOut(val: oldValue), oldEntry)
        if newValue.exists:
            ind.indexer(DataOut(val: newValue), newEntry)
        if oldEntry.data != newEntry.data:
            # Entries are different, so remove old one and add new one:
            let txn = ind.index.with(i_recoverTransaction(t))
            if oldEntry.data.len > 0:
                txn.del(oldEntry.data)
            if newEntry.data.len > 0:
                txn.put(newEntry.data, key)
    except:
        echo "EXCEPTION updating index ", ind.name


proc rebuild*(ind: Index) =
    ## Rebuilds an index from scratch. It should not be necessary to call this yourself.
    ind.index.inTransaction do (ct: CollectionTransaction):
        ct.delAll()
        var entry: Collatable
        for key, val in ind.source.with(ct.snapshot):
            entry.clear()
            ind.indexer(val, entry)
            if entry.data.len > 0:
                ct.put(entry.data, key)
        ct.commit()
    ind.index.initialized = true


proc openIndex*(on: Collection, name: string, indexer: IndexFunc): Index =
    let valueType = if on.keyType == IntegerKeys: IntegerValues else: StringValues
    let indexColl = on.db.openCollection(&"index::{on.name}::{name}",
                                         {CreateCollection, DuplicateKeys},
                                         StringKeys,
                                         valueType)
    let index = Index(source: on, index: indexColl, name: name, indexer: indexer)
    if not index.index.initialized:
        index.rebuild()
    on.i_addChangeHook proc(t: ptr MDBX_txn; key, oldValue, newValue: MDBX_val; flags: MDBX_put_flags_t) =
        index.update(t, key, oldValue, newValue, flags)
    return index


proc with*(ind: Index, snap: Snapshot): CollectionSnapshot {.inline.} =
    ## Creates an IndexSnapshot, a combination of a Index and a Snapshot.
    ind.index.with(snap)

proc beginSnapshot*(ind: Index): CollectionSnapshot =
    ## A convenience function that creates a new Snapshot and returns it scoped to a Index.
    return ind.index.beginSnapshot()


proc entryCount*(ind: Index): int =
    ## The number of items in the index.
    ind.beginSnapshot().entryCount
