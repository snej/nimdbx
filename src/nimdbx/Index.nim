# Index.nim

{.experimental: "notnil".}
# {.experimental: "strictFuncs".}

import Collatable, Collection, CRUD, Cursor, Data, Transaction
import private/libmdbx
import strformat


# An index is stored as a Collection whose name is of the form "index::<source>::<name>",
# where <source> is the name of the Collection being indexed, and <name> is the index's name.
#
# An index's keys are Collatable; these are produced by its IndexFunc.
# Its values are keys in the source collection, so its ValueType is the source's KeyType.
# Indexes always support duplicate keys.
#
# In other words, an entry in an index maps a Collatable value produced by the IndexFunc
# (the property being indexed) to the key in the source collection of the value that was indexed.
# For this reason it's sometimes called an "inverted index".
#
# The index is initially populated by the `rebuild` method, which iterates over the source
# Collection, calls the IndexFunc on each value, and if a non-empty Collatable is produced it
# writes that and the key to the index Collection.
#
# The index is updated by `updateEntry`, which is called as a changeHook on the Collection.
# `updateEntry` has to call the IndexFunc on both the old and the new value. If the resulting
# Collatables are different, it then deletes the old Collatable key and inserts the new one.


type
    IndexObj = object
        name: string
        source {.requiresInit.}: Collection not nil
        index {.requiresInit.}: Collection not nil
        indexer {.requiresInit.}: IndexFunc
        updateCount*: int

    Index* = ref IndexObj
        ## An object that maintains an index of a Collection.
        ## The index is stored as a separate Collection, that can be queried using key-value
        ## getters or Cursors.

    EmitFunc* = proc(indexKey: Collatable)
        ## A callback given to an IndexFunc, to be called by that function for each entry to add
        ## to the index.

    IndexFunc* = proc(value: DataOut, emit: EmitFunc)
        ## A user-supplied function that's given a value from the source Collection
        ## and calls the `emit` function with value(s) to be indexed, if any.
        ##
        ## NOTE: It's very important that this function be repeatable: given the same `value` it
        ## should always emit the same index keys. Otherwise the index will get mixed up.


proc updateEntry(ind: Index; txn: ptr MDBX_txn; key, oldValue, newValue: MDBX_val;
                 flags: MDBX_put_flags_t): bool =
    if ind.indexer == nil:
        return false
    elif newValue != oldValue:
        try:
            #echo "INDEX: ", ($DataOut(val: key)).escape, " -> old ", ($DataOut(val: oldValue)).escape, " / new ", ($DataOut(val: newValue)).escape
            let ct = ind.index.with(i_recoverTransaction(txn))
            var firstDel, firstIns: Collatable   # cache the first `emit` call on old and new value
            var updated = false

            if oldValue.exists:
                ind.indexer(DataOut(val: oldValue)) do (indexValue: Collatable):
                    if firstDel.isEmpty:
                        firstDel = indexValue
                    elif not indexValue.isEmpty:
                        discard ct.del(indexValue, key)
                        updated = true

            if newValue.exists:
                ind.indexer(DataOut(val: newValue)) do (indexValue: Collatable):
                    if firstIns.isEmpty:
                        firstIns = indexValue
                    elif not indexValue.isEmpty:
                        discard ct.insert(indexValue, key)
                        updated = true

            # See if the first-emitted values for old/new data are different:
            if firstDel != firstIns:
                if not firstDel.isEmpty:
                    discard ct.del(firstDel, key)
                if not firstIns.isEmpty:
                    discard ct.insert(firstIns, key)
                updated = true
            if updated:
                ind.updateCount += 1
        except:
            let x = getCurrentException()
            echo "EXCEPTION updating index ", ind.index.name, ": ", x.name, " ", x.msg
            writeStackTrace()
    return true


proc rebuild*(ind: Index) =
    ## Rebuilds an index from scratch.
    ## It's only necessary to call this if you open an existing Index with a different
    ## indexer function.
    ind.index.inTransaction do (ct: CollectionTransaction):
        ct.delAll()
        for key, val in ind.source.with(ct.snapshot):
            ind.indexer(val) do (indexValue: Collatable):
                discard ct.insert(indexValue, key)
        ct.commit()
    ind.index.initialized = true
    ind.updateCount = 0


proc openIndex*(on: Collection not nil, name: string, indexer: IndexFunc not nil): Index =
    ## Opens or creates a named index on a Collection.
    ## If this is the first time this Index has been opened, it will be populated based on all
    ## the key/value pairs in the Collection.
    ##
    ## From this point on, until the Database is closed, changes made to the Collection will
    ## automatically update the Index.
    ##
    ## If the Collection is used later (after re-opening the Database), this Index must be opened
    ## too, with the same indexer function. Changing the Collection without the Index open will cause
    ## the Index to be out of date, resulting in invalid results.
    ##
    ## Changing the behavior of the indexer function is also likely to corrupt the index. If you
    ## reopen an Index using a different indexer, call `rebuild` right afterwards.
    let valueType = if on.keyType == IntegerKeys: IntegerValues else: StringValues
    let indexColl = on.db.openCollection(&"index::{on.name}::{name}",
                                         {CreateCollection, DuplicateKeys},
                                         StringKeys,
                                         valueType)
    var index = Index(source: on, index: indexColl, name: name, indexer: indexer)
    if not index.index.initialized:
        index.rebuild()

    on.i_addChangeHook proc(t: ptr MDBX_txn; key, oldValue, newValue: MDBX_val; flags: MDBX_put_flags_t) =
        if index != nil and not index.updateEntry(t, key, oldValue, newValue, flags):
            index = nil    # breaks ref and prevents further calls after index closes
    return index


func with*(ind: Index, snap: Snapshot): CollectionSnapshot {.inline.} =
    ## Creates a CollectionSnapshot on an index's internal Collection so you can access it.
    ##
    ## This Collection's keys are Collatable values (the same ones your indexer function
    ## created) and its values are keys in the source Collection.
    ##
    ## There may be multiple values for the same key, since different values in the source
    ## Collection can produce the same indexed value.
    ind.index.with(snap)

proc beginSnapshot*(ind: Index): CollectionSnapshot =
    ## A convenience function that creates a new Snapshot and returns it scoped to a Index.
    return ind.index.beginSnapshot()


proc entryCount*(ind: Index): int =
    ## The number of items in the index.
    ind.beginSnapshot().entryCount


proc deleteIndex*(idx: Index) =
    ## Persistently deletes the index on disk.
    idx.indexer = nil
    idx.index.inTransaction do (ct: CollectionTransaction):
        ct.deleteCollection()
        ct.commit
