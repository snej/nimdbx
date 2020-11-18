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


proc openDBI(db: Database, name: string, flags: MDBX_db_flags_t): (MDBX_dbi, bool) =
    #db.mutex.lock() # FIX
    var envFlags: cuint
    check mdbx_env_get_flags(db.env, addr envFlags);
    let readOnly = (envFlags and cuint(MDBX_RDONLY)) != 0
    var txnFlags: MDBX_txn_flags_t
    if readOnly:
        txnFlags = MDBX_TXN_RDONLY

    var txn: ptr MDBX_txn
    check mdbx_txn_begin(db.env, nil, txnFlags, addr txn)

    var dbi: MDBX_dbi
    let err = mdbx_dbi_open(txn, name, flags, addr dbi)

    if err != MDBX_SUCCESS or readOnly:
        discard mdbx_txn_abort(txn);
        discard checkOptional(err)
        return (nil_DBI, false)
    else:
        var newFlags, state: cuint
        check(mdbx_dbi_flags_ex(txn, dbi, addr newFlags, addr state));
        let isNew = (state and cuint(MDBX_DBI_CREAT)) != 0
        check mdbx_txn_commit(txn)
        return (dbi, isNew);


proc openRequiredDBI(db: Database, name: string, flags: MDBX_db_flags_t): (MDBX_dbi, bool) =
    result = openDBI(db, name, flags)
    if result[0] == nil_DBI:
        check MDBX_NOTFOUND


proc openCollection*(db: Database, name: string, flags: CollectionFlags): Collection =
    ## Creates a Collection object giving access to a named collection in a Database.
    ## If the collection does not exist, and the ``Create`` flag is not set, raises MDBX_NOTFOUND.

    # WARNING: This assumes set[CollectionFlags] matches MDBX_db_flags_t, except for MDBX_CREATE.
    #          If you reorder CollectionFlags, this will break!
    var dbiflags = MDBX_db_flags_t(cast[uint](flags - {Create}))
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


# Looking for the accessor functions to, you know, _do stuff_ with a Collection?
# They're in CRUD.nim and Transaction.nim.
