# Collection.nim

import Database, private/libmdbx, private/utils
import tables

let nil_DBI = MDBX_dbi(0xFFFFFFFF)

type
    CollectionObj = object of RootObj
        m_dbi {.requiresInit.}     : MDBX_dbi
        db* {.requiresInit.}       : Database
        name* {.requiresInit.}     : string
        keyType* {.requiresInit.}  : KeyType
        valueType* {.requiresInit.}: ValueType
        initialized*               : bool

    Collection* = ref CollectionObj
        ## A namespace in a Database: a set of key/value pairs.
        ## A Database contains a set of named Collections.
        ##
        ## Accessing the contents of a Collection requires a Snapshot or Transaction.

    KeyType* = enum
        StringKeys,         ## Keys are arbitrary strings, compared as by C `strcmp`
        ReverseStringKeys,  ## Keys are arbitrary strings, compared in reverse
        IntegerKeys         ## Keys are 32- or 64-bit ints (native byte order)

    ValueType* = enum
        BlobValues,         ## Values are arbitrary blobs
        StringValues,       ## Values are strings, compared with `strcmp` (DuplicateKeys only)
        ReverseStringValues,## Values are strings, compared in reverse (DuplicateKeys only)
        FixedSizeValues,    ## Values are all the same size (DuplicateKeys only)
        IntegerValues       ## Values are 32- or 64-bit ints, all same size (DuplicateKeys only)

    CollectionFlag* = enum
        ## Flags that describe properties of a Collection when opening or creating it.
        CreateCollection,   ## Create Collection if it doesn't exist
        MustExist,          ## Raise an exception if it doesn't exist (instead of returning nil)
        DuplicateKeys       ## Allows dup keys (multiple values per key.) Must specify ValueType
    CollectionFlags* = set[CollectionFlag]
        ## Flags that describe properties of a Collection when opening or creating it.


const kKeyTypeDBIFlags: array[KeyType, MDBX_db_flags_t] =
        [MDBX_DB_DEFAULTS, MDBX_REVERSEKEY, MDBX_INTEGERKEY]
const kValTypeDBIFlags: array[ValueType, MDBX_db_flags_t] =
        [MDBX_DB_DEFAULTS, MDBX_DUPSORT, MDBX_REVERSEDUP or MDBX_DUPSORT,
         MDBX_DUPFIXED or MDBX_DUPSORT, MDBX_INTEGERDUP or MDBX_DUPFIXED or MDBX_DUPSORT]

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


proc getCollection*(db: Database, name: string): Collection =
    ## Returns an already-opened Collection with the given name, or nil.
    return Collection(db.m_collections.getOrDefault(name))


proc openCollection*(db: Database,
                     name: string,
                     flags: CollectionFlags = {},
                     keyType: KeyType = StringKeys,
                     valueType: ValueType = BlobValues): Collection =
    ## Returns a Collection object giving access to a persistent named collection in a Database.
    ## Multiple calls with the same name will return the same Collection object.
    ##
    ## If no collection with that name exists, the behavior depends on the flags:
    ## - If ``CreateCollection`` is given, the collection will be created.
    ## - If ``MustExist`` is given, ``MDBX_NOTFOUND`` is raised.
    ## - Otherwise ``nil`` is returned.
    ##
    ## If the collection does already exist, its key and value types must match the ones given,
    ## or ``MDBX_INCOMPATIBLE`` will be raised.
    ##
    ## If the ``DuplicateKeys`` flag is set, the collection will allow multiple values for a key,
    ## and sorts those values. You must then specify a ValueType other than ``BlobValues``, to
    ## define their sort order.

    result = db.getCollection(name)
    if result != nil:
        if keyType != result.keyType or valueType != result.valueType:
            throw MDBX_INCOMPATIBLE

        return

    var dbiflags = kKeyTypeDBIFlags[keyType] or kValTypeDBIFlags[valueType]
    if CreateCollection in flags:
        dbiflags = dbiflags or MDBX_CREATE
    if (dbiflags and MDBX_DUPSORT) != 0:
        assert(DuplicateKeys in flags, "Using sorted value type requires DuplicateKeys flag")
    else:
        assert(not (DuplicateKeys in flags), "Using DuplicateKeys flag requires a sorted value type")

    let (dbi, isNew) = db.openDBI(name, dbiflags)
    if dbi == nil_DBI:
        if MustExist in flags:
            throw MDBX_NOTFOUND
        return nil

    result = Collection(name: name, db: db, m_dbi: dbi, keyType: keyType, valueType: valueType,
                        initialized: not isNew)
    db.m_collections[name] = result

proc createCollection*(db: Database,
                       name: string,
                       keyType: KeyType = StringKeys): Collection =
    openCollection(db, name, {CreateCollection}, keyType)


proc duplicateKeys*(coll: Collection): bool =
    ## True if the Collection supports duplicate keys.
    coll.valueType > BlobValues

proc dbi*(coll: Collection): MDBX_dbi =
    coll.db.mustBeOpen()
    return coll.m_dbi


# Looking for the accessor functions to, you know, _do stuff_ with a Collection?
# They're in CRUD.nim and Transaction.nim.
