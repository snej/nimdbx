# Collection.nim

import Database, Error, private/libmdbx
import tables

let nil_DBI = MDBX_dbi(0xFFFFFFFF)

type
    CollectionObj = object of RootObj
        m_dbi {.requiresInit.}     : MDBX_dbi
        db* {.requiresInit.}       : Database
        name* {.requiresInit.}     : string
        keyType* {.requiresInit.}  : KeyType
        valueType* {.requiresInit.}: ValueType
        initialized*               : bool       ## False the first time this Collection is opened

    Collection* = ref CollectionObj
        ## A namespace in a Database: a set of key/value pairs.
        ## A Database contains a set of named Collections.
        ##
        ## Accessing the contents of a Collection requires a Snapshot or Transaction.

    CollectionFlag* = enum
        ## A flag that describes properties of a Collection, given when opening or creating it.
        CreateCollection,   ## Create Collection if it doesn't exist
        MustExist,          ## Raise an exception if it doesn't exist (instead of returning nil)
        DuplicateKeys       ## Allows dup keys (multiple values per key.) Must specify ValueType!
    CollectionFlags* = set[CollectionFlag]
        ## Flags that describe properties of a Collection, given when opening or creating it.

    KeyType* = enum
        ## Types of keys. This affects how entries are sorted.
        ##
        ## Note: "String" only refers to the way the keys are sorted. It doesn't imply any
        ## particular character set or encoding. The keys can contain arbitrary bytes, including 0.
        StringKeys,         ## Keys are arbitrary strings, compared as by C `strcmp`
        ReverseStringKeys,  ## Keys are arbitrary strings, compared *backwards*
        IntegerKeys         ## Keys are 32- or 64-bit signed ints

    ValueType* = enum
        ## Types of values.
        ##
        ## In a "normal" collection with unique keys, only ``BlobValues`` is allowed.
        ##
        ## If the collection supports duplicate keys (i.e. the ``DuplicateKeys`` flag is used),
        ## then the value type must be one of the types *other than* ``BlobValues``;
        ## this choice determines how the values under a single key are sorted.
        ##
        ## Note: "String" only refers to the way the values are sorted. It doesn't imply any
        ## particular character set or encoding. They can contain arbitrary bytes, including 0.
        BlobValues,         ## Values are arbitrary blobs (incompatible with ``DuplicateKeys`` flag)
        StringValues,       ## Values are strings, compared with `strcmp`
        ReverseStringValues,## Values are strings, compared *backwards*
        FixedSizeValues,    ## Values are all the same size (this helps optimize storage)
        IntegerValues       ## Values are 32- or 64-bit signed ints, all the same size


const kKeyTypeDBIFlags: array[KeyType, MDBX_db_flags_t] =
        [MDBX_DB_DEFAULTS, MDBX_REVERSEKEY, MDBX_INTEGERKEY]
const kValTypeDBIFlags: array[ValueType, MDBX_db_flags_t] =
        [MDBX_DB_DEFAULTS, MDBX_DUPSORT, MDBX_REVERSEDUP or MDBX_DUPSORT,
         MDBX_DUPFIXED or MDBX_DUPSORT, MDBX_INTEGERDUP or MDBX_DUPFIXED or MDBX_DUPSORT]


proc openDBI(db: Database, name: string, flags: MDBX_db_flags_t): (MDBX_dbi, bool) =
    ## The low-level code to open an MDBX_dbi. `mdbx_dbi_open` has to be called in a transaction,
    ## but we can't use the Transaction class here, so we have to use the C API to open one.

    let readOnly = db.isReadOnly
    var txn: ptr MDBX_txn
    let txnFlags = if readOnly: MDBX_TXN_RDONLY else: MDBX_TXN_READWRITE
    check mdbx_txn_begin(db.env, nil, txnFlags, addr txn)

    var dbi: MDBX_dbi
    let err = mdbx_dbi_open(txn, name, flags, addr dbi)
    if err != MDBX_SUCCESS:
        discard mdbx_txn_abort(txn)
        discard checkOptional(err)
        return (nil_DBI, false) # err is NOT_FOUND

    var newFlags, state: cuint
    check(mdbx_dbi_flags_ex(txn, dbi, addr newFlags, addr state));
    let preexisting = (state and cuint(MDBX_DBI_CREAT)) == 0
    if readOnly:
        discard mdbx_txn_abort(txn)
    else:
        check mdbx_txn_commit(txn)
    return (dbi, preexisting);


proc getOpenCollection*(db: Database, name: string): Collection =
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

    result = db.getOpenCollection(name)
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

    let (dbi, preexisting) = db.openDBI(name, dbiflags)
    if dbi == nil_DBI:
        if MustExist in flags:
            throw MDBX_NOTFOUND
        return nil

    result = Collection(name: name, db: db, m_dbi: dbi, keyType: keyType, valueType: valueType,
                        initialized: preexisting)
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
