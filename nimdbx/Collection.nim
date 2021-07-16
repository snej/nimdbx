# Collection.nim

{.experimental: "notnil".}
{.experimental: "strictFuncs".}

import Database, Data, Error, private/libmdbx
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
        i_changeHook*              : I_ChangeHook    ## NOT PUBLIC

    Collection* = ref CollectionObj
        ## A namespace in a Database: a set of key/value pairs.
        ## A Database contains a set of named Collections.
        ##
        ## Accessing the contents of a Collection requires a Snapshot or Transaction.

    CollectionFlag* = enum
        ## A flag that describes properties of a Collection, given when opening or creating it.
        CreateCollection,   ## Create Collection if it doesn't exist
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

    I_ChangeHook* = proc(txn: ptr MDBX_txn; key, oldVal, newVal: MDBX_val; flags: MDBX_put_flags_t)


const kKeyTypeDBIFlags: array[KeyType, MDBX_db_flags_t] =
        [MDBX_DB_DEFAULTS, MDBX_REVERSEKEY, MDBX_INTEGERKEY]
const kValTypeDBIFlags: array[ValueType, MDBX_db_flags_t] =
        [MDBX_DB_DEFAULTS, MDBX_DUPSORT, MDBX_REVERSEDUP or MDBX_DUPSORT,
         MDBX_DUPFIXED or MDBX_DUPSORT, MDBX_INTEGERDUP or MDBX_DUPFIXED or MDBX_DUPSORT]


proc openDBI(db: Database, name: string, flags: MDBX_db_flags_t): (MDBX_dbi, bool) =
    ## The low-level code to open an MDBX_dbi. `mdbx_dbi_open` has to be called in a transaction,
    ## but we can't use the Transaction class here, so we have to use the C API to open one.

    var txn = db.i_txn
    let readOnly = db.isReadOnly
    let isLocalTxn = txn == nil
    if isLocalTxn:
        let txnFlags = if readOnly: MDBX_TXN_RDONLY else: MDBX_TXN_READWRITE
        check mdbx_txn_begin(db.i_env, nil, txnFlags, addr txn)

    var dbi: MDBX_dbi
    let err = mdbx_dbi_open(txn, name, flags, addr dbi)
    if err != MDBX_SUCCESS:
        if isLocalTxn: discard mdbx_txn_abort(txn)
        discard checkOptional(err)
        return (nil_DBI, false) # err is NOT_FOUND

    var newFlags, state: cuint
    check(mdbx_dbi_flags_ex(txn, dbi, addr newFlags, addr state));
    let preexisting = (state and cuint(MDBX_DBI_CREAT)) == 0
    if isLocalTxn:
        if readOnly: discard mdbx_txn_abort(txn)
        else: check mdbx_txn_commit(txn)
    return (dbi, preexisting);


func getOpenCollection*(db: Database, name: string): Collection =
    ## Returns an already-opened Collection with the given name, or nil.
    return Collection(db.i_collections.getOrDefault(name))


proc openCollectionOrNil*(db: Database,
                          name: string,
                          flags: CollectionFlags = {},
                          keyType: KeyType = StringKeys,
                          valueType: ValueType = BlobValues): Collection =
    ## Returns a Collection object giving access to a persistent named collection in a Database.
    ##
    ## This is just like `openCollection`, except that if the collection is not found
    ## it returns `nil` instead of raising an exception.

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
        return nil

    result = Collection(name: name, db: db, m_dbi: dbi, keyType: keyType, valueType: valueType,
                        initialized: preexisting)
    db.i_collections[name] = result


proc openCollection*(db: Database,
                     name: string,
                     flags: CollectionFlags = {},
                     keyType: KeyType = StringKeys,
                     valueType: ValueType = BlobValues): Collection not nil =
    ## Returns a Collection object giving access to a persistent named collection in a Database.
    ## Multiple calls with the same name will return the same Collection object.
    ##
    ## If no collection with that name exists, the behavior depends on the flags:
    ## - If ``CreateCollection`` is given, the collection will be created.
    ## - Otherwise an `MDBX_NOTFOUND` exception is raised.
    ##
    ## If the collection does already exist, its key and value types must match the ones given,
    ## or ``MDBX_INCOMPATIBLE`` will be raised.
    ##
    ## If the ``DuplicateKeys`` flag is set, the collection will allow multiple values for a key,
    ## and sorts those values. You must then specify a ValueType other than ``BlobValues``, to
    ## define their sort order.
    let coll = openCollectionOrNil(db, name, flags, keyType, valueType)
    if coll == nil:
        throw MDBX_NOTFOUND
    else:
        return coll


proc createCollection*(db: Database,
                       name: string,
                       keyType: KeyType = StringKeys,
                       valueType: ValueType = BlobValues): Collection not nil =
    ## Returns a Collection object giving access to a persistent named collection in a Database.
    ## The Collection is created if it doesn't already exist.
    ##
    ## This is just like `openCollection`, except that the flags are implicitly given as
    ## `CreateCollection`.
    var flags = {CreateCollection}
    if valueType != BlobValues:
        flags = flags + {DuplicateKeys}
    return openCollection(db, name, flags, keyType, valueType)


func duplicateKeys*(coll: Collection): bool =
    ## True if the Collection supports duplicate keys.
    coll.valueType > BlobValues


func i_dbi*(coll: Collection): MDBX_dbi =
    coll.db.mustBeOpen()
    return coll.m_dbi


# Looking for the accessor functions to, you know, _do stuff_ with a Collection?
# They're in the CRUD, Cursor and Transaction modules.


#%%%%%%% CHANGE HOOK


proc i_addChangeHook*(coll: Collection not nil, hook: I_ChangeHook) =
    let prevHook = coll.i_changeHook
    if prevHook == nil:
        coll.i_changeHook = hook
    else:
        coll.i_changeHook = proc(txn: ptr MDBX_txn; key, oldVal, newVal: MDBX_val; flags: MDBX_put_flags_t) =
            hook(txn, key, oldVal, newVal, flags)
            prevHook(txn, key, oldVal, newVal, flags)


type ChangeHook* = proc(key, oldValue, newValue: DataOut)
    ## A callback function that's invoked just after a change to a key/value pair in a Collection.
    ## - The `oldValue` will be nil if this is an insertion.
    ## - The `newValue` will be nil if this is a deletion.
    ##
    ## It's legal for the callback to make changes to the Database, although of course if it changes
    ## the Collection it was registered on, it'll be invoked re-entrantly.
    ##
    ## As usual with DataOut values, the parameters point to ephemeral data, so if they're going to
    ## be stored or used after the callback returns, they should be copied to other types (like
    ## `string`, `seq`, `int`...) first.


proc addChangeHook*(coll: Collection not nil, hook: ChangeHook) =
    ## Registers a ChangeHook with a Collection.
    coll.i_addChangeHook proc(txn: ptr MDBX_txn; key, oldVal, newVal: MDBX_val; flags: MDBX_put_flags_t) =
        hook(DataOut(val: key), DataOut(val: oldVal), DataOut(val: newVal))
