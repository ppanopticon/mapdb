package org.mapdb

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder
import org.mapdb.data.Maker
import org.mapdb.data.Verifier
import org.mapdb.data.hashmap.*
import org.mapdb.data.indextree.*
import org.mapdb.data.list.LinkedListMaker
import org.mapdb.data.list.LinkedListVerifier
import org.mapdb.data.primitive.*
import org.mapdb.data.treemap.*
import org.mapdb.elsa.*
import org.mapdb.elsa.ElsaSerializerPojo.ClassInfo
import org.mapdb.serializer.GroupSerializer
import org.mapdb.serializer.GroupSerializerObjectArray
import java.io.Closeable
import java.io.DataInput
import java.io.DataOutput
import java.lang.ref.Reference
import java.lang.ref.WeakReference
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.logging.Level
import kotlin.collections.HashMap
import kotlin.reflect.KClass
import kotlin.reflect.full.companionObject
import kotlin.reflect.full.primaryConstructor

/**
 * A database with easy access to named maps and other collections.
 */
//TODO consistency lock
//TODO rename named object
//TODO delete named object
//TOOD metrics logger
open class DB(
        /** Stores all underlying data */
        internal val store:Store,
        /** True if store existed before and was opened, false if store was created and is completely empty */
        protected val storeOpened: Boolean,


        override val isThreadSafe: Boolean = true,
        /** type of shutdown hook, 0 is disabled, 1 is hard ref, 2 is weak ref*/
        val shutdownHook:Int = 0
): Closeable, ConcurrencyAware {


    /**
     *
     */
    internal data class CatVal(val msg:(String)->String?, val required:Boolean=true)

    /**
     *
     */
    companion object{


        /** Registry of type names to instances of [Verifier]. Allows for user-extension of different types of storage. */
        private val VERIFIER_REGISTRY = ConcurrentHashMap<String,Pair<KClass<out Maker<*>>,Verifier>>()
        init {
            registerVerifierForType("LinkedList", LinkedListMaker::class,LinkedListVerifier)
            registerVerifierForType("HashMap", HashMapMaker::class,HashMapVerifier)
            registerVerifierForType("HashSet", HashSetMaker::class,HashSetVerifier)
            registerVerifierForType("TreeMap", TreeMapMaker::class,TreeMapVerifier)
            registerVerifierForType("TreeSet", TreeSetMaker::class,TreeSetVerifier)
            registerVerifierForType("IndexTreeList", IndexTreeListMaker::class,IndexTreeListVerifier)
            registerVerifierForType("IndexTreeLongLongMap", IndexTreeLongLongMapMaker::class,IndexTreeLongLongMapVerifier)
            registerVerifierForType("AtomicLong", AtomicLongMaker::class,AtomicPrimitiveVerifier)
            registerVerifierForType("AtomicInteger", AtomicIntegerMaker::class,AtomicPrimitiveVerifier)
            registerVerifierForType("AtomicBoolean", AtomicBooleanMaker::class,AtomicPrimitiveVerifier)
            registerVerifierForType("AtomicString", AtomicStringMaker::class,AtomicPrimitiveVerifier)
            registerVerifierForType("AtomicVar", AtomicVarMaker::class,AtomicVarVerifier)
        }

        protected val NAME_CATALOG_SERIALIZER:Serializer<SortedMap<String, String>> = object:Serializer<SortedMap<String, String>>{

            override fun deserialize(input: DataInput2, available: Int): SortedMap<String, String>? {
                val size = input.unpackInt()
                val ret = TreeMap<String, String>()
                for(i in 0 until size){
                    ret.put(input.readUTF(), input.readUTF())
                }
                return ret
            }

            override fun serialize(out: DataOutput2, value: SortedMap<String, String>) {
                out.packInt(value.size)
                value.forEach { e ->
                    out.writeUTF(e.key)
                    out.writeUTF(e.value)
                }
            }
        }

        protected val NAMED_SERIALIZATION_HEADER = 1

        /** list of DB objects to be closed */
        private val shutdownHooks = Collections.synchronizedMap(IdentityHashMap<Any, Any>())

        private var shutdownHookInstalled = AtomicBoolean(false)

        protected fun addShutdownHook(ref:Any){
            if(shutdownHookInstalled.compareAndSet(false, true)){
                Runtime.getRuntime().addShutdownHook(object:Thread(){
                    override fun run() {
                        for(o in shutdownHooks.keys.toTypedArray()) { //defensive copy, DB.close() modifies the set
                            try {
                                var a = o
                                if (a is Reference<*>)
                                    a = a.get()
                                if (a is DB)
                                    a.close()
                            } catch(e: Throwable) {
                                //consume all exceptions from this DB object, so other DBs are also closed
                                Utils.LOG.log(Level.SEVERE, "DB.close() thrown exception in shutdown hook.", e)
                            }
                        }
                    }
                })
            }
            shutdownHooks.put(ref, ref)
        }

        /**
         *
         */
        fun registerVerifierForType(type: String, maker: KClass<out Maker<*>>, verifier: Verifier) {
            if (VERIFIER_REGISTRY[type] != null) throw java.lang.IllegalArgumentException("Registration for provided type '${type}' already exists!")
            VERIFIER_REGISTRY[type] = Pair(maker,verifier)
        }

        /**
         *
         */
        fun getRegisteredVerifierForType(type: String) = VERIFIER_REGISTRY.get(type)
    }

    fun getStore():Store{
        checkNotClosed()
        return store
    }

    object Keys {
        val type = "#type"
        val keySerializer = "#keySerializer"
        val valueSerializer = "#valueSerializer"
        val serializer = "#serializer"
        val valueInline = "#valueInline"
        val counterRecids = "#counterRecids"
        val hashSeed = "#hashSeed"
        val segmentRecids = "#segmentRecids"
        val expireCreateTTL = "#expireCreateTTL"
        val expireUpdateTTL = "#expireUpdateTTL"
        val expireGetTTL = "#expireGetTTL"
        val expireCreateQueue = "#expireCreateQueue"
        val expireUpdateQueue = "#expireUpdateQueue"
        val expireGetQueue = "#expireGetQueue"
        val rootRecids = "#rootRecids"
        val rootRecid = "#rootRecid"
        val concShift = "#concShift" /** Concurrency shift, 1<<it is number of concurrent segments in HashMap */
        val dirShift = "#dirShift"
        val levels = "#levels"
        val removeCollapsesIndexTree = "#removeCollapsesIndexTree"
        val rootRecidRecid = "#rootRecidRecid"
        val counterRecid = "#counterRecid"
        val maxNodeSize = "#maxNodeSize"
        val size = "#size"
        val recid = "#recid"
    }

    internal val lock = if(isThreadSafe) ReentrantReadWriteLock() else null

    internal fun checkNotClosed(){
        if(closed.get())
            throw IllegalAccessError("DB was closed")
    }

    private val closed = AtomicBoolean(false);



    /** Already loaded named collections. Values are weakly referenced. We need singletons for locking */
    internal var namesInstanciated: Cache<String, Any?> = CacheBuilder.newBuilder().concurrencyLevel(1).weakValues().build()


    private val classSingletonCat = IdentityHashMap<Any,String>()
    private val classSingletonRev = HashMap<String, Any>()

    private val unknownClasses = Collections.synchronizedSet(HashSet<Class<*>>())

    private fun namedClasses() = arrayOf(BTreeMap::class.java, HTreeMap::class.java,
            HTreeMap.KeySet::class.java,
            BTreeMapJava.KeySet::class.java,
            Atomic.Integer::class.java,
            Atomic.Long::class.java,
            Atomic.String::class.java,
            Atomic.Boolean::class.java,
            Atomic.Var::class.java,
            IndexTreeList::class.java
    )

    private val nameSer = object:ElsaSerializerBase.Ser<Any>(){
        override fun serialize(out: DataOutput, value: Any, objectStack: ElsaStack?) {
            val name = getNameForObject(value)
                    ?: throw DBException.SerializationError("Could not serialize named object, it was not instantiated by this db")

            out.writeUTF(name)
        }
    }

    private val nameDeser = object:ElsaSerializerBase.Deser<Any>(){
        override fun deserialize(input: DataInput, objectStack: ElsaStack): Any? {
            val name = input.readUTF()
            return this@DB.get(name)
        }
    }

    private val elsaSerializer:ElsaSerializerPojo = ElsaSerializerPojo(
            0,
            pojoSingletons(),
            namedClasses().map { Pair(it, nameSer) }.toMap(),
            namedClasses().map { Pair(it, NAMED_SERIALIZATION_HEADER)}.toMap(),
            mapOf(Pair(NAMED_SERIALIZATION_HEADER, nameDeser)),
            ElsaClassCallback { unknownClasses.add(it) },
            object:ElsaClassInfoResolver {
                override fun classToId(className: String): Int {
                    val classInfos = loadClassInfos()
                    classInfos.forEachIndexed { i, classInfo ->
                        if(classInfo.name==className)
                            return i
                    }
                    return -1
                }

                override fun getClassInfo(classId: Int): ElsaSerializerPojo.ClassInfo? {
                    return loadClassInfos()[classId]
                }
            } )

    /**
     * Default serializer used if collection does not specify specialized serializer.
     * It uses Elsa Serializer.
     */
    val defaultSerializer = object: GroupSerializerObjectArray<Any?>() {

        override fun deserialize(input: DataInput2, available: Int): Any? {
            return elsaSerializer.deserialize(input)
        }

        override fun serialize(out: DataOutput2, value: Any) {
            elsaSerializer.serialize(out, value)
        }

    }


    protected val classInfoSerializer = object : Serializer<Array<ClassInfo>> {

        override fun serialize(out: DataOutput2, ci: Array<ClassInfo>) {
            out.packInt(ci.size)
            for(c in ci)
                elsaSerializer.classInfoSerialize(out, c)
        }

        override fun deserialize(input: DataInput2, available: Int): Array<ClassInfo> {
            return Array(input.unpackInt(), {
                elsaSerializer.classInfoDeserialize(input)
            })
        }

    }

    init {
        if (storeOpened.not()) {
            //create new structure
            if(store.isReadOnly){
                throw DBException.WrongConfiguration("Can not create new store in read-only mode")
            }
            //preallocate 16 recids
            val nameCatalogRecid = store.put(TreeMap<String, String>(), NAME_CATALOG_SERIALIZER)
            if(CC.RECID_NAME_CATALOG != nameCatalogRecid)
                throw DBException.WrongConfiguration("Store does not support Reserved Recids: "+store.javaClass)

            val classCatalogRecid = store.put(arrayOf<ClassInfo>(), classInfoSerializer)
            if(CC.RECID_CLASS_INFOS != classCatalogRecid)
                throw DBException.WrongConfiguration("Store does not support Reserved Recids: "+store.javaClass)


            for(recid in 3L..CC.RECID_MAX_RESERVED){
                val recid2 = store.put(null, Serializer.LONG_PACKED)
                if (recid2 != recid){
                    throw DBException.WrongConfiguration("Store does not support Reserved Recids: "+store.javaClass)
                }
            }
            store.commit()
        }

        val msgs = nameCatalogVerifyGetMessages().toList()
        if(!msgs.isEmpty())
            throw DBException.NewMapDBFormat("Name Catalog has some new unsupported features: "+msgs.toString());
    }


    init{
        //read all singleton from Serializer fields
        Serializer::class.java.declaredFields.forEach { f ->
            val name = Serializer::class.java.canonicalName + "#"+f.name
            val obj = f.get(null)
            classSingletonCat.put(obj, name)
            classSingletonRev.put(name, obj)
        }
        val defSerName = "org.mapdb.DB#defaultSerializer"
        classSingletonCat.put(defaultSerializer, defSerName)
        classSingletonRev.put(defSerName, defaultSerializer)

    }


    private val shutdownReference:Any? =
            when(shutdownHook){
                0 -> null
                1 -> this@DB
                2 -> WeakReference(this@DB)
                else -> throw IllegalArgumentException()
            }

    init{
        if(shutdownReference!=null){
            DB.addShutdownHook(shutdownReference)
        }
    }


    private fun pojoSingletons():Array<Any>{
        // NOTE !!! do not change index of any element!!!
        // it is storage format definition
        return arrayOf(
                this@DB, this@DB.defaultSerializer,
                Serializer.CHAR, Serializer.STRING_ORIGHASH , Serializer.STRING, Serializer.STRING_DELTA,
                Serializer.STRING_DELTA2, Serializer.STRING_INTERN, Serializer.STRING_ASCII, Serializer.STRING_NOSIZE,
                Serializer.LONG, Serializer.LONG_PACKED, Serializer.LONG_DELTA, Serializer.INTEGER,
                Serializer.INTEGER_PACKED, Serializer.INTEGER_DELTA, Serializer.BOOLEAN, Serializer.RECID,
                Serializer.RECID_ARRAY, Serializer.ILLEGAL_ACCESS, Serializer.BYTE_ARRAY, Serializer.BYTE_ARRAY_DELTA,
                Serializer.BYTE_ARRAY_DELTA2, Serializer.BYTE_ARRAY_NOSIZE, Serializer.CHAR_ARRAY, Serializer.INT_ARRAY,
                Serializer.LONG_ARRAY, Serializer.DOUBLE_ARRAY, Serializer.JAVA, Serializer.ELSA, Serializer.UUID,
                Serializer.BYTE, Serializer.FLOAT, Serializer.DOUBLE, Serializer.SHORT, Serializer.SHORT_ARRAY,
                Serializer.FLOAT_ARRAY, Serializer.BIG_INTEGER, Serializer.BIG_DECIMAL, Serializer.CLASS,
                Serializer.DATE,
                Collections.EMPTY_LIST,
                Collections.EMPTY_SET,
                Collections.EMPTY_MAP
        )

    }

    private fun loadClassInfos():Array<ElsaSerializerPojo.ClassInfo>{
        return store.get(CC.RECID_CLASS_INFOS, classInfoSerializer)!!
    }


    /** List of executors associated with this database. Those will be terminated on close() */
    val executors:MutableSet<ExecutorService> = Collections.synchronizedSet(LinkedHashSet())

    fun nameCatalogLoad():SortedMap<String, String> {
        return Utils.lockRead(lock){
            checkNotClosed()
            nameCatalogLoadLocked()
        }

    }
    protected fun nameCatalogLoadLocked():SortedMap<String, String> {
        if(CC.ASSERT)
            Utils.assertReadLock(lock)
        return store.get(CC.RECID_NAME_CATALOG, NAME_CATALOG_SERIALIZER)
                ?: throw DBException.WrongConfiguration("Could not open store, it has no Named Catalog");
    }

    fun nameCatalogSave(nameCatalog: SortedMap<String, String>) {
        Utils.lockWrite(lock){
            checkNotClosed()
            nameCatalogSaveLocked(nameCatalog)
        }
    }

    internal fun nameCatalogSaveLocked(nameCatalog: SortedMap<String, String>) {
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)
        store.update(CC.RECID_NAME_CATALOG, nameCatalog, NAME_CATALOG_SERIALIZER)
    }


    private val nameRegex = "[A-Z0-9._-]".toRegex()

    protected fun checkName(name: String) {
        if(name.contains('#'))
            throw DBException.WrongConfiguration("Name contains illegal character, '#' is not allowed.")
        if(!name.matches(nameRegex))
            throw DBException.WrongConfiguration("Name contains illegal character")
    }

    protected fun nameCatalogGet(name: String): String? {
        return nameCatalogLoadLocked()[name]
    }


    fun  nameCatalogPutClass(
            nameCatalog: SortedMap<String, String>,
            key: String,
            obj: Any
    ) {
        val value:String? = classSingletonCat[obj]

        if(value== null){
            //not in singletons, try to resolve public no ARG constructor of given class
            //TODO get public no arg constructor if exist
        }

        if(value!=null)
            nameCatalog.put(key, value)
    }

    fun <E> nameCatalogGetClass(
            nameCatalog: SortedMap<String, String>,
            key: String
    ):E?{
        val clazz = nameCatalog.get(key)
                ?: return null

        val singleton = classSingletonRev.get(clazz)
        if(singleton!=null)
            return singleton as E

        throw DBException.WrongConfiguration("Could not load object: "+clazz)
    }

    fun nameCatalogParamsFor(name: String): Map<String,String> {
        val ret = TreeMap<String,String>()
        ret.putAll(nameCatalogLoad().filter {
            it.key.startsWith(name+"#")
        })
        return Collections.unmodifiableMap(ret)
    }


    private fun unknownClassesSave(){
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)
        //TODO batch class dump
        unknownClasses.forEach {
            defaultSerializerRegisterClass_noLock(it)
        }
        unknownClasses.clear()
    }

    fun commit(){
        Utils.lockWrite(lock) {
            checkNotClosed()
            unknownClassesSave()
            store.commit()
        }
    }

    fun rollback(){
        if(store !is StoreTx)
            throw UnsupportedOperationException("Store does not support rollback")

        Utils.lockWrite(lock) {
            checkNotClosed()
            unknownClasses.clear()
            store.rollback()
        }
    }

    fun isClosed() = closed.get();

    override fun close(){
        if(closed.compareAndSet(false,true).not())
            return

        // do not close this DB from JVM shutdown hook
        if(shutdownReference!=null)
            shutdownHooks.remove(shutdownReference)

        Utils.lockWrite(lock) {
            unknownClassesSave()

            //shutdown running executors if any
            executors.forEach { it.shutdown() }
            //await termination on all
            executors.forEach {
                // TODO LOG this could use some warnings, if background tasks fails to shutdown
                while (!it.awaitTermination(1, TimeUnit.DAYS)) {
                }
            }
            executors.clear()
            store.close()
        }
    }

    fun <E> get(name:String): E? {
        Utils.lockWrite(lock) {
            checkNotClosed()
            val type = nameCatalogGet(name + Keys.type) ?: throw DBException.WrongConfiguration("Collection has unknown type.")
            return DB.getRegisteredVerifierForType(name)?.first?.primaryConstructor?.call(this, name)?.open() as E?
        }
    }

    fun getNameForObject(e:Any):String? =
            namesInstanciated.asMap().filterValues { it===e }.keys.firstOrNull()

    fun exists(name: String): Boolean {
        Utils.lockRead(lock) {
            checkNotClosed()
            return nameCatalogGet(name + Keys.type) != null
        }
    }

    fun getAllNames():Iterable<String>{
        return nameCatalogLoad().keys
                .filter { it.endsWith(Keys.type) }
                .map {it.split("#")[0]}
    }

    fun getAll():Map<String, Any?>{
        val ret = TreeMap<String,Any?>();
        getAllNames().forEach { ret.put(it, get(it)) }
        return ret
    }


    /**
     * Returns a [LinkedListMaker] maker.
     *
     * @return New instance of [LinkedListMaker]
     */
    fun linkedList(name:String): LinkedListMaker<*> = LinkedListMaker<Any?>(this, name)


    fun hashMap(name:String): HashMapMaker<*, *> = HashMapMaker<Any?, Any?>(this, name)
    fun <K,V> hashMap(name:String, keySerializer: Serializer<K>, valueSerializer: Serializer<V>) =
            HashMapMaker<K, V>(this, name)
                    .keySerializer(keySerializer)
                    .valueSerializer(valueSerializer)

    abstract class TreeMapSink<K,V>:Pump.Sink<Pair<K,V>, BTreeMap<K, V>>(){

        fun put(key:K, value:V) {
            put(Pair(key, value))
        }

        fun putAll(map:SortedMap<K,V>){
            map.forEach { e ->
                put(e.key, e.value)
            }
        }
    }

    fun treeMap(name:String):TreeMapMaker<*,*> = TreeMapMaker<Any?, Any?>(this, name)
    fun <K,V> treeMap(name:String, keySerializer: GroupSerializer<K>, valueSerializer: GroupSerializer<V>) =
            TreeMapMaker<K,V>(this, name)
                    .keySerializer(keySerializer)
                    .valueSerializer(valueSerializer)

    fun treeSet(name:String): TreeSetMaker<*> = TreeSetMaker<Any?>(this, name)
    fun <E> treeSet(name:String, serializer: GroupSerializer<E>) =
            TreeSetMaker<E>(this, name)
                    .serializer(serializer)

    fun hashSet(name:String): HashSetMaker<*> = HashSetMaker<Any?>(this, name)
    fun <E> hashSet(name:String, serializer: Serializer<E>) =
            HashSetMaker<E>(this, name)
                    .serializer(serializer)

    fun atomicInteger(name:String) = AtomicIntegerMaker(this, name)

    fun atomicInteger(name:String, value:Int) = AtomicIntegerMaker(this, name, value)

    fun atomicLong(name:String) = AtomicLongMaker(this, name)

    fun atomicLong(name:String, value:Long) = AtomicLongMaker(this, name, value)

    fun atomicBoolean(name:String) = AtomicBooleanMaker(this, name)

    fun atomicBoolean(name:String, value:Boolean) = AtomicBooleanMaker(this, name, value)

    fun atomicString(name:String) = AtomicStringMaker(this, name)

    fun atomicString(name:String, value:String?) = AtomicStringMaker(this, name, value)

    fun atomicVar(name:String) = atomicVar(name, defaultSerializer)
    fun <E> atomicVar(name:String, serializer:Serializer<E> ) = AtomicVarMaker(this, name, serializer)

    fun <E> atomicVar(name:String, serializer:Serializer<E>, value:E? ) = AtomicVarMaker(this, name, serializer, value)

    //TODO this is thread unsafe, but locks should not be added directly due to code overhead on HTreeMap
    private fun indexTreeLongLongMap(name: String) = IndexTreeLongLongMapMaker(this, name)


    fun <E> indexTreeList(name: String, serializer:Serializer<E>) = IndexTreeListMaker(this, name, serializer)
    fun indexTreeList(name: String) = indexTreeList(name, defaultSerializer)


    override fun checkThreadSafe() {
        super.checkThreadSafe()
        if(store.isThreadSafe.not())
            throw AssertionError()
    }

    /**
     * Register Class with default POJO serializer. Class structure will be stored in store,
     * and will save space for collections which do not use specialized serializer.
     */
    fun defaultSerializerRegisterClass(clazz:Class<*>){
        Utils.lockWrite(lock) {
            checkNotClosed()
            defaultSerializerRegisterClass_noLock(clazz)
        }
    }
    private fun defaultSerializerRegisterClass_noLock(clazz:Class<*>) {
        if(CC.ASSERT)
            Utils.assertWriteLock(lock)
        var infos = loadClassInfos()
        val className = clazz.name
        if (infos.find { it.name == className } != null)
            return; //class is already present
        //add as last item to an array
        infos = Arrays.copyOf(infos, infos.size + 1)
        infos[infos.size - 1] = elsaSerializer.makeClassInfo(className)
        //and save
        store.update(CC.RECID_CLASS_INFOS, infos, classInfoSerializer)
    }

    /** verifies name catalog is valid (all parameters are known and have required values). If there are problems, it return list of messages */
    fun nameCatalogVerifyGetMessages():Iterable<String>{
        val ret = ArrayList<String>()
        val catalog = nameCatalogLoad()
        val names = catalog.keys.filter{it.endsWith(Keys.type)}.map{it.substring(0, it.lastIndexOf('#'))}.toSet()

        val known = HashSet<String>()

        //iterate over names, check all required parameters are present
        nameLoop@ for(name in names){

            //get type
            known+=name+Keys.type
            val type = catalog[name+Keys.type] ?: throw DBException.WrongConfiguration("Collection has unknown type.")
            val reqParams = DB.getRegisteredVerifierForType(type)?.second?.expected
            if (reqParams == null){
                ret += name+Keys.type+": unknown type '$type'"
                continue@nameLoop
            }
            paramLoop@ for((param, catVal) in reqParams){
                known+=name+param
                val value = catalog[name+param]
                if(value==null) {
                    if(catVal.required)
                        ret += name + param+": required parameter not found"
                    continue@paramLoop
                }
                val msg = catVal.msg(value) //validate value, get msg if not valid
                if(msg!=null)
                    ret+=name+param+": "+msg
            }
        }

        //check for extra params which are not known
        for(param in catalog.keys)
            if(known.contains(param).not())
                ret+=param+": unknown parameter"

        return ret;
    }
}