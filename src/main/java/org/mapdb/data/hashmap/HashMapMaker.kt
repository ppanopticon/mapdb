package org.mapdb.data.hashmap

import org.eclipse.collections.api.map.primitive.MutableLongLongMap
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.mapdb.*
import org.mapdb.data.Maker
import org.mapdb.data.indextree.IndexTreeListJava
import org.mapdb.data.indextree.IndexTreeLongLongMap
import org.mapdb.data.treemap.BTreeMap
import java.security.SecureRandom
import java.util.*
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class HashMapMaker<K,V>(
        override val db: DB,
        override val name:String,
        protected val hasValues:Boolean=true,
        protected val _storeFactory:(segment:Int)-> Store = { i-> db.getStore()}
): Maker<HTreeMap<K, V>>(){

    override val type = "HashMap"

    private var _keySerializer: Serializer<K> = db.defaultSerializer as Serializer<K>
    private var _valueSerializer: Serializer<V> = db.defaultSerializer as Serializer<V>
    private var _valueInline = false

    private var _concShift = CC.HTREEMAP_CONC_SHIFT
    private var _dirShift = CC.HTREEMAP_DIR_SHIFT
    private var _levels = CC.HTREEMAP_LEVELS

    private var _hashSeed:Int? = null
    private var _expireCreateTTL:Long = 0L
    private var _expireUpdateTTL:Long = 0L
    private var _expireGetTTL:Long = 0L
    private var _expireExecutor: ScheduledExecutorService? = null
    private var _expireExecutorPeriod:Long = 10000
    private var _expireMaxSize:Long = 0
    private var _expireStoreSize:Long = 0
    private var _expireCompactThreshold:Double? = null

    private var _counterEnable: Boolean = false

    private var _valueLoader:((key:K)->V?)? = null
    private var _modListeners:MutableList<MapModificationListener<K, V>> = ArrayList()
    private var _expireOverflow:MutableMap<K,V?>? = null;
    private var _removeCollapsesIndexTree:Boolean = true


    fun <A> keySerializer(keySerializer: Serializer<A>): HashMapMaker<A, V> {
        _keySerializer = keySerializer as Serializer<K>
        return this as HashMapMaker<A, V>
    }

    fun <A> valueSerializer(valueSerializer: Serializer<A>): HashMapMaker<K, A> {
        _valueSerializer = valueSerializer as Serializer<V>
        return this as HashMapMaker<K, A>
    }


    fun valueInline(): HashMapMaker<K, V> {
        _valueInline = true
        return this
    }


    fun removeCollapsesIndexTreeDisable(): HashMapMaker<K, V> {
        _removeCollapsesIndexTree = false
        return this
    }

    fun hashSeed(hashSeed:Int): HashMapMaker<K, V> {
        _hashSeed = hashSeed
        return this
    }

    fun layout(concurrency:Int, dirSize:Int, levels:Int): HashMapMaker<K, V> {
        fun toShift(value:Int):Int{
            return 31 - Integer.numberOfLeadingZeros(DataIO.nextPowTwo(Math.max(1,value)))
        }
        _concShift = toShift(concurrency)
        _dirShift = toShift(dirSize)
        _levels = levels
        return this
    }

    fun expireAfterCreate(): HashMapMaker<K, V> {
        return expireAfterCreate(-1)
    }

    fun expireAfterCreate(ttl:Long): HashMapMaker<K, V> {
        _expireCreateTTL = ttl
        return this
    }


    fun expireAfterCreate(ttl:Long, unit: TimeUnit): HashMapMaker<K, V> {
        return expireAfterCreate(unit.toMillis(ttl))
    }

    fun expireAfterUpdate(): HashMapMaker<K, V> {
        return expireAfterUpdate(-1)
    }


    fun expireAfterUpdate(ttl:Long): HashMapMaker<K, V> {
        _expireUpdateTTL = ttl
        return this
    }

    fun expireAfterUpdate(ttl:Long, unit: TimeUnit): HashMapMaker<K, V> {
        return expireAfterUpdate(unit.toMillis(ttl))
    }

    fun expireAfterGet(): HashMapMaker<K, V> {
        return expireAfterGet(-1)
    }

    fun expireAfterGet(ttl:Long): HashMapMaker<K, V> {
        _expireGetTTL = ttl
        return this
    }


    fun expireAfterGet(ttl:Long, unit: TimeUnit): HashMapMaker<K, V> {
        return expireAfterGet(unit.toMillis(ttl))
    }


    fun expireExecutor(executor: ScheduledExecutorService?): HashMapMaker<K, V> {
        _expireExecutor = executor;
        return this
    }

    fun expireExecutorPeriod(period:Long): HashMapMaker<K, V> {
        _expireExecutorPeriod = period
        return this
    }

    fun expireCompactThreshold(freeFraction: Double): HashMapMaker<K, V> {
        _expireCompactThreshold = freeFraction
        return this
    }


    fun expireMaxSize(maxSize:Long): HashMapMaker<K, V> {
        _expireMaxSize = maxSize;
        return counterEnable()
    }

    fun expireStoreSize(storeSize:Long): HashMapMaker<K, V> {
        _expireStoreSize = storeSize;
        return this
    }

    fun expireOverflow(overflowMap:MutableMap<K,V?>): HashMapMaker<K, V> {
        _expireOverflow = overflowMap
        return this
    }



    fun valueLoader(valueLoader:(key:K)->V): HashMapMaker<K, V> {
        _valueLoader = valueLoader
        return this
    }

    fun counterEnable(): HashMapMaker<K, V> {
        _counterEnable = true
        return this;
    }

    fun modificationListener(listener: MapModificationListener<K, V>): HashMapMaker<K, V> {
        if(_modListeners==null)
            _modListeners = ArrayList()
        _modListeners?.add(listener)
        return this;
    }

    override fun verify(){
        if (_expireOverflow != null && _valueLoader != null)
            throw DBException.WrongConfiguration("ExpireOverflow and ValueLoader can not be used at the same time")

        val expireOverflow = _expireOverflow
        if (expireOverflow != null) {
            //load non existing values from overflow
            _valueLoader = { key -> expireOverflow[key] }

            //forward modifications to overflow
            val listener = MapModificationListener<K, V> { key, oldVal, newVal, triggered ->
                if (!triggered && newVal == null && oldVal != null) {
                    //removal, also remove from overflow map
                    val oldVal2 = expireOverflow.remove(key)
                    if (oldVal2 != null && _valueSerializer.equals(oldVal as V, oldVal2 as V)) {
                        Utils.LOG.warning { "Key also removed from overflow Map, but value in overflow Map differs" }
                    }
                } else if (triggered && newVal == null) {
                    // triggered by eviction, put evicted entry into overflow map
                    expireOverflow.put(key, oldVal)
                }
            }
            _modListeners.add(listener)
        }

        if (_expireExecutor != null)
            db.executors.add(_expireExecutor!!)
    }

    override fun create2(catalog: SortedMap<String, String>): HTreeMap<K, V> {
        val segmentCount = 1.shl(_concShift)
        val hashSeed = _hashSeed ?: SecureRandom().nextInt()
        val stores = Array(segmentCount, _storeFactory)

        val rootRecids = LongArray(segmentCount)
        var rootRecidsStr = "";
        for (i in 0 until segmentCount) {
            val rootRecid = stores[i].put(IndexTreeListJava.dirEmpty(), IndexTreeListJava.dirSer)
            rootRecids[i] = rootRecid
            rootRecidsStr += (if (i == 0) "" else ",") + rootRecid
        }

        db.nameCatalogPutClass(catalog, name + if(hasValues) DB.Keys.keySerializer else DB.Keys.serializer, _keySerializer)
        if(hasValues) {
            db.nameCatalogPutClass(catalog, name + DB.Keys.valueSerializer, _valueSerializer)
        }
        if(hasValues)
            catalog[name + DB.Keys.valueInline] = _valueInline.toString()

        catalog[name + DB.Keys.rootRecids] = rootRecidsStr
        catalog[name + DB.Keys.hashSeed] = hashSeed.toString()
        catalog[name + DB.Keys.concShift] = _concShift.toString()
        catalog[name + DB.Keys.dirShift] = _dirShift.toString()
        catalog[name + DB.Keys.levels] = _levels.toString()
        catalog[name + DB.Keys.removeCollapsesIndexTree] = _removeCollapsesIndexTree.toString()

        val counterRecids = if (_counterEnable) {
            val cr = LongArray(segmentCount, { segment ->
                stores[segment].put(0L, Serializer.LONG_PACKED)
            })
            catalog[name + DB.Keys.counterRecids] = LongArrayList.newListWith(*cr).makeString("", ",", "")
            cr
        } else {
            catalog[name + DB.Keys.counterRecids] = ""
            null
        }

        catalog[name + DB.Keys.expireCreateTTL] = _expireCreateTTL.toString()
        if(hasValues)
            catalog[name + DB.Keys.expireUpdateTTL] = _expireUpdateTTL.toString()
        catalog[name + DB.Keys.expireGetTTL] = _expireGetTTL.toString()

        var createQ = LongArrayList()
        var updateQ = LongArrayList()
        var getQ = LongArrayList()


        fun emptyLongQueue(segment: Int, qq: LongArrayList): QueueLong {
            val store = stores[segment]
            val q = store.put(null, QueueLong.Node.SERIALIZER);
            val tailRecid = store.put(q, Serializer.RECID)
            val headRecid = store.put(q, Serializer.RECID)
            val headPrevRecid = store.put(0L, Serializer.RECID)
            qq.add(tailRecid)
            qq.add(headRecid)
            qq.add(headPrevRecid)
            return QueueLong(store = store, tailRecid = tailRecid, headRecid = headRecid, headPrevRecid = headPrevRecid)
        }

        val expireCreateQueues =
                if (_expireCreateTTL == 0L) null
                else Array(segmentCount, { emptyLongQueue(it, createQ) })

        val expireUpdateQueues =
                if (_expireUpdateTTL == 0L) null
                else Array(segmentCount, { emptyLongQueue(it, updateQ) })
        val expireGetQueues =
                if (_expireGetTTL == 0L) null
                else Array(segmentCount, { emptyLongQueue(it, getQ) })

        catalog[name + DB.Keys.expireCreateQueue] = createQ.makeString("", ",", "")
        if(hasValues)
            catalog[name + DB.Keys.expireUpdateQueue] = updateQ.makeString("", ",", "")
        catalog[name + DB.Keys.expireGetQueue] = getQ.makeString("", ",", "")

        val indexTrees = Array<MutableLongLongMap>(1.shl(_concShift), { segment ->
            IndexTreeLongLongMap(
                    store = stores[segment],
                    rootRecid = rootRecids[segment],
                    dirShift = _dirShift,
                    levels = _levels,
                    collapseOnRemove = _removeCollapsesIndexTree
            )
        })

        return HTreeMap(
                keySerializer = _keySerializer,
                valueSerializer = _valueSerializer,
                valueInline = _valueInline,
                concShift = _concShift,
                dirShift = _dirShift,
                levels = _levels,
                stores = stores,
                indexTrees = indexTrees,
                hashSeed = hashSeed,
                counterRecids = counterRecids,
                expireCreateTTL = _expireCreateTTL,
                expireUpdateTTL = _expireUpdateTTL,
                expireGetTTL = _expireGetTTL,
                expireMaxSize = _expireMaxSize,
                expireStoreSize = _expireStoreSize,
                expireCreateQueues = expireCreateQueues,
                expireUpdateQueues = expireUpdateQueues,
                expireGetQueues = expireGetQueues,
                expireExecutor = _expireExecutor,
                expireExecutorPeriod = _expireExecutorPeriod,
                expireCompactThreshold = _expireCompactThreshold,
                isThreadSafe = db.isThreadSafe,
                valueLoader = _valueLoader,
                modificationListeners = if (_modListeners.isEmpty()) null else _modListeners.toTypedArray(),
                closeable = db,
                hasValues = hasValues
        )
    }

    override fun open2(catalog: SortedMap<String, String>): HTreeMap<K, V> {
        _concShift = catalog[name + DB.Keys.concShift]!!.toInt()

        val segmentCount = 1.shl(_concShift)
        val stores = Array(segmentCount, _storeFactory)

        _keySerializer =
                db.nameCatalogGetClass(catalog, name + if(hasValues) DB.Keys.keySerializer else DB.Keys.serializer)
                ?: _keySerializer
        _valueSerializer = if(!hasValues) BTreeMap.NO_VAL_SERIALIZER as Serializer<V>
        else {
            db.nameCatalogGetClass(catalog, name + DB.Keys.valueSerializer)?: _valueSerializer
        }
        _valueInline = if(hasValues) catalog[name + DB.Keys.valueInline]!!.toBoolean() else true

        val hashSeed = catalog[name + DB.Keys.hashSeed]!!.toInt()
        val rootRecids = catalog[name + DB.Keys.rootRecids]!!.split(",").map { it.toLong() }.toLongArray()
        val counterRecidsStr = catalog[name + DB.Keys.counterRecids]!!
        val counterRecids =
                if ("" == counterRecidsStr) null
                else counterRecidsStr.split(",").map { it.toLong() }.toLongArray()

        _dirShift = catalog[name + DB.Keys.dirShift]!!.toInt()
        _levels = catalog[name + DB.Keys.levels]!!.toInt()
        _removeCollapsesIndexTree = catalog[name + DB.Keys.removeCollapsesIndexTree]!!.toBoolean()


        _expireCreateTTL = catalog[name + DB.Keys.expireCreateTTL]!!.toLong()
        _expireUpdateTTL = if(hasValues)catalog[name + DB.Keys.expireUpdateTTL]!!.toLong() else 0L
        _expireGetTTL = catalog[name + DB.Keys.expireGetTTL]!!.toLong()


        fun queues(ttl: Long, queuesName: String): Array<QueueLong>? {
            if (ttl == 0L)
                return null
            val rr = catalog[queuesName]!!.split(",").map { it.toLong() }.toLongArray()
            if (rr.size != segmentCount * 3)
                throw DBException.WrongConfiguration("wrong segment count");
            return Array(segmentCount, { segment ->
                QueueLong(store = stores[segment],
                        tailRecid = rr[segment * 3 + 0], headRecid = rr[segment * 3 + 1], headPrevRecid = rr[segment * 3 + 2]
                )
            })
        }

        val expireCreateQueues = queues(_expireCreateTTL, name + DB.Keys.expireCreateQueue)
        val expireUpdateQueues = queues(_expireUpdateTTL, name + DB.Keys.expireUpdateQueue)
        val expireGetQueues = queues(_expireGetTTL, name + DB.Keys.expireGetQueue)

        val indexTrees = Array<MutableLongLongMap>(1.shl(_concShift), { segment ->
            IndexTreeLongLongMap(
                    store = stores[segment],
                    rootRecid = rootRecids[segment],
                    dirShift = _dirShift,
                    levels = _levels,
                    collapseOnRemove = _removeCollapsesIndexTree
            )
        })
        return HTreeMap(
                keySerializer = _keySerializer,
                valueSerializer = _valueSerializer,
                valueInline = _valueInline,
                concShift = _concShift,
                dirShift = _dirShift,
                levels = _levels,
                stores = stores,
                indexTrees = indexTrees,
                hashSeed = hashSeed,
                counterRecids = counterRecids,
                expireCreateTTL = _expireCreateTTL,
                expireUpdateTTL = _expireUpdateTTL,
                expireGetTTL = _expireGetTTL,
                expireMaxSize = _expireMaxSize,
                expireStoreSize = _expireStoreSize,
                expireCreateQueues = expireCreateQueues,
                expireUpdateQueues = expireUpdateQueues,
                expireGetQueues = expireGetQueues,
                expireExecutor = _expireExecutor,
                expireExecutorPeriod = _expireExecutorPeriod,
                expireCompactThreshold = _expireCompactThreshold,
                isThreadSafe = db.isThreadSafe,
                valueLoader = _valueLoader,
                modificationListeners = if (_modListeners.isEmpty()) null else _modListeners.toTypedArray(),
                closeable = db,
                hasValues = hasValues
        )
    }
}