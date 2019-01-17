package org.mapdb.data.treemap

import org.mapdb.*
import org.mapdb.data.Maker
import org.mapdb.serializer.GroupSerializer
import java.util.*

class TreeMapMaker<K,V>(override val db: DB, override val name:String, val hasValues:Boolean=true): Maker<BTreeMap<K, V>>(){

    override val type = "TreeMap"

    private var _keySerializer: GroupSerializer<K> = db.defaultSerializer as GroupSerializer<K>
    private var _valueSerializer: GroupSerializer<V> =
            (if(hasValues) db.defaultSerializer else BTreeMap.NO_VAL_SERIALIZER) as GroupSerializer<V>
    private var _maxNodeSize = CC.BTREEMAP_MAX_NODE_SIZE
    private var _counterEnable: Boolean = false
    private var _valueLoader:((key:K)->V)? = null
    private var _modListeners:MutableList<MapModificationListener<K, V>>? = null

    private var _rootRecidRecid:Long? = null
    private var _counterRecid:Long? = null
    private var _valueInline:Boolean = true

    fun <A> keySerializer(keySerializer: GroupSerializer<A>):TreeMapMaker<A,V>{
        _keySerializer = keySerializer as GroupSerializer<K>
        return this as TreeMapMaker<A, V>
    }

    fun <A> valueSerializer(valueSerializer: GroupSerializer<A>):TreeMapMaker<K,A>{
        if(!hasValues)
            throw DBException.WrongConfiguration("Set, no vals")
        _valueSerializer = valueSerializer as GroupSerializer<V>
        return this as TreeMapMaker<K, A>
    }
//
//        fun valueLoader(valueLoader:(key:K)->V):TreeMapMaker<K,V>{
//            //TODO BTree value loader
//            _valueLoader = valueLoader
//            return this
//        }


    fun maxNodeSize(size:Int):TreeMapMaker<K,V>{
        _maxNodeSize = size
        return this;
    }

    fun counterEnable():TreeMapMaker<K,V>{
        _counterEnable = true
        return this;
    }

    fun valuesOutsideNodesEnable():TreeMapMaker<K,V>{
        _valueInline = false
        return this;
    }

    fun modificationListener(listener: MapModificationListener<K, V>):TreeMapMaker<K,V>{
        if(_modListeners==null)
            _modListeners = ArrayList()
        _modListeners?.add(listener)
        return this;
    }


    fun createFrom(iterator:Iterator<Pair<K,V>>): BTreeMap<K, V> {
        val consumer = createFromSink()
        while(iterator.hasNext()){
            consumer.put(iterator.next())
        }
        return consumer.create()
    }

    fun createFromSink(): DB.TreeMapSink<K, V> {

        val consumer = Pump.treeMap(
                store = db.store,
                keySerializer = _keySerializer,
                valueSerializer = _valueSerializer,
                //TODO add custom comparator, once its enabled
                dirNodeSize = _maxNodeSize *3/4,
                leafNodeSize = _maxNodeSize *3/4,
                valueInline = _valueInline
        )

        return object: DB.TreeMapSink<K, V>(){

            override fun put(e: Pair<K, V>) {
                consumer.put(e)
            }

            override fun create(): BTreeMap<K, V> {
                consumer.create()
                this@TreeMapMaker._rootRecidRecid = consumer.rootRecidRecid
                        ?: throw AssertionError()
                this@TreeMapMaker._counterRecid =
                        if(_counterEnable) db.store.put(consumer.counter, Serializer.LONG)
                        else 0L
                return this@TreeMapMaker.make2(create=true)
            }

        }
    }

    override fun create2(catalog: SortedMap<String, String>): BTreeMap<K, V> {
        db.nameCatalogPutClass(catalog, name +
                (if(hasValues) DB.Keys.keySerializer else DB.Keys.serializer), _keySerializer)
        if(hasValues) {
            db.nameCatalogPutClass(catalog, name + DB.Keys.valueSerializer, _valueSerializer)
            catalog[name + DB.Keys.valueInline] = _valueInline.toString()
        }

        val rootRecidRecid2 = _rootRecidRecid
                ?: BTreeMap.putEmptyRoot(db.store, _keySerializer , _valueSerializer)
        catalog[name + DB.Keys.rootRecidRecid] = rootRecidRecid2.toString()

        val counterRecid2 =
                if (_counterEnable) _counterRecid ?: db.store.put(0L, Serializer.LONG)
                else 0L
        catalog[name + DB.Keys.counterRecid] = counterRecid2.toString()

        catalog[name + DB.Keys.maxNodeSize] = _maxNodeSize.toString()

        return BTreeMap(
                keySerializer = _keySerializer,
                valueSerializer = _valueSerializer,
                rootRecidRecid = rootRecidRecid2,
                store = db.store,
                maxNodeSize = _maxNodeSize,
                comparator = _keySerializer, //TODO custom comparator
                isThreadSafe = db.isThreadSafe,
                counterRecid = counterRecid2,
                hasValues = hasValues,
                valueInline = _valueInline,
                modificationListeners = if (_modListeners == null) null else _modListeners!!.toTypedArray()
        )
    }

    override fun open2(catalog: SortedMap<String, String>): BTreeMap<K, V> {
        val rootRecidRecid2 = catalog[name + DB.Keys.rootRecidRecid]!!.toLong()

        _keySerializer =
                db.nameCatalogGetClass(catalog, name +
                        if(hasValues) DB.Keys.keySerializer else DB.Keys.serializer)
                ?: _keySerializer
        _valueSerializer =
                if(!hasValues) {
                    BTreeMap.NO_VAL_SERIALIZER as GroupSerializer<V>
                }else {
                    db.nameCatalogGetClass(catalog, name + DB.Keys.valueSerializer) ?: _valueSerializer
                }

        val counterRecid2 = catalog[name + DB.Keys.counterRecid]!!.toLong()
        _maxNodeSize = catalog[name + DB.Keys.maxNodeSize]!!.toInt()

        //TODO compatibility with older versions, remove before stable version
        if(_valueSerializer!= BTreeMap.Companion.NO_VAL_SERIALIZER &&
                catalog[name + DB.Keys.valueInline]==null
                && db.store.isReadOnly.not()){
            //patch store with default value
            catalog[name + DB.Keys.valueInline] = "true"
            db.nameCatalogSaveLocked(catalog)
        }

        _valueInline = (catalog[name + DB.Keys.valueInline]?:"true").toBoolean()
        return BTreeMap(
                keySerializer = _keySerializer,
                valueSerializer = _valueSerializer,
                rootRecidRecid = rootRecidRecid2,
                store = db.store,
                maxNodeSize = _maxNodeSize,
                comparator = _keySerializer, //TODO custom comparator
                isThreadSafe = db.isThreadSafe,
                counterRecid = counterRecid2,
                hasValues = hasValues,
                valueInline = _valueInline,
                modificationListeners = if (_modListeners == null) null else _modListeners!!.toTypedArray()
        )
    }
}