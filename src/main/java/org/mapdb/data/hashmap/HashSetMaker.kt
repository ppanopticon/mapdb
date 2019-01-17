package org.mapdb.data.hashmap

import org.mapdb.DB
import org.mapdb.Serializer
import org.mapdb.Store
import org.mapdb.data.Maker
import org.mapdb.data.indextree.IndexTreeListVerifier
import org.mapdb.data.treemap.BTreeMap
import java.util.*
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

class HashSetMaker<E>(
        override val db: DB,
        override val name:String,
        protected val _storeFactory:(segment:Int)-> Store = { i-> db.store}

) : Maker<HTreeMap.KeySet<E>>(){

    protected val maker = HashMapMaker<E, Any?>(db, name, hasValues = false, _storeFactory = _storeFactory)

    override val type = "HashSet"

    init{
        maker.valueSerializer(BTreeMap.NO_VAL_SERIALIZER).valueInline()
    }

    fun <A> serializer(serializer: Serializer<A>):HashSetMaker<A>{
        maker.keySerializer(serializer)
        return this as HashSetMaker<A>
    }

    fun counterEnable():HashSetMaker<E>{
        maker.counterEnable()
        return this;
    }

    fun removeCollapsesIndexTreeDisable():HashSetMaker<E>{
        maker.removeCollapsesIndexTreeDisable()
        return this
    }

    fun hashSeed(hashSeed:Int):HashSetMaker<E>{
        maker.hashSeed(hashSeed)
        return this
    }

    fun layout(concurrency:Int, dirSize:Int, levels:Int):HashSetMaker<E>{
        maker.layout(concurrency, dirSize, levels)
        return this
    }

    fun expireAfterCreate():HashSetMaker<E>{
        return expireAfterCreate(-1)
    }

    fun expireAfterCreate(ttl:Long):HashSetMaker<E>{
        maker.expireAfterCreate(ttl)
        return this
    }


    fun expireAfterCreate(ttl:Long, unit: TimeUnit):HashSetMaker<E> {
        return expireAfterCreate(unit.toMillis(ttl))
    }

    fun expireAfterGet():HashSetMaker<E>{
        return expireAfterGet(-1)
    }

    fun expireAfterGet(ttl:Long):HashSetMaker<E>{
        maker.expireAfterGet(ttl)
        return this
    }


    fun expireAfterGet(ttl:Long, unit: TimeUnit):HashSetMaker<E> {
        return expireAfterGet(unit.toMillis(ttl))
    }


    fun expireExecutor(executor: ScheduledExecutorService?):HashSetMaker<E>{
        maker.expireExecutor(executor)
        return this
    }

    fun expireExecutorPeriod(period:Long):HashSetMaker<E>{
        maker.expireExecutorPeriod(period)
        return this
    }

    fun expireCompactThreshold(freeFraction: Double):HashSetMaker<E>{
        maker.expireCompactThreshold(freeFraction)
        return this
    }


    fun expireMaxSize(maxSize:Long):HashSetMaker<E>{
        maker.expireMaxSize(maxSize)
        return this
    }

    fun expireStoreSize(storeSize:Long):HashSetMaker<E>{
        maker.expireStoreSize(storeSize)
        return this
    }

    override fun verify() {
        maker.`%%%verify`()
    }

    override fun open2(catalog: SortedMap<String, String>): HTreeMap.KeySet<E> {
        return maker.`%%%open2`(catalog).keys as HTreeMap.KeySet<E>
    }

    override fun create2(catalog: SortedMap<String, String>): HTreeMap.KeySet<E> {
        return maker.`%%%create2`(catalog).keys as HTreeMap.KeySet<E>
    }
}