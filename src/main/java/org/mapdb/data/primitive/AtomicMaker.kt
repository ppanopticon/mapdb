package org.mapdb.data.primitive

import org.mapdb.DB
import org.mapdb.Serializer
import org.mapdb.data.Maker
import java.util.*

class AtomicBooleanMaker(override val db: DB, override val name:String, protected val value:Boolean=false): Maker<Atomic.Boolean>(){

    override val type = "AtomicBoolean"

    override fun create2(catalog: SortedMap<String, String>): Atomic.Boolean {
        val recid = db.store.put(value, Serializer.BOOLEAN)
        catalog[name+ DB.Keys.recid] = recid.toString()
        return Atomic.Boolean(db.store, recid)
    }

    override fun open2(catalog: SortedMap<String, String>): Atomic.Boolean {
        val recid = catalog[name+ DB.Keys.recid]!!.toLong()
        return Atomic.Boolean(db.store, recid)
    }
}

class AtomicIntegerMaker(override val db: DB, override val name:String, protected val value:Int=0): Maker<Atomic.Integer>(){

    override val type = "AtomicInteger"

    override fun create2(catalog: SortedMap<String, String>): Atomic.Integer {
        val recid = db.store.put(value, Serializer.INTEGER)
        catalog[name+ DB.Keys.recid] = recid.toString()
        return Atomic.Integer(db.store, recid)
    }

    override fun open2(catalog: SortedMap<String, String>): Atomic.Integer {
        val recid = catalog[name+ DB.Keys.recid]!!.toLong()
        return Atomic.Integer(db.store, recid)
    }
}

class AtomicLongMaker(override val db: DB, override val name:String, protected val value:Long=0): Maker<Atomic.Long>(){

    override val type = "AtomicLong"

    override fun create2(catalog: SortedMap<String, String>): Atomic.Long {
        val recid = db.store.put(value, Serializer.LONG)
        catalog[name+ DB.Keys.recid] = recid.toString()
        return Atomic.Long(db.store, recid)
    }

    override fun open2(catalog: SortedMap<String, String>): Atomic.Long {
        val recid = catalog[name+ DB.Keys.recid]!!.toLong()
        return Atomic.Long(db.store, recid)
    }
}

class AtomicStringMaker(override val db: DB, override val name:String, protected val value:String?=null): Maker<Atomic.String>(){

    override val type = "AtomicString"

    override fun create2(catalog: SortedMap<String, String>): Atomic.String {
        val recid = db.store.put(value, Serializer.STRING_NOSIZE)
        catalog[name+ DB.Keys.recid] = recid.toString()
        return Atomic.String(db.store, recid)
    }

    override fun open2(catalog: SortedMap<String, String>): Atomic.String {
        val recid = catalog[name+ DB.Keys.recid]!!.toLong()
        return Atomic.String(db.store, recid)
    }
}

class AtomicVarMaker<E>(override val db: DB, override val name:String, protected val serializer: Serializer<E> = db.defaultSerializer as Serializer<E>, protected val value:E? = null):Maker<Atomic.Var<E>>(){

    override val type = "AtomicVar"

    override fun create2(catalog: SortedMap<String, String>): Atomic.Var<E> {
        val recid = db.store.put(value, serializer)
        catalog[name+ DB.Keys.recid] = recid.toString()
        db.nameCatalogPutClass(catalog, name+ DB.Keys.serializer, serializer)

        return Atomic.Var(db.store, recid, serializer)
    }

    override fun open2(catalog: SortedMap<String, String>): Atomic.Var<E> {
        val recid = catalog[name+ DB.Keys.recid]!!.toLong()
        val serializer = db.nameCatalogGetClass<Serializer<E>>(catalog, name+ DB.Keys.serializer)
                ?: this.serializer
        return Atomic.Var(db.store, recid, serializer)
    }
}