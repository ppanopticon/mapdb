package org.mapdb.data.indextree

import org.mapdb.*
import org.mapdb.data.Maker
import java.util.*

class IndexTreeListMaker<E>(override val db: DB, override val name:String, protected val serializer: Serializer<E>): Maker<IndexTreeList<E>>(){

    private var _dirShift = CC.HTREEMAP_DIR_SHIFT
    private var _levels = CC.HTREEMAP_LEVELS
    private var _removeCollapsesIndexTree:Boolean = true

    override val type = "IndexTreeList"

    fun layout(dirSize:Int, levels:Int):IndexTreeListMaker<E>{
        fun toShift(value:Int):Int{
            return 31 - Integer.numberOfLeadingZeros(DataIO.nextPowTwo(Math.max(1,value)))
        }
        _dirShift = toShift(dirSize)
        _levels = levels
        return this
    }


    fun removeCollapsesIndexTreeDisable():IndexTreeListMaker<E>{
        _removeCollapsesIndexTree = false
        return this
    }

    override fun create2(catalog: SortedMap<String, String>): IndexTreeList<E> {
        catalog[name+ DB.Keys.dirShift] = _dirShift.toString()
        catalog[name+ DB.Keys.levels] = _levels.toString()
        catalog[name + DB.Keys.removeCollapsesIndexTree] = _removeCollapsesIndexTree.toString()
        db.nameCatalogPutClass(catalog, name + DB.Keys.serializer, serializer)

        val counterRecid = db.store.put(0L, Serializer.LONG_PACKED)
        catalog[name+ DB.Keys.counterRecid] = counterRecid.toString()
        val rootRecid = db.store.put(IndexTreeListJava.dirEmpty(), IndexTreeListJava.dirSer)
        catalog[name+ DB.Keys.rootRecid] = rootRecid.toString()
        val map = IndexTreeLongLongMap(
                store = db.store,
                rootRecid = rootRecid,
                dirShift = _dirShift,
                levels = _levels,
                collapseOnRemove = _removeCollapsesIndexTree);

        return IndexTreeList(
                store = db.store,
                map = map,
                serializer = serializer,
                isThreadSafe = db.isThreadSafe,
                counterRecid = counterRecid
        )
    }

    override fun open2(catalog: SortedMap<String, String>): IndexTreeList<E> {
        val map = IndexTreeLongLongMap(
                store = db.store,
                dirShift = catalog[name + DB.Keys.dirShift]!!.toInt(),
                levels = catalog[name + DB.Keys.levels]!!.toInt(),
                rootRecid = catalog[name + DB.Keys.rootRecid]!!.toLong(),
                collapseOnRemove = catalog[name + DB.Keys.removeCollapsesIndexTree]!!.toBoolean())
        return IndexTreeList(
                store = db.store,
                map = map,
                serializer = db.nameCatalogGetClass(catalog, name + DB.Keys.serializer) ?: serializer,
                isThreadSafe = db.isThreadSafe,
                counterRecid = catalog[name + DB.Keys.counterRecid]!!.toLong()
        )
    }
}