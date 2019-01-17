package org.mapdb.data.indextree

import org.mapdb.CC
import org.mapdb.DB
import org.mapdb.DataIO
import org.mapdb.data.Maker

import java.util.*

internal class IndexTreeLongLongMapMaker(override val db: DB, override val name:String): Maker<IndexTreeLongLongMap>(){

    private var _dirShift = CC.HTREEMAP_DIR_SHIFT
    private var _levels = CC.HTREEMAP_LEVELS
    private var _removeCollapsesIndexTree:Boolean = true

    override val type = "IndexTreeLongLongMap"

    fun layout(dirSize:Int, levels:Int):IndexTreeLongLongMapMaker{
        fun toShift(value:Int):Int{
            return 31 - Integer.numberOfLeadingZeros(DataIO.nextPowTwo(Math.max(1,value)))
        }
        _dirShift = toShift(dirSize)
        _levels = levels
        return this
    }


    fun removeCollapsesIndexTreeDisable():IndexTreeLongLongMapMaker{
        _removeCollapsesIndexTree = false
        return this
    }



    override fun create2(catalog: SortedMap<String, String>): IndexTreeLongLongMap {
        catalog[name+ DB.Keys.dirShift] = _dirShift.toString()
        catalog[name+ DB.Keys.levels] = _levels.toString()
        catalog[name + DB.Keys.removeCollapsesIndexTree] = _removeCollapsesIndexTree.toString()

        val rootRecid = db.store.put(IndexTreeListJava.dirEmpty(), IndexTreeListJava.dirSer)
        catalog[name+ DB.Keys.rootRecid] = rootRecid.toString()
        return IndexTreeLongLongMap(
                store = db.store,
                rootRecid = rootRecid,
                dirShift = _dirShift,
                levels = _levels,
                collapseOnRemove = _removeCollapsesIndexTree);
    }

    override fun open2(catalog: SortedMap<String, String>): IndexTreeLongLongMap {
        return IndexTreeLongLongMap(
                store = db.store,
                dirShift = catalog[name + DB.Keys.dirShift]!!.toInt(),
                levels = catalog[name + DB.Keys.levels]!!.toInt(),
                rootRecid = catalog[name + DB.Keys.rootRecid]!!.toLong(),
                collapseOnRemove = catalog[name + DB.Keys.removeCollapsesIndexTree]!!.toBoolean())
    }
}