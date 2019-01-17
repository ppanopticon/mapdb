package org.mapdb.data.treemap

import org.mapdb.DB
import org.mapdb.data.Maker
import org.mapdb.serializer.GroupSerializer
import java.util.*

class TreeSetMaker<E>(override val db: DB, override val name:String) : Maker<NavigableSet<E>>(){

    protected val maker = TreeMapMaker<E, Any?>(db, name, hasValues = false)

    override val type = "TreeSet"

    fun <A> serializer(serializer: GroupSerializer<A>):TreeSetMaker<A>{
        maker.keySerializer(serializer)
        return this as TreeSetMaker<A>
    }

    fun maxNodeSize(size:Int):TreeSetMaker<E>{
        maker.maxNodeSize(size)
        return this;
    }

    fun counterEnable():TreeSetMaker<E>{
        maker.counterEnable()
        return this;
    }

    override fun verify() {
        maker.`%%%verify`()
    }

    override fun open2(catalog: SortedMap<String, String>): NavigableSet<E> {
        return maker.`%%%open2`(catalog).keys as NavigableSet<E>
    }

    override fun create2(catalog: SortedMap<String, String>): NavigableSet<E> {
        return maker.`%%%create2`(catalog).keys as NavigableSet<E>
    }
}