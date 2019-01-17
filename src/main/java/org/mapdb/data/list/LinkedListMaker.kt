package org.mapdb.data.list

import org.mapdb.DB
import org.mapdb.Serializer
import org.mapdb.data.Maker
import org.mapdb.data.list.LinkedListVerifier

import java.util.*


/**
 * A [DB.Maker] implementation for [PLinkedList]. Together with the extension function, this allows for
 * seamless integration with MapDB's [DB] class.
 */
class LinkedListMaker<T>(override val db: DB, override val name: String): Maker<PLinkedList<T>>(){
    override val type: String = "LinkedList"

    /** The [Serializer] used to serialize the payload. */
    private var _serializer: Serializer<T> = db.defaultSerializer as Serializer<T>

    /**
     * Applies a new [Serializer] and returns the modified [LinkedListMaker] instance.
     */
    fun <A> serializer(serializer: Serializer<A>): LinkedListMaker<A> {
        _serializer = serializer as Serializer<T>
        return this as LinkedListMaker<A>
    }

    /**
     * Initializes and returns a new [PLinkedList]
     */
    override fun create2(catalog: SortedMap<String, String>): PLinkedList<T> {
        /* Initialize PLinkedList. */
        val store = this.db.getStore()
        val root = store.put(PLinkedListHeader(0, PLinkedList.VOID_RECORD_ID, PLinkedList.VOID_RECORD_ID), PDLinkedListHeaderSerializer)
        catalog[name + DB.Keys.rootRecid] = root.toString()


        return PLinkedList(store, this.db, this._serializer, root)
    }

    override fun open2(catalog: SortedMap<String, String>): PLinkedList<T> {
        val root = catalog[name + DB.Keys.rootRecid]!!.toLong()
        return PLinkedList(this.db.getStore(), this.db, this._serializer, root)
    }
}

