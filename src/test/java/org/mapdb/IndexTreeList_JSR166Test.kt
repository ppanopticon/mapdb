package org.mapdb

import org.mapdb.data.indextree.IndexTreeList
import org.mapdb.data.indextree.IndexTreeLongLongMap
import org.mapdb.jsr166Tests.CopyOnWriteArrayListTest

class IndexTreeList_JSR166Test:CopyOnWriteArrayListTest(){

    override fun emptyArray():MutableList<Int?> {
        val store = StoreDirect.make();
        val index = IndexTreeLongLongMap.make(store)
        val list = IndexTreeList(store = store, serializer = Serializer.INTEGER, isThreadSafe = true,
                map = index, counterRecid = store.put(0L, Serializer.LONG_PACKED))
        return list
    }

}