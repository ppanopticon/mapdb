package org.mapdb

import org.mapdb.data.hashmap.HTreeMap
import org.mapdb.jsr166Tests.ConcurrentHashMapV8Test
import java.util.concurrent.ConcurrentMap

/**
 * Created by jan on 4/2/16.
 */
class HtreeMapV8Test: ConcurrentHashMapV8Test() {

    override fun newMap(): ConcurrentMap<*, *>? {
        return HTreeMap.make<Any,Any>()
    }

    override fun newMap(size: Int): ConcurrentMap<*, *>? {
        return newMap()
    }

}