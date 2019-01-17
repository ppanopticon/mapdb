package org.mapdb

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.data.treemap.TreeMapMaker
import java.io.IOException
import java.util.*


@RunWith(Parameterized::class)
class BTreeMap_PumpTest(
        val mapMaker: TreeMapMaker<Any?, Any?>
) {

    companion object {

        @Parameterized.Parameters
        @Throws(IOException::class)
        @JvmStatic
        fun params(): Iterable<Any> {
            val ret = ArrayList<Any>()

            val bools = if(TT.shortTest()) TT.boolsFalse else TT.bools

            for(valueInline in bools)
            for(otherComparator in bools)
            for(small in bools)
            for(generic in bools)
            for(storeType in 0..2)
            for(isThreadSafe in bools)
            for(counter in bools){
                val db:DB = when(storeType){
                    0-> {
                        val d = DBMaker.heapDB()
                        if(!isThreadSafe) d.concurrencyDisable()
                        d.make()
                    }
                    1-> DB(StoreTrivial(), isThreadSafe = isThreadSafe, storeOpened = false)
                    2-> {
                        val d = DBMaker.memoryDB()
                        if(!isThreadSafe) d.concurrencyDisable()
                        d.make()
                    }
                    else -> throw AssertionError()
                }

                val m =
                        if(generic) db.treeMap("aa")
                        else db.treeMap("aa", Serializer.INTEGER, Serializer.STRING)

                if(small) m.maxNodeSize(6)
                if(!valueInline) m.valuesOutsideNodesEnable()

                if(counter) m.counterEnable()

                ret += m
            }

            return ret
        }
    }

    @Test fun test(){
        val limit = (100 + TT.testScale()*1e6).toInt()

        val sink = mapMaker.createFromSink()
        (0 until limit).forEach {
            sink.put(it, it.toString())
        }
        val map = sink.create()

        (0 until limit).forEach {
            assert(it.toString()==map[it])
        }

        val iter = map.entryIterator()
        (0 until limit).forEach {
            val e = iter.next()
            assert(it == e.key)
            assert(it.toString() == e.value)
        }

        val iterRev = map.descendingKeyIterator()
        (limit-1 downTo 0).forEach {
            assert(it == iterRev.next())
        }

        assert(map.size == limit)
        assert(map.count{true} == limit)

        map.verify()
    }

}