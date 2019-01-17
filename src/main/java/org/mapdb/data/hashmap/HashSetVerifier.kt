package org.mapdb.data.hashmap

import org.mapdb.DB
import org.mapdb.data.Verifier

internal object HashSetVerifier : Verifier() {
    override val expected: Map<String, DB.CatVal> = mapOf(
            Pair(DB.Keys.serializer, DB.CatVal(serializer, required = false)),
            Pair(DB.Keys.rootRecids, DB.CatVal(recidArray)),
            Pair(DB.Keys.hashSeed, DB.CatVal(int)),
            Pair(DB.Keys.concShift, DB.CatVal(int)),
            Pair(DB.Keys.dirShift, DB.CatVal(int)),
            Pair(DB.Keys.levels, DB.CatVal(int)),
            Pair(DB.Keys.removeCollapsesIndexTree, DB.CatVal(boolean)),
            Pair(DB.Keys.counterRecids, DB.CatVal(recidArray)),
            Pair(DB.Keys.expireCreateQueue, DB.CatVal(all)),
            Pair(DB.Keys.expireGetQueue, DB.CatVal(all)),
            Pair(DB.Keys.expireCreateTTL, DB.CatVal(long)),
            Pair(DB.Keys.expireGetTTL, DB.CatVal(long))
    )
}