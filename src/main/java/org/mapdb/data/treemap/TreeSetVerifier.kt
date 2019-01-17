package org.mapdb.data.treemap

import org.mapdb.DB
import org.mapdb.data.Verifier


internal object TreeSetVerifier : Verifier() {
    override val expected: Map<String, DB.CatVal> = mapOf(
            Pair(DB.Keys.serializer, DB.CatVal(serializer, required = false)),
            Pair(DB.Keys.rootRecidRecid, DB.CatVal(recid)),
            Pair(DB.Keys.counterRecid, DB.CatVal(recidOptional)),
            Pair(DB.Keys.maxNodeSize, DB.CatVal(int))
    )
}