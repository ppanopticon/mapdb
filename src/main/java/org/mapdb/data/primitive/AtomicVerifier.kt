package org.mapdb.data.primitive

import org.mapdb.DB
import org.mapdb.data.Verifier


internal object AtomicPrimitiveVerifier : Verifier() {
    override val expected: Map<String, DB.CatVal> = mapOf(
            Pair(DB.Keys.recid, DB.CatVal(recid))
    )
}

internal object AtomicVarVerifier : Verifier() {
    override val expected: Map<String, DB.CatVal> = mapOf(
            Pair(DB.Keys.recid, DB.CatVal(recid)),
            Pair(DB.Keys.serializer, DB.CatVal(serializer, false))
    )
}