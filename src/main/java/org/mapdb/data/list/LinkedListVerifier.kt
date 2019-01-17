package org.mapdb.data.list

import org.mapdb.DB
import org.mapdb.data.Verifier
import org.mapdb.data.indextree.IndexTreeListVerifier

object LinkedListVerifier : Verifier() {
    override val expected: Map<String, DB.CatVal> = mapOf(
            Pair(DB.Keys.recid, DB.CatVal(IndexTreeListVerifier.recid, required = true)),
            Pair(DB.Keys.serializer, DB.CatVal(IndexTreeListVerifier.serializer, required = true))
    )
}