package org.mapdb.data.indextree

import org.mapdb.DB
import org.mapdb.data.Verifier

internal object IndexTreeListVerifier : Verifier() {
    override val expected: Map<String, DB.CatVal> = mapOf(
            Pair(DB.Keys.serializer, DB.CatVal(serializer, required = false)),
            Pair(DB.Keys.dirShift, DB.CatVal(int)),
            Pair(DB.Keys.levels, DB.CatVal(int)),
            Pair(DB.Keys.removeCollapsesIndexTree, DB.CatVal(boolean)),
            Pair(DB.Keys.counterRecid, DB.CatVal(recid)),
            Pair(DB.Keys.rootRecid, DB.CatVal(recid))
    )
}