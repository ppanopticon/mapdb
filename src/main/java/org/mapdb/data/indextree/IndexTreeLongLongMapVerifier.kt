package org.mapdb.data.indextree

import org.mapdb.DB
import org.mapdb.data.Verifier

internal object IndexTreeLongLongMapVerifier : Verifier() {
    override val expected: Map<String, DB.CatVal> = mapOf(
            Pair(DB.Keys.dirShift, DB.CatVal(int)),
            Pair(DB.Keys.levels, DB.CatVal(int)),
            Pair(DB.Keys.removeCollapsesIndexTree, DB.CatVal(boolean)),
            Pair(DB.Keys.rootRecid, DB.CatVal(recid))
    )
}