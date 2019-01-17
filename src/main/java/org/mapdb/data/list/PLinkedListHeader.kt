package org.mapdb.data.list

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * The fixed header of a [PLinkedList].
 */
internal data class PLinkedListHeader (var size: Int, var first: Long, var last: Long)

/**
 * [Serializer] for a [PLinkedListHeader].
 */
internal object PDLinkedListHeaderSerializer : Serializer<PLinkedListHeader> {

    /**
     *
     */
    override fun serialize(out: DataOutput2, value: PLinkedListHeader) {
        out.writeInt(value.size)
        out.writeLong(value.first)
        out.writeLong(value.last)
    }

    /**
     *
     */
    override fun deserialize(input: DataInput2, available: Int): PLinkedListHeader = PLinkedListHeader(input.readInt(), input.readLong(), input.readLong())
}