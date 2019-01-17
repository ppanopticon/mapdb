package org.mapdb.data.list

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A element of the [PLinkedList] that holds a payload and points to the next item.
 *
 * @param next Pointer to the next item.
 * @param data Payload of this [PLinkedListElement]. Cannot be null!
 */
internal data class PLinkedListElement<T>(var next: Long, var data: T)

/**
 * [Serializer] for a [PLinkedListElement].
 *
 * @param _inner The [Serializer] used to serialize the payload of this [PLinkedListElement].
 */
internal class PLinkedListElementSerializer<T> (val _inner: Serializer<T>) : Serializer<PLinkedListElement<T>> {

    /**
     *
     */
    override fun serialize(out: DataOutput2, value: PLinkedListElement<T>) {
        out.packLong(value.next)
        this._inner.serialize(out, value.data)
    }

    /**
     *
     */
    override fun deserialize(input: DataInput2, available: Int): PLinkedListElement<T> = PLinkedListElement(input.unpackLong(), _inner.deserialize(input, available))
}