package org.mapdb.data.list

import org.mapdb.DBException
import org.mapdb.Serializer
import org.mapdb.Store

import java.io.Closeable
import java.lang.IllegalArgumentException

/**
 *
 */
class PLinkedList<T> (val _store: Store, val _closeable: Closeable, _inner: Serializer<T>, val _root: Long = _store.preallocate()) : MutableList<T>, Closeable {
    /** The [PLinkedListHeader] instance for this [PLinkedList]. */
    private val _header : PLinkedListHeader = this._store.get(_root, PDLinkedListHeaderSerializer) ?: throw DBException("No valid header found for PLinkedList. File may be corrupt.")

    /** Instance used for de-/serialization of the element held by this [PLinkedList]. */
    private val _serializer: PLinkedListElementSerializer<T> = PLinkedListElementSerializer(_inner)

    /** Companion object defining some constants. */
    companion object {
        val VOID_RECORD_ID : Long = Long.MIN_VALUE
    }

    /**
     * Returns the size of this [PLinkedList], i.e. the number of elements it holds.
     *
     * @return Size of this [PLinkedList]
     */
    override val size: Int
        get() = this._header.size

    /**
     * Returns true if this [PLinkedList] is empty and false otherwise.
     *
     * @return True if this [PLinkedList] is empty and false otherwise.
     */
    override fun isEmpty(): Boolean = this._header.size == 0

    /**
     * Retrieves and returns the element at the specified position from [PLinkedList].
     *
     * @param index Position of the desired element (0 based).
     * @return Element at the specified position.
     */
    override fun get(index: Int): T = seek(index).second.data

    /**
     * Returns true if the element is contained in the [PLinkedList].
     *
     * @param element Element that is sought.
     * @return True if element is contained, false otherwise.
     */
    override fun contains(element: T): Boolean = indexOf(element) > -1

    /**
     * Returns true if all the provided elements are contained in the [PLinkedList].
     *
     * @param element Element that is sought.
     * @return True if element is contained, false otherwise.
     */
    override fun containsAll(elements: Collection<T>): Boolean = elements.all { e -> contains(e) }

    /**
     * Returns the index of the first occurrence of the provided element or -1 if the element is not contained in this [PLinkedList].
     *
     * @param element Element that is sought.
     * @return Index of element or -1 if it doesn't exist.
     */
    override fun indexOf(element: T): Int {
        var current: PLinkedListElement<T> = this._store.get(this._header.first, this._serializer) ?: throw DBException("Internal error: Header does not point to valid entry.")
        if (element == current.data) return 0
        for (i in 1..this._header.size) {
            if (element == current.data) return i
            current = this._store.get(current.next, this._serializer) ?: throw DBException("Internal error: Header does not point to valid entry.")
        }
        return -1
    }

    /**
     * Returns the index of the last occurrence of the provided element or -1 if the element is not contained in this [PLinkedList].
     *
     * @param element Element that is sought.
     * @return Index of element or -1 if it doesn't exist.
     */
    override fun lastIndexOf(element: T): Int {
        var current: PLinkedListElement<T> = this._store.get(this._header.first, this._serializer) ?: throw DBException("Internal error: Header does not point to valid entry.")
        var index = -1;
        if (element == current.data) index = 0
        for (i in 1..this._header.size) {
            if (element == current.data) {
                index = i
            }
            current = this._store.get(current.next, this._serializer) ?: throw DBException("Internal error: Header does not point to valid entry.")
        }
        return index
    }

    /**
     * Returns a [MutableIterator] for the current [PLinkedList].
     */
    override fun iterator(): MutableIterator<T> = object: MutableIterator<T> {
        /* Pointer to previous element. */
        var _previous = VOID_RECORD_ID

        /* Pointer to current element. */
        var _current = _header.first

        override fun hasNext(): Boolean = _current != VOID_RECORD_ID

        override fun next(): T {
            val current = _store.get(_current, _serializer) ?: throw DBException("Internal error: Header does not point to valid entry.")
            _previous = _current
            _current = current.next
            return current.data
        }

        override fun remove() {
            TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
        }
    }



    override fun listIterator(): MutableListIterator<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun listIterator(index: Int): MutableListIterator<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun subList(fromIndex: Int, toIndex: Int): MutableList<T> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /**
     * Appends a new element [T] at the end of the [PLinkedList]
     *
     * @param element The element to add.
     * @return true on success, false otherwise.
     */
    override fun add(element: T): Boolean {
        /* Create new record. */
        val new = PLinkedListElement(VOID_RECORD_ID, element)
        val newRecId = this._store.put(new, this._serializer)

        /* Update the predecessor of the new element. */
        if (this._header.last != VOID_RECORD_ID) {
            val lastRec: PLinkedListElement<T> = this._store.get(this._header.last, this._serializer) ?: throw DBException("Internal error: Header does not point to valid last entry.")
            lastRec.next = newRecId
            this._store.update(this._header.last, lastRec, this._serializer)
        }

        /* Update PLinkedList header. */
        if (this._header.first == VOID_RECORD_ID) {
            this._header.first = newRecId
        }
        this._header.last = newRecId
        this._header.size = this._header.size  + 1
        this._store.update(this._root, this._header, PDLinkedListHeaderSerializer)

        /* Success. */
        return true
    }

    /**
     * Appends a new element [T] at the specified position of the [PLinkedList]
     *
     * @param element The element to add.
     * @param index The position at which to add the element.
     *
     * @return true on success, false otherwise.
     */
    override fun add(index: Int, element: T) = when (index) {
        0 -> prepend(element)
        this._header.size -> append(element)
        else -> insert(element, index)
    }

    /**
     *
     */
    override fun addAll(index: Int, elements: Collection<T>): Boolean {
        if (index == this._header.size-1) return this.addAll(elements)
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun addAll(elements: Collection<T>): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /**
     * Replaces the element at the specified position by a new value.
     *
     * @param index The index of the element to replace.
     * @param element The new element at the specified index.
     *
     * @return The old value at the specified position.
     */
    override fun set(index: Int, element: T): T {
        /* Basic sanity checks. */
        if (index >= this._header.size) throw ArrayIndexOutOfBoundsException("The provided index i=$index is greater than the list's size s=${this._header.size}.")
        if (index < 0) throw IllegalArgumentException("The provided index i=$index is negative.")

        /* Replace value. */
        val replace = this.seek(index)
        val old = replace.second.data
        replace.second.data = element
        this._store.update(replace.first, replace.second, this._serializer)
        return old
    }

    /**
     *
     */
    override fun remove(element: T): Boolean {


        var currentRecId = this._header.first
        var currentRec: PLinkedListElement<T> ?= null
        var found = false
        for (i in 1..this._header.size) {
            currentRec = this._store.get(currentRecId, this._serializer) ?: throw DBException("Internal error: Link error (next) in element at index $i.")
            if (currentRec == element) {
                found = true
                break
            }
            currentRecId = currentRec.next
        }

        /* Remove. */
        if (found) {

        }

        return found
    }

    override fun removeAll(elements: Collection<T>): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun removeAt(index: Int): T {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun retainAll(elements: Collection<T>): Boolean {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /**
     * Clears this [PLinkedList] thereby removing all element. Since this method must explicitly delete all the individual entries,
     * executing this method may take a while for long [PLinkedList]s
     */
    override fun clear() {
        /* Delete all records. */
        var currentRecId = this._header.first
        for (i in 1..this._header.size) {
            val current = this._store.get(currentRecId, this._serializer) ?: throw DBException("Internal error: Link error (next) in element at index $i.")
            this._store.delete(currentRecId, this._serializer)
            currentRecId = current.next
        }

        /* Update header. */
        this._header.size = 0
        this._header.first = VOID_RECORD_ID
        this._header.last = VOID_RECORD_ID
        this._store.update(this._root, this._header, PDLinkedListHeaderSerializer)
    }

    /**
     * Closes the underlying store.
     */
    override fun close() {
        this._closeable.close()
    }

    /**
     * Prepends the element of type [T] to this [PLinkedList].
     *
     * @param element The element to append.
     */
    private fun prepend(element: T) {
        /* Create new record. */
        val new = PLinkedListElement(VOID_RECORD_ID, element)
        new.next = this._header.first

        /* Persist new record. */
        val newRecId = this._store.put(new, this._serializer)

        /* Update PLinkedList header. */
        this._header.first = newRecId
        this._header.size = this._header.size + 1;
        this._store.update(this._root, this._header, PDLinkedListHeaderSerializer)
    }

    /**
     * Inserts the element of type [T] at the specified index of [PLinkedList]. Index can neither be 0
     * nor equal to the size of the [PLinkedList].
     *
     * @param element The element to append.
     * @param index The index of the element.
     */
    private fun insert(element: T, index: Int) {
        /* Basic sanity checks. */
        if (index == 0 || index == this.size) throw IllegalArgumentException("Index cannot be 0 or equal to the list's size. Call append() or prepend() instead.")
        if (index >= this._header.size) throw ArrayIndexOutOfBoundsException("The provided index i=$index is greater than the list's size s=${this._header.size}.")
        if (index < 0) throw IllegalArgumentException("The provided index i=$index is negative.")

        /* Get record preceding the record at the specified index. */
        val predecessor = this.seek(index-1)

        /* Create new record. */
        val new = PLinkedListElement(predecessor.second.next, element)
        val newRecId = this._store.put(new, this._serializer)

        /* Update the replaced entry. */
        predecessor.second.next = newRecId
        this._store.update(predecessor.first, predecessor.second, this._serializer)

        /* Update PLinkedList header. */
        this._header.size = this._header.size + 1;
        this._store.update(this._root, this._header, PDLinkedListHeaderSerializer)
    }

    /**
     * Appends the element of type [T] to this [PLinkedList].
     *
     * @param element The element to append.
     */
    private fun append(element: T) {
        /* Create new record. */
        val new = PLinkedListElement(VOID_RECORD_ID, element)
        val newRecId = this._store.put(new, this._serializer)

        /* Update the predecessor of the new element. */
        if (this._header.last != VOID_RECORD_ID) {
            val lastRec: PLinkedListElement<T> = this._store.get(this._header.last, this._serializer) ?: throw DBException("Internal error: Header does not point to valid last entry.")
            lastRec.next = newRecId
            this._store.update(this._header.last, lastRec, this._serializer)
        }

        /* Update PLinkedList header. */
        if (this._header.first == VOID_RECORD_ID) {
            this._header.first = newRecId
        }
        this._header.last = newRecId
        this._header.size = this._header.size + 1;
        this._store.update(this._root, this._header, PDLinkedListHeaderSerializer)
    }


    /**
     * Internal method to access [PLinkedListElement] at the specified index.
     */
    private fun seek(index: Int): Pair<Long,PLinkedListElement<T>> {
        /* Provide some simple tests regarding the validity of the index. */
        if (index >= _header.size) throw ArrayIndexOutOfBoundsException("The provided index i=$index is greater than the list's size s=${this._header.size}.")
        if (index < 0) throw IllegalArgumentException("The provided index i=$index is negative.")

        /* Simple cases; first and last entry. */
        if (index == 0) return Pair(this._header.first, this._store.get(this._header.first, this._serializer) ?: throw DBException("Internal error: Header does not point to valid first entry."))
        if (index == this._header.size-1) return Pair(this._header.first, this._store.get(this._header.last, this._serializer) ?: throw DBException("Internal error: Header does not point to valid last entry."))

        /* TODO: Index to speed-up access (e.g. finger-table). */

        /* Start from first entry and traverse list. */
        var currentRecId = this._header.first
        var currentRec: PLinkedListElement<T> = this._store.get(this._header.first, this._serializer) ?: throw DBException("Internal error: Header does not point to valid entry.")
        for (i in 1..index) {
            currentRecId = currentRec.next
            currentRec = this._store.get(currentRecId, this._serializer) ?: throw DBException("Internal error: Link error (next) in element at index $i.")
        }
        return Pair(currentRecId, currentRec)
    }
}