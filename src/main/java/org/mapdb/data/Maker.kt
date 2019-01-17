package org.mapdb.data

import org.mapdb.DB
import org.mapdb.DBException
import org.mapdb.Utils
import java.util.*

abstract class Maker<E>{
    /**
     * Creates new collection if it does not exist, or throw {@link DBException.WrongConfiguration} if collection already exists.
     */
    open fun create():E = make2( true)

    /**
     * Create new collection or open existing.
     */
    @Deprecated(message="use createOrOpen() method", replaceWith=ReplaceWith("createOrOpen()"))
    open fun make():E = make2(null)

    @Deprecated(message="use createOrOpen() method", replaceWith=ReplaceWith("createOrOpen()"))
    open fun makeOrGet() = make2(null)

    /**
     * Create new collection or open existing.
     */
    open fun createOrOpen():E = make2(null)

    /**
     * Open existing collection, or throw {@link DBException.WrongConfiguration}
     * if collection already exists.
     */
    open fun open():E = make2( false)

    protected fun make2(create:Boolean?):E{
        Utils.lockWrite(db.lock){
            db.checkNotClosed()
            verify()

            val catalog = db.nameCatalogLoad()
            //check existence
            val typeFromDb = catalog[name+ DB.Keys.type]
            if (create != null) {
                if (typeFromDb!=null && create)
                    throw DBException.WrongConfiguration("Named record already exists: $name")
                if (!create && typeFromDb==null)
                    throw DBException.WrongConfiguration("Named record does not exist: $name")
            }
            //check typeg
            if(typeFromDb!=null && type!=typeFromDb){
                throw DBException.WrongConfiguration("Wrong type for named record '$name'. Expected '$type', but catalog has '$typeFromDb'")
            }

            val ref = db.namesInstanciated.getIfPresent(name)
            if(ref!=null)
                return ref as E;

            if(typeFromDb!=null) {
                val ret = open2(catalog)
                db.namesInstanciated.put(name,ret)
                return ret;
            }

            if(db.store.isReadOnly)
                throw UnsupportedOperationException("Read-only")
            catalog.put(name+ DB.Keys.type,type)
            val ret = create2(catalog)
            db.nameCatalogSaveLocked(catalog)
            db.namesInstanciated.put(name,ret)
            return ret
        }
    }

    open protected fun verify(){}
    abstract protected fun create2(catalog: SortedMap<String, String>):E
    abstract protected fun open2(catalog: SortedMap<String, String>):E

    //TODO this is hack to make internal methods not accessible from Java. Remove once internal method names are obfuscated in bytecode
    internal fun `%%%verify`(){verify()}
    internal fun `%%%create2`(catalog: SortedMap<String, String>) = create2(catalog)
    internal fun `%%%open2`(catalog: SortedMap<String, String>) = open2(catalog)

    abstract protected val db: DB
    abstract protected val name: String
    abstract internal val type: String
}