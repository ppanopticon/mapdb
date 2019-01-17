package org.mapdb.data

import org.mapdb.DB


/**
 * A class that facilitates data verification on [Storage] level for some type of [Maker]
 *
 * @see [Maker]
 * @see [DB.registerForType]
 */
abstract class Verifier {

    val all = {s:String->null}
    val recid = {s:String->
        try{
            val l = s.toLong()
            if(l<=0)
                "Recid must be greater than 0"
            else
                null
        }catch(e:Exception){
            "Recid must be a number"
        }
    }

    val recidOptional = {s:String->
        try{
            val l = s.toLong()
            if(l<0)
                "Recid can not be negative"
            else
                null
        }catch(e:Exception){
            "Recid must be a number"
        }
    }

    val long = { s: String ->
        try {
            s.toLong()
            null
        } catch(e: Exception) {
            "Must be a number"
        }
    }


    val int = { s: String ->
        try {
            s.toInt()
            null
        } catch(e: Exception) {
            "Must be a number"
        }
    }

    val recidArray = all

    val serializer = all
    val boolean = {s:String ->
        if(s!="true" && s!="false")
            "Not boolean"
        else
            null
    }

    /**
     * The expected catalogue values for this instance of [Verifier].
     */
    internal abstract val expected: Map<String, DB.CatVal>
}