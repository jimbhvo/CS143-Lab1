package simpledb;

import java.io.Serializable;
import java.util.*;
import static java.lang.System.out;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private Vector<TDItem> TDItemVector;

    public int TDItemVectorSize()
    {
        return TDItemVector.size();
    }


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */

    public Iterator<TDItem> iterator() {
        // some code goes here
        return TDItemVector.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here

        //Make new vector, fill in types and strings,
        TDItemVector = new Vector<TDItem>();

        //Here we assume 2 things
        //1. Arrays aren't empty and 2.Array lengths match
        for (int i = 0; i < typeAr.length; ++i)
        {
            TDItem temp = new TDItem(typeAr[i], fieldAr[i]);
            TDItemVector.add(temp);

        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        TDItemVector = new Vector<TDItem>();

        //We use null to represent unnamed field
        for (int i = 0; i < typeAr.length; ++i)
        {
            TDItem temp = new TDItem(typeAr[i], null);
            TDItemVector.add(temp);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return TDItemVector.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code went here
        if ((i >= 0 ) && (i < TDItemVector.size()))
        {
            return TDItemVector.get(i).fieldName;
        }
        else
        {
            throw new NoSuchElementException("Invalid field reference");
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here

        if ((i >= 0 ) && (i < TDItemVector.size()))
        {
            return TDItemVector.get(i).fieldType;
        }
        else
        {
            out.println("outside Range");
            out.println(i);
            throw new NoSuchElementException("Invalid field reference");
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code went here
        int i = 0;

        while (i < TDItemVector.size())
        {
            if (TDItemVector.get(i).fieldName == null)
                throw new NoSuchElementException("Nll");

            if (TDItemVector.get(i).fieldName.equals(name))
                break;
            ++i;
        }

        if (i < TDItemVector.size())
            return i;

        throw new NoSuchElementException("No matching field found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        //just read out the size of the tuples by adding them up?
        int returnvalue = 0;
        for(TDItem tuples : TDItemVector)
            returnvalue += tuples.fieldType.getLen();
        return returnvalue;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int maxlen = td1.TDItemVector.size() + td2.TDItemVector.size();
        Type[] tempTypeAr = new Type[maxlen];
        String[] tempFieldAr = new String[maxlen];

        for (int i = 0; i < td1.TDItemVector.size(); i++)
        {
            tempTypeAr[i] = td1.getFieldType(i);
            tempFieldAr[i] = td1.getFieldName(i);
        }

        int td1size = td1.TDItemVector.size();

        for (int i = 0; i < td2.TDItemVector.size(); i++)
        {
            tempTypeAr[i + td1size] = td2.getFieldType(i);
            tempFieldAr[i + td1size] = td2.getFieldName(i);
        }

        TupleDesc temp = new TupleDesc(tempTypeAr, tempFieldAr);

        return temp;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code went here
        // Assume we're comparing another tupledesc to this one?

        if (o == null || !(o instanceof TupleDesc))
            return false;

        TupleDesc temp = (TupleDesc) o;
        if (temp.getSize() != getSize())
            return false;

        boolean marker = true;
        for (int i = 0; i < TDItemVector.size(); i++)
        {
            if (getFieldType(i) != temp.getFieldType(i))
                marker = false;
        }
        return marker;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        String hashme = "";
        for (int i = 0; i < TDItemVector.size(); i++)
        {
            hashme += TDItemVector.get(i).fieldName;
            hashme += TDItemVector.get(i).fieldType.toString();
        }

        return hashme.hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String retstring = new String();
        if (TDItemVector.size() < 1)
            return "";
        retstring.concat(TDItemVector.get(0).toString());
        for(int i = 1; i < TDItemVector.size(); i++)
        {
            retstring.concat(",");
            retstring.concat(TDItemVector.get(i).toString());
        }
        return retstring;
    }
}
