package simpledb;

import java.util.*;
import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    TransactionId mytid;
    int myTableId;
    String myTableAlias;
    DbFileIterator myIterator	;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code went here
    	mytid =  tid;
    	myTableId = tableid;
    	myTableAlias = tableAlias;
    	myIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
    	// some code went here
    	return Database.getCatalog().getTableName(myTableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code went here
    	return myTableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code went here
    	myTableId = tableid;
    	myTableAlias = tableAlias;
    	myIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(mytid);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code went here
    	myIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code went here
    	//return Database.getCatalog().getTupleDesc(myTableId);

        // Create a new tuple desc with table aliases prefixing the column
    	TupleDesc oldTd = Database.getCatalog().getTupleDesc(myTableId);

        int len = oldTd.numFields();
        Type[] newTypes = new Type[len];
        String[] newNames = new String[len];

        Iterator<TDItem> it = oldTd.iterator();
        for (int k=0; it.hasNext(); k++)
        {
            // Get each column
            it.next();
            newTypes[k] = oldTd.getFieldType(k);
            newNames[k] = myTableAlias + "." + oldTd.getFieldName(k);
        }

        return new TupleDesc(newTypes, newNames);

    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code went here
    	return myIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code went here
        return myIterator.next();
    }

    public void close() {
        // some code went here
    	myIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code went here
    	myIterator.rewind();
    }
}
