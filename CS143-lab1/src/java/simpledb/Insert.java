package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId mytransid;
    private DbIterator mychild;
    private int mytableid;
    private TupleDesc resultTupleDesc;
    boolean inserted;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	mytransid = t;
    	mychild = child;
    	mytableid = tableid;
    	inserted = false;
    	
    	Type[] type = new Type[]{Type.INT_TYPE};
    	String[] name = new String[]{"Inserted"};
    	resultTupleDesc = new TupleDesc(type, name);
    	
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return resultTupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	mychild.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	mychild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	mychild.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (inserted)
    		return null;
    	int counter = 0;
    	while (mychild.hasNext())
    	{
    		Tuple t = mychild.next();
    		try {
    			Database.getBufferPool().insertTuple(mytransid, mytableid, t);
    		}
    		catch (IOException e){
    			throw new DbException("Exception adding tuple");
    		}
    		counter ++;
    	}
    	Tuple result = new Tuple(resultTupleDesc);
    	result.setField(0, new IntField(counter));
    	inserted = true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{mychild};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	mychild = children[0];
    }
}
