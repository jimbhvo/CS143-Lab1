package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId mytransid;
    private DbIterator mychild;
    private TupleDesc resultTupleDesc;
    boolean deleted;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	mytransid = t;
    	mychild = child;
    	deleted = false;
    	
    	Type[] type = new Type[]{Type.INT_TYPE};
    	String[] name = new String[]{"Deleted"};
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
    	deleted = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if (deleted) return null;
    	int counter = 0;
    	while (mychild.hasNext())
    	{
    		Tuple t = mychild.next();
    		try {
    			Database.getBufferPool().deleteTuple(mytransid, t);
    		}
    		catch (IOException e){
    			throw new DbException("Exception deleting tuple");
    		}
    		counter ++;
    	}
    	Tuple result = new Tuple(resultTupleDesc);
    	result.setField(0, new IntField(counter));
    	deleted = true;
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
