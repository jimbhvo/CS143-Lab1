package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate myp;
    private DbIterator mychild;
    
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	myp = p;
    	mychild = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return myp;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return mychild.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	mychild.open();
    }

    public void close() {
        // some code goes here
    	mychild.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	mychild.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // Get next tuple
    	while (mychild.hasNext())
    	{
    		//See if it passes test
    		Tuple temp = mychild.next();
    		if (myp.filter(temp))
    			return temp;
    	}
        return null;
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
