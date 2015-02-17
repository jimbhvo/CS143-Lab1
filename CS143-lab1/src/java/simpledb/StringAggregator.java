package simpledb;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int mygbfield;
    private Type mygbfieldtype;
    private int myafield;
    private Op myop;
    private String fname;
    private String afieldname;
    private Map<Field, Field> fieldMap;
    
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	mygbfield = gbfield;
    	mygbfieldtype = gbfieldtype;
    	myafield = afield;
    	myop = what;
    	fieldMap = new HashMap<Field, Field>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	fname = tup.getTupleDesc().getFieldName(mygbfield);
    	afieldname = tup.getTupleDesc().getFieldName(myafield);
    	Field mapkey = tup.getField(mygbfield);
    	Field newval = null;
    	int count; 
    	IntField getint = (IntField) fieldMap.get(mapkey);
    	if (getint == null)
    		count = 1;
    	else
    		count = 1 + getint.getValue();
    	newval = new IntField(count);
    	fieldMap.put(mapkey, newval);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	List<Tuple> tupleList = new ArrayList<Tuple>();
    	
    	
   	 	TupleDesc td;
	 
   	 	Type[] fieldType;
        String[] fieldName;
    	
    	if(mygbfield == NO_GROUPING){
            fieldType = new Type[1];
            fieldName = new String[1];
            fieldType[0] = Type.INT_TYPE;
            fieldName[0] = afieldname;
            td = new TupleDesc(fieldType, fieldName);
    	} else {
            fieldType = new Type[2];
            fieldName = new String[2];
            fieldType[0] = mygbfieldtype;
            fieldName[0] = fname;
            fieldType[1] = Type.INT_TYPE;
            fieldName[1] = afieldname;
            td =  new TupleDesc(fieldType, fieldName);
    	}
	 
    	if(mygbfield == NO_GROUPING){
            Tuple groupedTuple = new Tuple(td);
            groupedTuple.setField(0, fieldMap.get(new IntField(NO_GROUPING)));
            tupleList.add(groupedTuple);
            return new TupleIterator(td, tupleList);
    	} else {
            Set<Field> groups = fieldMap.keySet();
            for(Field temp : groups){
                    Tuple groupedTuple = new Tuple(td);
                    groupedTuple.setField(0, temp);
                    groupedTuple.setField(1, fieldMap.get(temp));
                    tupleList.add(groupedTuple);
            }
    	}         
        return new TupleIterator(td, tupleList);
         
    }
}
