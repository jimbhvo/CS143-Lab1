package simpledb;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Set;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int mygbfield;
    private Type mygbfieldtype;
    private int myafield;
    private Op myop;
    
    private Map<Field, Field> fieldMap;
    private Map<Field, Integer> keyLog;
    private String aFieldName = "";
    
    private String groupFieldName = "";
    
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here	
    	mygbfield = gbfield;
    	mygbfieldtype = gbfieldtype;
    	myafield = afield;
    	myop = what;
    	keyLog = new HashMap<Field, Integer>();
    	fieldMap = new HashMap<Field, Field>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	// merge based on grouping
    	Field mapkey;
    	aFieldName = tup.getTupleDesc().getFieldName(myafield);
    	
		if (mygbfield == NO_GROUPING){
			mapkey = new IntField(NO_GROUPING);
		}
		else{
	 		mapkey = tup.getField(mygbfield);
	 		groupFieldName = tup.getTupleDesc().getFieldName(mygbfield);
		}
				
		IntField mapval = (IntField) fieldMap.get(mapkey); 	
		IntField aggregator = (IntField) tup.getField(myafield);
		Field aggregatevalue = null;
		int sum, count;
		
		//Set newval depending on the op
		if(myop.equals(Op.COUNT)){
    		if(mapval == null)
            	count = 1;
    		else
            	count = mapval.getValue() + 1;
    		aggregatevalue = new IntField(count);
		}
		else if(myop.equals(Op.SUM)){
        	if(mapval == null)
                sum = 0;
        	else 
               	sum =  mapval.getValue();
        	sum += aggregator.getValue();
        	aggregatevalue = new IntField(sum);
		}
		else if(myop.equals(Op.AVG)){             
        	if(mapval == null){
                count = 1;
                keyLog.put(mapkey, 1);
                sum = aggregator.getValue();
        	} else {
                sum = mapval.getValue() + aggregator.getValue();
                count = keyLog.get(mapkey) + 1;
                keyLog.put(mapkey, count);
        	}                                 
        	aggregatevalue = new IntField(sum);
		}
		else if(myop.equals(Op.MIN)){
        	if(mapval == null)
                count = aggregator.getValue();
        	else 
                count = Math.min(aggregator.getValue(), mapval.getValue());
        	aggregatevalue = new IntField(count);
		}
		else if(myop.equals(Op.MAX)){
        	if(mapval == null)
        		count = aggregator.getValue();
        	else 
        		count = Math.max(aggregator.getValue(), mapval.getValue());
        	aggregatevalue = new IntField(count);
		}
		fieldMap.put(mapkey, aggregatevalue);
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	 ArrayList<Tuple> tupleList = new ArrayList<Tuple>();
         
    	 TupleDesc td;
    	 
    	 Type[] fieldType;
         String[] fieldName;
         
         if(mygbfield == NO_GROUPING){
                 fieldType = new Type[1];
                 fieldName = new String[1];
                 fieldType[0] = Type.INT_TYPE;
                 fieldName[0] = aFieldName;
                 td = new TupleDesc(fieldType, fieldName);
         } else {
                 fieldType = new Type[2];
                 fieldName = new String[2];
                 fieldType[0] = mygbfieldtype;
                 fieldName[0] = groupFieldName;
                 fieldType[1] = Type.INT_TYPE;
                 fieldName[1] = aFieldName;
                 td =  new TupleDesc(fieldType, fieldName);
         }
    	 
         if(mygbfield == NO_GROUPING){
                 // create a new tuple and set the value of the aggregate 
                 Tuple groupedTuple = new Tuple(td);
                 groupedTuple.setField(0, fieldMap.get(new IntField(NO_GROUPING)));
                 int value;
                 if (myop.equals(Aggregator.Op.AVG)) {
          			value = ((IntField) fieldMap.get(new IntField(NO_GROUPING))).getValue() 
          					/ keyLog.get(new IntField(NO_GROUPING)).intValue();
          			groupedTuple.setField(0, new IntField(value));
          		 }
                 tupleList.add(groupedTuple);
                 return new TupleIterator(td, tupleList);
                 
         } else {
                 Set<Field> groups = fieldMap.keySet();
                 
                 for(Field f : groups){
                         Tuple groupedTuple = new Tuple(td);
                         groupedTuple.setField(0, f);
                         groupedTuple.setField(1, fieldMap.get(f));
                         int value;
                         if (myop.equals(Aggregator.Op.AVG)) {
                 			value = ((IntField) fieldMap.get(f)).getValue() / keyLog.get(f).intValue();
                 			groupedTuple.setField(1, new IntField(value));
                 		 }
                         tupleList.add(groupedTuple);
                 }
                                 
                 return new TupleIterator(td, tupleList);
                 
         }
    }

}
