package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private int[] buckets;
	private int mymin;
	private int range;
	private int mod;
	private int total;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
    	this.buckets = new int[buckets];
    	mymin = min;
    	range = max - min + 1;
    	mod = (int)Math.ceil((double)(range)/buckets);
    	total = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	int bucketnum = (v - mymin)/mod;
    	buckets[bucketnum]++;
    	total++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	
    	//get the bucket that v would fall into
    	int bucketnum = (v - mymin)/mod;
    	if (bucketnum < 0)
    		bucketnum = -1;
    	if (bucketnum > buckets.length)
    		bucketnum = buckets.length;
    	
    	//get current count of bucket v
    	double bucketcount = 0;
    	if (bucketnum > -1 && bucketnum < buckets.length)
    		bucketcount = buckets[bucketnum];
    	
    	//get amount strictly greater than v
    	double greaterthan = 0;
    	if (bucketnum < 0)
    		greaterthan = total;
    	else if (bucketnum == range)
    		greaterthan = 0;
    	else 
    		for (int i = bucketnum + 1; i < buckets.length; i++) {
        		greaterthan += buckets[i];
        	}
    	
    	//total refers to total number of bucket entries
    	double result = 0;
    	switch(op){
    	case EQUALS:
    	case LIKE:
    		result = bucketcount/total;
    		break;
    	case GREATER_THAN:
    		result = greaterthan/total;
    		break;
    	case GREATER_THAN_OR_EQ:
    		result = greaterthan + bucketcount;
    		result /= total;
    		break;
    	case LESS_THAN:
    		result = (total - (greaterthan + bucketcount));
    		result /= total;
    		break;
    	case LESS_THAN_OR_EQ:
    		result = (total - greaterthan);
    		result /= total;
    		break;
    	case NOT_EQUALS:
    		result = (total - bucketcount);
    		result /= total;
    		break;
		default:
			return -1;
    	}
    	// some code goes here
        return result;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
    	String ret = "";
    	for (int i = 0; i < buckets.length; i++)
    	{
    		ret += "Bucket " + i + ": ";
    		for (int j = 0; j < buckets[i]; j++)
    			ret += ",";
    		ret += "\n";
    	}
        // some code goes here
        return ret;
    }
}
