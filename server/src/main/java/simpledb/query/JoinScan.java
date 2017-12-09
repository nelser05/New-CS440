package simpledb.query;

/**
 * The scan class corresponding to the <i>join</i> relational algebra
 * operator.
 *
 * @author Erik Nelson
 */
public class JoinScan implements Scan {

    private Scan s1, s2;
    private Predicate pred;
    private Scan product;

    /**
     * Creates a join scan having the two underlying scans.
     *
     * @param s1 the LHS scan
     * @param s2 the RHS scan
     */
    public JoinScan(Scan s1, Scan s2, Predicate pred) {
        this.product = new ProductScan(s1, s2);
        this.pred = pred;
        product.next();
    }

    /**
     * Positions the scan before its first record.
     * The Lefthand Side scan is at the first record, and the Righthand side
     * is right before the first record
     *
     * @see simpledb.query.Scan#beforeFirst()
     */
    public void beforeFirst() {
        product.beforeFirst();
    }

    /**
     * Moves the scan to the next record. The method moves to the next RHS
     * record, if possible. Otherwise, it moves to the next LHS record and the
     * first RHS record. If there are no more LHS records, return false
     * false.
     *
     * @see simpledb.query.Scan#next()
     */
    public boolean next() {
        while (product.next()) {
            if (pred.isSatisfied(product)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Closes both underlying scans.
     *
     * @see simpledb.query.Scan#close()
     */
    public void close() {
        product.close();
    }

    public Constant getVal(String fldname) {
        return product.getVal(fldname);
    }

    public int getInt(String fldname) {
        return product.getInt(fldname);
    }

    public String getString(String fldname) {
        return product.getString(fldname);
    }

    public boolean hasField(String fldname) {
        return product.hasField(fldname);
    }
}