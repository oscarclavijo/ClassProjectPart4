package CSCI485ClassProject;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import CSCI485ClassProject.fdb.FDBHelper;
import CSCI485ClassProject.models.AlgebraicOperator;
import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.AttributeType;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.TableMetadata;
import CSCI485ClassProject.models.ComparisonPredicate.Type;
import CSCI485ClassProject.models.Record.Value;
import CSCI485ClassProject.utils.ComparisonUtils;

public class SelectIterator extends Iterator {

    private Cursor c; 
   // private Cursor c_ahead; 
    private Database db; 
    private boolean init; 
    private Transaction tx; 
    private ComparisonPredicate predicate; 
    private TableMetadata t; 
    private String tableName; 
    private IndexType i; 

    private Record curr; 

    public SelectIterator(String tableName, ComparisonPredicate predicate, 
                        Iterator.Mode mode, IndexType i) { 
        db = FDBHelper.initialization(); 
        tx = db.createTransaction(); 
        setMode(mode);
        this.i = i; 
        this.predicate = predicate; 
        this.tableName = tableName; 
        TableMetadata t = TableMetadataTransformer.getTableByName(tableName, tx);
        this.t = t; 
        Record.Value v = new Record.Value();
        v.setValue(predicate.getRightHandSideValue());

        Cursor.Mode m; 
        if (mode == Mode.READ) m = Cursor.Mode.READ; 
        else m = Cursor.Mode.READ_WRITE;

        if (i == IndexType.NO_INDEX) {
            c = new Cursor(m, tableName, t, tx);
            if (predicate.getPredicateType() == Type.ONE_ATTR) {
                c.enablePredicate(predicate.getLeftHandSideAttrName(), v, predicate.getOperator());
            }
        } else {
            c = new Cursor(tableName, t, predicate.getLeftHandSideAttrName(), 
             i, tx);
        }
        
        init = true; 
    }

    @Override
    public Record next() {

        if (predicate.getPredicateType() == Type.TWO_ATTRS) {
            Record r; 
            if (init) {
                r = c.getFirst();
                init = false; 
            } else {
                r = c.next(false);
            }
            if (r == null) {
                curr = null; 
                return null; 
            }
            
            Value newCompVal = calculateNewComparisonValue(r, predicate);
            while (!matchesPredicate(
                r.getValueForGivenAttrName(predicate.getLeftHandSideAttrName()), 
                newCompVal, predicate)) {
                r = c.next(false);
                if (r == null){
                    curr = null; 
                    return null; 
                } 
                newCompVal = calculateNewComparisonValue(r, predicate);
            } 
            curr = r; 
            return r; 
        } 
       
        if (init) {
            init = false; 
            curr = c.getFirst();
            return curr;
        } else {
            curr = c.next(false);
            return curr; 
        } 
    }

    @Override
    public void commit() {
        c.commit();
        db.close();
    }

    @Override
    public void abort() {
        c.abort();
        db.close();
    }

    public void reset() {
       // System.out.println("Called reset");
        c.abort();
        tx = db.createTransaction(); 
        
        Record.Value v = new Record.Value();
        v.setValue(predicate.getRightHandSideValue());

        Cursor.Mode m; 
        if (getMode() == Mode.READ) m = Cursor.Mode.READ; 
        else m = Cursor.Mode.READ_WRITE;

        if (i == IndexType.NO_INDEX) {
            c = new Cursor(m, tableName, t, tx);
            if (predicate.getPredicateType() == Type.ONE_ATTR) {
                c.enablePredicate(predicate.getLeftHandSideAttrName(), v, predicate.getOperator());
            }
        } else {
            c = new Cursor(tableName, t, predicate.getLeftHandSideAttrName(), 
             i, tx);
        }
        
        init = true; 
    }

    public String getTableName() {
        return tableName; 
    }
    public TableMetadata getTableMetadata() {
        return t; 
    }

    public static Value calculateNewComparisonValue(Record r, ComparisonPredicate p) {

        String attrName = p.getRightHandSideAttrName(); 
        Object predVal = p.getRightHandSideValue(); 
        AlgebraicOperator operator = p.getRightHandSideOperator();
            
        // value of the record based on right side specs. 
        // e.g. given age * ... , retrieve age of record r 
        Value x = new Value(); 
        x.setValue(r.getValueForGivenAttrName(attrName));
        
        // now calculate a numeric value based on predicate and 
        // record r
        Value rightActual = new Value(); 
        rightActual.setValue(predVal);

        if (operator == AlgebraicOperator.PRODUCT) {
            rightActual.setValue(multiply(x.getValue(), predVal));
        } else if (operator == AlgebraicOperator.PLUS) {
            rightActual.setValue(add(x.getValue(), predVal));
        } else if (operator == AlgebraicOperator.MINUS) {
            rightActual.setValue(subtract(x.getValue(), predVal));
        } else if (operator == AlgebraicOperator.DIVISION) {
            rightActual.setValue(divide(x.getValue(), predVal));
        }
        
        return rightActual; 

    }
    public static Value calculateNewAssignmentValue(Record r, AssignmentExpression a) {

        String attrName = a.getRightHandSideAttrName(); 
        Object b = a.getRightHandSideValue(); 
        AlgebraicOperator operator = a.getRightHandSideOperator();
            
        // value of the record based on right side specs. 
        // e.g. given age * ... , retrieve age of record r 
        Value x = new Value(); 
        x.setValue(r.getValueForGivenAttrName(attrName));
       
        // now calculate a numeric value based on expression and 
        // record r
        Value rightActual = new Value(); 
        rightActual.setValue(b);

        if (operator == AlgebraicOperator.PRODUCT) {
            rightActual.setValue(multiply(x.getValue(), rightActual.getValue()));
        } else if (operator == AlgebraicOperator.PLUS) {
            rightActual.setValue(add(x.getValue(), rightActual.getValue()));
        } else if (operator == AlgebraicOperator.MINUS) {
            rightActual.setValue(subtract(x.getValue(), rightActual.getValue()));
        } else if (operator == AlgebraicOperator.DIVISION) {
            rightActual.setValue(divide(x.getValue(), rightActual.getValue()));
        }
        return rightActual; 

    }

    public static Object add(Object a, Object b) {
        if (a instanceof Long && b instanceof Long) {
            return (Long) a + (Long) b;
        } else if (a instanceof Long && b instanceof Integer) {
            return (Long) a + (Integer) b;
        } else if (a instanceof Integer && b instanceof Long) {
            return (Integer) a + (Long) b;
        } else if (a instanceof Integer && b instanceof Integer) {
            return (Integer) a + (Integer) b;
        }
        return null; 
    }

    public static Object multiply(Object a, Object b) {
        if (a instanceof Long && b instanceof Long) {
            return (Long) a * (Long) b;
        } else if (a instanceof Long && b instanceof Integer) {
            return (Long) a * (Integer) b;
        } else if (a instanceof Integer && b instanceof Long) {
            return (Integer) a * (Long) b;
        } else if (a instanceof Integer && b instanceof Integer) {
            return (Integer) a * (Integer) b;
        }
        return null; 
    }
    public static Object subtract(Object a, Object b) {

        if (a instanceof Long && b instanceof Long) {
            return (Long) a - (Long) b;
        } else if (a instanceof Long && b instanceof Integer) {
            return (Long) a - (Integer) b;
        } else if (a instanceof Integer && b instanceof Long) {
            return (Integer) a - (Long) b;
        } else if (a instanceof Integer && b instanceof Integer) {
            return (Integer) a - (Integer) b;
        }
        return null; 
        
    }
    public static Object divide(Object a, Object b) {

        if (a instanceof Long && b instanceof Long) {
            return (Long) a / (Long) b;
        } else if (a instanceof Long && b instanceof Integer) {
            return (Long) a / (Integer) b;
        } else if (a instanceof Integer && b instanceof Long) {
            return (Integer) a / (Long) b;
        } else if (a instanceof Integer && b instanceof Integer) {
            return (Integer) a / (Integer) b;
        }
        return null; 
        
    }

    public static boolean matchesPredicate(Object r, Value x, ComparisonPredicate p) {

        if (x.getType() == AttributeType.INT) {
            return ComparisonUtils.compareTwoINT(r, x.getValue(), p.getOperator());
        } else if (x.getType() == AttributeType.DOUBLE){
            return ComparisonUtils.compareTwoDOUBLE(r, x.getValue(), p.getOperator());
        } else if (x.getType() == AttributeType.VARCHAR) {
            return ComparisonUtils.compareTwoVARCHAR(r, x.getValue(), p.getOperator());
        }
        return false; 

    }

    @Override
    public StatusCode update(AssignmentExpression assignExp) {
    
        if (assignExp.getExpressionType() == AssignmentExpression.Type.ONE_ATTR) {
            String[] attrNames = {assignExp.getLeftHandSideAttrName()};
            Object[] attrValues = {assignExp.getRightHandSideValue()};
            System.out.println("Will update record to " + attrValues[0]); 
            return c.updateCurrentRecord(attrNames, attrValues);
        } else {
            String[] attrNames = {assignExp.getLeftHandSideAttrName()};
            Object[] attrValues = {calculateNewAssignmentValue(curr, assignExp).getValue()};
            System.out.println("Will update record to " + attrValues[0]); 
            return c.updateCurrentRecord(attrNames, attrValues);
        }
        
    }

    public StatusCode delete() {
        return c.deleteCurrentRecord(); 
    }
    
}
