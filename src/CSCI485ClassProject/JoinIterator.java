package CSCI485ClassProject;
import CSCI485ClassProject.models.Record;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

public class JoinIterator extends Iterator {

    private Iterator in; 
    private Iterator out;
    private ComparisonPredicate predicate;  
    private Set<String> attrNames; 

    private Record inRecord; 
    private Record outRecord; 

    public JoinIterator(Iterator out, Iterator in, ComparisonPredicate predicate, 
            Set<String> attrNames) { 

        this.in = in; 
        this.out = out; 
        this.predicate = predicate; 
        this.attrNames = attrNames; 

        inRecord = in.next();
        outRecord = out.next();
    }

    @Override
    public Record next() {

        if (inRecord == null) {
            return null;
        }
    
        String attrName = predicate.getLeftHandSideAttrName();
    
        while (inRecord != null) {
            if (outRecord != null) {
                Value rValue = SelectIterator.calculateNewComparisonValue(outRecord, predicate);
                Value lValue = new Value();
                lValue.setValue(inRecord.getValueForGivenAttrName(attrName));
    
                if (lValue.equals(rValue)) {
                    Record joined = joinTwoRecords(inRecord, outRecord);
                    outRecord = out.next();
                    return joined;
                }
                outRecord = out.next();
            } else {
                ((SelectIterator) out).reset();
                outRecord = out.next();
                inRecord = in.next();
            }
        }
        return null;
    }

    @Override
    public void commit() {
        in.commit();
        out.commit();
    }

    @Override
    public void abort() {
        in.abort();
        out.abort();
    }

    private Record joinTwoRecords(Record a, Record b) {
        Record joinedRecord = new Record(); 

        HashMap<String, Object> newRecord = new HashMap<>(); 
        HashMap<String, Record.Value> inVals = a.getMapAttrNameToValue();
        HashMap<String, Record.Value> outVals = b.getMapAttrNameToValue();

        for (Map.Entry<String, Record.Value> e : inVals.entrySet()) {
            if (attrNames == null) {
                String newName = ""; 
                if (outVals.containsKey(e.getKey())) {
                    newName = ((SelectIterator)in).getTableName() + "." + e.getKey();
                } else {
                    newName = e.getKey(); 
                }
                newRecord.put(newName, e.getValue().getValue()); 
            } else {
                if (attrNames.contains(e.getKey())) {
                    String newName = ""; 
                    if (outVals.containsKey(e.getKey())) {
                        newName = ((SelectIterator)in).getTableName() + "." + e.getKey();
                    } else {
                        newName = e.getKey(); 
                    }
                    newRecord.put(newName, e.getValue().getValue()); 
                }
            }

            
        }
        for (Map.Entry<String, Record.Value> e : outVals.entrySet()) {
            if (attrNames == null) {
                String newName = ""; 
                if (inVals.containsKey(e.getKey())) {
                    newName = ((SelectIterator)out).getTableName() + "." + e.getKey();
                } else {
                    newName = e.getKey(); 
                }
               newRecord.put(newName, e.getValue().getValue()); 
            } else {
                if (attrNames.contains(e.getKey())) {
                    String newName = ""; 
                    if (inVals.containsKey(e.getKey())) {
                        newName = ((SelectIterator)in).getTableName() + "." + e.getKey();
                    } else {
                        newName = e.getKey(); 
                    }
                    newRecord.put(newName, e.getValue().getValue()); 
                }
            }
        }
        System.out.println(joinedRecord.setMapAttrNameToValue(newRecord));
        return joinedRecord; 

    }

    @Override
    public StatusCode update(AssignmentExpression assignExp) {
        return StatusCode.OPERATOR_UPDATE_ITERATOR_TABLENAME_UNMATCHED;
    }

    
    
}
