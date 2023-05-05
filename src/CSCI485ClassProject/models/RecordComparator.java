package CSCI485ClassProject.models;


import java.util.Comparator;

import CSCI485ClassProject.models.Record.Value;

public class RecordComparator implements Comparator<Record> {

    private String attrName; 

    public RecordComparator(String attrName) { 
        this.attrName = attrName;
    }
    
    @Override
    public int compare(Record v1, Record v2) {
        Object a = v1.getValueForGivenAttrName(attrName);
        Object b = v2.getValueForGivenAttrName(attrName);

        if (a instanceof Integer) {
            return ((Integer)a).compareTo((Integer)b);
        } else if (a instanceof Long) { 
            return ((Long)a).compareTo((Long)b);
        } else if (a instanceof Double) {
            return ((Double)a).compareTo((Double)b);
        } else if (a instanceof String) {
            return ((String)a).compareTo((String)b);
        }
        return 0; 
    }
}