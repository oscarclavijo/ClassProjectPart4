package CSCI485ClassProject;

import CSCI485ClassProject.models.Record;
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

public class ProjectIterator extends Iterator {

    private Cursor c; 
    private Database db; 
    private boolean init; 
    private Transaction tx; 

    private String attrName; 
    private boolean isDuplicateFree;

    private boolean useIterator; 
    private Iterator i; 
    private TableMetadata t; 

    public ProjectIterator(String tableName, String attrName, boolean isDuplicateFree) { 

        db = FDBHelper.initialization(); 
        tx = db.createTransaction();
        this.attrName = attrName; 
        this.isDuplicateFree = isDuplicateFree; 
        TableMetadata t = TableMetadataTransformer.getTableByName(tableName, tx);
        init = true; 
        this.t = t; 
        setMode(Iterator.Mode.READ);

        c = new Cursor(Cursor.Mode.READ, tableName, t, tx);
        useIterator = false; 
    }

    public ProjectIterator(Iterator iterator, String attrName, boolean isDuplicateFree) { 

        useIterator = true; 
        i = iterator; 
        this.attrName = attrName; 
        this.isDuplicateFree = isDuplicateFree;

    }

    @Override
    public Record next() {

        Record r = null; 
        Record project = new Record();
        if (useIterator) {
            r = i.next(); 
        }
        else {
            if (init) {
                r = c.getFirst();
                init = false; 
            } else { 
                r = c.next(false);
            }
        }
        if (r == null) return null; 
         
        project.setAttrNameAndValue(attrName, r.getValueForGivenAttrName(attrName));
        return project; 
    }

    @Override
    public void commit() {
        if (useIterator) {
            i.commit();
            return; 
        }
        c.commit();
        db.close();
    }

    @Override
    public void abort() {
        if (useIterator) {
            i.abort();
            return; 
        }
        c.abort();
        db.close();
    }

    @Override
    public StatusCode update(AssignmentExpression assignExp) {
        throw new UnsupportedOperationException("Unimplemented method 'update'");
    }
    
}
