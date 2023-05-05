package CSCI485ClassProject;

import CSCI485ClassProject.Iterator.Mode;
import CSCI485ClassProject.models.AssignmentExpression;
import CSCI485ClassProject.models.ComparisonPredicate;
import CSCI485ClassProject.models.IndexType;
import CSCI485ClassProject.models.Record;
import CSCI485ClassProject.models.RecordComparator;
import CSCI485ClassProject.models.Record.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.apple.foundationdb.Database;

// your codes
public class RelationalAlgebraOperatorsImpl implements RelationalAlgebraOperators {

  @Override
  public Iterator select(String tableName, ComparisonPredicate predicate, Iterator.Mode mode, boolean isUsingIndex) {
    if (predicate.validate() == StatusCode.PREDICATE_OR_EXPRESSION_INVALID) {
      return null; 
    }
    IndexType i; 
    if (isUsingIndex) {
      i = null; 
    } else {
      i = IndexType.NO_INDEX;
    }
    Iterator selectItr = new SelectIterator(tableName, predicate, mode, i);
    return selectItr;
  }

  @Override
  public Set<Record> simpleSelect(String tableName, ComparisonPredicate predicate, boolean isUsingIndex) {
    if (predicate.validate() == StatusCode.PREDICATE_OR_EXPRESSION_INVALID) {
      return null; 
    }
    Iterator i = select(tableName, predicate, Iterator.Mode.READ, isUsingIndex);
    Record r = i.next(); 

    Set<Record> s = new HashSet<Record>();
    while (r != null) {
      s.add(r);
      r = i.next();
    }
    return s; 
  }

  @Override
  public Iterator project(String tableName, String attrName, boolean isDuplicateFree) {
    return new ProjectIterator(tableName, attrName, isDuplicateFree);
  }

  @Override
  public Iterator project(Iterator iterator, String attrName, boolean isDuplicateFree) {
    return new ProjectIterator(iterator, attrName, isDuplicateFree);
  }

  @Override
  public List<Record> simpleProject(String tableName, String attrName, boolean isDuplicateFree) {

    Iterator i = project(tableName, attrName, isDuplicateFree);
    Record r = i.next(); 

    if (isDuplicateFree) {
      Set<Record> s = new HashSet<>();
      while (r != null) {
        s.add(r);
        r = i.next();
      }

      List<Record> l = new ArrayList<>(s); 
      java.util.Collections.sort(l, new RecordComparator(attrName));
      return l; 
    } 

    List<Record> l = new ArrayList<>(); 
    while (r != null) {
      l.add(r);
      r = i.next();
    }
    return l; 

  }

  @Override
  public List<Record> simpleProject(Iterator iterator, String attrName, boolean isDuplicateFree) {
    Record r = iterator.next(); 

    if (isDuplicateFree) {
      Set<Record> s = new HashSet<>();
      while (r != null) {
        s.add(r);
        r = iterator.next();
      }

      List<Record> l = new ArrayList<>(s); 
      java.util.Collections.sort(l, new RecordComparator(attrName));
      return l; 
    } 

    List<Record> l = new ArrayList<>(); 
    while (r != null) {
      l.add(r);
      r = iterator.next();
    }
    return l; 
  }

  @Override
  public Iterator join(Iterator outerIterator, Iterator innerIterator, ComparisonPredicate predicate, Set<String> attrNames) {
    return new JoinIterator(outerIterator, innerIterator, predicate, attrNames);
  }

  @Override
  public StatusCode insert(String tableName, Record record, String[] primaryKeys) {

    HashMap<String, Value> vals = record.getMapAttrNameToValue();
    
    ArrayList<Object> pkValues = new ArrayList<>();
    ArrayList<Object> nonPkValues = new ArrayList<>();
    ArrayList<String> nonPkNames = new ArrayList<>(); 

    HashSet<String> pk = new HashSet<>(Arrays.asList(primaryKeys));

    for (Map.Entry<String, Value> e : vals.entrySet()) {
      if (pk.contains(e.getKey())) {
        pkValues.add(e.getValue().getValue());
      } else { 
        nonPkValues.add(e.getValue().getValue());
        nonPkNames.add(e.getKey());
      }
    }

    Object[] primaryKeysValues = pkValues.toArray(new Object[pkValues.size()]);
    Object[] attrValues = nonPkValues.toArray(new Object[nonPkValues.size()]); 
    String[] attrNames = nonPkNames.toArray(new String[nonPkNames.size()]);

    RecordsImpl x = new RecordsImpl(); 

    return x.insertRecord(tableName, primaryKeys, primaryKeysValues, attrNames, attrValues);
  }

  @Override
  public StatusCode update(String tableName, AssignmentExpression assignExp, Iterator dataSourceIterator) {

    if(dataSourceIterator == null){
      Iterator x = select(tableName, new ComparisonPredicate(), Mode.READ_WRITE, false);
      while (x.next() != null) {
        System.out.println("Update Record: " + x.update(assignExp)); 
      }
      x.commit();
      return StatusCode.SUCCESS; 
    } else {
      while (dataSourceIterator.next() != null) {
        System.out.println("Update Record: " + dataSourceIterator.update(assignExp));
      }
      dataSourceIterator.commit();
      return StatusCode.SUCCESS; 
    }
  }

  @Override
  public StatusCode delete(String tableName, Iterator iterator) {
    if(iterator == null){
      Iterator x = select(tableName, new ComparisonPredicate(), Mode.READ_WRITE, false);
      while (x.next() != null) {
        System.out.println("Delete Record: " + ((SelectIterator)x).delete()); 
      }
      x.commit();
      return StatusCode.SUCCESS; 
    } else {
      while (iterator.next() != null) {
        System.out.println("Delete Record: " + ((SelectIterator)iterator).delete());
      }
      iterator.commit();
      return StatusCode.SUCCESS; 
    }
  }
}
