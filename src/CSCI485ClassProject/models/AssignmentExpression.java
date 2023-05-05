package CSCI485ClassProject.models;

import CSCI485ClassProject.StatusCode;

import java.util.HashMap;
import java.util.Map;

import static CSCI485ClassProject.StatusCode.PREDICATE_OR_EXPRESSION_INVALID;

public class AssignmentExpression {
  public enum Type {
    ONE_ATTR, // only one attribute is referenced, e.g. Salary = 1500, Name = "Bob"
    TWO_ATTRS, // two attributes are referenced, e.g. Salary = 1.5 * Age
  }

  private Type expressionType;

  public Type getExpressionType() {
    return expressionType;
  }

  private String leftHandSideAttrName; // e.g. Salary = 1.1 * Age
  
  public String getLeftHandSideAttrName() {
    return leftHandSideAttrName;
  }

  public void setLeftHandSideAttrName(String leftHandSideAttrName) {
    this.leftHandSideAttrName = leftHandSideAttrName;
  }

  public AttributeType getLeftHandSideAttrType() {
    return leftHandSideAttrType;
  }

  public void setLeftHandSideAttrType(AttributeType leftHandSideAttrType) {
    this.leftHandSideAttrType = leftHandSideAttrType;
  }

  public Object getRightHandSideValue() {
    return rightHandSideValue;
  }

  public void setRightHandSideValue(Object rightHandSideValue) {
    this.rightHandSideValue = rightHandSideValue;
  }

  public AlgebraicOperator getRightHandSideOperator() {
    return rightHandSideOperator;
  }

  public void setRightHandSideOperator(AlgebraicOperator rightHandSideOperator) {
    this.rightHandSideOperator = rightHandSideOperator;
  }

  public String getRightHandSideAttrName() {
    return rightHandSideAttrName;
  }

  public void setRightHandSideAttrName(String rightHandSideAttrName) {
    this.rightHandSideAttrName = rightHandSideAttrName;
  }

  public AttributeType getRightHandSideAttrType() {
    return rightHandSideAttrType;
  }

  public void setRightHandSideAttrType(AttributeType rightHandSideAttrType) {
    this.rightHandSideAttrType = rightHandSideAttrType;
  }

  private AttributeType leftHandSideAttrType;

  // either a specific value, or another attribute
  private Object rightHandSideValue = null; // in the example, it is 1.1
  private AlgebraicOperator rightHandSideOperator; // in the example, it is *
  private String rightHandSideAttrName; // in the example, it is Age
  private AttributeType rightHandSideAttrType;

  // e.g. Salary = 2000
  public AssignmentExpression(String leftHandSideAttrName, AttributeType leftHandSideAttrType, Object rightHandSideValue) {
    expressionType = Type.ONE_ATTR;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.rightHandSideValue = rightHandSideValue;
  }

  // e.g. Salary = Age * 2
  public AssignmentExpression(String leftHandSideAttrName, AttributeType leftHandSideAttrType, String rightHandSideAttrName, AttributeType rightHandSideAttrType, Object rightHandSideValue, AlgebraicOperator rightHandSideOperator) {
    expressionType = Type.TWO_ATTRS;
    this.leftHandSideAttrName = leftHandSideAttrName;
    this.leftHandSideAttrType = leftHandSideAttrType;
    this.rightHandSideAttrName = rightHandSideAttrName;
    this.rightHandSideAttrType = rightHandSideAttrType;
    this.rightHandSideValue = rightHandSideValue;
    this.rightHandSideOperator = rightHandSideOperator;
  }

  public StatusCode validate() {
    if (expressionType == Type.ONE_ATTR) {
      // e.g. Salary = 2000
      if (leftHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long))
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float))
          || (leftHandSideAttrType == AttributeType.VARCHAR && !(rightHandSideValue instanceof String))) {
        return StatusCode.PREDICATE_OR_EXPRESSION_INVALID;
      }
    } else if (expressionType == Type.TWO_ATTRS) {
      // e.g. Salary = 10 * Age
      if (leftHandSideAttrType == AttributeType.NULL || rightHandSideAttrType == AttributeType.NULL
          || (leftHandSideAttrType == AttributeType.VARCHAR || rightHandSideAttrType == AttributeType.VARCHAR)
          || (leftHandSideAttrType != rightHandSideAttrType)
          || (leftHandSideAttrType == AttributeType.INT && !(rightHandSideValue instanceof Integer) && !(rightHandSideValue instanceof Long)
          || (leftHandSideAttrType == AttributeType.DOUBLE && !(rightHandSideValue instanceof Double) && !(rightHandSideValue instanceof Float)))) {
        return PREDICATE_OR_EXPRESSION_INVALID;
      }
    }
    return StatusCode.PREDICATE_OR_EXPRESSION_VALID;
  }
}
