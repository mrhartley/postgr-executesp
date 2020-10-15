package com.appiancorp.ps.function;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(namespace = ProcedureInput.NAMESPACE, name = ProcedureInput.LOCAL_PART)
@XmlType(namespace = ProcedureInput.NAMESPACE, name = ProcedureInput.LOCAL_PART)
public class ProcedureInput {

  public static final String NAMESPACE = "urn:appian:plugin:executestoredfunction:types";
  public static final String LOCAL_PART = "ProcedureInput";

  @XmlElement(name = "name")
  private String name;

  @XmlElement(name = "value")
  private Object value;

  public ProcedureInput() {
    this.name = "";
    this.value = new Object();
  }

  public ProcedureInput(String name, Object value) {
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public Object getValue() {
    return value;
  }
}