package com.appiancorp.ps.ss;

import com.appiancorp.ps.function.ProcedureInput;
import com.appiancorp.suiteapi.process.ActivityClassParameter;

public class ProcedureParameter {
  private String parameterName;
  private boolean isInput;
  private boolean isOutput;
  private int sqlDataType;
  private ActivityClassParameter acp;
  private Object value;

  public ProcedureParameter(String parameterName, boolean isInput, boolean isOutput, int sqlDataType, ActivityClassParameter acp) {
    this.parameterName = parameterName;
    this.isInput = isInput;
    this.isOutput = isOutput;
    this.sqlDataType = sqlDataType;
    this.acp = acp;
    if (acp != null) {
      this.value = acp.getValue();
    }
  }

  public ProcedureParameter(String parameterName, boolean isInput, boolean isOutput, int sqlDataType, ProcedureInput input) {
    this.parameterName = parameterName;
    this.isInput = isInput;
    this.isOutput = isOutput;
    this.sqlDataType = sqlDataType;
    if (input != null) {
      this.value = input.getValue();
    }
  }

  @Override
  public String toString() {
    return "[name: " + parameterName + ", value: " + value + ", type: " + sqlDataType + ", isInput: " + isInput + "]";
  }

  public String getParameterName() {
    return parameterName;
  }

  public boolean isInput() {
    return isInput;
  }

  public boolean isOutput() {
    return isOutput;
  }

  public int getSQLDataType() {
    return sqlDataType;
  }

  public ActivityClassParameter getACP() {
    return acp;
  }

  public Object getValue() {
    return value;
  }
}
