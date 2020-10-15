package com.appiancorp.ps.ss;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.log4j.Logger;

import com.appiancorp.suiteapi.common.Name;
import com.appiancorp.suiteapi.process.ActivityClassParameter;
import com.appiancorp.suiteapi.process.ProcessExecutionService;
import com.appiancorp.suiteapi.process.exceptions.SmartServiceException;
import com.appiancorp.suiteapi.process.framework.AppianSmartService;
import com.appiancorp.suiteapi.process.framework.Input;
import com.appiancorp.suiteapi.process.framework.MessageContainer;
import com.appiancorp.suiteapi.process.framework.Order;
import com.appiancorp.suiteapi.process.framework.Required;
import com.appiancorp.suiteapi.process.framework.SmartServiceContext;
import com.appiancorp.suiteapi.process.palette.PaletteInfo;
import com.appiancorp.suiteapi.type.AppianType;
import com.appiancorp.suiteapi.type.TypeService;

@Deprecated
@PaletteInfo(paletteCategory = "#Deprecated#", palette = "#Deprecated#")
@Order({ "DataSourceName", "ProcedureName" })
public class ExecuteStoredProcedurev2 extends AppianSmartService {
	private static final Logger LOG = Logger.getLogger(ExecuteStoredProcedurev2.class);
	private static final int TYPE_ORACLE_CURSOR = -10;

	private SmartServiceContext ctx;
	private ProcessExecutionService pes;
	private TypeService ts;
	private Context initialCtx;
	private Long processId;
	private Connection conn;
	private String dataSourceName;
	private String procedureName;
	private String searchCatalogName = null;
	private String searchSchemaName = null;
	private String searchProcedureName;
	private String fullProcedureName;
	private List<ProcedureParameter> parameterList = new ArrayList<ProcedureParameter>();
	private HashMap<Integer, ActivityClassParameter> resultSetList = new HashMap<Integer, ActivityClassParameter>();

	public ExecuteStoredProcedurev2(SmartServiceContext ctx, ProcessExecutionService pes, TypeService ts, Context initialCtx) throws SQLException, NamingException {
		super();

		this.ctx = ctx;
		this.pes = pes;
		this.ts = ts;
		this.initialCtx = initialCtx;
		this.processId = ctx.getProcessProperties().getId();
	}

	@Override
	public void run() throws SmartServiceException {
		try {
		  conn.setAutoCommit(true);
		  
			CallableStatement call = conn.prepareCall("{call " + procedureName + "(" + getParameters() + ")}");

			for (int i=0; i<parameterList.size(); i++) {
				int paramIndex = i + 1;
				ProcedureParameter param = parameterList.get(i);

				if (param.isInput()) {
					if (param.getValue() == null) {
						LOG.debug("[" + processId + "] " + fullProcedureName + " setting input " + param.getParameterName() + " as null");
						call.setNull(paramIndex, param.getSQLDataType());
					} else {
						LOG.debug("[" + processId + "] " + fullProcedureName + " setting input " + param.getParameterName() + " to " + param.getValue());
						call.setObject(paramIndex, param.getValue());
					}
				}

				if (param.isOutput()) {
					LOG.debug("[" + processId + "] " + fullProcedureName + " registering output " + param.getParameterName());
					call.registerOutParameter(paramIndex, param.getSQLDataType());
				}
			}

			boolean hasResultSet = call.execute();

			// set output values
			for (int i=0; i<parameterList.size(); i++) {
				int paramIndex = i + 1;
				ProcedureParameter param = parameterList.get(i);
				ActivityClassParameter acp = param.getACP();

				if (param.isOutput() && acp != null) {
					int instanceType = acp.getInstanceType().intValue();

					Object resultValue = ACPValue.populate(processId, fullProcedureName, ts, call, paramIndex, instanceType);

					LOG.debug("[" + processId + "] " + fullProcedureName + " setting output " + param.getParameterName() + " to " + resultValue);
					acp.setValue(resultValue);
				}
			}

			// set output result sets
			if (resultSetList.size() > 0) {
				int currentIndex = 1;

				while (hasResultSet) {
					LOG.debug("[" + processId + "] " + fullProcedureName + " found result set number: " + currentIndex);

					ResultSet rs = call.getResultSet();

					if (resultSetList.containsKey(currentIndex)) {
						ActivityClassParameter acp = resultSetList.get(currentIndex);

						Object resultValue = ACPValue.populate(processId, fullProcedureName, ts, acp.getInstanceType(), rs);

						LOG.debug("[" + processId + "] " + fullProcedureName + " setting rs output " + acp.getName() + " to " + resultValue);
						acp.setValue(resultValue);
					}

					++currentIndex;
					hasResultSet = call.getMoreResults();
				}
			}

			call.close();

			// save output ACPs
			pes.saveActivityParameters(ctx.getTaskProperties().getId(), ctx.getMetadata().getParameters());
		} catch (Exception e) {
			LOG.error("Error during execution", e);
			throw new SmartServiceException.Builder(ExecuteStoredProcedurev2.class, e).build();
		} finally {
			try {
				conn.close();
			} catch (SQLException sqle) {
				LOG.error("Error closing connection", sqle);
			}
		}
	}

	private String getFullProcedureName(String cName, String sName, String pName) {
		String name = pName;

		if (cName != null) {
			name = cName + "." + name;
		}

		if (sName != null) {
			name = sName + "." + name;
		}

		return name;
	}

	private String getParameters() {
		StringBuilder sb = new StringBuilder();

		for (int i=0; i<parameterList.size(); i++) {
			if (i == 0) {
				sb.append("?");
			} else {
				sb.append(", ?");
			}
		}

		return sb.toString();
	}

	public void validate(MessageContainer messages) {
		if (searchProcedureName == null || searchProcedureName.isEmpty()) {
			messages.addError("ProcedureName", "procName.empty");
			return;
		}

		if (dataSourceName == null || dataSourceName.isEmpty()) {
			messages.addError("DataSourceName", "dsn.empty");
			return;
		}

		DataSource ds;

		try {
			ds = (DataSource) initialCtx.lookup(dataSourceName);
		} catch (NamingException ne) {
			LOG.error("Error looking up data source: " + dataSourceName, ne);
			messages.addError("DataSourceName", "dsn.notFound");
			return;
		}

		try {
			conn = ds.getConnection();
		} catch (SQLException sqle) {
			LOG.error("Error obtaining connection", sqle);
			messages.addError("DataSourceName", "conn.unableToConnect", sqle.getMessage());
			return;
		}

		try {
			DatabaseMetaData dbMetaData = conn.getMetaData();

			ResultSet procRs = dbMetaData.getProcedures(searchCatalogName, searchSchemaName, searchProcedureName);
			StringBuilder procsAvailableSb = new StringBuilder();
			int procCount = 0;

			while (procRs.next()) {
				String cName = procRs.getString("PROCEDURE_CAT");
				String sName = procRs.getString("PROCEDURE_SCHEM");
				String pName = procRs.getString("PROCEDURE_NAME");

				fullProcedureName = getFullProcedureName(cName, sName, pName);

				if (procCount > 0) {
					procsAvailableSb.append(", ");
				}

				procsAvailableSb.append(fullProcedureName);

				++procCount;
			}

			procRs.close();

			if (procCount == 0) {
				messages.addError("ProcedureName", "proc.notFound", getFullProcedureName(searchCatalogName, searchSchemaName, searchProcedureName));
				return;
			} else if (procCount > 1) {
				messages.addError("ProcedureName", "proc.manyFound", procCount, procsAvailableSb);
				return;
			}

			ResultSet colRs = dbMetaData.getProcedureColumns(searchCatalogName, searchSchemaName, searchProcedureName, null);

			while (colRs.next()) {
				String cName = colRs.getString("PROCEDURE_CAT");
				String sName = colRs.getString("PROCEDURE_SCHEM");
				String pName = colRs.getString("PROCEDURE_NAME");
				short columnType = colRs.getShort("COLUMN_TYPE");
				String columnName = colRs.getString("COLUMN_NAME");
				int dataType = colRs.getInt("DATA_TYPE");
				String typeName = colRs.getString("TYPE_NAME");

				boolean isInput;
				boolean isOutput;
				ActivityClassParameter acp;

				LOG.debug("[" + processId + "] " + getFullProcedureName(cName, sName, pName) + " has column " + columnName + " with data type " + dataType + " (" + typeName + ") and column type " + columnType);

				dataType = resolveDataType(dataType, typeName);

				if (columnName == null) {
					continue;
				}

				if (columnName.startsWith("@")) {
					columnName = columnName.substring(1);
				}

				switch(columnType) {
					case DatabaseMetaData.procedureColumnIn:
						isInput = true;
						isOutput = false;
						acp = getACP(columnName, columnType);

						if (acp == null) {
							messages.addError("ProcedureName", "proc.missingRequiredInput", columnName);
						}
					break;
					case DatabaseMetaData.procedureColumnOut:
						isInput = false;
						isOutput = true;
						acp = getACP(columnName, columnType);
					break;
					case DatabaseMetaData.procedureColumnInOut:
						isInput = true;
						isOutput = true;
						acp = getACP(columnName, columnType);
					break;
					case DatabaseMetaData.procedureColumnReturn:
						// do nothing
						continue;
					default:
						messages.addError("ProcedureName", "proc.unsupportedInput", columnType, columnName);
						return;
				}

				if (dataType == TYPE_ORACLE_CURSOR && acp != null) {
					if (acp.getInstanceType() < AppianType.INITIAL_CUSTOM_TYPE) {
						messages.addError("ProcedureName", "proc.invalidType", acp.getName());
						return;
					}
				}

				parameterList.add(new ProcedureParameter(columnName, isInput, isOutput, dataType, acp));
			}

			colRs.close();

			// set result set list
			ActivityClassParameter[] acps = ctx.getMetadata().getParameters();

			for (ActivityClassParameter acp : acps) {
				if (acp.getName().toLowerCase().startsWith("resultset")) {
					LOG.debug("[" + processId + "] " + fullProcedureName + " found result set input: " + acp.getName());

					if (acp.getInstanceType() < AppianType.INITIAL_CUSTOM_TYPE) {
						messages.addError("ResultSet", "rs.invalidType", acp.getName());
						return;
					}

					try {
						resultSetList.put(
							Integer.parseInt(acp.getName().substring(9)),
							acp
						);
					} catch (NumberFormatException nfe) {
						LOG.warn("Unexpected value for ResultSet number", nfe);
						messages.addError("ResultSet", "rs.invalidName", acp.getName());
					}
				}
			}
		} catch (Throwable t) {
			LOG.error("Error during validation", t);
			messages.addError("ProcedureName", "proc.sqlError", t.getMessage());
		} finally {
			if (!messages.isEmpty()) {
				try {
					conn.close();
				} catch (SQLException e) {
					LOG.error("Error closing connection", e);
				}
			}
		}
	}

	// Oracle thin driver often returns Types.OTHER for known types, so correct those here.
	private int resolveDataType(int sqlDataType, String sqlTypeName) {
		if (sqlDataType == Types.OTHER){
			if (sqlTypeName.equals("REF CURSOR")) {
				return TYPE_ORACLE_CURSOR;
			} else if (sqlTypeName.equals("FLOAT")) {
				return Types.FLOAT;
			} else if (sqlTypeName.equals("CLOB")) {
				return Types.CLOB;
			}
		}

		return sqlDataType;
	}

	private ActivityClassParameter getACP(String name, short columnType) {
		ActivityClassParameter[] acps = ctx.getMetadata().getParameters();
		ActivityClassParameter foundACP = null;

		for (ActivityClassParameter acp : acps) {
			if (acp.getName().equalsIgnoreCase(name)) {
				foundACP = acp;
				break;
			}
		}

		return foundACP;
	}

	@Input(required = Required.ALWAYS)
	@Name("DataSourceName")
	public void setDataSourceName(String val) {
		dataSourceName = val;
	}

	@Input(required = Required.ALWAYS)
	@Name("ProcedureName")
	public void setProcedureName(String val) {
		procedureName = val;
		searchProcedureName = val;

		if (val.contains(".")) {
			String[] names = val.split("\\.");

			if (names.length > 3) {
				// invalid, do nothing
			} else if (names.length > 2) {
				searchCatalogName = names[1];
				searchSchemaName = names[0];
				searchProcedureName = names[2];
			} else if (names.length > 1) {
				searchSchemaName = names[0];
				searchProcedureName = names[1];
			}
		}
	}
}