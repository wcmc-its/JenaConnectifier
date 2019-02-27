/*******************************************************************************
 * Copyright (c) 2010-2011 VIVO Harvester Team. For full list of contributors, please see the AUTHORS file provided.
 * All rights reserved.
 * This program and the accompanying materials are made available under the terms of the new BSD license which accompanies this distribution, and is available at http://www.opensource.org/licenses/bsd-license.html
 ******************************************************************************/
package org.vivoweb.harvester.util.repo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vivoweb.harvester.util.repo.RecordMetaData.RecordMetaDataType;

/**
 * RecordHandler that stores data in a JDBC Database
 * @author Christopher Haines (hainesc@ctrip.ufl.edu)
 */
public class JDBCRecordHandler extends RecordHandler {
	/**
	 * SLF4J Logger
	 */
	protected static Logger log = LoggerFactory.getLogger(JDBCRecordHandler.class);
	/**
	 * Database connection
	 */
	protected Connection db;
	/**
	 * Database statement processor
	 */
	protected Statement cursor;
	/**
	 * Table name
	 */
	protected String table;
	/**
	 * Recod table must have this field to store the identifier for each record... should be indexed
	 */
	private static final String recordIdField = "recordID";
	/**
	 * Metadata table must have this field to store the recordID for it's record
	 */
	private static final String rmdRelField = "record_id";
	/**
	 * Metadata table must have this field to store the timestamp
	 */
	private static final String rmdCalField = "utc_milli_from_epoch";
	/**
	 * Metadata table must have this field to store the operation
	 */
	private static final String rmdOperationField = "rmdoperation";
	/**
	 * Metadata table must have this field to store the operator
	 */
	private static final String rmdOperatorField = "rmdoperator";
	/**
	 * Metadata table must have this field to store the md5
	 */
	private static final String rmdMD5Field = "md5";
	/**
	 * Field to store data in
	 */
	protected String dataField;
	
	/**
	 * Default Constructor
	 */
	protected JDBCRecordHandler() {
		// Nothing to do here
		// Used by config construction
		// Should only be used in conjunction with setParams()
	}
	
	/**
	 * Constructor
	 * @param jdbcDriverClass jdbc driver class
	 * @param connLine jdbc connection string
	 * @param username username to use for connection
	 * @param password password to use for connection
	 * @param tableName name of table to use
	 * @param dataFieldName name of field to store data in
	 * @throws IOException invalidly configured database
	 */
	public JDBCRecordHandler(String jdbcDriverClass, String connLine, String username, String password, String tableName, String dataFieldName) throws IOException {
		initAll(jdbcDriverClass, connLine, username, password, tableName, dataFieldName);
	}
	
	/**
	 * Constructor
	 * @param jdbcDriverClass jdbc driver class
	 * @param connType jdbc connection type
	 * @param host host to connect to
	 * @param port port to connect on
	 * @param dbName name of database to connect to
	 * @param username username to use for connection
	 * @param password password to use for connection
	 * @param tableName name of table to use
	 * @param dataFieldName name of field to store data in
	 * @throws IOException invalidly configured database
	 */
	public JDBCRecordHandler(String jdbcDriverClass, String connType, String host, String port, String dbName, String username, String password, String tableName, String dataFieldName) throws IOException {
		this(jdbcDriverClass, buildConnLine(connType, host, port, dbName), username, password, tableName, dataFieldName);
	}
	
	@Override
	protected void finalize() throws Throwable {
		this.cursor.close();
		this.db.close();
	}
	
	/**
	 * Build a connection line using its components
	 * @param connType connection type
	 * @param host host name
	 * @param port port number
	 * @param dbName database name
	 * @return jdbc connection line
	 */
	private static String buildConnLine(String connType, String host, String port, String dbName) {
		return "jdbc:" + connType + "://" + host + ":" + port + "/" + dbName;
	}
	
	/**
	 * Initialize variables
	 * @param jdbcDriverClass jdbc connection driver
	 * @param connLine jdbc connection line
	 * @param username username to connect with
	 * @param password password to connect with
	 * @param tableName tablename to write to
	 * @param dataFieldName field to store data in
	 * @throws IOException connection error
	 */
	private void initAll(String jdbcDriverClass, String connLine, String username, String password, String tableName, String dataFieldName) throws IOException {
		this.table = tableName;
		if(this.table == null) {
			this.table = "recordTable";
		}
		this.dataField = dataFieldName;
		if(this.dataField == null) {
			this.dataField = "dataField";
		}
		try {
			Class.forName(jdbcDriverClass);
			this.db = DriverManager.getConnection(connLine, username, password);
			this.cursor = this.db.createStatement();
			if(!checkTableExists(this.table)) {
				log.trace("Database Does Not Contain Table: " + this.table + ". Attempting to create.");
				createTable();
			}
			checkTableConfigured();
			if(!checkTableExists(this.table+"_rmd")) {
				log.trace("Database Does Not Contain Table: " + this.table + "_rmd. Attempting to create.");
				createMetaTable();
			}
			checkMetaTableConfigured();
		} catch(ClassNotFoundException e) {
			throw new IllegalArgumentException(e);
		} catch(SQLException e) {
			throw new IOException("Error in communication with database", e);
		}
	}
	
	/**
	 * Create the record table
	 * @throws IOException error creating table
	 */
	private void createTable() throws IOException {
		try {
			this.cursor.executeUpdate("CREATE TABLE IF NOT EXISTS `" + this.table + "` ( `" + recordIdField + "` varchar(255) NOT NULL PRIMARY KEY, `" + this.dataField + "` blob NOT NULL)");
		} catch(SQLException e) {
			throw new IOException("Cannot Create Table: " + this.table, e);
		}
	}
	
	/**
	 * Checks if the record table is properly configured
	 * @throws IOException table not configured correctly
	 */
	private void checkTableConfigured() throws IOException {
		try {
			this.cursor.execute("select " + recordIdField + ", " + this.dataField + " from " + this.table);
		} catch(SQLException e) {
			throw new IOException("Table '" + this.table + "' Is Not Structured Correctly", e);
		}
	}
	
	/**
	 * Checks if the record table exists, attempts to create if table does not exist
	 * @param tableName the table name to check
	 * @return if it exists
	 */
	private boolean checkTableExists(String tableName) {
		boolean a;
		try {
			// ANSI SQL way. Works in PostgreSQL, MSSQL, MySQL
			this.cursor.execute("select case when exists((select * from information_schema.tables where table_name = '" + tableName + "')) then 1 else 0 end");
			a = this.cursor.getResultSet().getBoolean(1);
		} catch(SQLException e) {
			try {
				// Other RDBMS. Graceful degradation
				a = true;
				this.cursor.execute("select 1 from " + tableName + " where 1 = 0");
			} catch(SQLException e1) {
				a = false;
			}
		}
		return a;
	}
	
	/**
	 * Create the record metadata table
	 * @throws IOException error creating table
	 */
	private void createMetaTable() throws IOException {
		try {
			this.cursor.executeUpdate("DROP TABLE IF EXISTS `" + this.table + "_rmd`");
			this.cursor.executeUpdate("CREATE TABLE IF NOT EXISTS `" + this.table + "_rmd` ( `" + rmdRelField + "` varchar(255) NOT NULL, `" + rmdCalField + "` varchar(25) NOT NULL, `" + rmdOperationField + "` varchar(10) NOT NULL, `" + rmdOperatorField + "` varchar(255) NOT NULL, `" + rmdMD5Field + "` varchar(32) NOT NULL, CONSTRAINT fk_RecordRel FOREIGN KEY (`" + rmdRelField + "`) REFERENCES `" + this.table + "` (`" + recordIdField + "`))");
			this.cursor.executeUpdate("CREATE INDEX `ind_" + this.table + "_" + rmdCalField + "` ON `" + this.table + "_rmd` (`" + rmdCalField + "`)");
			this.cursor.executeUpdate("CREATE INDEX `ind_" + this.table + "_" + rmdRelField + "` ON `" + this.table + "_rmd` (`" + rmdRelField + "`)");
		} catch(SQLException e) {
			throw new IOException("Cannot Create Table: " + this.table + "_rmd", e);
		}
	}
	
	/**
	 * Checks if the record metadata table is properly configured
	 * @throws IOException table not configured correctly
	 */
	private void checkMetaTableConfigured() throws IOException {
		try {
			this.cursor.execute("select " + rmdCalField + ", " + rmdMD5Field + ", " + rmdOperationField + ", " + rmdOperatorField + ", " + rmdRelField + " from " + this.table + "_rmd");
		} catch(SQLException e) {
			throw new IOException("Table '" + this.table + "_rmd' Is Not Structured Correctly", e);
		}
	}
	
	@Override
	public boolean addRecord(Record rec, Class<?> creator, boolean overwrite) throws IOException {
		if(!needsUpdated(rec)) {
			return false;
		}
		try {
			PreparedStatement ps = this.db.prepareStatement("insert into " + this.table + "(" + recordIdField + ", " + this.dataField + ") values (?, ?)");
			ps.setString(1, rec.getID());
			ps.setBytes(2, rec.getData().getBytes());
			ps.executeUpdate();
		} catch(SQLException e) {
			if(overwrite) {
				log.trace("Unable to add record: atempting to update existing record");
				try {
					PreparedStatement ps = this.db.prepareStatement("update " + this.table + " set " + this.dataField + " = ? where " + recordIdField + " = ?");
					ps.setString(2, rec.getID());
					ps.setBytes(1, rec.getData().getBytes());
					ps.executeUpdate();
				} catch(SQLException e2) {
					throw new IOException("Unable to update record: " + rec.getID(), e2);
				}
			} else {
				throw new IOException("Unable to add record: " + rec.getID(), e);
			}
		}
		addMetaData(rec, creator, RecordMetaDataType.written);
		return true;
	}
	
	@Override
	public void delRecord(String recID) throws IOException {
		delMetaData(recID);
		try {
			this.cursor.execute("delete from " + this.table + " where " + recordIdField + " = '" + recID + "'");
		} catch(SQLException e) {
			throw new IOException("Unable to delete record: " + recID, e);
		}
	}
	
	@Override
	public String getRecordData(String recID) throws IOException {
		try {
			ResultSet existrs = this.cursor.executeQuery("select count(*) from " + this.table + " where " + recordIdField + " = '" + recID + "'");
			existrs.first();
			if(existrs.getInt(1) < 1) {
				throw new IllegalArgumentException("Record " + recID + " does not exist!");
			}
			ResultSet rs = this.cursor.executeQuery("select " + this.dataField + " from " + this.table + " where " + recordIdField + " = '" + recID + "'");
			rs.first();
			return new String(rs.getBytes(1));
		} catch(SQLException e) {
			throw new IOException("Unable to retrieve record: " + recID, e);
		}
	}
	
	
	public Iterator<Record> iterator() {
		JDBCRecordIterator ri = null;
		try {
			ri = new JDBCRecordIterator();
		} catch(SQLException e) {
			log.error("Unable to retrieve records");
			log.debug("Stacktrace:",e);
		}
		return ri;
	}
	
	/**
	 * Iterator for JDBCRecordHandler
	 * @author Christopher Haines (hainesc@ctrip.ufl.edu)
	 */
	private class JDBCRecordIterator implements Iterator<Record> {
		/**
		 * The result set for records in a database
		 */
		ResultSet rs;
		
		/**
		 * Default Constructor
		 * @throws SQLException failed to read records
		 */
		protected JDBCRecordIterator() throws SQLException {
			this.rs = JDBCRecordHandler.this.db.createStatement().executeQuery("select " + JDBCRecordHandler.recordIdField + " from " + JDBCRecordHandler.this.table + " order by " + JDBCRecordHandler.recordIdField);
		}
		
		
		public boolean hasNext() {
			try {
				return this.rs.next();
			} catch(SQLException e) {
				log.error("Unable to retrieve next record");
				log.debug("Stacktrace:",e);
				return false;
			}
		}
		
		
		public Record next() {
			try {
				return getRecord(this.rs.getString(JDBCRecordHandler.recordIdField));
			} catch(SQLException e) {
				log.debug(e.getMessage(), e);
				throw new NoSuchElementException(e.getMessage());
			} catch(IOException e) {
				log.debug(e.getMessage(), e);
				throw new NoSuchElementException(e.getMessage());
			}
		}
		
		
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	@Override
	public void setParams(Map<String, String> params) throws IllegalArgumentException, IOException {
		String dbClass = getParam(params, "dbClass", true);
		String dbUrl = getParam(params, "dbUrl", true);
		String dbUser = getParam(params, "dbUser", true);
		String dbPass = getParam(params, "dbPass", true);
		String dbTable = getParam(params, "dbTable", false);
		String dataFieldName = getParam(params, "dataFieldName", false);
		initAll(dbClass, dbUrl, dbUser, dbPass, dbTable, dataFieldName);
	}
	
	@Override
	protected void addMetaData(Record rec, RecordMetaData rmd) throws IOException {
		try {
			this.cursor.executeUpdate("insert into " + this.table + "_rmd (" + rmdRelField + ", " + rmdCalField + ", " + rmdOperationField + ", " + rmdOperatorField + ", " + rmdMD5Field + ") values ('" + rec.getID() + "', '" + rmd.getDate().getTimeInMillis() + "', '" + rmd.getOperation() + "', '" + rmd.getOperator().getName() + "', '" + rmd.getMD5() + "')");
		} catch(SQLException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	protected void delMetaData(String recID) throws IOException {
		try {
			this.cursor.executeUpdate("delete from " + this.table + "_rmd where " + rmdRelField + "='" + recID + "'");
		} catch(SQLException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	public SortedSet<RecordMetaData> getRecordMetaData(String recID) throws IOException {
		SortedSet<RecordMetaData> retVal = new TreeSet<RecordMetaData>();
		try {
			ResultSet rs = this.cursor.executeQuery("select " + rmdCalField + ", " + rmdOperationField + ", " + rmdOperatorField + ", " + rmdMD5Field + " from " + this.table + "_rmd where " + rmdRelField + "='" + recID + "' order by " + rmdCalField + " desc");
			while(rs.next()) {
				Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.US);
				cal.setTimeInMillis(Long.parseLong(rs.getString(rmdCalField)));
				RecordMetaDataType operation = RecordMetaDataType.valueOf(rs.getString(rmdOperationField));
				Class<?> operator = Class.forName(rs.getString(rmdOperatorField));
				String md5 = rs.getString(rmdMD5Field);
				retVal.add(new RecordMetaData(cal, operator, operation, md5));
			}
		} catch(SQLException e) {
			throw new IOException(e);
		} catch(ClassNotFoundException e) {
			throw new IOException(e);
		}
		if(retVal.isEmpty()) {
			throw new IOException("No Matching MetaData Found");
		}
		return retVal;
	}
	
	@Override
	public void close() throws IOException {
		try {
			this.cursor.close();
			this.db.close();
		} catch(SQLException e) {
			throw new IOException(e);
		}
	}
	
	@Override
	public Set<String> find(String idText) throws IOException {
		Set<String> retVal = new HashSet<String>();
		String query = "SELECT " + recordIdField + " FROM " + this.table + " WHERE " + recordIdField + " LIKE '%" + idText + "%' ORDER BY " + recordIdField;
		try {
			ResultSet rs = this.cursor.executeQuery(query);
			while(rs.next()) {
				retVal.add(rs.getString(1));
			}
		} catch(SQLException e) {
			throw new IOException(e);
		}
		return retVal;
	}
}
