package org.vivoweb.harvester.ingest;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import org.slf4j.*;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.unboundid.ldap.sdk.SearchResultEntry;

import org.vivoweb.harvester.connectionfactory.LDAPConnectionFactory;
import org.vivoweb.harvester.connectionfactory.RDBMSConnectionFactory;
import org.vivoweb.harvester.beans.FacultyAppointments;
import java.util.*;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager; 


public class AppointmentsFetchtoDashboard {
	
	private Set<FacultyAppointments> appointments = new HashSet<FacultyAppointments>();
	private ArrayList<String> departments = new ArrayList<String>();

	private Properties props = new Properties();
	private static Logger log = LoggerFactory.getLogger(AppointmentsFetchtoDashboard.class);
	
	
	private static String propertyFilePath = null;
	
	private Connection con = null;
	
	RDBMSConnectionFactory mcf = RDBMSConnectionFactory.getInstance(this.propertyFilePath);	
	
	
	/**
	 * Main method
	 * 
	 * @param args
	 *            command-line arguments
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void main (String args[]) {
		if (args.length == 0) {
			log.info("Usage: java fetch.JSONPeopleFetch [properties filename]");
			log.info("e.g. java fetch.JSONPeopleFetch /usr/share/vivo-ed-people/examples/wcmc_people.properties");
		} else if (args.length == 1) { // path of the properties file
			propertyFilePath = args[0];
			log.info(propertyFilePath);
			new AppointmentsFetchtoDashboard().init(args[0]);
		}
	}
		
		
		private void init(String propertiesFile) {


			execute();

		}
		
		private void execute() {
			
			//getActivePeopleFromED();
			//getStudentsFromED();
			String sid = getSplunkSid();
			if(sid != null && !sid.isEmpty()) {
				callSplunkAPI(sid);
			}
			insertAppointmentsToPantheonDB(this.appointments);
			log.info("Appointment fetch completed successfully...");
		}
		
		private void getActivePeopleFromED() {
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssZ");
			sdf.setTimeZone(TimeZone.getTimeZone("EDT"));
			Date now = new Date();
			String strDate = sdf.format(now);
			
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_YEAR, -6);
			List<String> startYear = new ArrayList<String>();
			List<String> endYear = new ArrayList<String>();
			
			LDAPConnectionFactory lcf = LDAPConnectionFactory.getInstance(propertyFilePath);
			//String filter = "(&(ou=faculty)(objectClass=weillCornellEduSORRoleRecord)(|(&(createTimestamp>="+ sdf.format(cal.getTime()) + ")(createTimestamp<=" +strDate + "))(&(modifyTimestamp>="+ sdf.format(cal.getTime())+ ")(modifyTimestamp<=" +strDate + "))))";
			String filter = "(&(!(title=*Emerit*))(!(title=Adjunct*))(!(title=Visiting*))(!(title=*Courtesy*))(objectClass=weillCornellEduSORRoleRecord)(weillCornellEduFTE=100)(!(weillCornellEduRole=Voluntary Faculty))(!(weillCornellEduRole=Non-Faculty Academic Staff))(weillCornellEduType=Primary))";
		//		 + "(|(&(createTimestamp>="+ sdf.format(cal.getTime()).replace("+0000", "Z") + ")(createTimestamp<=" +strDate.replace("+0000", "Z") + "))(&(modifyTimestamp>="+ sdf.format(cal.getTime()).replace("+0000", "Z")+ ")(modifyTimestamp<=" +strDate.replace("+0000", "Z") + "))))";
			
			List<SearchResultEntry> results = lcf.searchWithBaseDN(filter, "ou=faculty,ou=sors,dc=weill,dc=cornell,dc=edu");
			
			if (results != null) {
	           
				String cwid = "";
				Date startDate = null;
				Date endDate = null;
				boolean firstRow = true;
	            for (SearchResultEntry entry : results) {
	                if(entry.getAttributeValue("weillCornellEduCWID") != null) {
	            	if(!cwid.equals(entry.getAttributeValue("weillCornellEduCWID"))) {
	            		if(!firstRow) {
	            			addWcmcAppointment(startDate, endDate , cwid, "Weill Cornell Medical College");
	            			startDate = null;
	            			endDate = null;
	            		}else {
	            			firstRow = false;
	            		}
	            	}
	            	cwid = entry.getAttributeValue("weillCornellEduCWID");
	            	if(startDate == null) {
	            		startDate = entry.getAttributeValueAsDate("weillCornellEduStartDate");
	            		
	            	}else {
		            	if(entry.getAttributeValueAsDate("weillCornellEduStartDate").compareTo(startDate) < 0) {
		            		startDate = entry.getAttributeValueAsDate("weillCornellEduStartDate");
		            		
		            	}	            		
	            	}
	            	
	            	if(endDate == null) {
	            		endDate = entry.getAttributeValueAsDate("weillCornellEduEndDate");
	            		
	            	}else {
		            	if(entry.getAttributeValueAsDate("weillCornellEduEndDate").compareTo(endDate) > 0) {
		            		endDate = entry.getAttributeValueAsDate("weillCornellEduEndDate");
		            		
		            	}
	            	}
	                        
	            	
	                   
	                    FacultyAppointments fa = new FacultyAppointments();
	                    fa.setUid(entry.getAttributeValue("uid"));
	                    fa.setCwid(entry.getAttributeValue("weillCornellEduCWID"));
	                    fa.setDepartment(entry.getAttributeValue("weillCornellEduDepartment"));
	                    fa.setStartDate(entry.getAttributeValue("weillCornellEduStartDate"));
	                    startYear.add(entry.getAttributeValue("weillCornellEduStartDate"));
	                    fa.setModifyTimeStamp(entry.getAttributeValue("modifyTimestamp"));
	                    fa.setEndDate(entry.getAttributeValue("weillCornellEduEndDate"));
	                    endYear.add(entry.getAttributeValue("weillCornellEduEndDate"));
	                    
	                    
						
	                    
	                    
	                    this.appointments.add(fa);
	                    //fa.toString();
	                    //importDepartments(entry.getAttributeValue("weillCornellEduDepartment"));
	                   
	            	
	                }
	            }
	            
	            //log.info("!!!!!!!!!!!Save: " + sdf.format(startDate).replace("+0000", "Z") + " " + sdf.format(endDate).replace("+0000", "Z") + " " + cwid);
	            addWcmcAppointment(startDate, endDate , cwid, "Weill Cornell Medical College");
	            log.info("Number of results found: " + results.size() +
	                "\n");
	            
	        }
	        else {
	            log.info("No results found");
	        }
			
			
			
				
				
		}
		
		
		private void addWcmcAppointment(Date startDate, Date endDate, String cwid, String program) {
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssZ");
			sdf.setTimeZone(TimeZone.getTimeZone("EDT"));
			FacultyAppointments fa = new FacultyAppointments();
			fa.setUid("WCMC-" + cwid);
			fa.setCwid(cwid);
			fa.setDepartment(program);
			if(startDate!= null)
				fa.setStartDate(sdf.format(startDate).replace("+0000", "Z"));
			else
				fa.setStartDate("NULL");
			if(endDate != null)
				fa.setEndDate(sdf.format(endDate).replace("+0000", "Z"));
			else
				fa.setEndDate("NULL");
			
			this.appointments.add(fa);
		}
		
		private void getStudentsFromED() {
			
			log.info("Students");
			log.info("-------------------");
			 String filter = "(&(objectClass=weillCornellEduSORRoleRecord)(weillCornellEduDegreeCode=PHD))";
			 
			 String studentProgram = null;
			 
			 LDAPConnectionFactory lcf = LDAPConnectionFactory.getInstance(propertyFilePath);
			
			 List<SearchResultEntry> results = lcf.searchWithBaseDN(filter, "ou=students,ou=sors,dc=weill,dc=cornell,dc=edu");
			
			if (results != null) {
				
				String cwid = "";
				Date startDate = null;
				Date endDate = null;
				String programName = null;
				boolean firstRow = true;
	           
	            for (SearchResultEntry entry : results) {       
	            	if(entry.getAttributeValue("weillCornellEduCWID") != null) {
	            		
	            		if(!cwid.equals(entry.getAttributeValue("weillCornellEduCWID"))) {
		            		if(!firstRow) {
		            			if(programName != null){
		            			if(programName.equals("MD-PhD Program")) {
		            				//log.info("!!!!!!!!!!!Save: " + startDate.toString() + " " + endDate.toString() + " " + cwid + " " + "WCMC");	
		            				addWcmcAppointment(startDate, endDate, cwid, "Weill Cornell Medical College");
		            			}else {
		            				//log.info("!!!!!!!!!!!Save: " + startDate.toString() + " " + endDate.toString() + " " + cwid + " " + "School");
		            				addWcmcAppointment(startDate, endDate, cwid, "Weill Cornell Graduate School");
		            			}
		            			}
		            			startDate = null;
		            			endDate = null;
		            		}else {
		            			firstRow = false;
		            		}
		            	}
		            	cwid = entry.getAttributeValue("weillCornellEduCWID");
		            	programName = entry.getAttributeValue("weillCornellEduProgram");
		            	if(startDate == null) {
		            		startDate = entry.getAttributeValueAsDate("weillCornellEduStartDate");
		            		
		            	}else {
			            	if(entry.getAttributeValueAsDate("weillCornellEduStartDate").compareTo(startDate) < 0) {
			            		startDate = entry.getAttributeValueAsDate("weillCornellEduStartDate");
			            		
			            	}	            		
		            	}
		            	
		            	if(endDate == null) {
		            		endDate = entry.getAttributeValueAsDate("weillCornellEduEndDate");
		            		
		            	}else {
			            	if(entry.getAttributeValueAsDate("weillCornellEduEndDate").compareTo(endDate) > 0) {
			            		endDate = entry.getAttributeValueAsDate("weillCornellEduEndDate");
			            		
			            	}
		            	}
	                   
	            	   FacultyAppointments fa = new FacultyAppointments();
	                   
	                   if(!entry.getAttributeValue("weillCornellEduCWID").equals("ado2003") || !entry.getAttributeValue("weillCornellEduCWID").equals("dyd2001")) {
	                	    
	                	    if(entry.getAttributeValue("weillCornellEduStartDate") ==null && entry.getAttributeValue("weillCornellEduEndDate")==null) 
	                	    	log.info("Both the start date and end date for this appointment for cwid: " + entry.getAttributeValue("weillCornellEduCWID") + " is null");
	                	    else {
	                	    	
	                	    	if(entry.getAttributeValue("weillCornellEduProgram")!= null) {
			            			studentProgram = entry.getAttributeValue("weillCornellEduProgram");
			            			
			            			if(studentProgram.contains("Biochemistry, Cell & Molecular Biology"))
			            				studentProgram = "PhD program - Biochemistry, Cell & Molecular Biology";
			            			else if(studentProgram.contains("Biochemistry & Structural Biology"))
			            				studentProgram = "PhD program - Biochemistry & Structural Biology";
			            			else if(studentProgram.contains("Cell & Developmental Biology"))
			            				studentProgram = "PhD program - Cell & Developmental Biology";
			            			else if(studentProgram.contains("Immunology & Microbial Pathogenesis"))
			            				studentProgram = "PhD program - Immunology & Microbial Pathogenesis";
			            			else if (studentProgram.contains("Molecular Biology"))
			            				studentProgram = "PhD program - Molecular Biology";
			            			else if(studentProgram.contains("Neuroscience"))
			            				studentProgram = "PhD program - Neuroscience";
			            			else if(studentProgram.contains("Physiology, Biophysics & Systems Biology"))
			            				studentProgram = "PhD program - Physiology, Biophysics & Systems Biology";
			            			else if(studentProgram.contains("Pharmacology"))
			            				studentProgram = "PhD program - Pharmacology";
			            			//log.info(studentProgram);
			            			fa.setDepartment(studentProgram);
	                	    	}
			            	   	//importDepartments(entry.getAttributeValue("weillCornellEduProgram"));
			        		   	fa.setUid(entry.getAttributeValue("uid"));
			                    fa.setCwid(entry.getAttributeValue("weillCornellEduCWID"));
			                    fa.setStartDate(entry.getAttributeValue("weillCornellEduStartDate"));
			                    fa.setModifyTimeStamp(entry.getAttributeValue("modifyTimestamp"));
			                    fa.setEndDate(entry.getAttributeValue("weillCornellEduEndDate"));
			                    //fa.toString();
			                    this.appointments.add(fa);
	                	    }
	                   }
	            	}
	            }
	            if(programName != null){
		            if(programName.equals("MD-PhD Program")) {
						//log.info("!!!!!!!!!!!Save: " + startDate.toString() + " " + endDate.toString() + " " + cwid + " " + "Weill Cornell Medical College");	
						addWcmcAppointment(startDate, endDate, cwid, "Weill Cornell Medical College");
					}else {
						//log.info("!!!!!!!!!!!Save: " + startDate.toString() + " " + endDate.toString() + " " + cwid + " " + "Weill Cornell Graduate School");
						addWcmcAppointment(startDate, endDate, cwid, "Weill Cornell Graduate School");
					}
	            }
			}
			
			for(FacultyAppointments fa :this.appointments) {
				fa.toString();
			}
			
			lcf.destroyConnectionPool();
				
				
		}
		private void insertAppointmentsToPantheonDB(Set<FacultyAppointments> appointments) {
			ResultSet rs = null;
			Statement st = null;
			PreparedStatement ps = null;
			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssZ");
			sdf.setTimeZone(TimeZone.getTimeZone("EDT"));
			
			this.con = this.mcf.getConnectionfromPool();
			
			String insertQuery = "INSERT INTO violin_appointments(uid, weillCornellEduCWID, weillCornellEduDepartment, weillCornellEduStartDate, weillCornellEduEndDate, modifyTimestamp) VALUES(?, ?, ?, ?, ?, ?)";

				try {
					Iterator<FacultyAppointments> it = appointments.iterator();
					while(it.hasNext()) {
						
						FacultyAppointments fa = new FacultyAppointments();
						fa = it.next();
						
						st = this.con.createStatement();
						rs = st.executeQuery("select count(*) from violin_appointments where uid = '" + fa.getUid() + "'");
						rs.next();
						int count = rs.getInt(1);
						if(count ==1) {
							//Update 
							boolean status = checkForUpdates(fa);
							if(status == true) {
								importDepartments(fa.getDepartment());
								ps = this.con.prepareStatement("UPDATE violin_appointments SET weillCornellEduDepartment = '" + fa.getDepartment() + "' , weillCornellEduStartDate = '" + fa.getStartDate() + "' , weillCornellEduEndDate = '" + fa.getEndDate() + "' where uid = '" + fa.getUid() + "'");
								ps.executeUpdate();
							}
							else {
								log.info("No update necessary for " + fa.getCwid() + " with uid: " + fa.getUid());
							}
							
						}
						else {
							importDepartments(fa.getDepartment());
							ps = this.con.prepareStatement(insertQuery);
							ps.setString(1, fa.getUid());
							ps.setString(2, fa.getCwid());
							if(fa.getDepartment().equals("NULL"))
								ps.setString(3, null);
							else
								ps.setString(3, fa.getDepartment());
							
							if(fa.getStartDate().equals("NULL"))
								ps.setString(4, null);
							else
								ps.setString(4, fa.getStartDate());
							
							if(fa.getEndDate().equals("NULL"))
								ps.setString(5, null);
							else
								ps.setString(5, fa.getEndDate());
							if(fa.getModifyTimeStamp() != null) {
								ps.setString(6, fa.getModifyTimeStamp());
							} else {
								ps.setString(6, sdf.format(new Date()));
							}
							ps.executeUpdate();
							log.info("Inserted appointments for cwid " + fa.getCwid() + " with uid: "+fa.getUid());
							
						}
					}
					
					String deptUpdateQuery = "UPDATE violin_appointments A join violin_org_units B on A.weillCornellEduDepartment = B.dept_name SET A.weillCornellEduDepartment = B.dept_id";
					if(this.con!=null) {
						ps = this.con.prepareStatement(deptUpdateQuery);
						ps.executeUpdate();
					}
					log.info("Departments in violin_appointments have been updated successfully from department look-up table");
					
				}
				catch(SQLException sqle) {
					sqle.printStackTrace();
				}
				finally {
					try {
						if(rs!=null)
							rs.close();
						if(st!= null)
							st.close();
						
						
					} catch(SQLException e) {
						log.error("Exception ", e);
					}
					
				}
				if(this.con!=null) {
					this.mcf.returnConnectionToPool(this.con);
					this.mcf.destroyConnectionPool();
				}
		}
		
		private void importDepartments(String department) {
			ResultSet rs = null;
			Statement st = null;
			PreparedStatement ps = null;
			boolean status = false;
			String selectQuery = "select count(*) from violin_org_units where dept_name like '" + department + "'";
			String insertQuery = "INSERT INTO violin_org_units(dept_name) VALUES(?)";
				try {
					st = this.con.createStatement();
					rs = st.executeQuery(selectQuery);
					rs.next();
					int count = rs.getInt(1);
					if(count == 1) {
						log.info("Department: "+ department + " already exists in database");
					}
					else
					{
						ps = this.con.prepareStatement(insertQuery);
						ps.setString(1, department);
						ps.executeUpdate();
						log.info("Inserted department: "+department +" into departments table");
					}
				}
				catch(SQLException sqle) {
					sqle.printStackTrace();
				}
				finally {
					try {
						if(rs!=null)
							rs.close();
						if(st!= null)
							st.close();
					} catch(SQLException e) {
						// TODO Auto-generated catch block
						log.error("SQL Exception", e);
					}
				}
			
		}
		
		private boolean checkForUpdates(FacultyAppointments fa) {
			ResultSet rs = null;
			Statement st = null;
			boolean status = false;
			String deptName = "NULL";
			String startDate = "NULL";
			String endDate = "NULL";
			String selectQuery = "select B.dept_name, A.weillCornellEduStartDate, A.weillCornellEduEndDate from violin_appointments A, violin_org_units B  where A.weillCornellEduDepartment = B.dept_id and uid = '" + fa.getUid() + "'";
			
				try {
					st = this.con.createStatement();
					rs = st.executeQuery(selectQuery);
					
					rs.next();
					
					if(rs.getString(1)!=null)
						deptName = rs.getString(1);
					if(rs.getString(2)!=null)
						startDate = rs.getString(2);
					if(rs.getString(3)!=null)
						endDate = rs.getString(3);
					
					
					if(!fa.getDepartment().equals("NULL") || !fa.getStartDate().equals("NULL") || !fa.getEndDate().equals("NULL")) {
					if(!fa.getDepartment().equals(deptName) || !fa.getStartDate().equals(startDate) || !fa.getEndDate().equals(endDate)) {
						status = true;
					}
					else
						status = false;
				}
				}
				catch(SQLException sqle) {
					sqle.printStackTrace();
				}
				finally {
					try {
						if(rs!=null)
							rs.close();
						if(st!= null)
							st.close();
						
					} catch(SQLException e) {
						log.error("SQLException", e);
					}
				}
			
			return status;
		}
		
		
		private void callJSONAPI() {
			StringBuffer respBuf = new StringBuffer();
			
			try{
				URL url = new URL("https://ed-test.weill.cornell.edu/api/v1/search");
				HttpURLConnection con = (HttpURLConnection)url.openConnection();
				con.setRequestMethod("GET");
				con.setRequestProperty("X-DS-Username", "cn=vivo,ou=binds,dc=weill,dc=cornell,dc=edu");
				con.setRequestProperty("X-DS-Password", "AePe3eJi-Ohs7^aec%");
				con.setRequestProperty("X-DS-Base-Dn", "ou=faculty,dc=weill,dc=cornell,dc=edu");
				con.setRequestProperty("X-DS-Filter", "(&(weillCornellEduPersonTypeCode=academic)(sn=albert))");
				con.setRequestProperty("X-DS-Attrs", "weillCornellEduCWID");
				con.setRequestProperty("accept", "application/json; charset=UTF-8");
				con.setDoOutput(true);
				String inputLine;
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				while ((inputLine = in.readLine()) != null) {
					respBuf.append(inputLine);
				}
				in.close();
				System.out.println(respBuf.toString());
				
				
			}
			catch(MalformedURLException mfe) {
				log.error("urlConnect MalformedURLException: ", mfe);
			}
			catch(IOException ioe) {
				log.error("IOException: ", ioe);
			}
			
		}
		
		private String getSplunkSid() {
			StringBuffer respBuf = new StringBuffer();
			String username = "svc_splunk_vivodashboard";
			String password = "v1vod3shboard17329";
			String authString = username + ":" + password;
			
			String authStringEnc = Base64.getEncoder().encodeToString(authString.getBytes());
			try{
				TrustManager[] trustAllCerts = new TrustManager[] { 
					    new X509TrustManager() {     
					        public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
					            return new X509Certificate[0];
					        } 
					        public void checkClientTrusted( 
					            java.security.cert.X509Certificate[] certs, String authType) {
					            } 
					        public void checkServerTrusted( 
					            java.security.cert.X509Certificate[] certs, String authType) {
					        }
					    } 
					}; 
				URL url = new URL("https://splunk-sec.med.cornell.edu:8089/services/search/jobs?output_mode=json");
				HttpsURLConnection con = (HttpsURLConnection)url.openConnection();
				
				SSLContext sc = SSLContext.getInstance("TLSv1.2");
				sc.init(null, trustAllCerts, new java.security.SecureRandom());
				
				con.setSSLSocketFactory(sc.getSocketFactory());
				// Also force it to trust all hosts
		        HostnameVerifier allHostsValid = new HostnameVerifier() {
		            public boolean verify(String hostname, SSLSession session) {
		                return true;
		            }
		        };
		        con.setHostnameVerifier(allHostsValid);
				con.setRequestMethod("POST");
				con.setRequestProperty("Authorization", "Basic " + authStringEnc);
				con.setDoOutput(true);
				OutputStream os = con.getOutputStream();
				OutputStreamWriter osw = new OutputStreamWriter(os, "UTF-8");
				osw.write("search=| inputlookup vivoDashboard-extraRoles.csv");
				osw.flush();
				osw.close();
				os.close();
				con.connect();
				
				String inputLine;
				BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
				while ((inputLine = in.readLine()) != null) {
					respBuf.append(inputLine);
				}
				in.close();
				ObjectNode object = new ObjectMapper().readValue(respBuf.toString(), ObjectNode.class);
				JsonNode node = object.get("sid");
			    return (node == null ? null : node.textValue());
			}
			catch(MalformedURLException mfe) {
				log.error("urlConnect MalformedURLException: ", mfe);
			}
			catch(IOException ioe) {
				log.error("IOException: ", ioe);
			} catch (NoSuchAlgorithmException e) {
				log.error("NoSuchAlgorithmException" , e);
			} catch (KeyManagementException e) {
				log.error("KeyManagementException" , e);
			}
			
			return null;
			
		}
		
		private void callSplunkAPI(String sid) {
			
			StringBuffer respBuf = new StringBuffer();
			String username = "svc_splunk_vivodashboard";
			String password = "v1vod3shboard17329";
			String authString = username + ":" + password;
			String authStringEnc = Base64.getEncoder().encodeToString(authString.getBytes());
			List<FacultyAppointments> appointments = new ArrayList<FacultyAppointments>();
			try{
				TrustManager[] trustAllCerts = new TrustManager[] { 
					    new X509TrustManager() {     
					        public java.security.cert.X509Certificate[] getAcceptedIssuers() { 
					            return new X509Certificate[0];
					        } 
					        public void checkClientTrusted( 
					            java.security.cert.X509Certificate[] certs, String authType) {
					            } 
					        public void checkServerTrusted( 
					            java.security.cert.X509Certificate[] certs, String authType) {
					        }
					    } 
					}; 
				URL url = new URL("https://splunk-sec.med.cornell.edu:8089/services/search/jobs/" + sid + "/results?output_mode=json&count=0");
				HttpsURLConnection con = (HttpsURLConnection)url.openConnection();
				
				SSLContext sc = SSLContext.getInstance("TLSv1.2");
				sc.init(null, trustAllCerts, new java.security.SecureRandom());
				
				con.setSSLSocketFactory(sc.getSocketFactory());
				// Also force it to trust all hosts
		        HostnameVerifier allHostsValid = new HostnameVerifier() {
		            public boolean verify(String hostname, SSLSession session) {
		                return true;
		            }
		        };
		        con.setHostnameVerifier(allHostsValid);
				con.setRequestMethod("GET");
				con.setRequestProperty("Authorization", "Basic " + authStringEnc);
				con.setDoOutput(true);
			/*String inputLine;
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			while ((inputLine = in.readLine()) != null) {
				respBuf.append(inputLine);
			}
			in.close();
			
			System.out.println(respBuf.toString());*/
				
				ObjectMapper objectMapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
				JsonNode json = objectMapper.readTree(con.getInputStream()).get("results");
				if(json != null) {
					appointments = Arrays.asList(objectMapper.treeToValue(json, FacultyAppointments[].class));
				}
					
				
			if(!appointments.isEmpty()) {
				this.appointments.addAll(appointments);
			}
				
			}
			catch(MalformedURLException mfe) {
				log.error("urlConnect MalformedURLException: ", mfe);
			}
			catch(IOException ioe) {
				log.error("IOException: ", ioe);
			} catch (NoSuchAlgorithmException e) {
				log.error("NoSuchAlgorithmException" , e);
			} catch (KeyManagementException e) {
				log.error("KeyManagementException" , e);
			}
		}
		
		
}

