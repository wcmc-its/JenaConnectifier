package org.vivoweb.harvester.ingest;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;

import java.util.List;
import java.util.Properties;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vivoweb.harvester.beans.PeopleBean;
import org.vivoweb.harvester.connectionfactory.JenaConnectionFactory;
import org.vivoweb.harvester.connectionfactory.LDAPConnectionFactory;
import org.vivoweb.harvester.util.repo.SDBJenaConnect;
import com.hp.hpl.jena.query.QuerySolution;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.SearchResultEntry;

/**
 * @author szd2013
 * This class fetches active faculty from Enterprise Directory and inserts them in VIVO along with details about their phone number, email, Primary Title etc.
 * Note - The release code in Enterprise Directory has to be public for faculty in order for the profile to be created in VIVO.
 * Future Scope - To include graduate students profile in VIVO.
 */
public class AcademicFetchFromED {
	
	/**
	 * List of people in VIVO
	 */
	private ArrayList<PeopleBean> people = new ArrayList<PeopleBean>();
	
	private int updateCount = 0;
	
	public static String propertyFilePath;
	
	/**
	 * Jena connection factory object for all the apache jena sdb related connections
	 */
	JenaConnectionFactory jcf = JenaConnectionFactory.getInstance(propertyFilePath);
	
	/**
	 * The default namespace for VIVO
	 */
	private String vivoNamespace = JenaConnectionFactory.nameSpace;
	

	private static Logger log = LoggerFactory.getLogger(AcademicFetchFromED.class);
	
	/**
	 * The LDAP connection gets saved in this variable
	 */
	private LDAPConnection conn = null;
	
	
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
			
			new AcademicFetchFromED().init(args[0]);
		}
	}
		
		
		/**
		 * Initializes all the required variables 
		 * @param propertiesFile The location of property file
		 */
		private void init(String propertiesFile) {
			
			if(this.vivoNamespace == null) {
				log.info("Please provide a namespace in property file");
			}
			else {
				try {
					execute();
				} catch(IOException e) {
					log.error("Error in Executing people import" , e);
				}
			}

		}
		
		/**
		 * This is the main execution method of the class
		 */
		private void execute() throws IOException {
			
			
			getActivePeopleFromED();
			
			int count = 0;
			
			
			Iterator<PeopleBean>  it = this.people.iterator();
			while(it.hasNext()) {
				PeopleBean pb = it.next();
				log.info("################################ " + pb.getCwid() + " - " + pb.getDisplayName() + " - Insert/Update Operation #####################");
				if(!checkPeopleInVivo(pb)) {
					log.info("Person: "+pb.getCwid() + " does not exist in VIVO");
					insertPeopleInVivo(pb);
					count = count + 1;
				}
				else {
					log.info("Person: "+pb.getCwid() + " already exist in VIVO");
					checkForUpdates(pb);
					syncPersonTypes(pb);
				}
				log.info("################################ End of " + pb.getCwid() + " - " + pb.getDisplayName() + " -  Insert/Update Operation ###################");
			}
			
			log.info("Number of new people inserted in VIVO: " + count);
			
			log.info("Number of people updated in VIVO: " + this.updateCount);
			
			
			log.info("People fetch completed successfully...");
			
			//Destroy jena connection pool
			if(this.jcf != null)
				this.jcf.destroyConnectionPool();
		}
		
		
		/**
		 * This function gets active people from Enterprise Directory having personTypeCode as academic
		 */
		private void getActivePeopleFromED() {
			
			LDAPConnectionFactory lcf = LDAPConnectionFactory.getInstance(propertyFilePath);
			int noCwidCount = 0;
			String filter = "(&(objectClass=eduPerson)(weillCornellEduPersonTypeCode=academic))";
			
			List<SearchResultEntry> results = lcf.searchWithBaseDN(filter,"ou=people,dc=weill,dc=cornell,dc=edu");
			
			
			
			
			if (results != null) {
	           
	            for (SearchResultEntry entry : results) {
	            	if(entry.getAttributeValue("weillCornellEduCWID") == null) {
	            		noCwidCount = noCwidCount + 1;
	            		//log.info(entry.getAttributeValue("uid"));
	            		
	            	}
	            	if(entry.getAttributeValue("weillCornellEduCWID") != null) {
	                   PeopleBean pb = new PeopleBean();
	                   pb.setCwid(entry.getAttributeValue("weillCornellEduCWID"));

	                   pb.setDisplayName(StringEscapeUtils.escapeJava(entry.getAttributeValue("displayName")));
	                   
	                   pb.setGivenName(StringEscapeUtils.escapeJava(entry.getAttributeValue("givenName")));
	                   
	                   if(entry.getAttributeValue("mail")!=null)
	                	   pb.setMail(entry.getAttributeValue("mail"));
	                   else
	                	   pb.setMail("");
	                   
	                   if(entry.getAttributeValue("weillCornellEduWorkingTitle") != null)
	                	   pb.setPrimaryTitle(entry.getAttributeValue("weillCornellEduWorkingTitle"));
	                   else if(entry.getAttributeValue("weillCornellEduPrimaryTitle")!=null)
	                	   pb.setPrimaryTitle(entry.getAttributeValue("weillCornellEduPrimaryTitle"));
	                   else
	                	   pb.setPrimaryTitle("");
	                   
	                   if(entry.getAttributeValue("weillCornellEduStatus")!=null)
	                	   pb.setStatus(entry.getAttributeValue("weillCornellEduStatus"));
	                   else
	                	   pb.setStatus("");
	                   
	                   if(entry.getAttributeValue("telephoneNumber")!= null)
	                	   pb.setTelephoneNumber(entry.getAttributeValue("telephoneNumber"));
	                   else
	                	   pb.setTelephoneNumber("");
	                   
	                   if(entry.getAttributeValue("weillCornellEduMiddleName")!=null)
	                	   pb.setMiddleName(entry.getAttributeValue("weillCornellEduMiddleName"));
	                   else
	                	   pb.setMiddleName("");
	                   
	                   if(entry.getAttributeValue("sn")!=null)
	                	   pb.setSn(entry.getAttributeValue("sn"));
	                   else
	                	   pb.setSn("");
	                   
	                   if(entry.getAttributeValue("labeledURI;pops") != null)
	                	   pb.setPopsProfile(entry.getAttributeValue("labeledURI;pops"));
	                   else
	                	   pb.setPopsProfile("");
	                   
	                   String personType[] = new String[entry.getAttributeValues("weillCornellEduPersonTypeCode").length];
	                   personType = entry.getAttributeValues("weillCornellEduPersonTypeCode");
	                   String ptype = assignVivoPersonType(personType);
	                   pb.setPersonCode(ptype);
	                  
	                   List<String> ptypes = Arrays.asList(personType);
	                   
	                   ArrayList<String> nsTypes = determineNsType(ptypes);
	                   
	                   pb.setNsTypes(nsTypes);
	                   log.info(pb.toString());
	                   for(String s: nsTypes) {
	                	   log.info(s);
	                   }
	                   log.info("------------------------------------------------------------------------------------------------------------");
	                   this.people.add(pb);
	                   
	                  
	            		
	            		
	                   
	            	}

	            }
			
	            log.info("Number of people found: " + this.people.size());
	            log.info("No of Records with no CWID: " + noCwidCount);
	        }
	        else {
	            log.info("No results found");
	        }
			
			lcf.destroyConnectionPool();
				
				
		}
		
		/**
		 * @param pb the people bean with all the data that has to be inserted from ED
		 * The function inserts all the data from ED and converts it into triples and insert into wcmcPeople graph in VIVO
		 */
		private void insertPeopleInVivo(PeopleBean pb) {
			
			String middleName = null;
			String mail = null;
			String phone = null;
			
			
			middleName = pb.getMiddleName();
			mail = pb.getMail();
			phone = pb.getTelephoneNumber();
			
			
			if(pb.getMiddleName() ==null) {
				middleName = "";
			}
			
			if(pb.getMail() ==null) {
				mail = "";
			}
			
			if(pb.getTelephoneNumber() == null) {
				phone = "";
			}
			
			String lastMiddleFirst =  pb.getSn().trim() + ", " + pb.getGivenName().trim()  + " " + middleName;
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = new Date();
			String currentDate = sdf.format(date);
			StringBuffer sb = new StringBuffer();
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n");
			sb.append( "PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> { \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type foaf:Person . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000001> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000002> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000004> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
			for(String nsType: pb.getNsTypes()) {
				sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type " + nsType + " . \n");
			}
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel \"" + pb.getDisplayName().trim() + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:cwid \"" + pb.getCwid().trim() + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdfs:label \"" + lastMiddleFirst + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vivo:DateTimeValue \"" + currentDate + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> obo:ARG_2000028 <" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> . \n");
			if(!phone.equals(""))
				sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + phone + "\" . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" +pb.getPersonCode().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> obo:ARG_2000029 <" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> rdf:type vcard:Individual . \n");
			if(!mail.equals(""))
				sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasEmail <" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasName <" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasTitle <" + this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			if(!mail.equals("")) {
				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> rdf:type vcard:Work . \n");
				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> rdf:type vcard:Email . \n");
				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> vcard:email \"" + mail + "\" . \n");
				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			}
			sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> rdf:type vcard:Name . \n");
			sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> vcard:givenName \"" + pb.getGivenName().trim() + "\" . \n");
			if(!middleName.equals(""))
				sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> core:middleName \"" + middleName + "\" . \n");
			sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> vcard:familyName \"" + pb.getSn().trim() + "\" . \n");
			sb.append("<" + this.vivoNamespace + "hasName-"  + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			sb.append("<" + this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Title . \n");
			sb.append("<" + this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> vcard:title \"" + pb.getPrimaryTitle().trim() + "\" . \n");
			sb.append("<" + this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
			if(!pb.getPopsProfile().equals("")) {
				sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasURL <" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> . \n");
				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> rdf:type vcard:URL . \n");
				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> core:rank \"99\"^^<http://www.w3.org/2001/XMLSchema#int> . \n");
				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> rdfs:label \"Clinical Profile \" . \n");
				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> vcard:url \"" + pb.getPopsProfile().trim() + "\"^^<http://www.w3.org/2001/XMLSchema#anyURI> . \n");
				
			}
			sb.append("}}");
			
			log.info(sb.toString());
			
			try {
				SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("wcmcPeople");
				runSparqlUpdateTemplate(sb.toString(), vivoJena);
				
				if(vivoJena!= null)
					this.jcf.returnConnectionToPool(vivoJena, "wcmcPeople");
			} catch(IOException e) {
				// TODO Auto-generated catch block
				log.error("Error connecting to Jena database", e);
			}
			
			insertInferenceTriples(pb);
				  
				  
				  
				  
		}
		
		/**
		 * @param pb the people bean with all the data that has to be inserted from ED
		 * This function inserts inference triples for the people data
		 */
		private void insertInferenceTriples(PeopleBean pb) {
			StringBuilder sb = new StringBuilder();
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n");
			sb.append( "PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> { \n");
			//Title Inference Triples
			/*sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Geo . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Explanatory . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Geographical . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Addressing . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Thing . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Communication . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Calendar . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Identification . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Organizational . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:TimeZone . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> rdf:type vcard:Security . \n");
			sb.append(this.vivoNamespace + "hasTitle-"  + pb.getCwid().trim() + "> vitro:mostSpecificType vcard:Title . \n");*/
			//For email and primary title
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:BFO_0000002 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:BFO_0000031 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:ARG_2000379 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type vcard:Kind . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:BFO_0000001 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type obo:IAO_0000030 . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
			sb.append("<" + this.vivoNamespace + "arg2000028-"  + pb.getCwid().trim() + "> vitro:mostSpecificType vcard:Individual . \n");
			sb.append("}}");
			
			
			SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("vitro-kb-inf");

			try{
				vivoJena.executeUpdateQuery(sb.toString(),true);
			}
			catch(IOException e) {
				log.error("Error connecting to Jena Database", e);
			}
			this.jcf.returnConnectionToPool(vivoJena, "vitro-kb-inf");
		}
		
		/**
		 * This function determine the vivo rdf:types for all the person types coming from ED
		 * @param type the list of person types from ED
		 * @return The list of vivo equivalent rdf:types 
		 */
		private ArrayList<String> determineNsType(List<String> type) {
			ArrayList<String> ptype = new ArrayList<String>();
			
			
			if(type != null && type.contains("academic-faculty-weillfulltime")) {
				ptype.add("wcmc:FullTimeWCMCFaculty");
				
	       }
	        if(type != null && type.contains("academic-faculty-weillparttime")) {
				ptype.add("wcmc:PartTimeWCMCFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-voluntary")) {
				ptype.add("wcmc:VoluntaryFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-adjunct")) {
				ptype.add("wcmc:AdjunctFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-courtesy")) {
				ptype.add("wcmc:CourtesyFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-emeritus")) {
				ptype.add("core:EmeritusFaculty");
				
			}
	        if(type != null && type.contains("academic-faculty-instructor")) {
				ptype.add("wcmc:Instructor");
				
			}
	        if(type != null && type.contains("academic-faculty-lecturer")) {
				ptype.add("wcmc:Lecturer");
				
			}
	        if(type != null && type.contains("academic-nonfaculty-fellow")) {
				ptype.add("wcmc:Fellow");
				
			}
	        if(type != null && type.contains("academic-nonfaculty-postdoc")) {
				ptype.add("core:Postdoc");
				
			}
	        if(type != null && type.contains("academic-faculty")) {
				ptype.add("vivo:FacultyMember");
				
			}
			 if(type != null && type.contains("academic-nonfaculty")) {
				ptype.add("core:NonAcademic");
				
			}
			
			//ptype.remove(mostSpecificType);
			
			return ptype;
		}
		
		private String assignVivoPersonType(String[] personType) {
			String ptype = null;
			String type = null;
			
	                for(String s: personType) {
	                	type = type + " " + s;
	                }
	                type = type.replace("null", "");
	       
	       if(type != null && type.contains("academic-faculty-weillfulltime")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#FullTimeWCMCFaculty";
				return ptype;
	       }
	       else if(type != null && type.contains("academic-faculty-weillparttime")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#PartTimeWCMCFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-voluntary")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#VoluntaryFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-adjunct")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#AdjunctFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-courtesy")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#CourtesyFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-emeritus")) {
				ptype = "http://vivoweb.org/ontology/core#EmeritusFaculty";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-instructor")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#Instructor";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty-lecturer")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#Lecturer";
				return ptype;
			}
	       else if(type != null && type.contains("academic-nonfaculty-fellow")) {
				ptype = "http://weill.cornell.edu/vivo/ontology/wcmc#Fellow";
				return ptype;
			}
	       else if(type != null && type.contains("academic-nonfaculty-postdoc")) {
				ptype = "http://vivoweb.org/ontology/core#Postdoc";
				return ptype;
			}
	       else if(type != null && type.contains("academic-faculty")) {
				ptype = "http://vivoweb.org/ontology/core#FacultyMember";
				return ptype;
			}
			else if(type != null && type.contains("academic-nonfaculty")) {
				ptype = "http://vivoweb.org/ontology/core#NonAcademic";
				return ptype;
			}
			
			
			return ptype;

		}
		
		/**
		 * @param pb  the people bean with all the data that has to be inserted from ED
		 * @return true or false based on whether the people exist in VIVO
		 * @throws IOException exception likely to be thrown by SDBJenaConnect
		 */
		private boolean checkPeopleInVivo(PeopleBean pb) throws IOException {
			int count = 0;
			String sparqlQuery = "PREFIX rdf:      <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
								 "PREFIX foaf:     <http://xmlns.com/foaf/0.1/> \n" +
								 "SELECT  (count(rdf:type) as ?c) \n" +
								 "FROM <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n" +
								 "WHERE \n" +
								 "{ \n" +
								 "<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type foaf:Person . \n" +
								 "}";
			
			SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("wcmcPeople");
			
			com.hp.hpl.jena.query.ResultSet rs = runSparqlTemplate(sparqlQuery, vivoJena);
			
			QuerySolution qs = rs.nextSolution();
			count = Integer.parseInt(qs.get("c").toString().replace("^^http://www.w3.org/2001/XMLSchema#integer", ""));
			
			//Close the connection
			if(vivoJena!= null)
				this.jcf.returnConnectionToPool(vivoJena, "wcmcPeople");
			
			if(count > 0)
				return true;
			else
				return false;
			
			
			
			
		}
		
		/**
		 * @param pb the people bean with all the data that has to be inserted from ED
		 * This function check for updates from ED and then apply them in VIVO. For email , displayName, lastName, Phone numbers, Primary Title etc.
		 */
		private void checkForUpdates(PeopleBean pb) {
			ArrayList<String> updateList = new ArrayList<String>();
			ArrayList<String> insertList = new ArrayList<String>();
			String phone = "";
			String middleName = "";
			String sparqlQuery = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n" +
				 "PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n" +
				 "PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n" +
				 "PREFIX core: <http://vivoweb.org/ontology/core#> \n" +
				 "PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n" +
				 "PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n" +
				 "SELECT ?label ?type ?phone ?title ?email ?firstName ?lastName ?middleName ?popsUrl\n" +
				 "FROM <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n" +
				 "WHERE \n" +
				 "{ \n" +
				 "<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel ?label .\n" +
				 "<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType ?type .\n" +
				 "<" + this.vivoNamespace + "hasTitle-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#title> ?title . \n" +
				 "<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#givenName> ?firstName . \n" +
				 "<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#familyName> ?lastName . \n" +
				 "OPTIONAL { <" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone ?phone . }\n" +
				 "OPTIONAL { <" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> ?email . }\n" +
				 "OPTIONAL { <" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName ?middleName . }\n" +
				 "OPTIONAL { <" + this.vivoNamespace + "popsUrl-" + pb.getCwid().trim() + "> vcard:url ?popsUrl } \n" +
				 "}";
			
			log.info(sparqlQuery);
			SDBJenaConnect vivoJena;
			try {
				vivoJena = this.jcf.getConnectionfromPool("wcmcPeople");
				com.hp.hpl.jena.query.ResultSet rs = runSparqlTemplate(sparqlQuery, vivoJena);
				QuerySolution qs = null;
					if(rs.hasNext()) {
						qs = rs.nextSolution();
						
						
						if(!qs.get("label").toString().equals(pb.getDisplayName().trim())) {
							updateList.add("DisplayName");
							log.info("Person Label was updated for cwid: " + pb.getCwid().trim());
						}
						if(!qs.get("type").toString().equals(pb.getPersonCode().trim())) {
							updateList.add("MostSpecificType");
							log.info("MostSpecificType was updated for cwid: " + pb.getCwid().trim());
						}
						if(qs.get("title") != null && !qs.get("title").toString().equals(pb.getPrimaryTitle().trim())) {
							updateList.add("PrimaryTitle");
							log.info("Title was updated for cwid: " + pb.getCwid().trim());
						}
						if(qs.get("email") != null && !qs.get("email").toString().equals(pb.getMail().trim())) {
							updateList.add("Mail");
							log.info("Email was updated for cwid: " + pb.getCwid().trim());
						}
						
						if(qs.get("email") == null && pb.getMail() != null && !pb.getMail().equals("")) {
							insertList.add("Mail");
							log.info("Email was inserted for cwid: " + pb.getCwid().trim());
						}
						
						if(qs.get("phone") == null && pb.getTelephoneNumber() != null && !pb.getTelephoneNumber().equals("")) {
							insertList.add("TelephoneNumber");
							log.info("Phone was inserted for cwid: " + pb.getCwid().trim());
						}
						
						if(qs.get("middleName") == null && pb.getMiddleName() != null && !pb.getMiddleName().equals("")) {
							insertList.add("MiddleName");
							log.info("Middle Name was inserted for cwid: " + pb.getCwid().trim());
						}
						
						if(qs.get("phone") != null && !qs.get("phone").toString().equals(pb.getTelephoneNumber().trim())) {
							updateList.add("TelephoneNumber");
							log.info("Phone was updated for cwid: " + pb.getCwid().trim());
						}
						if(!qs.get("firstName").toString().equals(pb.getGivenName().trim())) {
							updateList.add("FirstName");
							log.info("First was updated for cwid: " + pb.getCwid().trim());
						}
						if(!qs.get("lastName").toString().equals(pb.getSn().trim())) {
							updateList.add("LastName");
							log.info("Last Name was updated for cwid: " + pb.getCwid().trim());
						}
						if(qs.get("middleName") != null && !qs.get("middleName").toString().equals(pb.getMiddleName().trim())) {
							updateList.add("MiddleName");
							log.info("Middle Name was updated for cwid: " + pb.getCwid().trim());
						}
						if(qs.get("popsUrl") == null && pb.getPopsProfile() != null && !pb.getPopsProfile().equals("")) {
							insertList.add("PopsUrl");
							log.info("Pops Url was inserted for cwid: " + pb.getCwid().trim());
						}
					}
					
					if(updateList.isEmpty()) {
						log.info("No Updates are necessary for cwid : " + pb.getCwid().trim());
					}
					else {
						StringBuffer sb = new StringBuffer();
						
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
						sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n");
		                sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
		                sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
		                sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		                sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n");
		                sb.append("DELETE { \n");
		                if(updateList.contains("DisplayName")) {
		                    sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel \"" + qs.get("label").toString() + "\" .\n");
		                }
		                if(updateList.contains("PrimaryTitle")) {
		                	sb.append("<" + this.vivoNamespace + "hasTitle-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#title> \"" + qs.get("title").toString() + "\" .\n");
		                }
		                if(updateList.contains("Mail")) {
		                	sb.append("<" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> \"" + qs.get("email").toString() + "\" .\n");
		                }
		                if(updateList.contains("TelephoneNumber")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + qs.get("phone").toString() + "\" .\n");
		                }
		                if(updateList.contains("FirstName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#givenName> \"" + qs.get("firstName").toString() + "\" .\n");
		                }
		                if(updateList.contains("LastName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#familyName> \"" + qs.get("lastName").toString() + "\" .\n");
		                }
		                if(updateList.contains("MiddleName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName \"" + qs.get("middleName").toString() + "\" .\n");
		                }
		                if(updateList.contains("MostSpecificType")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" + qs.get("type").toString() + "> .\n");
		                }
		                sb.append("} \n");
		                sb.append("INSERT { \n");
		                if(updateList.contains("DisplayName")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel \"" + pb.getDisplayName().trim() + "\" .\n");
		                }
		                if(updateList.contains("PrimaryTitle")) {
		                	sb.append("<" + this.vivoNamespace + "hasTitle-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#title> \"" + pb.getPrimaryTitle().trim() + "\" .\n");
		                }
		                if(updateList.contains("Mail")) {
		                	sb.append("<" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> \"" + pb.getMail().trim() + "\" .\n");
		                }
		                if(updateList.contains("TelephoneNumber")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + pb.getTelephoneNumber().trim() + "\" .\n");
		                }
		                if(updateList.contains("FirstName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#givenName> \"" + pb.getGivenName().trim() + "\" .\n");
		                }
		                if(updateList.contains("LastName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#familyName> \"" + pb.getSn().trim() + "\" .\n");
		                }
		                if(updateList.contains("MiddleName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName \"" + pb.getMiddleName().trim() + "\" .\n");
		                }
		                
		                if(updateList.contains("MostSpecificType")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" + pb.getPersonCode().trim() + "> .\n");
		                }
		                
		                sb.append("} \n");
		                sb.append("WHERE { \n");
		                if(updateList.contains("DisplayName")) {
		                    sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:personLabel \"" + qs.get("label").toString() + "\" .\n");
		                }
		                if(updateList.contains("PrimaryTitle")) {
		                	sb.append("<" + this.vivoNamespace + "hasTitle-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#title> \"" + qs.get("title").toString() + "\" .\n");
		                }
		                if(updateList.contains("Mail")) {
		                	sb.append("<" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> \"" + qs.get("email").toString() + "\" .\n");
		                }
		                if(updateList.contains("TelephoneNumber")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + qs.get("phone").toString() + "\" .\n");
		                }
		                if(updateList.contains("FirstName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#givenName> \"" + qs.get("firstName").toString() + "\" .\n");
		                }
		                if(updateList.contains("LastName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#familyName> \"" + qs.get("lastName").toString() + "\" .\n");
		                }
		                if(updateList.contains("MiddleName")) {
		                	sb.append("OPTIONAL { <" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName \"" + qs.get("middleName").toString() + "\" . }\n");
		                }
		                if(updateList.contains("MostSpecificType")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" + qs.get("type").toString() + "> .\n");
		                }
		                sb.append("} \n");
		                
		                log.info("Update Query: " + sb.toString());
		                
		                
		                
		                runSparqlUpdateTemplate(sb.toString(), vivoJena);
		                
		                this.updateCount = this.updateCount + 1;
						
						
						if(!updateList.isEmpty() && updateList.contains("MostSpecificType")) {
							log.info("Updating inference triple for mostSpecificType update");
							SDBJenaConnect vivoJenaInf = this.jcf.getConnectionfromPool("vitro-kb-inf");
							sb.setLength(0);
							
							sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
							sb.append("WITH <http://vitro.mannlib.cornell.edu/default/vitro-kb-inf> \n");
			                sb.append("DELETE { \n");
			                sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType ?o .\n");
			                sb.append("} \n");
			                sb.append("INSERT { \n");
			                sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType <" + pb.getPersonCode().trim() + "> .\n");
			                sb.append("} \n");
			                sb.append("WHERE { \n");
			                sb.append("OPTIONAL {<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> vitro:mostSpecificType ?o . }\n");
			                sb.append("}");
			                
			                log.info("Update Query for person type: " + sb.toString());
			                runSparqlUpdateTemplate(sb.toString(), vivoJenaInf);

							this.jcf.returnConnectionToPool(vivoJenaInf, "vitro-kb-inf");
						}
					}
					
					if(!insertList.isEmpty()) {
	                	StringBuilder sb = new StringBuilder();
						
						sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
						sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
						sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/> \n");
		                sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
		                sb.append("PREFIX core: <http://vivoweb.org/ontology/core#> \n");
		                sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
		                sb.append("PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n");
		                sb.append("INSERT DATA { GRAPH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> { \n");
		                if(insertList.contains("TelephoneNumber")) {
		                	sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> wcmc:officePhone \"" + pb.getTelephoneNumber().trim() + "\" .\n");			                	
		                }
		                if(insertList.contains("MiddleName")) {
		                	sb.append("<" + this.vivoNamespace + "hasName-" + pb.getCwid().trim() + "> core:middleName \"" + pb.getMiddleName().trim() + "\" .\n");
		                }
		                if(insertList.contains("Mail")) {
		                	sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasEmail <" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> . \n");
		                	sb.append("<" + this.vivoNamespace + "hasEmail-" + pb.getCwid().trim() + "> <http://www.w3.org/2006/vcard/ns#email> \"" + pb.getMail().trim() + "\" .\n");
		                	sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> rdf:type vcard:Work . \n");
		    				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> rdf:type vcard:Email . \n");
		    				sb.append("<" + this.vivoNamespace + "hasEmail-"  + pb.getCwid().trim() + "> <http://vivo.ufl.edu/ontology/vivo-ufl/harvestedBy> \"wcmc-harvester\" . \n");
		                }
		                if(insertList.contains("PopsUrl")) {
		                	if(!pb.getPopsProfile().equals("")) {
		        				sb.append("<" + this.vivoNamespace + "arg2000028-" + pb.getCwid().trim() + "> vcard:hasURL <" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> . \n");
		        				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> rdf:type vcard:URL . \n");
		        				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> core:rank \"99\"^^<http://www.w3.org/2001/XMLSchema#int> . \n");
		        				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> rdfs:label \"Clinical Profile \" . \n");
		        				sb.append("<" + this.vivoNamespace + "popsUrl-"  + pb.getCwid().trim() + "> vcard:url \"" + pb.getPopsProfile().trim() + "\"^^<http://www.w3.org/2001/XMLSchema#anyURI> . \n");
		        				
		        			}
		                }
		                sb.append("}}");
		                
		                log.info("Insert Query: " + sb.toString());
		                runSparqlUpdateTemplate(sb.toString(), vivoJena);
	                }
					
					if(vivoJena!= null)
						this.jcf.returnConnectionToPool(vivoJena, "wcmcPeople");
					
					
					
					
			} catch(IOException e) {
				// TODO Auto-generated catch block
				log.error("IOException: ",e);
			}
			
			//Run inferencing on the updated triples
			
			insertInferenceTriples(pb);
			
			
		}
		
		/**
		 * This function will sync all the personTypeCodes from ED with VIVO rdf types
		 * @param pb The PeopleBean conatining all people information from ED
		 */
		private void syncPersonTypes(PeopleBean pb) {
			
			log.info("Syncing personTypeCodes from ED with VIVO for " + pb.getCwid());
			StringBuilder sb = new StringBuilder();
			
			sb.append("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n");
			sb.append("PREFIX wcmc: <http://weill.cornell.edu/vivo/ontology/wcmc#> \n");
			sb.append("PREFIX foaf: <http://xmlns.com/foaf/0.1/>\n");
			sb.append("PREFIX vivo: <http://vivoweb.org/ontology/core#> \n");
			sb.append("PREFIX vitro: <http://vitro.mannlib.cornell.edu/ns/vitro/0.7#> \n");
			sb.append("PREFIX obo: <http://purl.obolibrary.org/obo/> \n");
			sb.append("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> \n");
			sb.append("PREFIX vcard: <http://www.w3.org/2006/vcard/ns#> \n");
			sb.append( "PREFIX core: <http://vivoweb.org/ontology/core#> \n");
			sb.append("WITH <http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople> \n");
            sb.append("DELETE { \n");
            sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type ?o .\n");
            sb.append("} \n");
            sb.append("INSERT { \n");
            sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type foaf:Person . \n");
            sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://xmlns.com/foaf/0.1/Agent> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000001> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000002> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://purl.obolibrary.org/obo/BFO_0000004> . \n");
			sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() +"> rdf:type <http://www.w3.org/2002/07/owl#Thing> . \n");
			for(String nsType: pb.getNsTypes()) {
				sb.append("<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type " + nsType + " . \n");
			}
            sb.append("} \n");
            sb.append("WHERE { \n");
            sb.append("OPTIONAL {<" + this.vivoNamespace + "cwid-" + pb.getCwid().trim() + "> rdf:type ?o . }\n");
            sb.append("}");
			
            log.info(sb.toString());
			
			SDBJenaConnect vivoJena = this.jcf.getConnectionfromPool("wcmcPeople");
			try {
				runSparqlUpdateTemplate(sb.toString(), vivoJena);
			} catch(IOException e) {
				// TODO Auto-generated catch block
				log.error("IOException: ",e);
			}
			if(vivoJena!= null)
				this.jcf.returnConnectionToPool(vivoJena, "wcmcPeople");
		}
		
		
	    /**
		 * Template to fit in different JenaConnect queries.
		 * @param sparqlQuery contains the query
		 * @return ResultSet containing all the results
		 * @throws IOException default exception thrown
		 */
		private com.hp.hpl.jena.query.ResultSet runSparqlTemplate(String sparqlQuery, SDBJenaConnect vivoJena) throws IOException {
			
			
			com.hp.hpl.jena.query.ResultSet rs = vivoJena.executeSelectQuery(sparqlQuery);		
			return rs;
			
		
		}
		
		/**
		 * Template to fit in different JenaConnect queries.
		 * @param sparqlQuery contains the query
		 * @return ResultSet containing all the results
		 * @throws IOException default exception thrown
		 */
		private void runSparqlUpdateTemplate(String sparqlQuery, SDBJenaConnect vivoJena) throws IOException {
			
			vivoJena.executeUpdateQuery(sparqlQuery, true);
			//log.info("Inserted success");
			
			
		
		}
		

}
