package org.vivoweb.harvester.ingest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vivoweb.harvester.connectionfactory.RDBMSConnectionFactory;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

public class VivoDashboardFileGenerator {
	
	public static String propertyFilePath = null;
	
	private static String filePath = null;
	//Delimiter used in CSV file
    private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";


	

	private static Properties props = new Properties();
	private static Logger log = LoggerFactory.getLogger(VivoDashboardFileGenerator.class);
	
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
			try {
				props.load(new FileInputStream(propertyFilePath));
			} catch (FileNotFoundException e) {
				log.info("File not found error: " + e);
			} catch (IOException e) {
				log.info("IOException error: " + e);
			}
			
			filePath = props.getProperty("Filepath");
			new VivoDashboardFileGenerator().init();
		}
	}
		
		
		private void init() {
			try {
				execute();
			} catch(IOException e) {
				// TODO Auto-generated catch block
				log.error("IOException: " ,e);
			}

		}
		
		private void execute() throws IOException {
			this.con = this.mcf.getConnectionfromPool();
			fetchAuthors();
			fetchAuthorship();
			fetchJournals();
			fetchPublications();
			if(this.con!=null) {
				this.mcf.returnConnectionToPool(this.con);
				this.mcf.destroyConnectionPool();
			}
			
		}
		
		private void fetchAuthors() {
			
			FileWriter fw = null;
			File file = null;
			String query = "select distinct \n" +
							"case\n" +
							" when e.cwid is not null then concat('http://vivo.med.cornell.edu/individual/cwid-', cast(e.cwid as char(20))) \n" +
							" when e.cwid is null then concat('http://vivo.med.cornell.edu/individual/person', cast(wcmc_authorship_pk as char(20)))\n" +
							"end as authorshipURI,\n" +
							"case\n" +
							" when e.cwid is not null then concat(e.sn,', ',e.givenName,\n" +
							" if(weillCornellEduMiddleName is not null, concat(' ',weillCornellEduMiddleName),''))\n" +
							" when e.cwid is null then replace(concat(surname,' ',initials),'.','')\n" +
							"end as label,\n" +
							"case\n" +
							" when e.cwid is not null then concat(e.givenName,if(weillCornellEduMiddleName is not null, concat(' ',weillCornellEduMiddleName),'')) \n" +
							" when e.cwid is null then replace(given_name,'.','')\n" +
							"end as firstName,\n" +
							"case\n" +
							" when e.cwid is not null then e.sn\n" +
							" when e.cwid is null then replace(surname,'.','')\n" +
							"end as lastName,\n" +
							"e.cwid as cwid, \n" +
							"e.o as affiliation, \n" +
							"e.primaryDepartment as department, \n" +
							"concat(\n" +
							"if(personTypeCodes like '%academic-faculty-weillfulltime%','http://weill.cornell.edu/vivo/ontology/wcmc#FullTimeWCMCFaculty|',''),\n" +
							"if(personTypeCodes like '%academic-faculty-weillparttime%', 'http://weill.cornell.edu/vivo/ontology/wcmc#PartTimeWCMCFaculty|',''),\n" +
							"if(personTypeCodes like '%academic-faculty-voluntary%', 'http://weill.cornell.edu/vivo/ontology/wcmc#VoluntaryFaculty|',''),\n" +
							"if(personTypeCodes like '%academic-faculty-adjunct%', 'http://weill.cornell.edu/vivo/ontology/wcmc#AdjunctFaculty|',''),\n" +
							"if(personTypeCodes like '%academic-faculty-emeritus%', 'http://vivoweb.org/ontology/core#EmeritusFaculty|',''),\n" +
							"if(personTypeCodes like '%academic-faculty-adjunct%', 'http://weill.cornell.edu/vivo/ontology/wcmc#AdjunctFaculty|',''),\n" +
							"if(personTypeCodes like '%academic-nonfaculty-postdoc-fellow%', 'http://weill.cornell.edu/vivo/ontology/wcmc#Fellow|',''),\n" +
							"if(personTypeCodes like '%academic-nonfaculty-postdoc%', 'http://vivoweb.org/ontology/core#Postdoc|',''),\n" +
							"if(personTypeCodes like '%academic-faculty%', 'http://vivoweb.org/ontology/core#FacultyMember|',''),\n" +
							"if(personTypeCodes like '%academic-nonfaculty%', 'http://vivoweb.org/ontology/core#NonAcademic|',''),\n" +
							"if(personTypeCodes like '%student-md-phd-tri-i%', 'http://weill.cornell.edu/vivo/ontology/wcmc#StudentMdPhdTriI|',''),\n" +
							"if(personTypeCodes like '%student-phd-weill%', 'http://weill.cornell.edu/vivo/ontology/wcmc#StudentPhdWeill|',''),\n" +
							"if(personTypeCodes like '%student-phd-tri-i%', 'http://weill.cornell.edu/vivo/ontology/wcmc#StudentPhdTriI|',''),\n" +
							"'http://xmlns.com/foaf/0.1/Person' \n" +
							") \n" +
							"as RDFtypes, \n" +
							"e.popsURI as popsURI, \n" +
							"e.directoryURI as directoryURI, \n" +
							"e.vivoURI as vivoURI, \n" +
							"e.primaryTitle \n" +
							"from wcmc_authorship a \n" +
							"left join people_ed e on \n" +
							"e.cwid = a.cwid";
			
			log.info("Running Authors query: " + query);
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				file = new File(filePath + "/authors.csv");
				if(file.exists()) {
					file.delete();
				}
				fw = new FileWriter(file);
				ps = this.con.prepareStatement(query);
				rs = ps.executeQuery();
				while(rs.next()) {
					if(rs.getString(1) != null) {
						fw.append("\"");
						fw.append(rs.getString(1));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(2) != null && rs.getString(2).contains(",")) {
						fw.append("\"");
						fw.append(rs.getString(2).replace(",", ""));
						fw.append("\"");
					}
					else if(rs.getString(2) != null && !rs.getString(2).contains(",")) {
						fw.append("\"");
						fw.append(rs.getString(2));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(3) != null) {
						fw.append("\"");
						fw.append(rs.getString(3));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(4) != null) {
						fw.append("\"");
						fw.append(rs.getString(4));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(5) != null) {
						fw.append("\"");
						fw.append(rs.getString(5));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(6) != null) {
						fw.append("\"");
						fw.append(rs.getString(6));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(7) != null) {
						fw.append("\"");
						fw.append(rs.getString(7));
						fw.append("\"");
					}
					//fw.append(COMMA_DELIMITER);
					//fw.append("\"");
					//fw.append("\"");
					fw.append(COMMA_DELIMITER);
					if(rs.getString(8) != null) {
						fw.append("\"");
						fw.append(rs.getString(8));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(9) != null) {
						fw.append("\"");
						fw.append(rs.getString(9));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(10) != null) {
						fw.append("\"");
						fw.append(rs.getString(10));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(11) != null) {
						fw.append("\"");
						fw.append(rs.getString(11));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(12) != null) {
						fw.append("\"");
						fw.append(rs.getString(12));
						fw.append("\"");
					}
					fw.append(NEW_LINE_SEPARATOR);
				}
				
				log.info("File generated successfully in Author in " + filePath );
			} catch(SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch(IOException ioe) {
				ioe.printStackTrace();
			}
			finally {
				try{
					if(fw != null) {
						fw.flush();
						fw.close();
					}
					if(ps != null)
						ps.close();
					if(rs!=null)
						rs.close();
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
				
		}
		
		private void fetchAuthorship() {
			
			FileWriter fw = null;
			File file = null;
			String query = "select distinct concat('http://vivo.med.cornell.edu/individual/pubid',cast(wcmc_document.scopus_doc_id as char(30)),'authorship',cast(wcmc_authorship_rank as char(20))) authorship, NULL as label, \n" +
						   "wcmc_authorship_rank as rank, \n" +
						   //"case \n" +
						   //"when exists_in_vivo = 'Y' then concat(\"http://vivo.med.cornell.edu/individual/pubid\",scopus_doc_id) \n" +
						   //"when exists_in_vivo = 'N' and pmid is not null then concat(\"https://www.ncbi.nlm.nih.gov/pubmed/\",pmid) \n" +
						   //"else concat(\"http://www.scopus.com/inward/record.url?partnerID=HzOxMe3b&scp=\",scopus_doc_id) \n" +
						   "wcmc_document_pk as publication, \n" +
							"case \n" +
							 "when e.cwid is not null then concat('http://vivo.med.cornell.edu/individual/cwid-',cast(e.cwid as char(20))) \n" + 
							 "when e.cwid is null then concat('http://vivo.med.cornell.edu/individual/person',cast(wcmc_authorship_pk as char(20))) \n" +
							"end as person, \n" +
							"case \n" +
							 "when e.cwid is not null then concat('http://vivo.med.cornell.edu/individual/arg2000028-cwid-',cast(e.cwid as char(20))) \n" + 
							 "when e.cwid is null then concat('http://vivo.med.cornell.edu/individual/arg2000028-person',cast(wcmc_authorship_pk as char(20))) \n" +
							"end as vcard \n" +
							"FROM wcmc_document left JOIN wcmc_document_authorship ON wcmc_document_authorship.wcmc_document_fk = wcmc_document.wcmc_document_pk inner JOIN wcmc_authorship a \n" +
							"ON a.wcmc_authorship_pk = wcmc_document_authorship.wcmc_authorship_fk left join people_ed e on e.cwid = a.cwid \n" +
							"where wcmc_document_authorship.ignore_flag is null and (duplicate = 'N' OR duplicate is NULL) AND pubtype <> 'er' AND pubtype not like '%erratum%' and publication_name is not null and title is not null";
			log.info("Running Authorship query: " + query);
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				file = new File(filePath + "/authorships.csv");
				if(file.exists()) {
					file.delete();
				}
				fw = new FileWriter(file);
				ps = this.con.prepareStatement(query);
				rs = ps.executeQuery();
				while(rs.next()) {
					if(rs.getString(1) != null) {
						fw.append("\"");
						fw.append(rs.getString(1));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(2) != null) {
						fw.append("\"");
						fw.append(rs.getString(2));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(3) != null) {
						fw.append(rs.getString(3));
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(4) != null) {
						fw.append("\"");
						fw.append(rs.getString(4));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(5) != null) {
						fw.append("\"");
						fw.append(rs.getString(5));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(6) != null) {
						fw.append("\"");
						fw.append(rs.getString(6));
						fw.append("\"");
					}
					fw.append(NEW_LINE_SEPARATOR);
				}
				
				log.info("File generated successfully for Authorship in " + filePath );
			} catch(SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch(IOException ioe) {
				ioe.printStackTrace();
			}
			finally {
				try{
					if(fw != null) {
						fw.flush();
						fw.close();
					}
					if(ps != null)
						ps.close();
					if(rs!=null)
						rs.close();
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
				
		}
		private void fetchJournals() {
			
			FileWriter fw = null;
			File file = null;

			String query = "select distinct \n" +
				"replace( \n" +
				 "case \n" +
				 "when n.nlmissn <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n.nlmissn) \n" +
				 "when n1.nlmissn <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n1.nlmissn) \n" +
				 "when n.nlmeissn  <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n.nlmeissn) \n" +
				 "when n2.nlmissn <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n2.nlmissn) \n" +
				 "when n2.nlmeissn <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n2.nlmeissn) \n" + 
				 "when issn is not null then concat(\"http://vivo.med.cornell.edu/journal/\",issn) \n" +
				 "when eissn is not null then concat(\"http://vivo.med.cornell.edu/journal/\",eissn) \n" +
				 "when isbn13 is not null then concat(\"http://vivo.med.cornell.edu/book/\",isbn13) \n" +
				 "when isbn10 is not null then concat(\"http://vivo.med.cornell.edu/book/\",isbn10) \n" +
				 "else concat(\"http://vivo.med.cornell.edu/journal/\",md5(publication_name)) \n" +
				 "end \n" + 
				",'-','') as URI, \n" +
				 "case \n" +
				 "when n.nlmfulltitle is not null then n.nlmfulltitle \n" +
				 "when n1.nlmfulltitle is not null then n1.nlmfulltitle \n" +
				 "when n2.nlmfulltitle is not null then n2.nlmfulltitle \n" + 
				 "else publication_name \n" + 
				 "end \n" +
				"as label, \n" + 
				"coalesce(n.nlmissn,n1.nlmissn,n2.nlmissn,issn) AS issn, \n" +
				"coalesce(n.nlmeissn,n1.nlmeissn,n2.nlmeissn,eissn) AS eissn, \n" +
				"if(coalesce(n.nlmissn,n1.nlmissn,n2.nlmissn,issn,n.nlmeissn,n1.nlmeissn,n2.nlmeissn,eissn) is null,isbn13,null) as isbn13, \n" +
				"if(coalesce(n.nlmissn,n1.nlmissn,n2.nlmissn,issn,n.nlmeissn,n1.nlmeissn,n2.nlmeissn,eissn) is null,isbn10,null) as isbn10, \n" +
				"case \n" + 
				 "when (n.nlmcatalog is not null OR n1.nlmcatalog is not null OR n2.nlmcatalog is not null OR issn is not null or eissn is not null) then 'http://purl.org/ontology/bibo/Journal' \n" +
				 "when (isbn13 is not null OR isbn10 is not null) then 'http://purl.org/ontology/bibo/Book' \n" +
				 "else 'http://purl.org/ontology/bibo/Collection' \n" +
				"end as type \n" +
				"from wcmc_document d left join NLM n on issn = replace(n.nlmissn,\"-\",\"\") left join NLM n1 on eissn = replace(n1.nlmeissn,\"-\",\"\") left join NLM n2 on n2.nlmabbreviation = d.nlmabbreviation where publication_name is not null and title is not null";
			log.info("Running Journals query: " + query);
			
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				file = new File(filePath + "/journals.csv");
				if(file.exists()) {
					file.delete();
				}
				fw = new FileWriter(file);
				ps = this.con.prepareStatement(query);
				rs = ps.executeQuery();
				while(rs.next()) {
					if(rs.getString(1) != null) {
						fw.append("\"");
						fw.append(rs.getString(1));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(2) != null) {
						fw.append("\"");
						fw.append(rs.getString(2));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(3) != null) {
						fw.append("\"");
						fw.append(rs.getString(3));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(4) != null) {
						fw.append("\"");
						fw.append(rs.getString(4));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(5) != null) {
						fw.append("\"");
						fw.append(rs.getString(5));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(6) != null) {
						fw.append("\"");
						fw.append(rs.getString(6));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(7) != null) {
						fw.append("\"");
						fw.append(rs.getString(7));
						fw.append("\"");
					}
					fw.append(NEW_LINE_SEPARATOR);
				}
				
				log.info("File generated successfully for Journals in " + filePath );
			} catch(SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch(IOException ioe) {
				ioe.printStackTrace();
			}
			finally {
				try{
					if(fw != null) {
						fw.flush();
						fw.close();
					}
					if(ps != null)
						ps.close();
					if(rs!=null)
						rs.close();
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
				
		}
		
		private void fetchPublications() {
			
			FileWriter fw = null;
			File file = null;
			
			String query = "select distinct \n" +
				//"case \n" +
			  	//"when exists_in_vivo = 'Y' then concat(\"http://vivo.med.cornell.edu/individual/pubid\",scopus_doc_id) \n" +
			  	//"when exists_in_vivo = 'N' and pmid is not null then concat(\"https://www.ncbi.nlm.nih.gov/pubmed/\",pmid) \n" +
			  	//"else concat(\"http://www.scopus.com/inward/record.url?partnerID=HzOxMe3b&scp=\",scopus_doc_id) \n" +
			  	"d.wcmc_document_pk as publication, \n" +
				"title as label,\n" +
				"date_format(cover_date, '%Y-%m-%d %T') as date,\n" +
				"replace( \n" +
				"case \n" +
				"when n.nlmissn <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n.nlmissn) \n" +
				"when n1.nlmissn <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n1.nlmissn) \n" +
				"when n.nlmeissn <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n.nlmeissn) \n" +
				"when n2.nlmissn <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n2.nlmissn) \n" +
				"when n2.nlmeissn <> '' then concat(\"http://vivo.med.cornell.edu/journal/\",n2.nlmeissn) \n" + 
				"when issn is not null then concat(\"http://vivo.med.cornell.edu/journal/\",issn) \n" +
				"when eissn is not null then concat(\"http://vivo.med.cornell.edu/journal/\",eissn) \n" +
				"when isbn13 is not null then concat(\"http://vivo.med.cornell.edu/book/\",isbn13) \n" +
				"when isbn10 is not null then concat(\"http://vivo.med.cornell.edu/book/\",isbn10) \n" +
				"else concat(\"http://vivo.med.cornell.edu/journal/\",md5(publication_name)) \n" +
				"end \n" + 
				",'-','') as journal, \n" +
				"volume,\n" +
				"issue,\n" +
				"concat(\"DOI: \",doi) as doi,\n" +
				"case \n" +
				"when pmid is not null then pmid \n" +
				"when pmid_lookup is not null and pmid <> '0' and pmid <> '1' then pmid_lookup \n" +
				"end as pmid, \n" +
				"pmcid,\n" +
				"scopus_doc_id as scopusID,\n" +
				"pages AS pageStart,\n" +
				"NULL,\n" +
				"citation_count,\n" +
				"case \n" +
				"when pubtype='re' then 'http://purl.org/ontology/bibo/Report'\n" +
				"when pubtype='cp' OR \n" +
				"(((d.publication_name like '%proceedings%' and (d.publication_name like '%20%' OR d.publication_name like '%19%')) OR \n" +
				"(d.publication_name like '%proceedings%'  AND d.publication_name like '%symposi%') OR \n" +
				"(d.publication_name like '%proceedings%'  AND d.publication_name like '%congress%') OR \n" +
				"(d.publication_name like '%conference%') OR \n" +
				"(d.publication_name like '%workshop%') OR \n" +
				"(d.publication_name like '%colloqui%') OR \n" +
				"(d.publication_name like '%meeting%'))) then 'http://vivoweb.org/ontology/core#ConferencePaper'\n" +
				"when pubtype='ch' then 'http://purl.org/ontology/bibo/Chapter'\n" +
				"when pubtype='ed' then 'http://vivoweb.org/ontology/core#EditorialArticle' \n" +
				"when pubtype='ip' then 'http://weill.cornell.edu/vivo/ontology/wcmc#InProcess'\n" +
				"when pubtype='bk' then 'http://purl.org/ontology/bibo/Book' \n" +
				"when pubtype='le' then 'http://weill.cornell.edu/vivo/ontology/wcmc#Letter'\n" +
				"when pubtype='no' then 'http://weill.cornell.edu/vivo/ontology/wcmc#Comment' \n" +
				"when pubtype='ar' then 'http://purl.org/ontology/bibo/AcademicArticle'\n" +
				"when (pubtype='ab' OR pubtype='bz' OR pubtype='cr' OR pubtype='sh') then 'http://purl.org/ontology/bibo/Article' \n" +
				"when pubtype like '%editorial%' then 'http://vivoweb.org/ontology/core#EditorialArticle' \n" +
				"when pubtype like '%letter%' then 'http://weill.cornell.edu/vivo/ontology/wcmc#Letter' \n" +
				"when pubtype='%comment%' then 'http://weill.cornell.edu/vivo/ontology/wcmc#Comment' \n" +
				"when (pubtype like '%Consensus Development Conference%' OR \n" +
				"pubtype like '%addresses%' OR \n" +
				"pubtype like '%clinical conference%' OR \n" +
				"pubtype like '%congresses%' OR \n" +
				"pubtype like '%lectures%' \n" +
				") then 'http://vivoweb.org/ontology/core#ConferencePaper' \n" +
				"when (pubtype like '%meta-analysis%' OR \n" +
				"pubtype like '%review%' OR \n" +
				"pubtype like '%classical article%' OR \n" +
				"pubtype like '%scientific integrity review%' OR \n" +
				"pubtype like '%guideline%' OR \n" +
				"pubtype like '%practice guideline%' \n" +
				") then 'http://vivoweb.org/ontology/core#Review'\n" +
				"when (pubtype like '%journal article%' OR \n" +
				"pubtype like '%Clinical Trial, Phase I%' OR \n" +
				"pubtype like '%Clinical Trial, Phase II%' OR \n" +
				"pubtype like '%Clinical Trial, Phase III%' OR \n" +
				"pubtype like '%Clinical Trial, Phase IV%' OR \n" +
				"pubtype like '%Clinical trial, Controlled %' OR \n" +
				"pubtype like '%randomized controlled trial%' OR \n" +
				"pubtype like '%multicenter study%' OR \n" +
				"pubtype like '%twin study%' OR \n" +
				"pubtype like '%validation studies%' OR \n" +
				"pubtype like '%case reports%' OR \n" +
				"pubtype like '%comparative study%' OR \n" +
				"pubtype like '%technical report%' \n" +
				") then 'http://purl.org/ontology/bibo/AcademicArticle' \n" +
				"else 'http://purl.org/ontology/bibo/Article' \n" +
				"end as pubtype\n" +
				"from wcmc_document d\n" +
				"left join NLM n on\n" +
				"issn = replace(n.nlmissn,\"-\",\"\")\n" +
				"left join NLM n1 on\n" +
				"eissn = replace(n1.nlmeissn,\"-\",\"\")\n" +
				"left join NLM n2 on\n" +
				"n2.nlmabbreviation = d.nlmabbreviation\n" +
				"where (duplicate = 'N' OR duplicate is NULL) AND pubtype <> 'er' AND pubtype not like '%erratum%' and title is not null and publication_name is not null";
			
			log.info("Running Publications query: " + query);
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				file = new File(filePath + "/publications.csv");
				if(file.exists()) {
					file.delete();
				}
				fw = new FileWriter(file);
				ps = this.con.prepareStatement(query);
				rs = ps.executeQuery();
				while(rs.next()) {
					fw.append("\"");
					fw.append(rs.getString(1));
					fw.append("\"");
					fw.append(COMMA_DELIMITER);
					if(rs.getString(2) != null && rs.getString(2).contains(",")) {
						fw.append("\"");
						fw.append(rs.getString(2).replace(",", ""));
						fw.append("\"");
					}
					else if(rs.getString(2) != null && !rs.getString(2).contains(",")) {
						fw.append("\"");
						fw.append(rs.getString(2));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(3) != null) {
						fw.append(rs.getString(3));
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(4) != null) {
						fw.append("\"");
						fw.append(rs.getString(4));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(5) != null) {
						fw.append("\"");
						fw.append(rs.getString(5));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(6) != null) {
						fw.append("\"");
						fw.append(rs.getString(6));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(7) != null) {
						fw.append("\"");
						fw.append(rs.getString(7));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(8) != null) {
						fw.append("\"");
						fw.append(rs.getString(8));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(9) != null) {
						fw.append("\"");
						fw.append(rs.getString(9));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(10) != null) {
						fw.append("\"");
						fw.append(rs.getString(10));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(11) != null) {
						fw.append("\"");
						fw.append(rs.getString(11));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(12) != null) {
						fw.append("\"");
						fw.append(rs.getString(12));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(13) != null) {
						fw.append("\"");
						fw.append(rs.getString(13));
						fw.append("\"");
					}
					fw.append(COMMA_DELIMITER);
					if(rs.getString(14) != null) {
						fw.append("\"");
						fw.append(rs.getString(14));
						fw.append("\"");
					}
					fw.append(NEW_LINE_SEPARATOR);
				}
				
				log.info("File generated successfully for Publications in " + filePath );
			} catch(SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			catch(IOException ioe) {
				ioe.printStackTrace();
			}
			finally {
				try{
					if(fw != null) {
						fw.flush();
						fw.close();
					}
					if(ps != null)
						ps.close();
					if(rs!=null)
						rs.close();
				}
				catch(Exception e) {
					log.error("Exception",e);
				}
				
			}
				
		}
	
}
