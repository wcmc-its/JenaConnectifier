# JenaConnectifier

Using [Harvester](https://github.com/vivo-project/VIVO-Harvester) to import data into VIVO can be inefficient.

JenaConnectifier is a fork of Harvester, which inserts triples directly into VIVO including the reinference graph without the need for a shadow system or a system restart. Additionally, this tool can update exist data.

We at Weill Cornell Medicine have found that it's pretty fast: we can have a fully updated and inferenced VIVO within 40 minutes.

It was described and demo'd on a recent Apps and Tools call:
https://wiki.duraspace.org/display/VIVO/Apps+and+Tools+Call+20180222



# Requirements
* Java 1.8 or higher
* Latest verion of Maven
* Latest version of git


## Our old legacy process 

Here's how Weill Cornell would historically import data into our VIVO.

1. Fetch data from database using a custom Java class (e.g., Scopus-Harvester)
2. Transform data into valid RDF using XSLTranslator. XSLTranslator uses different inputs, one of which is rawRecords.config.xml. This fetches individual records and outputs to “data-raw-records” folder, one per day.
3. Determine if any of the above retrieved data exists in VIVO. If the data doesn’t exist, it is added to VIVO using the harvesterTransfer Java class. The changes are now applied to the VIVO data model, by addition. Our experience has been that the subtractions don’t apply.
4. The local data model is stored in a folder where the script is running. If by chance, the folder is deleted, you lose knowledge of what’s in your system.

Updates take several hours at least. In the case of publications, we’ve found that it takes 5 or 6 hours. 

## Using SDBJenaConnect to import data into VIVO

We highly recommend not to import all data into a single http://vitro.mannlib.cornell.edu/default/vitro-kb-2 graph rather split intot multiple graphs based on different data types. 

### For e.g. we use the following graphs for all our data.
For people related data including preferred names, email, phone, primary title - http://vitro.mannlib.cornell.edu/a/graph/wcmcPeople  
For grants related data - http://vitro.mannlib.cornell.edu/a/graph/wcmcCoeus  
For appointment and education and training related data - http://vitro.mannlib.cornell.edu/a/graph/wcmcOfa  
For publications - http://vitro.mannlib.cornell.edu/a/graph/wcmcPublications  
For overview statements and user inputed data - http://vitro.mannlib.cornell.edu/default/vitro-kb-2  
For inference data - http://vitro.mannlib.cornell.edu/default/vitro-kb-inf

If you decide to go with this approach you can change the graph name in https://github.com/wcmc-its/vivo-import-data/blob/master/vivo-import-data/src/main/java/org/vivoweb/harvester/connectionfactory/JenaConnectionFactory.java and also in the files itself for different operations.

 <b>N.B.</b> Currently work in progress on making this changes more robust which would enable you to change a property file and it would take all the graphs name from there 

Here's how this code works.

1. Fetch person identifiers from VIVO itself.
2. Use person ID to grab grants from grant source system (in our case, Coeus).
3. Check grantExistInVIVO (in [GrantsFetchFromED.java](https://github.com/wcmc-its/vivo-import-data/blob/master/vivo-import-data/src/main/java/org/vivoweb/harvester/ingest/GrantsFetchFromED.java)) to see if the grant exists in VIVO.
4. If it exists, checkForUpdates looks for changes in dates, new contributors, etc.
5. For each property, computes the changes in RDF, change data that needs to be deleted or updated, by directly deleting triples and adding triples to the data store.
6. Cycle through each of the property and making changes as needed.
7. Add triples to the KB-INF (inference graph).

We've found that this import is more reliable and takes substantially less time.

## How to run?

Follow the steps to run the application:
1. Clone the repository - `git clone https://github.com/wcmc-its/vivo-import-data.git`
2. Navigate to the folder and run `mvn install -Dmaven.test.skip=true` 
3. Go to ./vivo-import-data/src/main/resources and setup the scripts related to different graphs like People, Grants, Publications etc. There are folders for each including for deletion of profile from VIVO as well.
4. Make changes to the run script in each folder and set path for JENACONNECTIFIER_INSTALL_DIR and JENACONNECTIFIER_DIR to the path where the repo is cloned. 
5. Add your host name and email address to get notification incase of any errors or exceptio in the script.
6. Go to install_path/bin/* to change the path of property files supplied to each workflow.
5. Run the script using ./run-*.sh. The order should be to run the people script first then rest of the scripts.

## Delete Profile
The delete profile workflow runs to delete a profile if a unique ID is supplied to it. First it checks against your identity system to see if a person is active or not based on logic which can be changed in files. This workflow works with the idea that each aspect of data is compartmentalized into different graphs e.g. people data in people graph, grants data in grant graph & publication data in seperate publication graph. Seperating the data into different graphs helps us manage the data efficiently and have nice decoupling amongst different types of data in the VIVO.

The workflow check against all the graphs and delete any traces of triples pertaining to the inactive persons unique indentifier.

## Summary of changes

The following slide was presented at a WCM VIVO leadership meeting.
![](https://github.com/wcmc-its/vivo-import-data/blob/master/SDBJenaConnect.png?raw=true "")
