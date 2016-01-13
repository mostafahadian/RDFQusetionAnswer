import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.openrdf.OpenRDFException;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.rio.ParserConfig;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.helpers.BasicParserSettings;
import org.openrdf.rio.helpers.RDFParserBase;
import org.openrdf.rio.ntriples.NTriplesUtil;

import com.bigdata.journal.Options;
import com.bigdata.rdf.rio.RDFParserOptions;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;



public class WikidataMain {
	static final String baseAddr="D:\\Projects\\Knowledge Graph\\wikidata\\wikidata-statements.nt\\";
	
	static final String	wikidataproperties=baseAddr+"wikidata-properties.nt", 
						wikidatainstances=baseAddr+"wikidata-instances.nt",
						wikidatasimplestatements=baseAddr+"wikidata-simple-statements.nt",
						wikidatasitelinks=baseAddr+"wikidata-sitelinks.nt",
						wikidataterms=baseAddr+"wikidata-terms.nt",
						wikidatastatements=baseAddr+"wikidata-statements.nt";
	
	static final String files[]={WikidataMain.wikidataproperties,
			WikidataMain.wikidatainstances, 
			WikidataMain.wikidatasimplestatements, 
			WikidataMain.wikidatasitelinks, 
			WikidataMain.wikidataterms, 
			WikidataMain.wikidatastatements};
	
	public static void main(String[] args)  {
		
		RepositoryConnection cxn =null;
//		try {
//			cxn = getLocalRepoConnection();
//		} catch (OpenRDFException e) {
//			e.printStackTrace();
//		}
		
		QueryProcessorLocalServer q=new QueryProcessorLocalServer();
		q.test();
		
//		QueryProcessor qp=new QueryProcessor(cxn);
//		qp.test();
		
//		extractPersianEntitiesAndInsertIntoDB();
	}
	
	void extractPersianEntitiesAndInsertIntoDB(){
		//extract those entities whose subject or predicate has something farsi (filter others)!
		RDFTripleStoreFarsiExtractor extractor=new RDFTripleStoreFarsiExtractor();
		extractor.filterAllFiles();
		//insert the filtered files into local sparql server
		String filesfiltered[]={files[0]+".filtered1", files[1]+".filtered1", files[2]+".filtered1",
									files[3]+".filtered1", files[4]+".filtered1", files[5]+".filtered1"};
		insertFilesToLocalDB(filesfiltered, "all.jnl");	
	}
	
	static void insertFilesToLocalDB(String files[], String journalFile){
		for(int i=0; i<files.length; i++){
			try {
				writeToLocalRepo(files[i], journalFile);
				System.out.println("inserted "+ files[i] +" into local repo");
			} catch (OpenRDFException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	static RepositoryConnection getLocalRepoConnection() throws OpenRDFException{
		String filename=wikidataterms+".filtered1";
	
		
		final Properties props = new Properties();
		
		//props.put(Options.BUFFER_MODE, "DiskRW"); // persistent file system located journal
		props.put(Options.BUFFER_MODE, "MemStore"); // persistent file system located journal
		
		
		props.put(Options.FILE, "/tmp/blazegraph/all.jnl"); // journal file location
		
		//props.put(Options.BUFFER_MODE, com.bigdata.journal.BufferMode.DiskRW);
		props.put(BigdataSail.Options.AXIOMS_CLASS, com.bigdata.rdf.axioms.NoAxioms.class.getName());
		props.put(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
		props.put(BigdataSail.Options.JUSTIFY, "false");
		props.put(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
		props.put(BigdataSail.Options.TEXT_INDEX, "false");	
		props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.quads", "false");
		props.put(BigdataSail.Options.QUADS, "false");
		props.put(com.bigdata.btree.IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "80000");
		
		
		long start=System.currentTimeMillis();
		
		final BigdataSail sail = new BigdataSail(props); // instantiate a sail
		
		
//		try {
//			FileInputStream is=new FileInputStream(filename);
//			sail.getDatabase().getDataLoader().loadData(is, "http://", RDFFormat.NTRIPLES);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		final Repository repo = new BigdataSailRepository(sail); // create a Sesame repository
		
		repo.initialize();
		RepositoryConnection cxn=null;
		try {
			cxn = repo.getConnection();
			
			//added this to simply ignore invalid datatypes while parsing the triples and avoid InvalidDataType Exception
			ParserConfig pc=cxn.getParserConfig();
			pc.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
			cxn.setParserConfig(pc);
			
			//need to correct the escape characters to avoid :org.openrdf.rio.RDFParseException: IRI includes string escapes: '\34' [line 85268] 
			//NTriplesUtil.escapeString()	
			//ok: used NTriples instead of N3 and it became ok!
			
			try {
				cxn.begin();
				cxn.add(new File(filename), "http://", RDFFormat.NTRIPLES);
				cxn.commit();
			} catch (OpenRDFException ex) {
				cxn.rollback();
				throw ex;
			}catch (IOException ex){
				cxn.close();
			} finally {
				cxn.close();
			}
			
			long end=System.currentTimeMillis();
			System.out.println("time(s): "+(end-start)/1000.0);
			System.out.println("time(min): "+(end-start)/1000.0/60);			


			// evaluate sparql query
			//String q="SELECT (COUNT(?s) AS ?c)  WHERE { ?s ?p ?d .}";
			String q="SELECT * {?s ?p \"جوایز\"@fa } LIMIT 100";
			
			try {
				TupleQuery tupleQuery = cxn.prepareTupleQuery(QueryLanguage.SPARQL,	q);				
				TupleQueryResult result = tupleQuery.evaluate();

				while (result.hasNext()) {
					BindingSet bindingSet = result.next();
					System.err.println(bindingSet);
				}
				result.close();
			} finally {
				cxn.close();
			}
			
			end=System.currentTimeMillis();
			System.out.println("time(s): "+(end-start)/1000.0);
			System.out.println("time(min): "+(end-start)/1000.0/60);

		} finally {
			//repo.shutDown();
		}		
		return cxn;
	}
	
	
	static void writeToLocalRepo(String srcFile, String journalFile) throws OpenRDFException{
		String filename=srcFile;
	
		final Properties props = new Properties();	
		
		props.put(Options.FILE, "/tmp/blazegraph/"+journalFile); // journal file location
		
		//added the following properties to make it faster to insert
		//props.put(Options.BUFFER_MODE, com.bigdata.journal.BufferMode.DiskRW);
		props.put(Options.BUFFER_MODE, "DiskRW"); // persistent file system located journal
		props.put(BigdataSail.Options.AXIOMS_CLASS, com.bigdata.rdf.axioms.NoAxioms.class.getName());
		props.put(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
		props.put(BigdataSail.Options.JUSTIFY, "false");
		props.put(BigdataSail.Options.STATEMENT_IDENTIFIERS, "false");
		props.put(BigdataSail.Options.TEXT_INDEX, "false");	
		props.setProperty("com.bigdata.rdf.store.AbstractTripleStore.quads", "false");
		props.put(BigdataSail.Options.QUADS, "false");
		props.put(com.bigdata.btree.IndexMetadata.Options.WRITE_RETENTION_QUEUE_CAPACITY, "8000");
		
		
		long start=System.currentTimeMillis();
		
		final BigdataSail sail = new BigdataSail(props); // instantiate a sail
		
		final Repository repo = new BigdataSailRepository(sail); // create a Sesame repository
		
		repo.initialize();
		RepositoryConnection cxn=null;
		try {
			cxn = repo.getConnection();
			
			//added this to simply ignore invalid datatypes while parsing the triples and avoid InvalidDataType Exception
			ParserConfig pc=cxn.getParserConfig();
			pc.set(BasicParserSettings.VERIFY_DATATYPE_VALUES, false);
			cxn.setParserConfig(pc);
			
			//need to correct the escape characters to avoid :org.openrdf.rio.RDFParseException: IRI includes string escapes: '\34' [line 85268] 
			//NTriplesUtil.escapeString()	
			//ok: used NTriples instead of N3 and it became ok!
			
			try {
				cxn.begin();
				cxn.add(new File(filename), "http://", RDFFormat.NTRIPLES);
				cxn.commit();
			} catch (OpenRDFException ex) {
				cxn.rollback();
				throw ex;
			}catch (IOException ex){
				cxn.close();
			} finally {
				cxn.close();
			}
			
			long end=System.currentTimeMillis();
			System.out.println("time(s): "+(end-start)/1000.0);
			System.out.println("time(min): "+(end-start)/1000.0/60);			


		} finally {
			repo.shutDown();
		}		
	}	
}