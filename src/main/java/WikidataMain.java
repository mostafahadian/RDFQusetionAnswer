import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
						
	
	public static void main(String[] args)  {
		
		RepositoryConnection cxn =null;
//		try {
//			cxn = getLocalRepoConnection();
//		} catch (OpenRDFException e) {
//			e.printStackTrace();
//		}
		
		//QueryProcessor qp=new QueryProcessor(cxn);
		//qp.test();

		//RDFTripleStoreFarsiExtractor extractor=new RDFTripleStoreFarsiExtractor();
		//extractor.filterAllFiles();
	}
	
	
	static RepositoryConnection getLocalRepoConnection() throws OpenRDFException{
		String filename=wikidataterms+".filtered1";
	
		
		final Properties props = new Properties();
		
		props.put(Options.BUFFER_MODE, "DiskRW"); // persistent file system located journal
		
		
		props.put(Options.FILE, "/tmp/blazegraph/all.jnl"); // journal file location
		
		//props.put(Options.BUFFER_MODE, com.bigdata.journal.BufferMode.DiskRW);
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
}