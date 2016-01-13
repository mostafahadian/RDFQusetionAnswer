

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

import com.bigdata.journal.Options;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepository;
import com.bigdata.rdf.sail.remote.BigdataSailRemoteRepositoryConnection;
import com.bigdata.rdf.sail.sparql.ast.ASTQueryContainer;
import com.bigdata.rdf.sail.webapp.SD;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.sail.webapp.client.IPreparedQuery;
import com.bigdata.rdf.sail.webapp.client.IPreparedTupleQuery;
import com.bigdata.rdf.sail.webapp.client.JettyResponseListener;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository.AddOp;
import com.bigdata.rdf.sail.webapp.client.RemoteRepositoryManager;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.DataLoader;

import org.apache.log4j.Logger;
import org.openrdf.http.protocol.transaction.operations.AddStatementOperation;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.rio.RDFFormat;

public class BlazegraphRemote {

	protected static final Logger log = Logger.getLogger(BlazegraphRemote.class);
	private static final String serviceURL = "http://localhost:9999/blazegraph";

	public static RemoteRepository getRemoteConnection() throws Exception {

		final RemoteRepositoryManager repo = new RemoteRepositoryManager(serviceURL, false);
		
		RemoteRepository remoteRepo=null;
		remoteRepo = repo.getRepositoryForDefaultNamespace();
		
//		try {
//
//			String q="SELECT (COUNT(?s) AS ?c)  WHERE { ?s ?p ?d .}";
//			q="SELECT * {?s ?p ?o} LIMIT 100";
//			
//			
//			final TupleQueryResult result = repo.getRepositoryForDefaultNamespace()
//					.prepareTupleQuery(q)
//					.evaluate();
//			
//			//result processing
//			try {
//				while (result.hasNext()) {
//					final BindingSet bs = result.next();
//					System.out.println(bs.toString());
//					log.info(bs);
//				}
//			} finally {
//				result.close();
//			}
//			
//
//		} finally {
//			//repo.close();
//		}
		return remoteRepo;

	}

}
