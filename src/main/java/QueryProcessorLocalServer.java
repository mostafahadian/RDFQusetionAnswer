import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import com.bigdata.rdf.sail.webapp.client.RemoteRepository;



public class QueryProcessorLocalServer {
	final String nl="\r\n";
	final String querybase="https://query.wikidata.org/bigdata/namespace/wdq/sparql?query=";
	final String SPARQL_DESCRIPTION_PROPERTY_URI= "<http://schema.org/description>";
	final String RDFS_LABEL="http://www.w3.org/2000/01/rdf-schema#label";
	final String LANG_FILTER="FILTER (LANG(?result1)=\"en\" || LANG(?result1)=\"fa\")";
	
	RemoteRepository localSparqlConnection;
	
	public QueryProcessorLocalServer() {
		try {
			localSparqlConnection=BlazegraphRemote.getRemoteConnection();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void test(){
		String queries[]={"بلندترین قله ایران","شغل حسن روحانی","سفیران ایران در آلمان","پایتخت ایران","شغل احمدینژاد","پدر زهرا" ,"ارتفاع  دماوند", "محل تولد حسن روحانی", "محل تولد باراک اوباما", "کشور شهروندی کلینت ایستوود", "پست سیاسی امام خمینی", "سعید جلیلی عضو حزب چیست؟", "ایدئولوژی سیاسی جبهه پایداری انقلاب اسلامی", "برادر امام حسن", "امام حسین", "", ""};

		processQuery(queries[3]);
		
		try {
			localSparqlConnection.getRemoteRepositoryManager().close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	void processQuery(String q){
		String prop=null;
		String entity=null;
		String terms[]=q.split(" ");

		
		//find all properties and all entities possible!
		LinkedList<DoubleString>[] sets=findAllEntitiesPossible(terms);
		List<DoubleString> entitySet=sets[0], propSet=sets[1];
		Set<DoubleString> settemp=new TreeSet<DoubleString>();
		
		//remove repetitions
		settemp.addAll(entitySet);
		entitySet.clear();
		entitySet.addAll(settemp);
		settemp.clear();
		
		settemp.addAll(propSet);
		propSet.clear();
		propSet.addAll(settemp);
		
		entitySet.sort(new DoubleString());
		propSet.sort(new DoubleString());
		
		for (DoubleString s : entitySet) {
			System.out.println(s);
		}
		for (DoubleString s : propSet) {
			System.out.println(s);
		}		
		
		
		System.out.println("result:--------------------------------");

		for (int i=0; i<10 ; i++){
			for(int j=0; j<10 ; j++){
			
				if(j>=entitySet.size() || i>=propSet.size()){
					continue;
				}
				//System.out.println(j+" of "+entitySet.size()+", "+i+" of "+propSet.size());
				Set<DoubleString> resultUrisSet=new TreeSet<DoubleString>();
				String query = getObjectForSpecificSubjectPropSparqlQuery(entitySet.get(j).id, propSet.get(i).id+"c", "");
				
				try {
					TupleQueryResult result = runQueryOnLocalSparqlServer(query);
					while(result.hasNext()){
						BindingSet bs = result.next();
						String resultUri=bs.getValue("result1").toString();
						
						String resultLabel="";
						query = getObjectForSpecificSubjectPropSparqlQuery(resultUri, RDFS_LABEL, LANG_FILTER);
						TupleQueryResult labelresult = runQueryOnLocalSparqlServer(query);
						while(labelresult.hasNext()){
							BindingSet labelbs = labelresult.next();
							resultLabel+=labelbs.getValue("result1").toString()+"    ";
						}
						labelresult.close();
						resultUrisSet.add(new DoubleString(resultUri, resultLabel));
						System.out.println(resultUri+": "+resultLabel);
					}
					result.close();
				} catch (QueryEvaluationException e) {
					e.printStackTrace();
				}
			}
		}
	}

	LinkedList<DoubleString>[] findAllEntitiesPossible(String terms[]){
		int maxEntityTerms=3;
		int n=terms.length;
		LinkedList<DoubleString> entitySet=new LinkedList<DoubleString>(),
						  propSet=new LinkedList<DoubleString>();
		for(int i=0; i<n; i++){
			for(int j=i; j<Math.min(i+maxEntityTerms, n); j++){
				String candidateEntityLabel=concatenateStrings(terms, i, j);
				LinkedList<DoubleString>[] sets=getPropertiesWithLabelFromLocalSparqlServer(candidateEntityLabel);
				//List l=new LinkedList<String>();
				entitySet.addAll(sets[0]);
				propSet.addAll(sets[1]);		
			}
		}
		return new LinkedList[] {entitySet,propSet};
	}
	
	String concatenateStrings(String strs[], int from, int to){
		String s=strs[from];
		for(int i=from+1; i<=to; i++){
			s=s+" "+strs[i];
		}
		return s;
	}
	
	
	LinkedList<DoubleString>[] getPropertiesWithLabelFromLocalSparqlServer(String propLabel){
		String query=getEntityWithLabelSparqlQuery(propLabel);
		TupleQueryResult result = runQueryOnLocalSparqlServer(query);
		LinkedList<DoubleString> entitySet=new LinkedList<DoubleString>(),
							propSet=new LinkedList<DoubleString>();
		try {
			while (result.hasNext()) {
				BindingSet bindingSet = result.next();
				Value b = bindingSet.getValue("entity");
				String uri=b.toString();
				DoubleString entity = new DoubleString(uri, propLabel);
				if(!entity.isProperty()){
					entitySet.add(entity);
				}else{
					propSet.add(entity);
				}
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}	
		return new LinkedList[] {entitySet, propSet};
	}
	
	
	class DoubleString implements Comparable<DoubleString>,Comparator<DoubleString>{
		String id, label;

		boolean isProperty(){
			String strs[]=id.split("/");
			if(strs.length==0 || strs[strs.length-1].length()==0){
				return false;
			}
			String entityCode=strs[strs.length-1];
			if(entityCode.charAt(0)=='P'){
				return true;
			}
			return false;
		}
		
		public DoubleString(String id, String label) {
			this.id = id;
			this.label = label;
		}
		
		public DoubleString() {
		}

		public int compareTo1(DoubleString arg0) {
			return label.split(" ").length - arg0.label.split(" ").length;
		}
		
		@Override
		public String toString() {
			return "("+id+", "+label+")";
		}

		public int compare(DoubleString arg0, DoubleString arg1) {
			return -arg0.compareTo1(arg1);
		}

		public int compareTo(DoubleString arg0) {
			return id.compareTo(arg0.id);
		}
	}
	
	
	
	
	TupleQueryResult runQueryOnLocalSparqlServer(String query){
		TupleQueryResult result = null;
		try {
			result = localSparqlConnection.prepareTupleQuery(query).evaluate();				
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	

	String getEntityWithLabelSparqlQuery(String entityLabel){
		String sparqlQuery=
					"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+nl+
					"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+nl+
					"SELECT ?entity WHERE {"+nl+
					    "{{ ?entity rdfs:label \""+entityLabel+"\"@fa . } UNION { ?entity skos:altLabel \""+entityLabel+"\"@fa . }}"+nl+
					"} limit 10";
		return sparqlQuery;
	}
	
	
	String  getObjectForSpecificSubjectPropSparqlQuery(String subjectUri, String propUri, String filter){
		String sparqlQuery="PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+nl+
				"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+nl+
				"PREFIX hint: <http://www.bigdata.com/queryHints#>"+nl+
				"SELECT ?result1 WHERE {"+
					"hint:Query hint:optimizer \"None\" ."+nl +
				    "<"+subjectUri+"> <" + propUri + "> ?result1 ."+nl+
				    filter+
				"}";		
		return sparqlQuery;
	}	
	

}
