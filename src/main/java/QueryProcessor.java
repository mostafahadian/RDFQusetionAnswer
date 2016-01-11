import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;

import javax.net.ssl.HttpsURLConnection;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.repository.RepositoryConnection;



public class QueryProcessor {
	final String nl="\r\n";
	final String querybase="https://query.wikidata.org/bigdata/namespace/wdq/sparql?query=";
	final String SPARQL_DESCRIPTION_PROPERTY_URI= "<http://schema.org/description>";
	final int LOCAL_REPO=0,REMOTE_END_POINT=1;
	
	final RepositoryConnection localSparqlConnection;
	
	public QueryProcessor(RepositoryConnection cxn) {
		localSparqlConnection=null;
	}
	
	public void test(){
		String queries[]={"زهرا اشراقی" ,"ارتفاع دماوند", "محل تولد حسن روحانی", "محل تولد باراک اوباما", "کشور شهروندی کلینت ایستوود", "پست سیاسی امام خمینی", "سعید جلیلی عضو حزب چیست؟", "ایدئولوژی سیاسی جبهه پایداری انقلاب اسلامی", "برادر امام حسن", "امام حسین", "", ""};
		//"ارتفاع دماوند" doesn't work because the type of the returned result is not a label therefore it doesn't have any lang and it gets filtered!
		String noanswerqueries[] ={"محل تولد رییس جمهور"};
		String exceptionqueries[]={"موقعیت جغرافیایی جمهوری خلق چین", "امام حسن برادرش که بود؟"};
		processQuery(queries[0]);		
	}
	
	//redis
	//bloomfilter
	//taha.ghasemi@gmail.com
	
	void processQuery(String q){
		String prop=null;
		String entity=null;
		String terms[]=q.split(" ");

		
		//find the longest property possible
		//int propfromto[]=findEntityStartEndIndeces(terms, LOCAL_REPO, false);
		int propfromto[]=findEntityStartEndIndeces(terms, REMOTE_END_POINT, true);
		if (propfromto[1]-propfromto[0]<0){
			System.out.println("nothing matched as a property, so we'll return the description of the entity");
		}else{
			prop=concatenateStrings(terms, propfromto[0], propfromto[1]);
			System.out.println("assume "+prop+" is a property");
		}
		
		
		//now remove the property terms from the terms array
		String[] remainTerms =removeSubsequence(terms, propfromto[0], propfromto[1]);
		
		//now in the remained terms look for an entity
		int entityfromto[]=findEntityStartEndIndeces(remainTerms, REMOTE_END_POINT, false);
		if (entityfromto[1]-entityfromto[0]<0){
			System.out.println("nothing matched as an entity! so I can't process anything for you!");
			return;
		}else{
			entity=concatenateStrings(remainTerms, entityfromto[0], entityfromto[1]);
			System.out.println("assume "+entity+"is an entity");
		}
		
		
		//now look for the entity1 which is related to entity with the property prop
		System.out.println("result:--------------------------------");
		String query;
		String result;
		if (prop!=null){
			query=getEntityPropSparqlQuery(entity, prop);
			result=runQueryOnSparqlEndpoint(query);
		}else{
			query=getEntitySpecificPropSparqlQuery(entity, SPARQL_DESCRIPTION_PROPERTY_URI);
			result=runQueryOnSparqlEndpoint(query);
		}
		
		//now parse the JSON in order to extract the result label
		try{
			JSONObject json=new JSONObject(result);
			JSONArray jsonArray=json.getJSONObject("results").getJSONArray("bindings");
			for(int i=0;i<jsonArray.length();i++){
				JSONObject resultlabelJson=jsonArray.getJSONObject(i).getJSONObject("result1");
				String label=resultlabelJson.getString("value");
				System.out.println("result "+i+" :"+label);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		System.out.println(result);
		
	}
	
	String[] removeSubsequence(String[] terms, int from,int to){
		int i;
		int removeLen=(to-from+1);
		System.out.println(removeLen+",");
		int remainLen=terms.length-removeLen;
		System.out.println(remainLen+",");
		String[] remainTerms=new String[remainLen];		
		for(i=0;i<from;i++){
			remainTerms[i]=terms[i];
		}
		for(i=to+1;i<terms.length;i++){
			remainTerms[i-removeLen]=terms[i];
		}		
		return remainTerms;
	}
	
	int[] findEntityStartEndIndeces(String terms[], int hostType, boolean isProp){
		int maxEntityTerms=3;
		int n=terms.length;
		int mj=-1, mi=0, maxDeltaji=-1;
		for(int i=0; i<n; i++){
			for(int j=i; j<Math.min(i+maxEntityTerms, n); j++){
				String candidateEntityLabel=concatenateStrings(terms, i, j);
				int count=getNumberOfNodesNamedFromSparql(candidateEntityLabel, hostType, isProp);
				System.out.println(i+","+j+"------"+candidateEntityLabel+":"+count);
				if(count>0){
					if(j-i>maxDeltaji){
						maxDeltaji=j-i;
						mi=i;
						mj=j;
					}
				}
			}
		}
		return new int[]{mi,mj};
	}
	
	String concatenateStrings(String strs[], int from, int to){
		String s=strs[from];
		for(int i=from+1; i<=to; i++){
			s=s+" "+strs[i];
		}
		return s;
	}
	
	
	int getNumberOfNodesNamedFromSparql(String nodeLabel, int hostType, boolean isProp){
		int count=-1;
		if(hostType==LOCAL_REPO){
			count=getNumberOfEntitiesNamedFromLocalSparqlRepo(nodeLabel);
		}else if(hostType==REMOTE_END_POINT){
			//count=getNumberOfEntitiesNamedFromSparqlEndPoint(entity);
			count=getNumberOfNodesNamedFromSparqlEndPoint(nodeLabel, isProp);
		}
		return count;
	}
	
	int getNumberOfEntitiesNamedFromLocalSparqlRepo(String entity){
		String query=getEntityCountSparqlQuery(entity);
		TupleQueryResult result = runQueryOnLocalSparqlRepo(query);
		try {
			if (result.hasNext()) {
				BindingSet bindingSet = result.next();
				Value b = bindingSet.getValue("cnt");
				int count=Integer.parseInt(b.toString().split("\"")[1]);
				return count;
			}
		} catch (QueryEvaluationException e) {
			e.printStackTrace();
		}	
		return -1;
	}
	
	
	int getNumberOfNodesNamedFromSparqlEndPoint(String nodeLabel, boolean isProp){
		String query;
		if(isProp){
			query=getPropertyCountSparqlQuery(nodeLabel);
		}else{
			query=getEntityCountSparqlQuery(nodeLabel);
		}
		String result=runQueryOnSparqlEndpoint(query);
		try {
			JSONObject json=new JSONObject(result);
			json=json.getJSONObject("results").getJSONArray("bindings").getJSONObject(0).getJSONObject("cnt");
			int count=json.getInt("value");
			return count;
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	
	
	
	TupleQueryResult runQueryOnLocalSparqlRepo(String query){
		TupleQueryResult result = null;
		try {
			TupleQuery tupleQuery = localSparqlConnection.prepareTupleQuery(QueryLanguage.SPARQL, query);				
			result = tupleQuery.evaluate();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	
	
	
	String getEntityCountSparqlQuery(String entity){
		String sparqlQuery=
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+nl+
					"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+nl+
					"SELECT (count(?predicate1) as ?cnt) WHERE {"+nl+
					    "{{ ?predicate1 rdfs:label \""+entity+"\"@fa . } UNION { ?predicate1 skos:altLabel \""+entity+"\"@fa . }}"+nl+
					"} limit 10";
		return sparqlQuery;
	}
	
	String getPropertyCountSparqlQuery(String prop){
		String sparqlQuery=
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+nl+
					"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+nl+
					"PREFIX wikibase: <http://wikiba.se/ontology#>"+nl+
					"SELECT (count(?predicate1) as ?cnt) WHERE {"+nl+
					    "{{ ?predicate1 rdfs:label \""+prop+"\"@fa . } UNION { ?predicate1 skos:altLabel \""+prop+"\"@fa . }}"+nl+
					    "?predicate1 a wikibase:Property ." + nl+
					    "?predicate1 wikibase:directClaim ?directPredicate2 ." + nl+					    
					"} limit 10";
		return sparqlQuery;
	}	
	String getEntityPropSparqlQueryOld(String entity, String prop){
		String sparqlQuery="PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+nl+
				"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+nl+
				"PREFIX wikibase: <http://wikiba.se/ontology#>"+nl+
				"PREFIX hint: <http://www.bigdata.com/queryHints#>"+nl+
				"SELECT ?subject0 ?result WHERE {"+
					"hint:Query hint:optimizer \"None\" ."+nl +
				    "{{ ?subject0 rdfs:label \""+entity+"\"@fa . } UNION { ?subject0 skos:altLabel \""+entity+"\"@fa . }}"+ nl+
				    "{{ ?predicate1 rdfs:label \""+prop+"\"@fa . }  UNION { ?predicate1 skos:altLabel \""+prop+"\"@fa . }}"+ nl+
				    "?predicate1 a wikibase:Property ." +nl+
				    "?predicate1 wikibase:directClaim ?directPredicate2 ."+nl+
				    "?subject0 ?directPredicate2 ?result ."+nl+
				"}";		
		return sparqlQuery;
	}
	
	
	String getEntityPropSparqlQuery(String entity, String prop){
		String sparqlQuery="PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+nl+
				"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+nl+
				"PREFIX wikibase: <http://wikiba.se/ontology#>"+nl+
				"PREFIX hint: <http://www.bigdata.com/queryHints#>"+nl+
				"PREFIX wdt: <http://www.wikidata.org/prop/direct/>"+nl+
				"SELECT ?subject0 ?result ?result1 ?predicate0 WHERE {"+
					"hint:Query hint:optimizer \"None\" ."+nl +
				    "{{ ?subject0 rdfs:label \""+entity+"\"@fa . } UNION { ?subject0 skos:altLabel \""+entity+"\"@fa . }}"+ nl+
				    "{{ ?predicate0 rdfs:label \""+prop+"\"@fa . }  UNION { ?predicate0 skos:altLabel \""+prop+"\"@fa . }}"+ nl+
				    "{{?predicate0 wdt:P1687 ?predicate1 .} UNION { LET (?predicate1 := ?predicate0). }}"+ nl+
				    "?predicate1 a wikibase:Property ." +nl+
				    "?predicate1 wikibase:directClaim ?directPredicate2 ."+nl+
				    "?subject0 ?directPredicate2 ?result ."+nl+
				    "?result rdfs:label ?result1 ."+nl+
				    "FILTER (LANG(?result1)=\"en\" || LANG(?result1)=\"fa\")" +
				"}";		
		return sparqlQuery;
	}
	
	
	String  getEntitySpecificPropSparqlQuery(String entity, String rdfPropUri){
		String sparqlQuery="PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>"+nl+
				"PREFIX skos: <http://www.w3.org/2004/02/skos/core#>"+nl+
				"PREFIX hint: <http://www.bigdata.com/queryHints#>"+nl+
				"SELECT ?subject0 ?result1 WHERE {"+
					"hint:Query hint:optimizer \"None\" ."+nl +
				    "{{ ?subject0 rdfs:label \""+entity+"\"@fa . } UNION { ?subject0 skos:altLabel \""+entity+"\"@fa . }}"+ nl+
				    "?subject0 " + rdfPropUri + " ?result1 ."+nl+
				"}";		
		return sparqlQuery;
	}
	
	String runQueryOnSparqlEndpoint(String sparqlQuery){
		String jsonStr="";
		try {
			String s=URLEncoder.encode(sparqlQuery,"UTF-8");
			String q=querybase+s+"&format=json";
			
			URL url=new URL(q);
			HttpsURLConnection connection=(HttpsURLConnection) url.openConnection(); 
			connection.setRequestProperty("Accept-Charset", "UTF-8");
			InputStream in = connection.getInputStream();
			BufferedReader br=new BufferedReader(new InputStreamReader(in));
			String l="";
			do{
				jsonStr+=l;
				l=br.readLine();
			}while(l!=null);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonStr;
	}
}
