import java.awt.image.BufferedImageFilter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.jetty.util.log.Log;
import org.openrdf.model.vocabulary.RDF;

public class RDFTripleStoreFarsiExtractor {
	final static String FA="@fa", LINE_FEED="\n";
	Set<String> set=new HashSet<String>();
	
	void filterAllFiles(){
		String files[]={WikidataMain.wikidataproperties,
						WikidataMain.wikidatainstances, 
						WikidataMain.wikidatasimplestatements, 
						WikidataMain.wikidatasitelinks, 
						WikidataMain.wikidataterms, 
						WikidataMain.wikidatastatements};
		
		int distinctFarsiEntityCount=insertEntitiesWithFarsiLabelIntoSet(files);
		System.out.println("distinct farsi entities count: "+distinctFarsiEntityCount);
		
		int totalTriplesRemained=filterTriplesWithoutSelectedEntities(files);
		System.out.println("total triples remained across all files: "+totalTriplesRemained);
	}
	
	int filterTriplesWithoutSelectedEntities(String filenames[]){
		int total=0;
		for(int i=0;i<filenames.length; i++){
			int n=filterTriplesWithoutSelectedEntities(filenames[i]);
			System.out.println(filenames[i]+" remained triples count after filtering: "+n);
			total+=n;
		}
		return total;
	}
	
	int filterTriplesWithoutSelectedEntities(String filename){
		BufferedReader br=null;
		BufferedWriter bw=null;
		int n=0;
		try {
			br=new BufferedReader(new FileReader(filename));
			bw=new BufferedWriter(new FileWriter(filename+".filtered1"));
			
			String line;
			int i=0;
			line=br.readLine();
			String triple[];
			while(line!=null){
				i++;
				if(i%1000000==0){
					System.out.println("i:"+i);
				}
				
				//extract triple
				triple=line.split("> ");  //we expect triple to be an array with 3 elements
				if(triple.length<3){
					System.err.println("in file "+filename+" at index "+i+", we have triple problem!"+" triples count "+triple.length);
					System.err.println("line: "+line);
				}else{
					triple[0]+=">";
					triple[1]+=">";
					triple[2]+=">"; //in case we have a literal in the third place, it will make it wrong, but there's no problem because it wouldn't be matched!		
					
					//check if any of the triples is in our white list set.
					//if(set.contains(triple[0]) || set.contains(triple[1]) || set.contains(triple[2])){
					if(set.contains(triple[0]) || set.contains(triple[1])){
						bw.write(line);
						bw.write(LINE_FEED);
						n++;
						if(n%10000==0){
							System.out.println("write triple index: "+i+" from source file to dest file at index"+n);
						}
					}
				}
				line=br.readLine();
			} 
			System.out.println(n);
			br.close();
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		return n;
	}
	
	int insertEntitiesWithFarsiLabelIntoSet(String filenames[]){
		for(int i=0; i<filenames.length; i++){
			int n=insertEntitiesWithFarsiLabelIntoSet(filenames[i]);
			System.out.println(filenames[i]+" URI white list size: "+n);
		}
		return set.size();
	}
	
	int insertEntitiesWithFarsiLabelIntoSet(String filename){
		BufferedReader br=null;
		int n=0;
		try {
			br=new BufferedReader(new FileReader(filename));
			String line, lang, uri;
			int idx, i=0;
			line=br.readLine();
			while(line!=null){
				i++;
				if(i%1000000==0){
					System.out.println("i:"+i);
				}
				lang=line.substring(line.length()-5, line.length()-2);  //e.g. extract '@fa'
				if(lang.equals(FA)){
					idx=line.indexOf("> <");
					if (idx!=-1){
						uri=line.substring(0, idx+1);
						set.add(uri);
						n++;
						if(n%10000==0){
							System.out.println("@fa:"+n);
						}
						//System.out.println(uri);
					}else{
						System.err.println("in file "+filename+" at index"+i+", we can't find '> <' at all");
					}
				}
				line=br.readLine();
			} 
			System.out.println(n);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return n;
	
	}

}
