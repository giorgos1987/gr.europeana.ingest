package gr.europeana.ingest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
//import org.glassfish.jersey.client.ClientConfig;




import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.stream.JsonWriter;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;




public class Ingest {

	  public static void main(String[] args) throws IOException {

//			String xml=null;
//			String str=null;
//			int start=0;
//			int end=0;
			if (args[0] == null ||  args[2]==null )
	        {
	            System.err.println( "Usage : " ) ;
	            System.err.println( "java -jar <java_name.jar> <output path>  <start number> <search string> <type> <optional rows>" ) ;
	            System.err.println( "Note that default fetch 100 rows/records  , type =text and start=0 per run .For default values use 0." ) ;
	            System.exit( -1 ) ;
	        }

		//	start = Integer.parseInt(args[0]);
			 System.out.println("args[0]:" + args[0] + "args[1]:" + args[1] + "args[2]:" + args[2]+"args[3]:" + args[3]+"args[4]:" + args[4]); 
			    String wskey= "cozftPtE3";
			  //  String qf = null;//args[3];//"TYPE%3ATEXT";
			    String query= args[2].toString();// "farm+cooperative";
				String rows= "500";
				String qt="false";
				String type = null;
				String out=null;
				String [] links = null;
				String start = "";
				String[] parts = null;// string.split("/");
				
				if( args[1].equals( "0" ) |  args[1].equals( 0 ) ){
					start = args[1].toString();
				}else {
					start = "0";
				}
				//start = "0";
				if( args[3].equals( "0" ) |  args[3].equals( 0 ) ){
					type = args[3].toString();
				}else{
					type = "TYPE%3ATEXT";
				}
				//type = "TYPE%3ATEXT";
				if(args[4].equals( "0" ) |  args[4].equals( 0 ) ){
					rows = args[4].toString();
				}else{
					rows = "100";
				}
				//rows = "100";
				links = getBaseURI( rows,  start, query,  wskey, qt, type);
		


			for(int j = 0; j < links.length; j ++ ) {
				Client client = Client.create();
				WebResource webResource = client
				   .resource(links[j]);
				//   .queryParam("wskey", wskey);
				//   .queryParam("query", query)
				//   .queryParam("qt", qt)
				//    .queryParam("rows", rows)
			//	   .queryParam("qf", qf);
				
				ClientResponse response = webResource
						.type("application/json")
						.accept("*/*")
						.get(ClientResponse.class);
				          
			  	      //  .post(ClientResponse.class);
			  
				 out = response.getEntity(String.class);
			     


			 // try-with-resources statement based on post comment below :)
				FileWriter file = null;
				try {
					parts = links[j].split("/");
					System.out.println(  links[j]);
					file = new FileWriter(args[0] +  parts[6]+".json");//C:\\Users\\papou_000\\Desktop\\agroknow\\Europeana\\jsons\\"
				//	  System.out.println(out);
					file.write(out);
					file.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				response.getClient().destroy();	
			}//for //	file.write(response);
		}
		  	  
	
//	  }for

	//  @SuppressWarnings("null")
	private static String[] getBaseURI(String rows, String start,String query, String wskey,String qt,String type) {
		  String  str = null;
		  //int strt = Integer.parseInt(start);
		  int rws = Integer.parseInt(rows);
		  String[] links = new String[rws];// {""};
		  String link = null;
		  Object context = new Object();
		  JSONObject jsonObject = new JSONObject();
		  Object obj = new Object();
		  JSONObject	 cntx = new JSONObject();
		  JSONParser parser = new JSONParser();	 	
				Client client = Client.create();
				WebResource webResource = client
						   .resource("http://www.europeana.eu/api/v2/search.json")
						   .queryParam("wskey", wskey)
						   .queryParam("query", query)
						   .queryParam("qt", qt)
			//			   .queryParam("start", start)
						   .queryParam("rows", rows)
						   .queryParam("qf", type);

						ClientResponse response = webResource
								//.type("application/json")
								//.accept("application/json")
					  	        .post(ClientResponse.class);
					  
						// out = response.getEntity(String.class);
						
					    	try {
					    						    	
							   	str = response.getEntity(String.class);
						    	 obj =  parser.parse(str) ;
						    	 jsonObject = (JSONObject) obj;		
						    	 JSONArray graph = (JSONArray) jsonObject.get("items");//jsonObject
									//////////System.out.print("@@@@graph ");
									Iterator<String> iterator = graph.iterator();
									
									int i = 0;
									while (iterator.hasNext()) {			
										 context = iterator.next();					 
										 cntx = (JSONObject) context;
										 if(cntx.get("link")!=null){
								           link = (String) cntx.get("link");
								           System.out.println("link:" + link); 
								           links[i] = link;
								           i++;
										 }
										 
									}
						    	
						    	
							} catch (ClientHandlerException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (UniformInterfaceException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (ParseException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} 
					    	
					    	
					     response.getClient().destroy();
				
				
				return links;
				
				
					    
	  }
	  

	  
	  


	  
}
