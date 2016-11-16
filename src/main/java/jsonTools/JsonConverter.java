package jsonTools;

import java.io.*;

import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;

import mapBean.BulkProcessorDataBean;
import mapBean.IndexBean;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonConverter {
	public static void main(String[] args) {
		String inputPath = "C:\\Users\\suse\\Desktop\\ARH_2016-8-17_4139_0.json";
		String outputPath = "C:\\Users\\suse\\Desktop\\to_ARH_2016-8-17_4139_0.json";
		try {
			//converter(inputPath, outputPath,"test","12306",0L);
			BulkProcessorDataBean dataBean = readJsonFile(outputPath);
			TransportClient client = ElasticTools.getClient(ElasticTools.HOST,ElasticTools.PORT);
			BulkProcessor bulkProcessor =ElasticTools.getBulkProcessor(client);
			for (int i = 0; i < dataBean.getIndexProps().size(); i++) {
				bulkProcessor.add(new IndexRequest(
								dataBean.getIndexProps().get(i).get_index(), 
								dataBean.getIndexProps().get(i).get_type(),
								String.valueOf(dataBean.getIndexProps().get(i).get_id()))
				.source(dataBean.getDataSourses().get(i)));
			}
			//bulkProcessor.flush();
			bulkProcessor.close();
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 将不符合要求的文件转换成es能接受的json格式的文件
	 * @param inputPath
	 * @param outputPath
	 * @param index
	 * @param type
	 * @param id
	 * @throws Exception
	 */
	public static void converter(String inputPath,String outputPath,String index,String type, long id)throws Exception{
		File file = new File(inputPath);
		if(file.exists()&&file.isFile()){
			Reader reader = null; 
			FileWriter writer = null;
			StringBuilder stringBuilder = null;
			try {  
	            // 一次读一个字符  
	            reader = new InputStreamReader(new FileInputStream(file)); 
	            writer = new FileWriter(outputPath);
            	BufferedWriter bw = new BufferedWriter(writer);
	            stringBuilder = new StringBuilder();
	            int tempchar;
	            String generatedIndex = generateIndex(index, type, id);
	            stringBuilder.append(generatedIndex).append('\n');
	            
	            while ((tempchar = reader.read()) != -1) {  
	            	char c = (char) tempchar;
	            	stringBuilder.append(c);
	            	//当c == '}'时，说明一条json格式的doc读取完毕，在'}'后面写入换行符'\n'
	                if (c == '}') {  
	                	stringBuilder.append('\n');
	                	//过滤掉','
	                	if((char)(tempchar = reader.read()) == ','){
	                		stringBuilder.append(generateIndex(index, type, ++id)).append('\n');
	                		continue;
	                		
	                	}
	                	String s = stringBuilder.toString();
	                	//System.out.println(s);
	                	bw.write(s);//将读取出的json格式的doc写入目标文件
	                	stringBuilder.delete(0, stringBuilder.length());//清空stringBuilder
	                }  
	            } 
	            //关闭输入输出流
	            reader.close();
	            bw.close();
	            writer.close();
	        } catch (Exception e) {  
	            e.printStackTrace();  
	        } 
		}else {
			throw new FileNotFoundException("找不到文件："+inputPath);
		}
	}
	
	/**
	 * 生成索引行
	 * @param index
	 * @param type
	 * @param id
	 * @return
	 */
	public static String generateIndex(String index,String type, long id) {
		return "{\"index\":{\"_index\":\""+index+"\",\"_type\":\""+type+"\",\"_id\":"+id+"}}";
	}
   
	/**
	 * 读取json文件，生成写入es中的数据
	 * @param filePath
	 * @return
	 * @throws Exception
	 */
	public static BulkProcessorDataBean readJsonFile(String filePath) throws Exception{
		File file = new File(filePath);
		if(file.exists()&&file.isFile()){
			 FileReader reader =null;
		     BufferedReader bufferedReader =null;
		     ObjectMapper mapper = new ObjectMapper();
		     BulkProcessorDataBean dataBean = new BulkProcessorDataBean();
		     try{
		    	 reader = new FileReader(file);
		    	 bufferedReader = new BufferedReader(reader);
		    	 String line = null;
		    	 while((line=bufferedReader.readLine())!=null){
		    		 //Map map = mapper.readValue(line, Map.class);
		    		 try{
		    			 IndexBean index = mapper.readValue(line, IndexBean.class);//读取index属性这一行
		    			 dataBean.getIndexProps().add(index.getIndex());//将index属性对象添加到列表里
		    		 }catch(Exception e){
		    			 e.printStackTrace();
		    			 System.out.println("无法将以下内容转换成IndexBean对象,程序将退出：\n"+line);
		    			 break;
		    		 }
		    		
		    		 if((line=bufferedReader.readLine())!=null){//读取数据这一行
		    			 String dataSource = line;
		    			 dataBean.getDataSourses().add(dataSource);
		    		 }else {
		    			 throw new Exception("读取到空行，请检查文件 '"+file.getName()+"' 是否包含空行");
		    		 }
		    	 }
		    	 		 
		     }catch(Exception e){
		    	 e.printStackTrace();
		     }finally{
		    	 try{
		    		 bufferedReader.close();
		             reader.close();
		    	 }catch(Exception e){
		    		 e.printStackTrace();
		    	 }
		     }
		     return dataBean;//返回BulkProcessorDataBean
		}else{
			throw new FileNotFoundException("找不到文件："+filePath);
		}
		
	}
	
   
}
