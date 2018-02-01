# Motivation
  - Running a UIMA pipeline in Hadoop is not easy because of descriptor files
  - UIMA reads a descriptor file first and if there is another descriptor file addressed in it, it will load it 
  - Here we face to a problem, adrresses in descriptor files are absoloute and if we embed the files in jar, first file works well but the other addresses can not be reached by UIMA
  - Hadoop has a cache which is accessible for all the cluster. So the best solution is to add resources into hadoop cache and then reading them in mapper.

-----------------------------------------------------------------------------------------------
# Serializer and Deserializer JCas objects
  - To serialize an object in Hadoop there are two common ways 
    - Use Writeable interface from Hadoop.
        - Here the problem is that it is only supported by Java and if someone wants to de-serialize them with another language he can not. 
    - The other solution is using Avro which is supported by the other programming languages. 
        -  But to serialize a JCast it's not an easy task. For each object we need to make a schema like a mapper and for a JCas object it is almost impossible because of its structure.
- Thankfully UIMA provides a serializer class for JCas which converts a JCas to binary as output. 

I use bytesWritable as output in hadoop mapper and writing the serialized JCas object as binary with hadoop which you can find the code in uima.hadoop.HadoopJCasWriter. Then in HadoopJCasReader I read the bytes and desrialized them to a JCas object. 
The advantage is that because I don't use Hadoop serialization we can deserialize them with other pipelines.

--------------------------------------------------------------------------------------------------

# How to run

- if you don't need to use serializer you need to make your cstom class:
    - Make a copy of class "de.webis.hadoop.jcas.TemplateClass"
    - Complete all TODO comments
    - External the project as a runnable jar
    - Open the jar with a file compressor manager 
    - Add resources folder of the main project to it
    - Done

- But if you want to use serializer then :
	- First of all you need to add to public method somewhere in your project. First one should return an AnalysisEngine and it should have an input as String for the basePath of desriptor which this app will generate that address and will pass it to this method. the method should looks like this:
    
    ```
    	public AnalysisEngine getAnalysisEngine(String basePath) throws IOException, InvalidXMLException, ResourceInitializationException {
    		XMLInputSource in = new XMLInputSource(basePath+"/desc/Resources/conf/mining/pipelines/ArgumentUnitExtractor.xml");
    		ResourceSpecifier aSpecifier = UIMAFramework.getXMLParser().parseResourceSpecifier(in);
    		AnalysisEngine ae = UIMAFramework.produceAnalysisEngine(aSpecifier);
    		return ae;
    	}
    ```
    - The second method should get a JCas as input and return a string as result. this method will use in deserializer:
    
    ```
    	public static String getString(JCas jCas)
    		{
    			String result="";
    			mining.argumentation.ArgumentUnitExtractor argUnitExtractor=new ArgumentUnitExtractor();
    			List<ArgumentativeDiscourseUnit> argumentUnitAnnotations=argUnitExtractor.extractArgumentUnitAnnotations(jCas);
    			List<ArgumentUnit> argumentUnits = argUnitExtractor.getArgumentUnitsFromAnnotations(argumentUnitAnnotations);
    			for (ArgumentUnit au : argumentUnits) {
    				result+=(String.format("%s %s", au.type,jCas.getDocumentText()));
    			}
    			return result;
    		}
    ```
    - Then if your pipeline doesn't use any model or any external project you can simply make a runnable jar of your project which all the libraries are embeded into it and then run hadoop script/uima-hadoop.jar uima_hadoop_serializer.config
        -  call script/makeConfig.sh and it will help you to make a configuration file and archive he resources will you need later

		- otherwise add your UIMA pipeline to the build path of this project and make a runnable jar whith all embeded libraries into it and then run hadoop script/uima-hadoop.jar uima_hadoop_serializer.config .

