package de.webis.hadoop.jcas;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceConfigurationException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;

public class TemplateClass {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.printf("Usage: Input Output ArchivedDescribtorsFilePath\n");
			System.exit(1);
		}
		String input = args[0];
		String output = args[1];
		String archivedDescribtorsFilePath = args[2];
		Configuration config = new Configuration();
		// Copy file to Hadoop cache archive resources, sharing it for all nodes
		String archiveClusterPath = archivedDescribtorsFilePath;
		if (new File(archivedDescribtorsFilePath).exists()) {
			System.out.println("Copying files on the cluster...");
			archiveClusterPath = Helper.copyToHDFS(archivedDescribtorsFilePath, config);
		} else if (!FileSystem.get(config).exists(new Path(archivedDescribtorsFilePath))) {
			System.out.println(archivedDescribtorsFilePath + " not exist in local and cluster.");
			System.exit(1);
		}

		DistributedCache.addCacheArchive(new URI(archiveClusterPath), config);
		// share the name of archive file with all the nodes in mapper
		config.set("uimaResourceArchivedFileName", Helper.getFileName(archiveClusterPath));

		Job job = new Job(config, "Argumentative sentences extractor");
		//TODO: Change the class name here
		job.setJarByClass(TemplateClass.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		//TODO: Change the output values if you need
		//Default for input is textfile reading it line by line and doesn't need to define
		//job.setInputFormatClass(YourType.class);
		//Default for output is text 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(input)); 
		FileOutputFormat.setOutputPath(job, new Path(output));
		FileOutputFormat.setCompressOutput(job, true);
		FileInputFormat.setInputDirRecursive(job, true);
		//TODO: compress output, comment this line if you don't need 
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	

	/**
	 * -- Mapper: 
	 */
	//TODO: change the input key and value and output if you changed the default value 
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		AnalysisEngine ae = null;
		/**
		 * @param key
		 *            any integer
		 * @param value
		 *            the web page
		 * @param output
		 *            (text, text)
		 */
		//TODO: change the input key and value and output if you changed the default value 
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			if (ae == null) {
				Configuration jobConf = context.getConfiguration();
				String uimaResourceArchivedFileName = jobConf.get("uimaResourceArchivedFileName");
				Path[] files = DistributedCache.getLocalCacheArchives(context.getConfiguration());				
				String basePath = "";
				for (Path file : files)
					if ((file.toString().contains(uimaResourceArchivedFileName)))
						basePath = file.toString();
				try {
					//TODO set the path to your analyzing path. Consider the starting point as what it is in compressed file
					String resourcePath="/desc/Resources/conf/mining/pipelines/ArgumentUnitExtractor.xml";
					ResourceSpecifier specifier = 
						UIMAFramework.getXMLParser().parseResourceSpecifier(
								new XMLInputSource(basePath+ resourcePath));
					ae = UIMAFramework.produceAnalysisEngine(specifier);
					ae.reconfigure();
				} catch (ResourceInitializationException e) {
					e.printStackTrace();
				} catch (InvalidXMLException e) {
					e.printStackTrace();
				} catch (ResourceConfigurationException e) {
					e.printStackTrace();
				}
							

			}
			CAS cas;
			try {
				cas = ae.newCAS();
				cas.reset();
				cas.setDocumentText(value.toString());
				JCas jcas = cas.getJCas();
				//TODO here you have the jCas and you need to get your value from it and then write the value in context 
				context.write(new Text("Your key"), new Text("Your value"));
				
			} catch (ResourceInitializationException | CASException e) {
				e.printStackTrace();
			}

		}
	}

	/**
	 * -- Combiner: Glues local anchor texts together.
	 */
	public static class Combine extends Reducer<Text, Text, Text, Text> {

		
		public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

			String result = "";
			for (Text value : values) {
				//TODO change this part as you need
				result += "\n" + value.toString();
			}
			context.write(key, new Text(result.toString()));
		}
	}

	/**
	 * -- Reducer: Glues anchor texts together, and recovers TREC-ID.
	 */
	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		/**
		 * @param key
		 *            URL
		 * @param values
		 *            anchor text <i>or</i> TREC-ID
		 * @param output
		 *            (TREC-ID, URL, anchor texts)</i>
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

			String result = "";
			for (Text value : values) {
				result += value.toString();
			}
			context.write(key, new Text(result.toString()));
		}
	}
}