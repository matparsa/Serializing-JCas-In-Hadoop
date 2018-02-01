package de.webis.UIMAhadoop.Arumentative;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

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
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceConfigurationException;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.jsoup.Jsoup;

import dal.ArgumentUnit;
import de.aitools.ie.uima.type.argumentation.ArgumentativeDiscourseUnit;
import de.webis.chatnoir2.mapfilegenerator.inputformats.ClueWeb12InputFormat;
import de.webis.chatnoir2.mapfilegenerator.warc.WarcRecord;
import de.webis.hadoop.jcas.Helper;
//import mining.argumentation.ArgumentUnitExtractor;
import mining.argumentation.ArgumentUnitExtractor;

public class CluewebArgumentativeSentences {

	static Logger logger = Logger.getLogger(CluewebArgumentativeSentences.class);

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.printf("Usage: Input Output ArchivedDescribtorsFilePath\n");
			System.exit(1);
		}
		BasicConfigurator.configure();

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
		logger.info("Entering application.");
		DistributedCache.addCacheArchive(new URI(archiveClusterPath), config);
		// share the name of archive file with all the nodes in mapper
		config.set("uimaResourceArchivedFileName", Helper.getFileName(archiveClusterPath));

		Job job = new Job(config, "Argumentative sentences extractor");
		job.setJarByClass(CluewebArgumentativeSentences.class);
		job.setMapperClass(Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setCombinerClass(Combine.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(ClueWeb12InputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		FileOutputFormat.setCompressOutput(job, true);
		FileInputFormat.setInputDirRecursive(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public final static String MirexId = "MIREX-TREC-ID: ";
	private final static int maxHtml = 50000; // not more than 50 KB used per web page

	/**
	 * -- Mapper: Extracts anchors.
	 */
	public static class Map extends Mapper<LongWritable, WarcRecord, Text, Text> {

		private final static Pattern scriptPat = Pattern.compile("<script(.*?)</script>",
				Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);
		AnalysisEngine ae = null;
		static Logger logger = Logger.getLogger(Map.class);

		/**
		 * @param key
		 *            any integer
		 * @param value
		 *            the web page
		 * @param output
		 *            (URL, anchor text <i>or</i> TREC-ID)
		 */
		public void map(LongWritable key, WarcRecord value, Context context) throws IOException, InterruptedException {
			final String recordId = value.getRecordId();
			if (!value.getRecordType().equals("response") && !value.getRecordType().equals("request")) {
				return;
			}
			if (ae == null) {
				BasicConfigurator.configure();
				Configuration jobConf = context.getConfiguration();
				
				String uimaResourceArchivedFileName = jobConf.get("uimaResourceArchivedFileName");
				Path[] files = DistributedCache.getLocalCacheArchives(context.getConfiguration());
				String basePath = "";
				for (Path file : files)
					if ((file.toString().contains(uimaResourceArchivedFileName)))
						basePath = file.toString();
				try {
					logger.info("loadin analyzer...");
					ResourceSpecifier specifier = UIMAFramework.getXMLParser()
							.parseResourceSpecifier(new XMLInputSource(
									basePath + "/desc/Resources/conf/mining/pipelines/ArgumentUnitExtractor.xml"));
					logger.info("Get Analyzer " + basePath
							+ "/desc/Resources/conf/mining/pipelines/ArgumentUnitExtractor.xml" + "");
					ae = UIMAFramework.produceAnalysisEngine(specifier);
					ae.reconfigure();
					logger.info("Analyzer configured.");
					
				} catch (ResourceInitializationException e) {
					logger.info("Erro: " + e.getLocalizedMessage());
					e.printStackTrace();
				} catch (InvalidXMLException e) {
					logger.info("Erro: " + e.getLocalizedMessage());
					e.printStackTrace();
				} catch (ResourceConfigurationException e) {
					logger.info("Erro: " + e.getLocalizedMessage());
					e.printStackTrace();
				}

			}
			// String baseUri, trecId, content;
			// we want to keep track of the TREC-IDs
			String content = value.getContent();
			if (content.length() > maxHtml)
				content = content.substring(0, maxHtml); // truncate websites
			content = scriptPat.matcher(content).replaceAll(" ");
			CAS cas;
			try {
				cas = ae.newCAS();
				cas.reset();
				// Get the content of html
				String textOfContent = Jsoup.parse(content).text();
				cas.setDocumentText(textOfContent);
				JCas jcas = cas.getJCas();
				ae.process(jcas);
				List<ArgumentativeDiscourseUnit> argumentUnitAnnotations = extractArgumentUnitAnnotations(jcas);
				for (ArgumentativeDiscourseUnit argumentUnitAnnotation : argumentUnitAnnotations) {
					String typeStr = argumentUnitAnnotation.getUnitType();
					context.write(new Text(recordId),
							new Text(typeStr + ":" + argumentUnitAnnotation.getCoveredText()));
				}
			} catch (ResourceInitializationException e) {
				logger.info("Error: " + e.getLocalizedMessage());
				System.exit(1);
			} 
			catch (NullPointerException e) {
				logger.info("Error: " + e.getCause().getMessage());
				e.printStackTrace();
				System.exit(1);
			}
			catch (Exception e) {
				logger.info("Error: " + e.getLocalizedMessage());
				e.printStackTrace();
				System.exit(1);
			}
		}
		public static List<ArgumentativeDiscourseUnit> extractArgumentUnitAnnotations(JCas newspieceJcas) {
			// TODO Auto-generated method stub
			List<ArgumentativeDiscourseUnit> argumentUnitAnnotations = new ArrayList<ArgumentativeDiscourseUnit>();
			FSIterator<Annotation> argumentUnitAnnotationIterator = newspieceJcas
					.getAnnotationIndex(ArgumentativeDiscourseUnit.type).iterator();

			while (argumentUnitAnnotationIterator.hasNext()) {
				ArgumentativeDiscourseUnit argumentUnitAnnotation = (ArgumentativeDiscourseUnit) argumentUnitAnnotationIterator
						.next();
				if (argumentUnitAnnotation.getUnitType().equals("Argumentative"))
					argumentUnitAnnotations.add(argumentUnitAnnotation);
			}
			return argumentUnitAnnotations;

		}
	}

	/**
	 * -- Combiner: Glues local anchor texts together.
	 */
	public static class Combine extends Reducer<Text, Text, Text, Text> {

		/**
		 * @param key
		 *            URL
		 * @param values
		 *            anchor text <i>or</i> TREC-ID
		 * @param output
		 *            (URL, anchor texts <i>or</i> TREC-ID)</i>
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {

			String argumantativeSentences = "";
			for (Text value : values) {
				argumantativeSentences += "\n" + value.toString();
			}
			context.write(key, new Text(argumantativeSentences.toString()));
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

			String argumantativeSentences = "";
			for (Text value : values) {
				argumantativeSentences += value.toString();
			}
			context.write(key, new Text(argumantativeSentences.toString()));
		}
	}
}