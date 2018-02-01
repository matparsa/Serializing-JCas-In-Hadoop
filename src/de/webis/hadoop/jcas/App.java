package de.webis.hadoop.jcas;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashMap;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.BasicConfigurator;
import org.apache.uima.analysis_engine.AnalysisEngine;


public class App {

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.printf("Usage: ConfigFilePath\n");
			System.out.println("To make the config file use the helper script in App/makeConfig.sh ");
			System.exit(1);
		}
		System.out.println("Parsing config file....");
		CustomConfig customConfig = parseConfigs(args[0]);
		System.out.println("Seems everything is fine.");
		if (!new File(customConfig.archivedDescribtorsFilePath).exists()) {
			System.out.println(customConfig.archivedDescribtorsFilePath + " not exist");
			System.exit(1);
		}
		System.out.println("Copying files on the cluster...");
		Configuration config = new Configuration();
		// Add jar and descriptor to Hadoop cache archive resources
		DistributedCache.addFileToClassPath(new Path(copyToHDFS(customConfig.jarFilePath, config).getPath()), config);
		URI archiveClusterPath = copyToHDFS(customConfig.archivedDescribtorsFilePath, config);
		DistributedCache.addCacheArchive(archiveClusterPath, config);
		// share the name of archive file with all the nodes in mapper
		config.set("uimaResourceArchivedFileName", customConfig.archivedDescribtorsFileName);
		config.set("targetJarFileName", customConfig.jarFileName);
		config.set("piplineClassName", customConfig.className);
		config.set("piplineGetAnalyserEngineMethodName", customConfig.analysisEngineMethod);
		config.set("piplineProccessJCassMethodName", customConfig.processMethod);
		System.out.println("Testing the jar file with the given method names...");
		testJarFile(customConfig, config);

		Path input = new Path(customConfig.input);
		Path output = new Path(customConfig.output);
		Job job = getJob(config, !customConfig.deserializing ? "JCas Serializer" : "JCas Deserializer",
				customConfig.deserializing);
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	private static void testJarFile(CustomConfig customConfig, Configuration config) {
		try {
			System.out.println("Loading " + customConfig.className + " ...");
			UIMAClassLoader uimaClassLoader = new UIMAClassLoader(new File(customConfig.jarFilePath).toURL().toURI(),
					customConfig.className);
			System.out.println(
					"Test method for getting AnalysisEngine by calling " + customConfig.analysisEngineMethod + " ");
			AnalysisEngine ae = uimaClassLoader.getAnalysisEngine(customConfig.analysisEngineMethod,
					customConfig.describtorsFilePath);
			System.out.println("Get AnalysisEngine succcesfully.");
			System.out.println(
					"Test method for getting String from a Jcas by calling " + customConfig.processMethod + " ...");
			System.out.println("Congrats! your jar works very well.");
		} catch (InvocationTargetException ex) {
			System.out.println("Oooops! There is something wrong in your jar by following error.");
			System.out.println("Please read readme for more information to how to create a jar for using in hadoop.");
			ex.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (SecurityException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (InstantiationException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (URISyntaxException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private static URI copyToHDFS(String path, Configuration conf) throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(conf);
		String fileName = new File(path).getName();
		OutputStream os = fs.create(new Path(fs.getWorkingDirectory() + "/" + fileName));
		InputStream is = new BufferedInputStream(new FileInputStream(path));// Data set is getting copied into input
		IOUtils.copyBytes(is, os, conf);
		return new URI(fs.getWorkingDirectory() + "/" + fileName);
	}

	private static CustomConfig parseConfigs(String path) throws Exception {
		// Read the config file
		BufferedReader brConfig = new BufferedReader(new FileReader(path));
		String configLine = brConfig.readLine();
		String[] configsArr = configLine.split(" ");
		CustomConfig customConfig = new CustomConfig();
		for (String configStr : configsArr) {
			String[] tmpKeyVal = configStr.split(":");
			if (tmpKeyVal.length != 2)
				throw new Exception("Config file format is not correct!\n" + configStr);
			switch (tmpKeyVal[0].toLowerCase()) {
			// :$input_path
			case "jarpath":
				customConfig.jarFilePath = tmpKeyVal[1];
				int idx = customConfig.jarFilePath.replaceAll("\\\\", "/").lastIndexOf("/");
				customConfig.jarFileName = idx >= 0 ? customConfig.jarFilePath.substring(idx + 1)
						: customConfig.jarFilePath;

				break;
			case "classname":
				customConfig.className = tmpKeyVal[1];
				break;
			case "analysisenginemethod":
				customConfig.analysisEngineMethod = tmpKeyVal[1];
				break;
			case "proccesmethod":
				customConfig.processMethod = tmpKeyVal[1];
				if (tmpKeyVal[1].equals("non"))
					customConfig.deserializing = false;
				else
					customConfig.deserializing = true;
				break;
			case "archiveddescribtorsfilepath":
				customConfig.archivedDescribtorsFilePath = tmpKeyVal[1];
				int idxArch = customConfig.archivedDescribtorsFilePath.replaceAll("\\\\", "/").lastIndexOf("/");
				customConfig.archivedDescribtorsFileName = idxArch >= 0
						? customConfig.archivedDescribtorsFilePath.substring(idxArch + 1)
						: customConfig.archivedDescribtorsFilePath;
				break;
			case "input_path":
				customConfig.input = tmpKeyVal[1];
				break;
			case "output_path":
				customConfig.output = tmpKeyVal[1];
				break;
			case "describtorsfilepath":
				customConfig.describtorsFilePath = tmpKeyVal[1];
				break;
			}

		}
		return customConfig;
	}

	private static Job getJob(Configuration config, String jobName, Boolean deserializing) throws IOException {
		Job job = new Job(config, jobName);
		job.setJarByClass(App.class);
		if (!deserializing) {
			job.setMapperClass(BinaryJCasWriterMapper.class);
			job.setOutputValueClass(BytesWritable.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		} else {
			job.setInputFormatClass(SequenceFileInputFormat.class);
			job.setMapperClass(BinaryJCasReaderMapper.class);
			job.setOutputValueClass(Text.class);
		}
		// this job doesn't need a reducer
		job.setNumReduceTasks(0);
		return job;
	}

	private static class CustomConfig {
		public CustomConfig() {
			// TODO Auto-generated constructor stub
		}

		Boolean deserializing = false;
		String jarFilePath = "";
		String jarFileName = "";
		String archivedDescribtorsFileName = "";
		String archivedDescribtorsFilePath = "";
		String className = "";
		String analysisEngineMethod = "";
		String hdfsPath = "";
		// For deserializing
		String processMethod = "";
		String input = "";
		String output = "";
		String describtorsFilePath = "";
	}

}
