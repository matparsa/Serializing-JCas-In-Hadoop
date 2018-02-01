package de.webis.hadoop.jcas;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;

import com.amazonaws.services.elastictranscoder.model.Pipeline;

public class BinaryJCasWriterMapper extends Mapper<Writable, Writable, LongWritable, BytesWritable> {

	AnalysisEngine ae = null;
	JCas jcas = null;
	UIMAClassLoader uimaClassLoader=null;
	public BinaryJCasWriterMapper() {

	}

	@Override
	public void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
		try {
			if (ae == null && uimaClassLoader==null) {
				Configuration jobConf = context.getConfiguration();
				Path[] files = DistributedCache.getLocalCacheArchives(context.getConfiguration());
				String uimaResourceArchivedFileName = jobConf.get("uimaResourceArchivedFileName");
				String targetJarFileName = jobConf.get("targetJarFileName");
				String piplineClassName = jobConf.get("piplineClassName");
				String piplineGetAnalyserEngineMethodName = jobConf.get("piplineGetAnalyserEngineMethodName");
				URI targetJarFilePath=null;
				String basePath = "";
				for (Path file : files)
					if ((file.toString().contains(uimaResourceArchivedFileName)))
						basePath = file.toString();
				for (Path file : DistributedCache.getLocalCacheFiles(jobConf))
					if ((file.toString().contains(targetJarFileName)))
						targetJarFilePath = file.toUri();
				uimaClassLoader=new UIMAClassLoader(targetJarFilePath,piplineClassName);
				ae = uimaClassLoader.getAnalysisEngine(piplineGetAnalyserEngineMethodName,basePath);
			}
			jcas = ae.newJCas();
			String val = value.toString();
			jcas.setDocumentText(val);
			ae.process(jcas);
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Serialization.serializeCAS(jcas.getCas(), baos);
			byte[] barr = baos.toByteArray();
			BytesWritable bw = new BytesWritable(barr);
			context.write(new LongWritable(1), bw);
		} catch (Exception ex) {
			   System.err.println("ERROR: "+ex.toString()+" "+ex.getLocalizedMessage() );
			    context.setStatus("Detected possibly corrupt record: see logs.");
			    System.exit(1);
		}

	}

}
