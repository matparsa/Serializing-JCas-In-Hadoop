package de.webis.hadoop.jcas;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.jcas.JCas;
import de.webis.hadoop.jcas.UIMAClassLoader;

public class BinaryJCasReaderMapper extends
		Mapper<Writable, Writable, NullWritable, Text> {
	AnalysisEngine ae = null;
	JCas jcas = null;
	UIMAClassLoader uimaClassLoader=null;
	String piplineProccessJCassMethodName=null;
	@Override
	public void map(Writable key, Writable value, Context context)
			throws IOException, InterruptedException {
		try {
			if (ae == null || uimaClassLoader==null || piplineProccessJCassMethodName==null) {
				Configuration jobConf = context.getConfiguration();
				Path[] files = DistributedCache.getLocalCacheArchives(context.getConfiguration());
				String uimaResourceArchivedFileName = jobConf.get("uimaResourceArchivedFileName");
				String targetJarFileName = jobConf.get("targetJarFileName");
				String piplineClassName = jobConf.get("piplineClassName");
				String piplineGetAnalyserEngineMethodName = jobConf.get("piplineGetAnalyserEngineMethodName");
				piplineProccessJCassMethodName = jobConf.get("piplineProccessJCassMethodName");
				URI targetJarFilePath =null;
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
			
			JCas jcas = ae.newJCas();
			ByteArrayInputStream bais = new ByteArrayInputStream(
					((BytesWritable)value).getBytes());
			Serialization.deserializeCAS(jcas.getCas(), bais);
			context.write(null, new Text(uimaClassLoader.getStringFromJCas(jcas,piplineProccessJCassMethodName)));
			jcas.reset();
			ae.destroy();
				} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
