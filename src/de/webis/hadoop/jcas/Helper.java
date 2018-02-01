package de.webis.hadoop.jcas;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class Helper {

	public static String getFileName(String filePath) {
		int idxArch = filePath.toString().replaceAll("\\\\", "/").lastIndexOf("/");
		return idxArch >= 0 ? filePath.toString().substring(idxArch + 1) : filePath.toString();
	}

	public static String copyToHDFS(String path, Configuration conf) throws IOException, URISyntaxException {
		FileSystem fs = FileSystem.get(conf);
		String fileName = new File(path).getName();
		OutputStream os = fs.create(new Path(fs.getWorkingDirectory() + "/" + fileName));
		InputStream is = new BufferedInputStream(new FileInputStream(path));// Data set is getting copied into input
		IOUtils.copyBytes(is, os, conf);
		return fs.getWorkingDirectory() + "/" + fileName;
	}

}
