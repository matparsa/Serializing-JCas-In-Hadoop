package de.webis.hadoop.jcas;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.hadoop.fs.Path;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.jcas.JCas;

public class UIMAClassLoader {

	Class<?> customClass = null;
	private Object customClassInstance = null;

	public UIMAClassLoader(URI jarPath, String piplineClassName)
			throws MalformedURLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		URLClassLoader child = new URLClassLoader(new URL[] { jarPath.toURL() },
				UIMAClassLoader.class.getClassLoader());
		customClass = child.loadClass(piplineClassName);
		customClassInstance = customClass.newInstance();
	}

	public AnalysisEngine getAnalysisEngine(String piplineMethodName, String descriptorsBasePath)
			throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
			SecurityException {
		Object result = customClass.getMethod(piplineMethodName, String.class).invoke(customClassInstance,
				descriptorsBasePath);
		return (AnalysisEngine) result;
	}

	public String getStringFromJCas(JCas jcas, String piplineMethodName)
			throws MalformedURLException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		Object result = customClass.getMethod(piplineMethodName, String.class).invoke(customClassInstance, jcas);
		return (String) result;
	}
}
