package com.java.cassandra.demo;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.slf4j.helpers.MessageFormatter;

/**
 * This class is used to provide some common functions used across the project.
 * 
 * @author revanthreddy
 */
public class Utils {
	private final static Logger logger = Logger.getLogger(Utils.class);
	private static FileSystem fs;
	private static Configuration conf;

	/**
	 * This method is used tp parse the input arguments
	 * 
	 * @param args
	 * @return
	 */
	public static Map<String, String> argsParser(String[] args) {
		Map<String, String> result = new HashMap<>();
		int index = 0;
		for (String arg : args) {
			index++;
			String trimmedArg = arg.trim();
			if (trimmedArg.startsWith("--")) {
				String key = trimmedArg.replaceAll("--", "");
				if (index < args.length) {
					String value = args[index].trim();
					result.put(key, value);
				}
			}
		}
		return result;
	}

	/**
	 * This method is used to create local directory
	 * 
	 * @param localPath
	 */
	public static void createLocalDir(String localPath) {
		File dir = new File(localPath);
		if (dir.exists()) {
			logger.warn(MessageFormatter.format("Local directory : {} exists. Deleting it first", localPath));
			dir.delete();
		}
		logger.info("Creating local directory at location :" + localPath);
		dir.mkdir();
	}

	/**
	 * This method is used to copy csv files from hdfs to local directory
	 * 
	 * @param hdfsPath
	 * @param localPath
	 */
	public static void copyToLocal(String hdfsPath, String localPath) {
		try {
			createLocalDir(localPath);

			conf = new Configuration();
			fs = FileSystem.get(conf);
			List<Path> paths = getAllFilePaths(new Path(hdfsPath), fs);
			if (paths.size() > 0) {
				for (Path path : paths) {
					// Ignoring empty files
					long fileLength = fs.getFileStatus(path).getLen();
					if (fileLength > 0) {
						fs.copyToLocalFile(path, new Path(localPath));
					}
				}
			} else {
				logger.warn("No files found at location : " + hdfsPath + " to copy");
			}

		} catch (IOException e) {
			e.printStackTrace();
			logger.error("Error while copying files from : " + hdfsPath + " to : " + localPath + " :" + e.getMessage(),
					e);
			throw new RuntimeException("Error while copying files from hdfs to local. Failing job");
		}
	}

	/**
	 * This method is used get the list of file paths in hdfs
	 * 
	 * @param filePath
	 * @param fs
	 * @return List<Path> fileList
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private static List<Path> getAllFilePaths(Path filePath, FileSystem fs) throws FileNotFoundException, IOException {
		List<Path> fileList = new ArrayList<Path>();
		FileStatus[] fileStatus = fs.listStatus(filePath);
		for (FileStatus fileStat : fileStatus) {
			if (fileStat.isDirectory()) {
				fileList.addAll(getAllFilePaths(fileStat.getPath(), fs));
			} else {
				fileList.add(fileStat.getPath());
			}
		}
		return fileList;
	}

	/**
	 * This method is used to get the list of csv files from local directory
	 * 
	 * @param csvFilePath
	 * @return File[] files
	 */
	public static File[] getFilesList(String csvFilePath) {
		File[] files = new File(csvFilePath).listFiles(new FileFilter() {
			@Override
			public boolean accept(File path) {
				if (path.isFile()) {
					return true;
				}
				return false;
			}
		});
		return files;
	}

	static String newLine = System.getProperty("line.separator");

	public static void writeToFile(String msg, String outPath) {

		String fileName = outPath;
		PrintWriter printWriter = null;
		File file = new File(fileName);
		try {
			if (!file.exists()) {
				file.createNewFile();
			}
			printWriter = new PrintWriter(new FileOutputStream(fileName, true));
			printWriter.write(newLine + msg);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error in writeToFile. caused by :" + e.getMessage(), e);
		} finally {
			if (printWriter != null) {
				printWriter.flush();
				printWriter.close();
			}
		}
	}

}
