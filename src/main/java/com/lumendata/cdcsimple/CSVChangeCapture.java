package com.lumendata.cdcsimple;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author vinay.mulakkayala@lumendata.com
 *
 */
public class CSVChangeCapture {
	
	private static final Logger logger = LoggerFactory.getLogger(CSVChangeCapture.class);

	private Map<String, CSVRecord> mapReference = new HashMap<String, CSVRecord>();
	private List<CSVRecord> newRecList = new LinkedList<CSVRecord>();
	private List<CSVRecord> updatedRecList = new LinkedList<CSVRecord>(); 
	
	private String prevCsvFile;
	private String updatedCsvFile;
	private String deltaFilesPath;
	
	public CSVChangeCapture(String prevCsvFile, String updatedCsvFile, String deltaFilesPath) {
		this.prevCsvFile = prevCsvFile;
		this.updatedCsvFile = updatedCsvFile;
		this.deltaFilesPath = deltaFilesPath;
	}
	
	public static void main(String[] args) {
//		try (FileReader csvReaderMain = new FileReader("C:\\Users\\vinayram\\Downloads\\Sample100Orig.csv");
//				FileReader csvReaderShadow = new FileReader("C:\\\\Users\\\\vinayram\\\\Downloads\\\\Sample100Mod.csv")) {
		
		if (args.length < 3) {
			System.out.println("Required application parameters: <initial-csv-file-path> <updated-csv-file-path> <delta-files-path>");
			logger.error("Missing one or more application paramaters: <initial-csv-file-path> <updated-csv-file-path> <delta-files-path>");
			return;
		}
		
		String prevCsvFile = args[0];
		String updatedCsvFile = args[1];
		String deltaFilesPath = args[2];
		
		CSVChangeCapture csvChangeCapture = new CSVChangeCapture(prevCsvFile, updatedCsvFile, deltaFilesPath);
		csvChangeCapture.captureChangedData();
	}
	
	public void captureChangedData() {
		// Read both the files and identify new or updated records. Delete unchanged records from shadow map
		// Records remaining in reference map will be deleted records
		List<String> headerList = null;
		try (FileReader csvReaderMain = new FileReader(prevCsvFile);
				FileReader csvReaderShadow = new FileReader(updatedCsvFile)) {

			Iterable<CSVRecord> recordsShadow = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(csvReaderShadow);
			for (CSVRecord record: recordsShadow) {
				String pk = record.get(0);
				mapReference.put(pk, record);
			}

			CSVParser recordsMain = CSVParser.parse(csvReaderMain, CSVFormat.DEFAULT.withFirstRecordAsHeader());
			headerList = recordsMain.getHeaderNames();
			for (CSVRecord record : recordsMain) {
				String pk = record.get(0);
				// This makes the assumption that there will be no duplicate records
				// Remove this record from reference map if it exists. This will help lookup performance better as we progress
				// and also only deleted records will then remain in the map.
				CSVRecord recordShadow = mapReference.remove(pk);
				if (null == recordShadow) {
					// New record
					newRecList.add(record);
				} else if (!recordsAreEqual(record, recordShadow)) {
					// have record and is updated
					updatedRecList.add(record);			
				} // If we have a matching record and is unchanged, nothing to do, since we already removed from mapShadow
			}
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		String[] headerArr = headerList.toArray(new String[headerList.size()]);
		// Write out new csv records to file
		try (FileWriter writer = new FileWriter(deltaFilesPath + "_new.csv");
				CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(headerArr))) {
			for (CSVRecord record : newRecList) {
				printer.printRecord(record);
			}	
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		// Write out updated csv records to file
		try (FileWriter writer = new FileWriter(deltaFilesPath + "_updated.csv");
				CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(headerArr))) {
			for (CSVRecord record : updatedRecList) {
				printer.printRecord(record);
			}	
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

		// Write out deleted csv records to file
		try (FileWriter writer = new FileWriter(deltaFilesPath + "_deleted.csv");
				CSVPrinter printer = new CSVPrinter(writer, CSVFormat.DEFAULT.withHeader(headerArr))) {
			for (CSVRecord record : mapReference.values()) {
				printer.printRecord(record);
			}	
		} catch (IOException ioe) {
			logger.error(MessageFormat.format("IO Exception while processing CSV files for Change Data Capture. {0}", ioe.getMessage()), ioe);
		}
	}
	
	/*
	 * 
	 */
	private boolean recordsAreEqual(CSVRecord record1, CSVRecord record2) {
		for (int i = 0; i < record1.size(); i++) {
			
			if (null == record1.get(i)) {
				if (null != record2.get(i)) {
					return false;
				}
			} else if (!record1.get(i).equals(record2.get(i))) {
				return false;
			}
		}
		
		return true;
	}

}
