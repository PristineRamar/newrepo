package com.pristine.util;

import java.io.FileReader;

import com.csvreader.CsvReader;

public class CsvReaderUtil {

	
	public static CsvReader readFilecheck(String fileName, char delimiter) throws Exception {

		CsvReader reader = null;
		try {
			reader = new CsvReader(new FileReader(fileName));
			if (delimiter != '0') {
				reader.setDelimiter(delimiter);
			}
		} catch (Exception e) {
			throw new Exception("File read error ", e);
		}
		return reader;

	}
}
