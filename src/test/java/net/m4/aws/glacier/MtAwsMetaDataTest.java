package net.m4.aws.glacier;

import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.metafour.aws.glacier.MtAwsMetaData;

public class MtAwsMetaDataTest {
	
	private static final Logger logger = Logger.getLogger(MtAwsMetaDataTest.class);
	
	public static final String ARCHIVE_DESCRIPTION = "archiveDescription";
	public static final String MT2_ENCODED_ARCHIVE_DESCRIPTION = "mt2EncodedArchiveDescription";
	public static final String FILE_NAME = "fileName";
	public static final String FILE_MTIME = "fileMtime";
	private static String archiveDescription;
	private static String mt2EncodedArchiveDescription;
	private static String fileName;
	private static String fileMtime;


	@Before
	public void setUp() throws Exception {
		Properties p = new Properties();
		p.load(VaultsTest.class.getResourceAsStream("/MtAwsMetaDataTest.properties"));
		archiveDescription = p.getProperty(ARCHIVE_DESCRIPTION);
		mt2EncodedArchiveDescription = p.getProperty(MT2_ENCODED_ARCHIVE_DESCRIPTION);
		fileName = p.getProperty(FILE_NAME);
		fileMtime = p.getProperty(FILE_MTIME);
	}

	@Test
	public void testEncodeMt2() {
		String encoded = MtAwsMetaData.encodeMt2(fileName, fileMtime);
		assertEquals(mt2EncodedArchiveDescription, encoded);
	}
	
	@Test
	public void testDecodeMt2() {
		String decoded = MtAwsMetaData.decodeMt2(mt2EncodedArchiveDescription);
		assertEquals(archiveDescription, decoded);
	}

}
