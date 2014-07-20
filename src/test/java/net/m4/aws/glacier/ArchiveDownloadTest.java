package net.m4.aws.glacier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.DescribeJobRequest;
import com.amazonaws.services.glacier.model.DescribeJobResult;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ArchiveDownloadTest {

	private static final Logger logger = Logger.getLogger(ArchiveDownloadTest.class);

	// Test properties keys
	public static final String VAULT_NAME = "vaultName";
	public static final String JOB_ID = "jobId";
	public static final String ARCHIVE_DOWNLOAD_DIR = "archiveDownloadDir";
	// Test properties 
	private static String vaultName;
	private static String jobId;
	private static String archiveDownloadDir;

	// AWS
	private static AWSCredentials credentials;
	private static AmazonGlacierClient glacierClient;
	
	private static final long DOWNLOAD_CHUNK_SIZE = 1048576; // 1 MB // 4194304; // 4 MB
	private static final int THREAD_POOL_SIZE = 3;

	// Jackson JSON Mapper
	private static ObjectMapper mapper;
	
	/**
	 * Initialises AWS client and Jackson JSON mapper objects.
	 * 
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		Properties p = new Properties();
		p.load(VaultsTest.class.getResourceAsStream("/ArchiveDownloadTest.properties"));
		vaultName = p.getProperty(VAULT_NAME);
		jobId = p.getProperty(JOB_ID);
		archiveDownloadDir = p.getProperty(ARCHIVE_DOWNLOAD_DIR);

		credentials = new PropertiesCredentials(VaultsTest.class.getResourceAsStream("/AwsCredentials.properties"));
		glacierClient = new AmazonGlacierClient(credentials);
		glacierClient.setEndpoint("https://glacier.eu-west-1.amazonaws.com/");

		mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		mapper.registerModule(new AwsGlacierMixInModule());
	}

	/**
	 * Download an archive given a Job ID in chunks (assuming the job was initiated 4+ hours ago and completed successfully).
	 * 
	 * XXX: FIXIT!
	 * 
	 * @throws IOException
	 */
	@Test
	public void testArchiveDownloadInChunks() throws IOException {
		DescribeJobRequest request = new DescribeJobRequest()
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		DescribeJobResult result = glacierClient.describeJob(request);
		logger.info("Job details:");
		logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
		assertEquals("ArchiveRetrieval", result.getAction());
		
		long archiveSizeInBytes = result.getArchiveSizeInBytes();
		
		if (archiveSizeInBytes < 0) {
            logger.error("Nothing to download.");
            return;
        }

        logger.info("archiveSizeInBytes: " + archiveSizeInBytes);
        
        // create download folder
        String dirName = archiveDownloadDir + "/" + UUID.randomUUID().toString();
        boolean success = (new File(dirName)).mkdirs();
        assertTrue("Failed to create director: " + dirName, success);
        
        int chunk = 0;
        long startRange = 0;
        long endRange = (DOWNLOAD_CHUNK_SIZE > archiveSizeInBytes) ? archiveSizeInBytes -1 : DOWNLOAD_CHUNK_SIZE - 1;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        
        do {
        	chunk++;
        	Runnable downloader = new DownloadThread(dirName, chunk, startRange, endRange);
        	executor.execute(downloader);
        	
        	startRange = startRange + (long) DOWNLOAD_CHUNK_SIZE;
            endRange = ((endRange + DOWNLOAD_CHUNK_SIZE) >  archiveSizeInBytes) ? archiveSizeInBytes : (endRange + DOWNLOAD_CHUNK_SIZE);
        } while (endRange <= archiveSizeInBytes  && startRange < archiveSizeInBytes);
        
        executor.shutdown();
        while (!executor.isTerminated()) {
        	;
        }
        logger.info("Archive downloaded to: " + dirName);

		assertTrue("Successfully completed.", true);
	}
	
	class DownloadThread implements Runnable {
		String dirName;
		String fileName;
		int chunk;
		long startRange, endRange;

		public DownloadThread(String dirName, int chunk, long startRange, long endRange) {
			this.dirName = dirName;
			this.chunk = chunk;
			this.startRange = startRange;
			this.endRange = endRange;
		}
		
		public void run() {
			logger.info(String.format("Start. startRange: %d endRange: %d.", startRange, endRange));
	        download();
	        logger.info("End.");
		}
		
		private void download() {
	        try {
	        	fileName = String.format("%s/part-%04d", dirName, chunk);
	        	FileOutputStream fstream = new FileOutputStream(fileName);
	        	
	            GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
	                .withVaultName(vaultName)
	                .withRange("bytes=" + startRange + "-" + endRange)
	                .withJobId(jobId);
	            GetJobOutputResult getJobOutputResult = glacierClient.getJobOutput(getJobOutputRequest);

	            BufferedInputStream is = new BufferedInputStream(getJobOutputResult.getBody());     
	            byte[] buffer = new byte[(int)(endRange - startRange + 1)];
	
	            logger.info("Checksum received: " + getJobOutputResult.getChecksum());
	            logger.info("Content range " + getJobOutputResult.getContentRange());
	            
	            int totalRead = 0;
	            while (totalRead < buffer.length) {
	                int bytesRemaining = buffer.length - totalRead;
	                int read = is.read(buffer, totalRead, bytesRemaining);
	                if (read > 0) {
	                    totalRead = totalRead + read;                             
	                } else {
	                    break;
	                }
	                
	            }
	            logger.info("Calculated checksum: " + TreeHashGenerator.calculateTreeHash(new ByteArrayInputStream(buffer)));
	            logger.info("read = " + totalRead);
	            fstream.write(buffer);
	            
	            is.close();
	            
	            fstream.close();
	        } catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	}
	
	/**
	 * Test random access writing to a file.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testRandomAccessFile() throws IOException {
		String fileName = archiveDownloadDir + "/" + UUID.randomUUID().toString();
		RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
		raf.setLength(DOWNLOAD_CHUNK_SIZE);
		// write at the start of file
		raf.seek(0);
		raf.write("At the start".getBytes());
		// the middle
		raf.seek(DOWNLOAD_CHUNK_SIZE/2);
		raf.write("In the middle".getBytes());
		// and at the end
		raf.seek(DOWNLOAD_CHUNK_SIZE-10);
		raf.write("At the end".getBytes());
	}
}
