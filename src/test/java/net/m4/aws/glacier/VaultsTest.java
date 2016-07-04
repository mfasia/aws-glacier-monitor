package net.m4.aws.glacier;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

import javax.management.relation.Relation;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
import com.amazonaws.services.glacier.model.DeleteVaultRequest;
import com.amazonaws.services.glacier.model.DescribeJobRequest;
import com.amazonaws.services.glacier.model.DescribeJobResult;
import com.amazonaws.services.glacier.model.DescribeVaultOutput;
import com.amazonaws.services.glacier.model.GetJobOutputRequest;
import com.amazonaws.services.glacier.model.GetJobOutputResult;
import com.amazonaws.services.glacier.model.GlacierJobDescription;
import com.amazonaws.services.glacier.model.InitiateJobRequest;
import com.amazonaws.services.glacier.model.InitiateJobResult;
import com.amazonaws.services.glacier.model.JobParameters;
import com.amazonaws.services.glacier.model.ListJobsRequest;
import com.amazonaws.services.glacier.model.ListJobsResult;
import com.amazonaws.services.glacier.model.ListVaultsRequest;
import com.amazonaws.services.glacier.model.ListVaultsResult;
import com.amazonaws.services.glacier.transfer.ArchiveTransferManager;
import com.amazonaws.services.glacier.transfer.UploadResult;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.metafour.aws.glacier.MtAwsMetaData;

/**
 * Tests for AWS Glacier.
 * 
 * @author Tanvir
 *
 */
public class VaultsTest {
	
	private static final Logger logger = Logger.getLogger(VaultsTest.class);

	// Test properties keys
	public static final String VAULT_NAME = "vaultName";
	public static final String JOB_ID = "jobId";
	public static final String ARCHIVE_ID = "archiveId";
	public static final String ARCHIVE_TO_UPLOAD = "archiveToUpload";
	public static final String ARCHIVE_DOWNLOAD_DIR = "archiveDownloadDir";
	// Test properties 
	private static String vaultName;
	private static String jobId;
	private static String archiveId;
	private static String archiveToUpload;
	private static String archiveDownloadDir;

	// AWS
	private static AWSCredentials credentials;
	private static AmazonGlacierClient glacierClient;
	private static AmazonSQSClient sqsClient;
	private static AmazonSNSClient snsClient;
	
	private static final long DOWNLOAD_CHUNK_SIZE = 1048576; // 1 MB // 4194304; // 4 MB  

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
		p.load(VaultsTest.class.getResourceAsStream("/VaultsTest.properties"));
		vaultName = p.getProperty(VAULT_NAME);
		jobId = p.getProperty(JOB_ID);
		archiveId = p.getProperty(ARCHIVE_ID);
		archiveToUpload = p.getProperty(ARCHIVE_TO_UPLOAD);
		archiveDownloadDir = p.getProperty(ARCHIVE_DOWNLOAD_DIR);

		credentials = new PropertiesCredentials(VaultsTest.class.getResourceAsStream("/AwsCredentials.properties"));
		
		glacierClient = new AmazonGlacierClient(credentials);
		sqsClient = new AmazonSQSClient(credentials);
        snsClient = new AmazonSNSClient(credentials);
        
		glacierClient.setEndpoint("https://glacier.eu-west-1.amazonaws.com/");
		sqsClient.setEndpoint("https://sqs.eu-west-1.amazonaws.com/");
		snsClient.setEndpoint("https://sns.eu-west-1.amazonaws.com/");

		mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		mapper.registerModule(new AwsGlacierMixInModule());
	}

	/**
	 * Fetch all vault information in the account.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testListAllVaults() throws IOException {
		String marker = null;
		do {
			ListVaultsRequest request = new ListVaultsRequest().withAccountId("-").withMarker(marker);
			ListVaultsResult result = glacierClient.listVaults(request);
			assertFalse("There are no vaults!", result.getVaultList().size() == 0);
	
			marker = result.getMarker();
			for (DescribeVaultOutput vault : result.getVaultList()) {
				logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vault));
			}
	
			assertTrue("Successfully completed.", result.getVaultList().size() > 0);
		} while (marker != null);
	}

	/**
	 * Delete a vault.
	 * 
	 */
	@Test
	@Ignore
	public void testDeleteVault() {
		DeleteVaultRequest request = new DeleteVaultRequest().withAccountId("-");
		request.withVaultName(vaultName);
		glacierClient.deleteVault(request);

		assertTrue("Successfully completed.", true);
	}
	
	/**
	 * Fetch job information from a vault.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testListJobsInAVault() throws IOException {
		ListJobsRequest request = new ListJobsRequest().withVaultName(vaultName);
		ListJobsResult result = glacierClient.listJobs(request);
		assertFalse(String.format("There are no jobs in %s!", vaultName), result.getJobList().size() == 0);
		
		logger.info(String.format("Jobs found: %d", result.getJobList().size()));
		for (GlacierJobDescription job : result.getJobList()) {
			logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job));
		}

		assertTrue("Successfully completed.", result.getJobList().size() > 0);
	}
	
	/**
	 * Fetch job information from all vaults in the account.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testListJobsInAllVaults() throws IOException {
		String marker = null;
		do {
			ListVaultsRequest request = new ListVaultsRequest().withAccountId("-").withMarker(marker);
			ListVaultsResult result = glacierClient.listVaults(request);
			assertFalse("There are no vaults!", result.getVaultList().size() == 0);
	
			marker = result.getMarker();
			for (DescribeVaultOutput vault : result.getVaultList()) {
//				logger.info(String.format("Vault: %s", vault.getVaultName()));
//				logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vault));
				ListJobsRequest jlrequest = new ListJobsRequest().withVaultName(vault.getVaultName());
	
				ListJobsResult jlresult = glacierClient.listJobs(jlrequest);
				logger.info(String.format("Jobs in %s: %d", vault.getVaultName(), jlresult.getJobList().size()));
				for (GlacierJobDescription job : jlresult.getJobList()) {
					logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job));
				}
			}
		} while (marker != null);

		assertTrue("Successfully completed.", true);
	}

	/**
	 * Initiate an inventory retrieval job in the named vault.
	 * 
	 * @throws IOException
	 */
	@Test
	@Ignore
	public void testInitiateJobInventoryRetrieval() throws IOException {
		InitiateJobRequest initJobRequest = new InitiateJobRequest()
				.withAccountId("-").withVaultName(vaultName)
				.withJobParameters(new JobParameters().withType("inventory-retrieval"));

		InitiateJobResult initJobResult = glacierClient.initiateJob(initJobRequest);
		jobId = initJobResult.getJobId();
		logger.info("Job ID: " + jobId);

		assertTrue("Successfully completed.", true);
	}
	
	/**
	 * Initiate an inventory retrieval job in all vaults.
	 * 
	 * @throws IOException
	 */
	@Test
//	@Ignore
	public void testInitiateJobInventoryRetrievalInAllVaults() throws IOException {
		String marker = null;
		do {
			ListVaultsRequest request = new ListVaultsRequest().withAccountId("-").withMarker(marker);
			ListVaultsResult result = glacierClient.listVaults(request);
			assertFalse("There are no vaults!", result.getVaultList().size() == 0);
	
			marker = result.getMarker();
			for (DescribeVaultOutput vault : result.getVaultList()) {
//				logger.info(String.format("Vault: %s", vault.getVaultName()));
//				logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vault));
				InitiateJobRequest initJobRequest = new InitiateJobRequest()
						.withAccountId("-").withVaultName(vault.getVaultName())
						.withJobParameters(new JobParameters().withType("inventory-retrieval"));

				InitiateJobResult initJobResult = glacierClient.initiateJob(initJobRequest);
				logger.info(String.format("Vault: %s, Job ID:", vault.getVaultName(), initJobResult.getJobId()));
			}
		} while (marker != null);

		assertTrue("Successfully completed.", true);
	}
	
	/**
	 * Download the inventory of the named vault given a Job ID (assuming the job was initiated 4+ hours ago and completed successfully).
	 *  
	 * @throws IOException
	 */
	@Test
	public void testJobOutputInventoryRetrieval() throws IOException {
		DescribeJobRequest request = new DescribeJobRequest()
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		DescribeJobResult result = glacierClient.describeJob(request);
		assertEquals("InventoryRetrieval", result.getAction());
		logger.info("Job details:");
		logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
		
		GetJobOutputRequest jorequest = new GetJobOutputRequest()
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId)
//				.withRange(String.format("0-%d", result.getInventorySizeInBytes()))
				.withGeneralProgressListener((ProgressEvent e) -> {
						// TODO Auto-generated method stub
						System.out.println(String.format("Event type: %s, Bytes: %d, BytesTransferred: %d", 
								e.getEventType(), e.getBytes(), e.getBytesTransferred()));
					}
				);
		GetJobOutputResult joresult = glacierClient.getJobOutput(jorequest);

		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(new InputStreamReader(joresult.getBody()));
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		logger.info("Job output:");
//		logger.info(sb.toString());
		Object json = mapper.readValue(sb.toString(), Object.class);
		logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

		assertTrue("Successfully completed.", true);
	}

	/**
	 * Download the latest inventory of the named vault (assuming the job was initiated 4+ hours ago and completed successfully).
	 *  
	 * @throws IOException
	 * @throws ParseException 
	 */
	@Test
	public void testDownloadLatestInventory() throws IOException, ParseException {
		ListJobsRequest request = new ListJobsRequest().withVaultName(vaultName);
		ListJobsResult result = glacierClient.listJobs(request);
		logger.info("Total Jobs: " + result.getJobList().size());
		
		GlacierJobDescription latestJob = null;
		for (GlacierJobDescription job : result.getJobList()) {
			if (job.isCompleted() && "Succeeded".equals(job.getStatusCode()) && "InventoryRetrieval".equals(job.getAction())) {
				if (latestJob == null) {
					latestJob = job;
					continue;
				}
				if (job.getCompletionDate().compareTo(latestJob.getCompletionDate()) > 0) {
					latestJob = job;
				}
			}
		}
		assertNotNull("There is no completed inventory job", latestJob);
		logger.info("The Latest Inventory Retrieval Job:");
		logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(latestJob));
		
		jobId = latestJob.getJobId();
		
		GetJobOutputRequest jorequest = new GetJobOutputRequest()
			.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		GetJobOutputResult joresult = glacierClient.getJobOutput(jorequest);
		
		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(new InputStreamReader(joresult.getBody()));
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
//		logger.info(sb.toString());
		Map<String, Object> inventory = (Map<String, Object>) mapper.readValue(sb.toString(), Object.class);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date date = sdf.parse(inventory.get("InventoryDate").toString());
		logger.info("The Latest Inventory: " + DateFormat.getDateTimeInstance(DateFormat.LONG, DateFormat.LONG).format(date));
		for (Map<String, Object> archive : (List<Map<String, Object>>) inventory.get("ArchiveList")) {
			// decoding mt2 encoded ArchiveDescription
			String mt2Desc = archive.get("ArchiveDescription").toString();
//			logger.info(MtAwsMetaData.decodeMt2(mt2Desc));
			Map<String, String> plainDesc = (Map<String, String>) mapper.readValue(MtAwsMetaData.decodeMt2(mt2Desc), Object.class);
			archive.put("FileName", plainDesc.get("filename"));
			sdf.applyPattern("yyyyMMdd'T'HHmmss'Z'");
			date = sdf.parse(plainDesc.get("mtime"));   
			sdf.applyPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
			archive.put("ModifiedDate", sdf.format(date));
		}
		logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(inventory));
		

		assertTrue("Successfully completed.", true);
	}
	
	/**
	 * Initiate an archive retrieval job in the named vault given an archiveId.
	 * 
	 * @throws IOException
	 */
	@Test
	@Ignore
	public void testInitiateJobArchiveRetrieval() throws IOException {
		InitiateJobRequest initJobRequest = new InitiateJobRequest()
				.withAccountId("-").withVaultName(vaultName)
				.withJobParameters(
						new JobParameters()
							.withType("archive-retrieval")
							.withArchiveId(archiveId)
						);

		InitiateJobResult initJobResult = glacierClient.initiateJob(initJobRequest);
		jobId = initJobResult.getJobId();
		logger.info("Job ID: " + jobId);
		DescribeJobRequest request = new DescribeJobRequest()
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		DescribeJobResult result = glacierClient.describeJob(request);
		assertEquals("ArchiveRetrieval", result.getAction());
		logger.info("Job details:");
		logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));

		assertTrue("Successfully completed.", true);
	}
	
	/**
	 * Download the archive given a Job ID (assuming the job was initiated 4+ hours ago and completed successfully).
	 * 
	 * @throws IOException
	 */
	@Test
	public void testJobOutputArchiveRetrieval() throws IOException {
		DescribeJobRequest request = new DescribeJobRequest()
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		DescribeJobResult result = glacierClient.describeJob(request);
		logger.info("Job details:");
		logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
		assertEquals("ArchiveRetrieval", result.getAction());
		
		GetJobOutputRequest jorequest = new GetJobOutputRequest()
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		GetJobOutputResult joresult = glacierClient.getJobOutput(jorequest);

		String fileName = archiveDownloadDir + "/" + UUID.randomUUID().toString();
		FileOutputStream fos = new FileOutputStream(new File(fileName));
		BufferedInputStream bis = new BufferedInputStream(joresult.getBody());
		byte[] buffer = new byte[(int)DOWNLOAD_CHUNK_SIZE];
		int c = 0;
		while ((c = bis.read(buffer)) != -1) {
			fos.write(buffer, 0, c);
		}
		fos.close();
		bis.close();
		logger.info("Archive downloaded to: " + fileName);

		assertTrue("Successfully completed.", true);
	}

	/**
	 * Download an archive given a Job ID in chunks (assuming the job was initiated 4+ hours ago and completed successfully).
	 * 
	 * NOTE: FIXED! Download chunk size 1 MB did the trick. It may depend on available bandwidth.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testJobOutputArchiveRetrievalInChunks() throws IOException {
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
        String fileName = archiveDownloadDir + "/" + UUID.randomUUID().toString();
        FileOutputStream fstream = new FileOutputStream(fileName);
        long startRange = 0;
        long endRange = (DOWNLOAD_CHUNK_SIZE > archiveSizeInBytes) ? archiveSizeInBytes -1 : DOWNLOAD_CHUNK_SIZE - 1;

        do {

            GetJobOutputRequest getJobOutputRequest = new GetJobOutputRequest()
                .withVaultName(vaultName)
                .withRange(String.format("bytes=%d-%d", startRange, endRange))
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
            
            startRange = startRange + (long)totalRead;
            endRange = ((endRange + DOWNLOAD_CHUNK_SIZE) >  archiveSizeInBytes) ? archiveSizeInBytes : (endRange + DOWNLOAD_CHUNK_SIZE); 
            is.close();
        } while (endRange <= archiveSizeInBytes  && startRange < archiveSizeInBytes);
        
        fstream.close();
        logger.info("Archive downloaded to: " + fileName);

		assertTrue("Successfully completed.", true);
	}

	/**
	 * Upload an archive to a named vault.
	 * 
	 * @throws FileNotFoundException 
	 * @throws AmazonClientException 
	 * @throws AmazonServiceException 
	 * 
	 */
	@Test
	public void testUploadArchive() throws AmazonServiceException, AmazonClientException, FileNotFoundException {
		ArchiveTransferManager atm = new ArchiveTransferManager(glacierClient, sqsClient, snsClient);
        
		// TODO: Use mt2 encoded archive description
		String description = archiveToUpload + " [" + (new Date()) + "]";
        UploadResult result = atm.upload("-", vaultName, description, new File(archiveToUpload), 
        		new ProgressListener() {
					public void progressChanged(ProgressEvent e) {
						logger.info("Bytes transferred: " + e.getBytesTransferred());
					}
        		});
        logger.info("Archive ID: " + result.getArchiveId());
	}
	
	/**
	 * Download an archive from a named vault and given archiveId
	 * 
	 */
	@Test
	public void testDownloadArchive() {
		ArchiveTransferManager atm = new ArchiveTransferManager(glacierClient, sqsClient, snsClient);
        
        logger.info("Downloading Archive ID: " + archiveId);
        String fileName = archiveDownloadDir + "/" + UUID.randomUUID().toString();
        atm.download("-", vaultName, archiveId, new File(fileName),
        		new ProgressListener() {
					public void progressChanged(ProgressEvent e) {
						logger.info("Bytes transferred: " + e.getBytesTransferred());
					}
        		});
        logger.info("Archive downloaded to: " + fileName);
	}

}
