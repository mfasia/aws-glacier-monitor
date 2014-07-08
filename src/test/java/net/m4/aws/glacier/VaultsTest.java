package net.m4.aws.glacier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Date;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.TreeHashGenerator;
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

import javax.xml.bind.DatatypeConverter;

/**
 * Tests for AWS Glacier.
 * 
 * @author Tanvir
 *
 */
public class VaultsTest {
	
	private static final Logger logger = Logger.getLogger(VaultsTest.class);

	// AWS
	public static AWSCredentials credentials;
	public static AmazonGlacierClient glacierClient;
	public static AmazonSQSClient sqsClient;
    public static AmazonSNSClient snsClient;
    public static long downloadChunkSize = 4194304; // 4 MB  

	// Jackson JSON Mapper
	ObjectMapper mapper;

	// metafour - backup
	String vaultName = "116-demo2.metafour.com";
//	String vaultName = "304-arrow.m4.net";
//	String vaultName = "Backup";
//	String vaultName = "NetCourier";
//	String vaultName = "fourdhn2-2014-27";
//	String vaultName = "fourdhn2-2014-28";
//	String vaultName = "testvault";
	
//	String jobId = "-X-U6-WqL3HUWbNBtjtif_Jr2L7yWGVwWJ2hAfC4-3JgD_diQ9n92uf_y-bzP01jGrY0ajfw7W-mypRTL42NsoKYEclD";
//	String jobId = "F3z6zyRc8Gl9lHRePPVgFhzf3y9QpNV7VK9WQm4Gpdu_Cqna3FIGrUw-RgQI9GW4jmX66XhFqtxH_CzfjnbPFD3_cF8K";
//	String jobId = "dDsW_nDKptnGi-IYjEDMQfk-tKSdKzxNhMqYpeFRW6GP4ZM-8v88k6l9su94MiCkRnNnRbHYvqDcihew2bcV2RXNX4Uy";
	String jobId = "Ysw6CpgQ0DfewNyMeLHiQJKRLC4EEDPpboSMOgtthjgUCCIOkHpn0HuRoRGqJxNV96I-wE6YgovlWHHS0YVFTCAEZi6G";
//	String archiveId = "DHPLE7Zkq2fp3bjlaxBOLBKiQzE-7zWLVc9BEz1T1bwzAYDnAdVu_kXVwO8wLrX5T4DvA_IUVSXWJe5hlaPXuMazfGf9k-wkBfy7BHV0L2quqPlAWjm5bpOSoRV6Q-spaw0nKCqttQ";
	String archiveId = "SlR6vzJqJf4ASIEXpCx6O_Tpn9aUGq_1U7KLdrlJSRBSvYkVYmnNPq-dEsFiBRAWdoK0o3SVxKHAzC33pF13nKA2-5x6MmcIWaFhZsBu725LR0m5iRejJbOKbv9j5JkUuQc9R3ks3w";

	// ftahmed - glacier
//	String vaultName = "testvault";
//	String jobId = "VmWAzSDpMk6KoR_e5AvdUSrrh42y4ay6pvkgX10UsfTwV7hlz0ShIraU9wRsFuW69nlb7EBEvRIinzJp6BHKg3YXZ9TT"; // Inventory retrieval
//	String jobId = "12Tb4wniPRDLmCn9DrfrBaDiwY5Wz744EMihqcpTuRPlDgxrFT7_Qxp25VVPhDU7asWf_ZMvUFFLa0TZ9aDch80weV5j"; // Inventory retrieval
//	String jobId = "UYVO1FsZ8_LnOEEBVa-rM3kQ4s7s6-nfeWsdH9-2DCZzKYc6LAYOrc9nApfNnfh75r1dPpsMO8rLwmtYt0pG56eeI5Bj"; // Archive retrieval
//	String jobId = "6WXMaEyXgkFKvTQUMjOQyOU6LL1cmXG11Fh4jgzwaMOe-x0TrhU4TiZh-tZ7G4c9yyrAu7vmkfFJ1mOhmcOk3Qs7qAsq";
//	String jobId = "L7Ecv1hTwnjWZo7850i60hfU7tLrgLB9hFPmA9GNQZT0xwcTmhPhu6L-Q4phB89baEhglUpeZTEkJXksq-76Qb6VTiyr";
//	String archiveId = "gxrGyk6m8ccwFf59SF4gH9OK6w3T638L7NB64camYUtwtWUCtZ0MAyO6JtUnbr6lHaT-kJwSmYMj-DxZS7VRiHPhoQjLAMuRBabldnhtwMv8909W2bK67xmtXiNyQsP4b-UAqBOwvg";
//	String archiveId = "BfUz1-73uq4-FvwEyV1ZrKlPCidvtahjsXwTAu87ThMet-YmNhnOJzLL3KEa-TG43RFwDcpdT1qpuO3P6DcuXK2nxwawkvQUvq_X4ZQdhpSjxIsOQVn7UspjI0xG7smDDfSFneqqWg";

	String archiveToUpload = "target/test-classes/sample.zip";
	String archiveToDownloadJob = "target/JobArchiveRetrieval.bin";
	String archiveToDownloadAtm = "target/AtmArchiveRetrieval.bin";
	/**
	 * Initialises AWS client and Jackson JSON mapper objects.
	 * 
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		credentials = new PropertiesCredentials(VaultsTest.class.getResourceAsStream("/AwsCredentials.properties"));
		
		glacierClient = new AmazonGlacierClient(credentials);
		sqsClient = new AmazonSQSClient(credentials);
        snsClient = new AmazonSNSClient(credentials);
        
		glacierClient.setEndpoint("https://glacier.eu-west-1.amazonaws.com/");
		sqsClient.setEndpoint("https://sqs.eu-west-1.amazonaws.com/");
		snsClient.setEndpoint("https://sns.eu-west-1.amazonaws.com/");

		mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		mapper.registerModule(new VaultsTestModule());
	}

	/**
	 * Fetch all vault information in the account.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testListVaults() throws IOException {
		ListVaultsRequest request = new ListVaultsRequest().withAccountId("-");
		ListVaultsResult result = glacierClient.listVaults(request);
		assertFalse("There are no vaults!", result.getVaultList().size() == 0);

		for (DescribeVaultOutput vault : result.getVaultList()) {
			logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vault));
		}

		assertTrue("Successfully completed.", result.getVaultList().size() > 0);
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
		ListVaultsRequest request = new ListVaultsRequest().withAccountId("-");
		ListVaultsResult result = glacierClient.listVaults(request);
		assertFalse("There are no vaults!", result.getVaultList().size() == 0);

		for (DescribeVaultOutput vault : result.getVaultList()) {
			logger.info(String.format("Vault: %s", vault.getVaultName()));
			logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vault));
			ListJobsRequest jlrequest = new ListJobsRequest().withVaultName(vault.getVaultName());

			ListJobsResult jlresult = glacierClient.listJobs(jlrequest);
			logger.info(String.format("Jobs in %s: %d", vault.getVaultName(), jlresult.getJobList().size()));
			for (GlacierJobDescription job : jlresult.getJobList()) {
				logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job));
			}
		}

		assertTrue("Successfully completed.", true);
	}

	/**
	 * Initiate an inventory retrieval job in the named vault.
	 * 
	 * @throws IOException
	 */
	@Test
//	@Ignore
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
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
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
	 */
	@Test
	public void testDownloadLatestInventory() throws IOException {
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
		logger.info("The Latest Inventory Job:");
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
		logger.info("The Latest Inventory:");
		//logger.info(sb.toString());
		Object json = mapper.readValue(sb.toString(), Object.class);
		logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

		assertTrue("Successfully completed.", true);
	}
	
	/**
	 * Initiate an archive retrieval job in the named vault given an archiveId.
	 * 
	 * @throws IOException
	 */
	@Test
//	@Ignore
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

		FileOutputStream fos = new FileOutputStream(new File(archiveToDownloadJob));
		BufferedInputStream bis = new BufferedInputStream(joresult.getBody());
		byte[] buffer = new byte[1024*1024];
		int c = 0;
		while ((c = bis.read(buffer)) != -1) {
			fos.write(buffer, 0, c);
		}
		fos.close();
		bis.close();
		logger.info("Job output: " + archiveToDownloadJob);

		assertTrue("Successfully completed.", true);
	}

	/**
	 * Download an archive given a Job ID in chunks (assuming the job was initiated 4+ hours ago and completed successfully).
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
            System.err.println("Nothing to download.");
            return;
        }

        logger.info("archiveSizeInBytes: " + archiveSizeInBytes);
        FileOutputStream fstream = new FileOutputStream(archiveToDownloadJob);
        long startRange = 0;
        long endRange = (downloadChunkSize > archiveSizeInBytes) ? archiveSizeInBytes -1 : downloadChunkSize - 1;

        do {

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
            
            startRange = startRange + (long)totalRead;
            endRange = ((endRange + downloadChunkSize) >  archiveSizeInBytes) ? archiveSizeInBytes : (endRange + downloadChunkSize); 
            is.close();
        } while (endRange <= archiveSizeInBytes  && startRange < archiveSizeInBytes);
        
        fstream.close();
        logger.info("Retrieved file to " + archiveToDownloadJob);

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
        
        UploadResult result = atm.upload("-", vaultName, "my archive " + (new Date()), new File(archiveToUpload), 
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
        atm.download("-", vaultName, archiveId, new File(archiveToDownloadAtm),//);
        		new ProgressListener() {
					public void progressChanged(ProgressEvent e) {
						logger.info("Bytes transferred: " + e.getBytesTransferred());
					}
        		});
        logger.info("Atm output: " + archiveToDownloadAtm);
	}

	/**
	 * Decode base64 encoded archive description.
	 * 
	 * @see
	 * <a href="https://github.com/vsespb/mt-aws-glacier/blob/master/lib/App/MtAws/MetaData.pm">mt-aws-glacier: MetaData.pm</a>
	 */
	@Test
	public void testDecodeArchiveDescription() {
		String archiveDescription = "mt2 eyJmaWxlbmFtZSI6InZhci93d3cvbmV0Y3Y0XzExL25jZGVscmVjLmpzIiwibXRpbWUiOiIyMDA4MDgwN1QwNjUwMzFaIn0";
		String b64 = archiveDescription.split(" ")[1];
		int padding = 4 - (b64.length() % 4);
		if (padding > 0) {
			char[] c = new char[padding];
			Arrays.fill(c, '=');
			b64 = b64.concat(new String(c));
		}
		byte[] b = DatatypeConverter.parseBase64Binary(b64);
        logger.info("Decoded archive description: " + new String(b));
	}

	/**
	 * Jackson JSON mapper configuration.
	 * 
	 * @see
	 * <a href="http://wiki.fasterxml.com/JacksonMixInAnnotations">http://wiki.fasterxml.com/JacksonMixInAnnotations</a>
	 * <a href="http://wiki.fasterxml.com/JacksonFeatureModules">http://wiki.fasterxml.com/JacksonFeatureModules</a>
	 *
	 */
	class VaultsTestModule extends SimpleModule {
		private static final long serialVersionUID = -7213534997633659786L;

		public VaultsTestModule() {
			super("VaultsTestModule", new Version(0, 0, 1, null, null, null));
		}

		@Override
		public void setupModule(SetupContext context) {
			context.setMixInAnnotations(GlacierJobDescription.class, GlacierJobDescriptionMixIn.class);
			context.setMixInAnnotations(DescribeJobResult.class, DescribeJobResultMixIn.class);
			// and other set up, if any
		}
	}
	
	/**
	 * MixIn class for GlacierJobDescription
	 *
	 */
	abstract class GlacierJobDescriptionMixIn extends GlacierJobDescription {
		private static final long serialVersionUID = -1866228620607371709L;

		@JsonIgnore
		public abstract Boolean getCompleted(); // we don't need it!

	}
	
	/**
	 * MixIn class for DescribeJobResult
	 *
	 */
	abstract class DescribeJobResultMixIn extends DescribeJobResult {
		private static final long serialVersionUID = 8577745463832177536L;

		@JsonIgnore
		public abstract Boolean getCompleted(); // we don't need it!
	}
}
