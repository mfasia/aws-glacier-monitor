package net.m4.aws.glacier;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * Tests for AWS Glacier.
 * 
 * @author Tanvir
 *
 */
public class VaultsTest {

	// AWS
	AWSCredentials credentials;
	AmazonGlacierClient client;

	// Jackson JSON Mapper
	ObjectMapper mapper;

	// metafour - backup
	String vaultName = "Backup";
	String jobId = "-X-U6-WqL3HUWbNBtjtif_Jr2L7yWGVwWJ2hAfC4-3JgD_diQ9n92uf_y-bzP01jGrY0ajfw7W-mypRTL42NsoKYEclD";
	String archiveId = "";

	// ftahmed - glacier
//	String vaultName = "testvault";
//	String jobId = "VmWAzSDpMk6KoR_e5AvdUSrrh42y4ay6pvkgX10UsfTwV7hlz0ShIraU9wRsFuW69nlb7EBEvRIinzJp6BHKg3YXZ9TT"; // Inventory retrieval
//	String jobId = "12Tb4wniPRDLmCn9DrfrBaDiwY5Wz744EMihqcpTuRPlDgxrFT7_Qxp25VVPhDU7asWf_ZMvUFFLa0TZ9aDch80weV5j"; // Inventory retrieval
//	String jobId = "UYVO1FsZ8_LnOEEBVa-rM3kQ4s7s6-nfeWsdH9-2DCZzKYc6LAYOrc9nApfNnfh75r1dPpsMO8rLwmtYt0pG56eeI5Bj"; // Archive retrieval
//	String archiveId = "gxrGyk6m8ccwFf59SF4gH9OK6w3T638L7NB64camYUtwtWUCtZ0MAyO6JtUnbr6lHaT-kJwSmYMj-DxZS7VRiHPhoQjLAMuRBabldnhtwMv8909W2bK67xmtXiNyQsP4b-UAqBOwvg";

	/**
	 * Initialises AWS client and Jackson JSON mapper objects.
	 * 
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		credentials = new PropertiesCredentials(VaultsTest.class.getResourceAsStream("/AwsCredentials.properties"));
		client = new AmazonGlacierClient(credentials);
		client.setEndpoint("https://glacier.eu-west-1.amazonaws.com/");

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
		ListVaultsResult result = client.listVaults(request);
		assertFalse("Failed to get the list of vaults!", result.getVaultList().size() == 0);

		for (DescribeVaultOutput vault : result.getVaultList()) {
			// System.out.println(vault.toString());
			// Object json = mapper.readValue(vault.toString(), Object.class);
			// System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
			System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vault));
		}

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

		ListJobsResult result = client.listJobs(request);
		System.out.println("Jobs: " + result.getJobList().size());
		for (GlacierJobDescription job : result.getJobList()) {
//			System.out.println(job);
//			jobId = job.getJobId();
			System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job));
		}

		assertTrue("Successfully completed.", true);
	}
	
	/**
	 * Fetch job information from all vaults in the account.
	 * 
	 * @throws IOException
	 */
	@Test
	public void testListJobsInAllVaults() throws IOException {
		ListVaultsRequest request = new ListVaultsRequest().withAccountId("-");
		ListVaultsResult result = client.listVaults(request);
		assertFalse("Failed to get the list of vaults!", result.getVaultList().size() == 0);

		for (DescribeVaultOutput vault : result.getVaultList()) {
			System.out.println("=== Vault: " + vault.getVaultName() + " ===");
			System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vault));
			ListJobsRequest jlrequest = new ListJobsRequest().withVaultName(vault.getVaultName());

			ListJobsResult jlresult = client.listJobs(jlrequest);
			System.out.println("Jobs: " + jlresult.getJobList().size());
			for (GlacierJobDescription job : jlresult.getJobList()) {
//				System.out.println(job);
//				jobId = job.getJobId();
				System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(job));
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
	@Ignore
	public void testInitiateJobInventoryRetrieval() throws IOException {
		InitiateJobRequest initJobRequest = new InitiateJobRequest()
				.withAccountId("-").withVaultName(vaultName)
				.withJobParameters(new JobParameters().withType("inventory-retrieval"));

		InitiateJobResult initJobResult = client.initiateJob(initJobRequest);
		jobId = initJobResult.getJobId();
		System.out.println("Job ID: " + jobId);

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
		DescribeJobResult result = client.describeJob(request);
		assertEquals("InventoryRetrieval", result.getAction());
		System.out.println("Job details:");
		System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
		
		GetJobOutputRequest jorequest = new GetJobOutputRequest()
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		GetJobOutputResult joresult = client.getJobOutput(jorequest);

		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(new InputStreamReader(joresult.getBody()));
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		System.out.println("Job output:");
//		System.out.println(sb.toString());
		Object json = mapper.readValue(sb.toString(), Object.class);
		System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

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
		ListJobsResult result = client.listJobs(request);
		System.out.println("Total Jobs: " + result.getJobList().size());
		
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
		System.out.println("The Latest Inventory Job:");
		System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(latestJob));
		
		jobId = latestJob.getJobId();
		
		GetJobOutputRequest jorequest = new GetJobOutputRequest()
			.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		GetJobOutputResult joresult = client.getJobOutput(jorequest);
		
		StringBuilder sb = new StringBuilder();
		BufferedReader br = new BufferedReader(new InputStreamReader(joresult.getBody()));
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		System.out.println("The Latest Inventory:");
		//System.out.println(sb.toString());
		Object json = mapper.readValue(sb.toString(), Object.class);
		System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));

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

		InitiateJobResult initJobResult = client.initiateJob(initJobRequest);
		jobId = initJobResult.getJobId();
		System.out.println("Job ID: " + jobId);
		DescribeJobRequest request = new DescribeJobRequest()
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		DescribeJobResult result = client.describeJob(request);
		assertEquals("ArchiveRetrieval", result.getAction());
		System.out.println("Job details:");
		System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));

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
		DescribeJobResult result = client.describeJob(request);
		System.out.println("Job details:");
		System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(result));
		assertEquals("ArchiveRetrieval", result.getAction());
		
		GetJobOutputRequest jorequest = new GetJobOutputRequest()
				.withAccountId("-").withVaultName(vaultName).withJobId(jobId);
		GetJobOutputResult joresult = client.getJobOutput(jorequest);

		FileOutputStream fos = new FileOutputStream(new File("target/ArchiveRetrieval.bin"));
		BufferedInputStream bis = new BufferedInputStream(joresult.getBody());
		byte[] buffer = new byte[1024];
		while (bis.read(buffer) != -1) {
			fos.write(buffer);
		}
		fos.close();
		bis.close();
		System.out.println("Job output: " + "target/ArchiveRetrieval.bin");

		assertTrue("Successfully completed.", true);
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
