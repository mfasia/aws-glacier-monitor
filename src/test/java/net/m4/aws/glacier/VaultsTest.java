package net.m4.aws.glacier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
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
import com.fasterxml.jackson.databind.ObjectMapper;

public class VaultsTest {

	AWSCredentials credentials;
	AmazonGlacierClient client;
	String jobId = "VmWAzSDpMk6KoR_e5AvdUSrrh42y4ay6pvkgX10UsfTwV7hlz0ShIraU9wRsFuW69nlb7EBEvRIinzJp6BHKg3YXZ9TT";
	
	ObjectMapper mapper;

	@Before
	public void setUp() throws Exception {
		credentials = new PropertiesCredentials(
				VaultsTest.class
						.getResourceAsStream("/AwsCredentials.properties"));
		client = new AmazonGlacierClient(credentials);
		client.setEndpoint("https://glacier.eu-west-1.amazonaws.com/");

		mapper = new ObjectMapper();
		// mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

	}

	@Test
	public void testListVaults() throws IOException {
		ListVaultsRequest request = new ListVaultsRequest().withAccountId("-");
		ListVaultsResult result = client.listVaults(request);
		assertFalse("Failed to get the list of vaults!", result.getVaultList()
				.size() == 0);

		for (DescribeVaultOutput vault : result.getVaultList()) {
			// System.out.println(vault.toString());
			// Object json = mapper.readValue(vault.toString(), Object.class);
			// System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
			System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vault));
		}

		assertTrue("Successfully completed.", true);
	}

	@Test @Ignore
	public void testInitiateJobInventoryRetrieval() throws IOException {
		InitiateJobRequest initJobRequest = new InitiateJobRequest()
				.withVaultName("testvault")
				.withJobParameters(new JobParameters().withType("inventory-retrieval"));

		InitiateJobResult initJobResult = client.initiateJob(initJobRequest);
		jobId = initJobResult.getJobId();
		System.out.println("Job ID: " + jobId);

		assertTrue("Successfully completed.", true);
	}
	
	@Test
	public void testListJobs() throws IOException {
		ListJobsRequest request = new ListJobsRequest().withVaultName("testvault");

		ListJobsResult result = client.listJobs(request);
		for (GlacierJobDescription job : result.getJobList()) {
			System.out.println(job);
//			jobId = job.getJobId();
		}

		assertTrue("Successfully completed.", true);
	}

	@Test
	public void testJobOutput() throws IOException {
		GetJobOutputRequest request = new GetJobOutputRequest()
				.withAccountId("-").withVaultName("testvault").withJobId(jobId);
		GetJobOutputResult result = client.getJobOutput(request);

		BufferedInputStream bis = new BufferedInputStream(result.getBody());
		byte[] buf = new byte[1024];
		while (bis.read(buf) != -1) {
			System.out.print(buf.toString());
		}

		assertTrue("Successfully completed.", true);
	}

}
