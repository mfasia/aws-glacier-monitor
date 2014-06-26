package net.m4.aws.glacier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.glacier.AmazonGlacierClient;
import com.amazonaws.services.glacier.model.DescribeVaultOutput;
import com.amazonaws.services.glacier.model.ListVaultsRequest;
import com.amazonaws.services.glacier.model.ListVaultsResult;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VaultsTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testListVaults() throws IOException {
		AWSCredentials credentials = new PropertiesCredentials(VaultsTest.class.getResourceAsStream("/AwsCredentials.properties"));
		AmazonGlacierClient client = new AmazonGlacierClient(credentials);
		client.setEndpoint("https://glacier.eu-west-1.amazonaws.com/");
		ListVaultsRequest request = new ListVaultsRequest().withAccountId("-");
		ListVaultsResult result = client.listVaults(request);
		assertFalse("Failed to get the list of vaults!", result.getVaultList().size() == 0);
		
		ObjectMapper mapper = new ObjectMapper();
//		mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
		for (DescribeVaultOutput vault : result.getVaultList()) {
//			System.out.println(vault.toString());
//			Object json = mapper.readValue(vault.toString(), Object.class);
//			System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json));
			System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vault));
		}
		
		assertTrue("Successfully completed.", true);
	}

}
