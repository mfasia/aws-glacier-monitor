package net.m4.aws.glacier;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for multipart upload including a file.
 * 
 * @author Tanvir
 *
 */
public class HttpClientTest {

	private static final Logger logger = Logger.getLogger(HttpClientTest.class);
	
	private static final String UPLOAD_URL = "http://requestb.in/tknk6stk";
	private static final String UPLOAD_FILE = "target/test-classes/355310035652464_1.txt";
	
	@Before
	public void setUp() throws Exception {
	}

	/**
	 * Testing multipart upload using Apache HttpClient 4.x
	 * 
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	@Test
	public void testMultipartUpload() throws ClientProtocolException, IOException {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpPost uploadFile = new HttpPost(UPLOAD_URL);

		MultipartEntityBuilder builder = MultipartEntityBuilder.create();
		builder.addTextBody("up_path", "/path/to/upload", ContentType.TEXT_PLAIN);
		builder.addBinaryBody("up_file1", new File(UPLOAD_FILE), ContentType.TEXT_PLAIN, "355310035652464_1.txt");
		HttpEntity multipart = builder.build();

		uploadFile.setEntity(multipart);

		CloseableHttpResponse response = httpClient.execute(uploadFile);
		assertEquals(200, response.getStatusLine().getStatusCode());
		
		HttpEntity responseEntity = response.getEntity();
		assertTrue(responseEntity.getContentLength() > 0);

		logger.info("Content Length: " + responseEntity.getContentLength());
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		responseEntity.writeTo(baos);
		logger.info("Content: " + baos.toString());
	}

}
