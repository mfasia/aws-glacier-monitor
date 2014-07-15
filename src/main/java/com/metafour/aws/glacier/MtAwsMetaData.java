package com.metafour.aws.glacier;

import java.util.Arrays;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * Java port of utility functions from Perl Multithreaded Multipart sync to Amazon Glacier http://mt-aws.com/

 * @author Tanvir
 *
 */
public class MtAwsMetaData {
	
	private static final Logger logger = Logger.getLogger(MtAwsMetaData.class);

	/**
	 * Encode archive description.
	 * 
	 * @see
	 * <a href="https://github.com/vsespb/mt-aws-glacier/blob/master/lib/App/MtAws/MetaData.pm">mt-aws-glacier: MetaData.pm</a>
	 */
	public static String encodeMt2(String filename, String mtime) {
//		{"filename":"2221-1404762035.tgz.gpg","mtime":"20140707T194155Z"}
		String desc = String.format("{\"filename\":\"%s\",\"mtime\":\"%s\"}", filename, mtime);
		logger.debug("Plain-text: " + desc);
		String b64 = DatatypeConverter.printBase64Binary(desc.getBytes());
		// Perl: $res =~ s/=+\z//;
		String encoded = b64.replaceAll("(=+)$", "");
		logger.debug("Chomped trailing =s: " + encoded);
		// Perl: $res =~ tr{+/}{-_};
		encoded = StringUtils.replaceChars(encoded, "+/", "-_");
		logger.debug("Replaced +/ with -_: " + encoded);
		encoded = "mt2 ".concat(encoded);
		logger.debug("Encoded: " + encoded);
		
		return encoded;
	}
	
	/**
	 * Decode archive description.
	 * 
	 * @see
	 * <a href="https://github.com/vsespb/mt-aws-glacier/blob/master/lib/App/MtAws/MetaData.pm">mt-aws-glacier: MetaData.pm</a>
	 */
	public static String decodeMt2(String encoded) {
		logger.debug("Encoded: " + encoded);
		if (encoded.indexOf(' ') < 0)
			return null;
		
		String b64 = encoded.split(" ")[1];
		// Perl: $str =~ tr{-_}{+/};
		b64 = StringUtils.replaceChars(b64, "-_", "+/");
		logger.debug("Replaced -_ with +/: " + b64);
		int padding = 4 - (b64.length() % 4);
		if (padding > 0) {
			char[] c = new char[padding];
			Arrays.fill(c, '=');
			b64 = b64.concat(new String(c));
		}
		logger.debug("Added trailing =s: " + b64);
		byte[] b = DatatypeConverter.parseBase64Binary(b64);
		String desc = new String(b);
		logger.debug("Plain-text: " + desc);
		
		return desc;
	}

}
