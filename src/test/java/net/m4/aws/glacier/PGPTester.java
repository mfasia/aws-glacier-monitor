package net.m4.aws.glacier;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.junit.Ignore;
import org.junit.Test;

import com.metafour.aws.glacier.PGPFileProcessor;

public class PGPTester {

//	private static final String PASSPHRASE = "letmepass";
	private static final String PASSPHRASE = "Mon1t0r";

	private static final String DE_INPUT = "src/test/resources/pgp/encrypted.pgp";
	private static final String DE_OUTPUT = "src/test/resources/pgp/cleartext.dat";
	private static final String DE_KEY_FILE = "src/test/resources/pgp/secring.skr";

	private static final String E_INPUT = "src/test/resources/pgp/cleartext.dat";
	private static final String E_OUTPUT = "src/test/resources/pgp/encrypted.pgp";
	private static final String E_KEY_FILE = "src/test/resources/pgp/pubring.pkr";

	@Test
	public void testDecrypt() throws Exception {
		PGPFileProcessor p = new PGPFileProcessor();
		p.setInputFileName(DE_INPUT);
		p.setOutputFileName(DE_OUTPUT);
		p.setPassphrase(PASSPHRASE);
		p.setSecretKeyFileName(DE_KEY_FILE);
		System.out.println(p.decrypt());
	}
	
	@Test
	@Ignore
	/**
	 * FIXME!!
	 * 
	 * @throws Exception
	 */
	public void testVerify() throws Exception {
		PGPFileProcessor p = new PGPFileProcessor();
		p.setInputFileName(DE_INPUT);
		p.setOutputFileName(DE_OUTPUT);
//		p.setPassphrase(PASSPHRASE);
		p.setSecretKeyFileName(DE_KEY_FILE);
		System.out.println(p.verify());
	}

	@Test
	public void testTarGzip() throws Exception {
		TarArchiveInputStream tarIn = new TarArchiveInputStream(new GZIPInputStream(new FileInputStream(DE_OUTPUT)));
		System.out.println(String.format("%s\t%s\t%s.%s", "Name", "Size", "UserId", "GroupId"));
		TarArchiveEntry e = tarIn.getNextTarEntry();
	    while (e != null) {
	        System.out.println(String.format("%s\t%d\t%s.%s", e.getName(), e.getSize(), e.getUserId(), e.getGroupId()));
	        e = tarIn.getNextTarEntry();
	    }
	    tarIn.close();
	}
	
	@Test
	public void testEncrypt() throws Exception {
		PGPFileProcessor p = new PGPFileProcessor();
		p.setInputFileName(E_INPUT);
		p.setOutputFileName(E_OUTPUT);
		p.setPassphrase(PASSPHRASE);
		p.setPublicKeyFileName(E_KEY_FILE);
		System.out.println(p.encrypt());
	}
	
	@Test
	public void testGetSecretKeys() throws Exception {
        FileInputStream keyIn = new FileInputStream(DE_KEY_FILE);
        
//        PGPPrivateKey privateKey = PGPUtils.findPrivateKey(keyIn, 0, PASSPHRASE.toCharArray());
        
        PGPSecretKeyRingCollection keyRingCollection = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(keyIn),
        		new BcKeyFingerprintCalculator());

        //
        // We just loop through the collection till we find a key suitable for signing.
        // In the real world you would probably want to be a bit smarter about this.
        //
        PGPSecretKey secretKey = null;

        Iterator<PGPSecretKeyRing> rIt = keyRingCollection.getKeyRings();
        while (secretKey == null && rIt.hasNext()) {
            PGPSecretKeyRing keyRing = rIt.next();
            System.out.println(keyRing);
            Iterator<PGPSecretKey> kIt = keyRing.getSecretKeys();
            while (secretKey == null && kIt.hasNext()) {
                PGPSecretKey key = kIt.next();
                System.out.println("Key ID: " + key.getKeyID());
                if (key.isSigningKey()) {
                    secretKey = key;
                }
            }
        }
        
        keyIn.close();
	}
}
