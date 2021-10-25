package utils;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashGenerator {

	public static BigInteger generateHash(String s) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");

			md.update(s.getBytes());
			byte[] hash = md.digest();
			BigInteger bi = new BigInteger(hash);
			return bi;
		} catch (NoSuchAlgorithmException e) {
			System.err.println("SHA-1 not available.");
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}

}
