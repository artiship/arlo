package io.github.artiship.arlo.model.utils;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.SecureRandom;
import java.util.Base64;


public class DESUtils {

    private static final String key = "arlov1";
    private static final String DES = "DES";

    public static void main(String[] args) {

        String passJM = encryptFromBase64(null);
        String pass = decryptFromBase64(passJM);
        System.out.println(" passJM = " + passJM);
        System.out.println(" pass = " + pass);

    }

    public static String encryptFromBase64(String content) {
        if (null == content) {
            return content;
        }
        byte[] encryptRaw = encrypt(content);
        assert encryptRaw != null;
        return new String(Base64.getEncoder()
                                .encode(encryptRaw));
    }


    public static String decryptFromBase64(String content) {
        if (null == content) {
            return content;
        }
        byte[] decoded = Base64.getDecoder()
                               .decode(content);
        return decrypt(decoded);
    }


    private static byte[] encrypt(String content) {
        try {
            SecureRandom random = new SecureRandom();
            DESKeySpec desKey = new DESKeySpec(key.getBytes());
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);
            SecretKey securekey = keyFactory.generateSecret(desKey);
            Cipher cipher = Cipher.getInstance(DES);
            cipher.init(Cipher.ENCRYPT_MODE, securekey, random);
            byte[] result = cipher.doFinal(content.getBytes());
            return result;
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }


    private static String decrypt(byte[] content) {
        try {
            SecureRandom random = new SecureRandom();
            DESKeySpec desKey = new DESKeySpec(key.getBytes());
            SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(DES);
            SecretKey securekey = keyFactory.generateSecret(desKey);
            Cipher cipher = Cipher.getInstance(DES);
            cipher.init(Cipher.DECRYPT_MODE, securekey, random);
            byte[] result = cipher.doFinal(content);
            return new String(result);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return null;
    }
}
