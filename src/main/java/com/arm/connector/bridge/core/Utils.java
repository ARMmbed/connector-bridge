/**
 * @file    Utils.java
 * @brief   misc collection of static utilities
 * @author  Doug Anson
 * @version 1.0
 * @see
 *
 * Copyright 2015. ARM Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.arm.connector.bridge.core;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.ByteBuffer;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

/**
 * Static support utilities
 * @author Doug Anson
 */
public class Utils {
    // static variables
    private static char[] hexArray = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    private static String __cache_hash = null;
    private static String _externalIPAddress = null;

    // get local timezone offset from UTC in milliseconds
    public static int getUTCOffset() {
        TimeZone tz = TimeZone.getDefault();
        Calendar cal = GregorianCalendar.getInstance(tz);
        return tz.getOffset(cal.getTimeInMillis());
    }

    // get the local time in seconds since Jan 1 1970
    public static int getLocalTime() {
        int utc = (int) (System.currentTimeMillis() / 1000);
        int localtime = utc;

        return localtime;
    }

    // get UTC time in seconds since Jan 1 1970
    public static long getUTCTime() {
        return Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis() - Utils.getUTCOffset();
    }

    // get our base URL
    public static String getBaseURL(String endpoint, HttpServletRequest request) {
        String url = "";
        try {
            url = request.getRequestURL().toString().replace(request.getRequestURI().substring(1), request.getContextPath());
            url += "//" + endpoint;
            url = url.replace("://", "_TEMP_");
            url = url.replace("//", "/");
            url = url.replace("_TEMP_", "://");
        }
        catch (Exception ex) {
            url = request.getRequestURL().toString();
        }
        return url;
    }

    // convert boolean to string
    public static String booleanToString(boolean val) {
        if (val) {
            return "true";
        }
        return "false";
    }

    // convert string to boolean
    public static boolean stringToBoolean(String val) {
        boolean bval = false;
        if (val != null && val.equalsIgnoreCase("true")) {
            bval = true;
        }
        return bval;
    }

    // START DATE FUNCTIONS
    
    // get the current date and time
    public static java.util.Date now() {
        java.util.Date rightnow = new java.util.Date(System.currentTimeMillis());
        return rightnow;
    }

    // convert a JAVA Date to a SQL Timestamp and back
    public static java.sql.Timestamp convertDate(java.util.Date date) {
        java.sql.Timestamp sql_date = new java.sql.Timestamp(date.getTime());
        sql_date.setTime(date.getTime());
        return sql_date;
    }

    // convert SQL Date to Java Date
    public static java.util.Date convertDate(java.sql.Timestamp date) {
        java.util.Date java_date = new java.util.Date(date.getTime());
        return java_date;
    }

    // convert a Date to a String (java)
    public static String dateToString(java.util.Date date) {
        return Utils.dateToString(date, "MM/dd/yyyy HH:mm:ss");
    }

    // Date to Date String
    public static String dateToString(java.util.Date date, String format) {
        if (date != null) {
            DateFormat df = new SimpleDateFormat(format);
            return df.format(date);
        }
        else {
            return "[no date]";
        }
    }

    // convert a SQL Timestamp to a String (SQL)
    public static String dateToString(java.sql.Timestamp timestamp) {
        if (timestamp != null) {
            return Utils.dateToString(new java.util.Date(timestamp.getTime()));
        }
        else {
            return "[no date]";
        }
    }

    // convert a Date to a String (SQL)
    public static String dateToString(java.sql.Date date) {
        if (date != null) {
            return Utils.dateToString(new java.util.Date(date.getTime()));
        }
        else {
            return "[no date]";
        }
    }

    // convert a String (Java) to a java.util.Date object
    public static java.util.Date stringToDate(ErrorLogger err, String str_date) {
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
            String stripped = str_date.replace('"', ' ').trim();
            return dateFormat.parse(stripped);
        }
        catch (ParseException ex) {
            err.warning("Unable to parse string date: " + str_date + " to format: \"MM/dd/yyyy HH:mm:ss\"", ex);
        }
        return null;
    }

    // END DATE FUNCTIONS
    
    // Hex String to ByteBuffer or byte[]
    public static ByteBuffer hexStringToByteBuffer(String str) {
        return ByteBuffer.wrap(Utils.hexStringToByteArray(str));
    }

    // hex String to ByteArray
    public static byte[] hexStringToByteArray(String str) {
        int len = str.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(str.charAt(i), 16) << 4) + Character.digit(str.charAt(i + 1), 16));
        }
        return data;
    }

    // convert a hex byte array to a string
    public static String bytesToHexString(ByteBuffer bytes) {
        return Utils.bytesToHexString(bytes.array());
    }

    // ByteArray to hex string
    public static String bytesToHexString(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; ++j) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    // read in a HTML file
    public static String readHTMLFileIntoString(HttpServlet svc, ErrorLogger err, String filename) {
        try {
            String text = null;
            String file = "";
            ServletContext context = svc.getServletContext();
            try (InputStream is = context.getResourceAsStream("/" + filename); InputStreamReader isr = new InputStreamReader(is); BufferedReader reader = new BufferedReader(isr)) {
                while ((text = reader.readLine()) != null) {
                    file += text;
                }
            }
            return file;
        }
        catch (IOException ex) {
            err.critical("error while trying to read HTML template: " + filename, ex);
        }
        return null;
    }
    
    // decode CoAP payload Base64
    public static String decodeCoAPPayload(String payload) {
        String decoded = null;
        
        try {
            String b64_payload = payload.replace("\\u003d", "=");
            Base64 decoder = new Base64();
            byte[] data = decoder.decode(b64_payload);
            decoded = new String(data);
        }
        catch (Exception ex) {
            decoded = "<unk>";
        }
        
        return decoded;
    }
    
    // create a URL-safe Token
    public static String createURLSafeToken(String seed) {
        try {
            byte[] b64 = Base64.encodeBase64(seed.getBytes());
            return new String(b64);
        }
        catch (Exception ex) {
            return "exception";
        }
    }
    
    // create Authentication Hash
    public static String createHash(String data) {
        try {
            if (data == null) {
               return "none";
            }
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(data.getBytes());
            String hex = Hex.encodeHexString(digest);
            return Base64.encodeBase64URLSafeString(hex.getBytes());
        }
        catch (NoSuchAlgorithmException ex) {
            return "none";
        }
    }
    
    // validate the Authentication Hash
    public static boolean validateHash(String header_hash,String calc_hash) {
        boolean validated = false;
        try {
            if (Utils.__cache_hash == null) {
                validated = (header_hash != null && calc_hash != null && calc_hash.equalsIgnoreCase(header_hash) == true);
                if (validated && Utils.__cache_hash == null) {
                    Utils.__cache_hash = header_hash;
                }
            }
            else {
                validated = (header_hash != null && Utils.__cache_hash != null && Utils.__cache_hash.equalsIgnoreCase(header_hash) == true);
            }
            return validated;
        }
        catch (Exception ex) {
            return false;
        }
    }
    
    // get our external IP Address
    public static String getExternalIPAddress() {
        return Utils.getExternalIPAddress(false,null);
    }
    
    // get our external IP Address
    public static String getExternalIPAddress(boolean use_gw_address,String gw_address) {
        if (Utils._externalIPAddress == null) {
            if (use_gw_address == true && gw_address != null && gw_address.length() > 0) {
                Utils._externalIPAddress = gw_address;
            }
            else {
                BufferedReader in = null;
                try {
                    URL whatismyip = new URL("http://checkip.amazonaws.com");
                    in = new BufferedReader(new InputStreamReader(whatismyip.openStream()));
                    Utils._externalIPAddress = in.readLine();
                    in.close();
                }
                catch (Exception ex) {
                    try {
                        if (in != null) in.close();
                    }
                    catch(Exception ex2) {
                        // silent
                    }
                }
            }
        }
        return Utils._externalIPAddress;
    }
    
    // convert a InputStream to a String
    public static String convertStreamToString(java.io.InputStream is) {
        java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
    
    /**
     * Execute the AWS CLI
     * @param logger - ErrorLogger instance
     * @param args - arguments for the AWS CLI 
     * @return response from CLI action
     */
    public static String awsCLI(ErrorLogger logger,String args) {
       // construct the arguments
       String cmd = "./aws " + args;
       String response = null;
       String error = null;
       
       try {
           // invoke the AWS CLI
           Process proc = Runtime.getRuntime().exec(cmd);
           response = Utils.convertStreamToString(proc.getInputStream());
           error= Utils.convertStreamToString(proc.getErrorStream());
           
           // wait to completion
           proc.waitFor();
           int status = proc.exitValue();
           
           // DEBUG
           if (status != 0) {
               // non-zero exit status
               logger.info("AWS CLI: Invoked: " + cmd);
               logger.info("AWS CLI: Response: " + response);
               logger.info("AWS CLI: Error: " + error);
               logger.info("AWS CLI: Exit Code: " + status);
           }
           else {
               // successful exit status
               logger.info("AWS CLI: Invoked: " + cmd);
               logger.info("AWS CLI: Response: " + response);
               logger.info("AWS CLI: Exit Code: " + status);
           }
       } 
       catch (IOException | InterruptedException ex) {
           logger.warning("AWS CLI: Exception for command: " + cmd,ex);
           response = null;
       }
       
       // return the resposne
       return response;
    }
   
    // escape chars utility
    public static String escapeChars(String str) {
        return str.replace("\\n","");
    }
    
    // Create CA Root certificate
    public static X509Certificate createCACertificate(ErrorLogger logger) {
        // Root CA for AWS IoT (5/6/2016)
        // https://www.symantec.com/content/en/us/enterprise/verisign/roots/VeriSign-Class%203-Public-Primary-Certification-Authority-G5.pem
        String pem =    "-----BEGIN CERTIFICATE-----" +
                            "MIIE0zCCA7ugAwIBAgIQGNrRniZ96LtKIVjNzGs7SjANBgkqhkiG9w0BAQUFADCB" +
                            "yjELMAkGA1UEBhMCVVMxFzAVBgNVBAoTDlZlcmlTaWduLCBJbmMuMR8wHQYDVQQL" +
                            "ExZWZXJpU2lnbiBUcnVzdCBOZXR3b3JrMTowOAYDVQQLEzEoYykgMjAwNiBWZXJp" +
                            "U2lnbiwgSW5jLiAtIEZvciBhdXRob3JpemVkIHVzZSBvbmx5MUUwQwYDVQQDEzxW" +
                            "ZXJpU2lnbiBDbGFzcyAzIFB1YmxpYyBQcmltYXJ5IENlcnRpZmljYXRpb24gQXV0" +
                            "aG9yaXR5IC0gRzUwHhcNMDYxMTA4MDAwMDAwWhcNMzYwNzE2MjM1OTU5WjCByjEL" +
                            "MAkGA1UEBhMCVVMxFzAVBgNVBAoTDlZlcmlTaWduLCBJbmMuMR8wHQYDVQQLExZW" +
                            "ZXJpU2lnbiBUcnVzdCBOZXR3b3JrMTowOAYDVQQLEzEoYykgMjAwNiBWZXJpU2ln" +
                            "biwgSW5jLiAtIEZvciBhdXRob3JpemVkIHVzZSBvbmx5MUUwQwYDVQQDEzxWZXJp" +
                            "U2lnbiBDbGFzcyAzIFB1YmxpYyBQcmltYXJ5IENlcnRpZmljYXRpb24gQXV0aG9y" +
                            "aXR5IC0gRzUwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCvJAgIKXo1" +
                            "nmAMqudLO07cfLw8RRy7K+D+KQL5VwijZIUVJ/XxrcgxiV0i6CqqpkKzj/i5Vbex" +
                            "t0uz/o9+B1fs70PbZmIVYc9gDaTY3vjgw2IIPVQT60nKWVSFJuUrjxuf6/WhkcIz" +
                            "SdhDY2pSS9KP6HBRTdGJaXvHcPaz3BJ023tdS1bTlr8Vd6Gw9KIl8q8ckmcY5fQG" +
                            "BO+QueQA5N06tRn/Arr0PO7gi+s3i+z016zy9vA9r911kTMZHRxAy3QkGSGT2RT+" +
                            "rCpSx4/VBEnkjWNHiDxpg8v+R70rfk/Fla4OndTRQ8Bnc+MUCH7lP59zuDMKz10/" +
                            "NIeWiu5T6CUVAgMBAAGjgbIwga8wDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8E" +
                            "BAMCAQYwbQYIKwYBBQUHAQwEYTBfoV2gWzBZMFcwVRYJaW1hZ2UvZ2lmMCEwHzAH" +
                            "BgUrDgMCGgQUj+XTGoasjY5rw8+AatRIGCx7GS4wJRYjaHR0cDovL2xvZ28udmVy" +
                            "aXNpZ24uY29tL3ZzbG9nby5naWYwHQYDVR0OBBYEFH/TZafC3ey78DAJ80M5+gKv" +
                            "MzEzMA0GCSqGSIb3DQEBBQUAA4IBAQCTJEowX2LP2BqYLz3q3JktvXf2pXkiOOzE" +
                            "p6B4Eq1iDkVwZMXnl2YtmAl+X6/WzChl8gGqCBpH3vn5fJJaCGkgDdk+bW48DW7Y" +
                            "5gaRQBi5+MHt39tBquCWIMnNZBU4gcmU7qKEKQsTb47bDN0lAtukixlE0kF6BWlK" +
                            "WE9gyn6CagsCqiUXObXbf+eEZSqVir2G3l6BFoMtEMze/aiCKm0oHw0LxOXnGiYZ" +
                            "4fQRbxC1lfznQgUy286dUV4otp6F01vvpX1FQHKOtw5rDgb7MzVIcbidJ4vEZV8N" +
                            "hnacRHr2lVz2XTIIM6RUthg/aFzyQkqFOFSDX9HoLPKsEdao7WNq" +
                        "-----END CERTIFICATE-----";
        
        return Utils.createX509CertificateFromPEM(logger,pem,"X509");
    }
    
    // create a Keystore
    public static String createKeystore(ErrorLogger logger,String base,String sep,String filename,X509Certificate cert,PrivateKey priv_key,String pw) {
        String basedir = base + File.separator + sep;
        String keystore_filename = basedir + File.separator + filename;
        
        try {
            // first create the directory if it does not exist
            File file = new File(basedir);
            
            // make the directories
            logger.info("createKeystore: Making directories for keystore...");
            file.mkdirs();
            
            // create the KeyStore
            logger.info("createKeystore: Creating keystore: " + keystore_filename);
            file = new File(keystore_filename);
            if (file.createNewFile()){
	        logger.info("createKeystore: keystore created:  " + keystore_filename);
	    }
            else {
	        logger.warning("createKeystore: keystore already exists " + keystore_filename);
	    }
            
            // store data into the keystore
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(null,pw.toCharArray());
            
            // set the certificate, priv and pub keys
            if (cert != null) {
                Certificate[] cert_list = new Certificate[2];
                cert_list[0] = cert;
                cert_list[1] = Utils.createCACertificate(logger);
                
                ks.setCertificateEntry("aws",cert_list[0]);
                ks.setCertificateEntry("verisign", cert_list[1]);
                
                if (priv_key != null) {
                    try {
                        ks.setKeyEntry("privkey",priv_key,pw.toCharArray(),cert_list);
                    }
                    catch (Exception ex2) {
                        logger.warning("createKeystore: Exception during priv addition... not added to keystore",ex2);
                    }
                }
                else {
                    logger.warning("createKeystore: privkey is NULL... not added to keystore");
                }
            }
            else {
                logger.warning("createKeystore: certificate is NULL... not added to keystore");
            }

            try (FileOutputStream fos = new FileOutputStream(keystore_filename)) {
                // store away the keystore content
                ks.store(fos,pw.toCharArray());
                
                // close
                fos.flush();
            }
        }
        catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException ex) {
            logger.warning("createKeystore: Unable to create keystore: " + keystore_filename,ex);
        }
       
        // return the keystore filename
        return keystore_filename;
    }
    
    // generate a keystore password
    public static String generateKeystorePassword(String base_pw,String salt) {
        // XXX TO DO
        return base_pw;
    }
    
    // remove the keystore from the filesystem
    public static void deleteKeystore(ErrorLogger logger,String filename,String keystore_name) {
        try {
            // DEBUG
            logger.info("deleteKeystore: deleting keystore: " + filename);
            
            // Delete the KeyStore
            File file = new File(filename);
            if(file.delete()){
                // success
                logger.info(file.getName() + " is deleted!");
            }
            else {
                // failure
                logger.warning("Delete operation is failed: " + filename);
            }
            
            // Create the parent directory
            String basedir = filename.replace("/" + keystore_name,"");
            
            // DEBUG
            logger.info("deleteKeystore: deleting keystore parent directory: " + basedir);
            
            // Delete the Base Directory
            file = new File(basedir);
            if (file.isDirectory()) {
                if (file.delete()) {
                    // success
                    logger.info(basedir + " is deleted!");
                }
                else {
                    // failure
                    logger.warning("Delete operation is failed : " + basedir);
                }
            }
            
        }
        catch (Exception ex) {
            // exception caught
            logger.warning("deleteKeystore: Exception during deletion of keystore: " + filename,ex);
        }
    }
    
    // Create X509Certificate from PEM
    static public X509Certificate createX509CertificateFromPEM(ErrorLogger logger,String pem, String cert_type) {
        try {
            String temp = Utils.escapeChars(pem);
            String certPEM = temp.replace("-----BEGIN CERTIFICATE-----", "");
            certPEM = certPEM.replace("-----END CERTIFICATE-----", "");
            
            // DEBUG
            //logger.info("createX509CertificateFromPEM: " + certPEM);

            Base64 b64 = new Base64();
            byte [] decoded = b64.decode(certPEM);
            
            CertificateFactory cf = CertificateFactory.getInstance(cert_type);
            return (X509Certificate)cf.generateCertificate(new ByteArrayInputStream(decoded));
        }
        catch (Exception ex) {
            // exception caught
            logger.warning("createX509CertificateFromPEM: Exception during private key gen",ex);
        }
        return null;
    }
    
    // Create PrivateKey from PEM
    static public PrivateKey createPrivateKeyFromPEM(ErrorLogger logger,String pem, String algorithm) {
        try {
            String temp = Utils.escapeChars(pem);
            String privKeyPEM = temp.replace("-----BEGIN RSA PRIVATE KEY-----","");
            privKeyPEM = privKeyPEM.replace("-----END RSA PRIVATE KEY-----", "");
            
            // DEBUG
            //logger.info("createPrivateKeyFromPEM: " + privKeyPEM);
            
            Base64 b64 = new Base64();
            byte [] decoded = b64.decode(privKeyPEM);
            
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(decoded);
            KeyFactory kf = KeyFactory.getInstance(algorithm);
            return kf.generatePrivate(spec);
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
            // exception caught
            logger.warning("createPrivateKeyFromPEM: Exception during private key gen",ex);
        }
        return null;
    }

     // Create PublicKey from PEM
    static public PublicKey createPublicKeyFromPEM(ErrorLogger logger,String pem, String algorithm) {
        try {
            String temp = Utils.escapeChars(pem);
            String publicKeyPEM = temp.replace("-----BEGIN PUBLIC KEY-----", "");
            publicKeyPEM = publicKeyPEM.replace("-----END PUBLIC KEY-----", "");
            
            // DEBUG
            //logger.info("createPublicKeyFromPEM: " + publicKeyPEM);

            Base64 b64 = new Base64();
            byte [] decoded = b64.decode(publicKeyPEM);

            X509EncodedKeySpec spec = new X509EncodedKeySpec(decoded);
            KeyFactory kf = KeyFactory.getInstance(algorithm);
            return kf.generatePublic(spec);
        }
        catch (NoSuchAlgorithmException | InvalidKeySpecException ex) {
            // exception caught
            logger.warning("createPublicKeyFromPEM: Exception during public key gen",ex);
        }
        return null;
    }
    
    // Create a String Array from the TLV
    public static String[] formatTLVToStringArray(byte tlv[]) {
        String tlv_str = "";
        
        // convert to array, removing separators... 
        for(int i=0;i<tlv.length;++i) {
            if (tlv[i] == 63) {
                tlv_str += " ";
            }
            else {
                tlv_str += (char)tlv[i];
            }
        }
        
        // trim
        tlv_str = tlv_str.trim();
        
        // split into an array
        String tlv_split[] = tlv_str.split(" ");
        
        // trim array elements
        for(int i=0;i<tlv_split.length;++i) tlv_split[i] = tlv_split[i].trim();
        
        // return the cleaned up array
        return tlv_split;
    }
    
    // ensure that an HTTP response code is in the 200's
    public static boolean httpResponseCodeOK(int code) {
        int check = code - 200;
        if (check >= 0 && check < 100) {
            return true;
        }
        return false;
    }
    
    // re-type a JSON Map
    public static Map retypeMap(Map json,TypeDecoder decoder) {
        HashMap<String,Object> remap = new HashMap<>();
        
        // iterate through the existing map and re-type each entry
        Iterator it = json.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            Object value = decoder.getFundamentalValue(pair.getValue());
            if (value instanceof Double || value instanceof Integer || value instanceof String) {
                // fundamental types get mapped directly
                remap.put((String)pair.getKey(),value);
            }
            else {
                // this is a complex embedded type...
                if (value instanceof Map) {
                    // embedded JSON - directly recurse.
                    remap.put((String)pair.getKey(),Utils.retypeMap((Map)value,decoder));
                }
                if (value instanceof List) {
                    // list of embedded JSON - loop and recurse each Map(i)... 
                    List list = (List)value;
                    for(int i=0;i<list.size();++i) {
                        list.set(i, Utils.retypeMap((Map)(Map)list.get(i),decoder));
                    }
                    // replace list with new one...
                    remap.put((String)pair.getKey(),list);
                }
            }
        }
        
        return remap;
    }
}
