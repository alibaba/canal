package com.alibaba.otter.canal.parse.inbound.mysql.rds.request;

import java.net.URLDecoder;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that the Signature parameter of an RDS OpenAPI request is URL encoded.
 * <p>
 * Background: the base64 output of HMAC-SHA1 may contain '+'. When the raw value is appended to the query string,
 * the server decodes '+' as a space, the signature is corrupted, and some requests fail randomly with
 * IncompleteSignature.
 */
public class AbstractRequestSignatureTest {

    @Test
    public void testBuildSignatureParameterEncode() throws Exception {
        Assert.assertEquals("Signature=ab%2Bcd%2Fef%3D", request().buildSignatureParameter("ab+cd/ef="));
        Assert.assertEquals("Signature=YWJj", request().buildSignatureParameter("YWJj"));
    }

    @Test
    public void testRequestStringSignatureIsUrlEncoded() throws Exception {
        DescribeBinlogFilesRequest request = request();
        request.setRdsInstanceId("rm-test-instance-id");
        request.setPageNumber(18);
        request.setPageSize(500);
        Date now = new Date();
        request.setStartDate(new Date(now.getTime() - 30L * 24 * 60 * 60 * 1000));
        request.setEndDate(now);

        int decodedLength = -1;
        int plusCount = 0;
        for (int i = 0; i < 300; i++) {
            // Change one custom parameter to get a different signature, so samples whose base64 contains '+' are covered
            request.putQueryString("probe", "p" + i);
            String requestString = request.buildRequestString();

            int index = requestString.lastIndexOf("Signature=");
            Assert.assertTrue(requestString, index > 0);
            String signatureParam = requestString.substring(index + "Signature=".length());

            // Raw '+', '/' and '=' of the base64 result must not leak into the query string
            Assert.assertFalse("raw '+' leaked: " + signatureParam, signatureParam.contains("+"));
            Assert.assertFalse("raw '/' leaked: " + signatureParam, signatureParam.contains("/"));
            Assert.assertFalse("raw '=' leaked: " + signatureParam, signatureParam.contains("="));

            String decoded = URLDecoder.decode(signatureParam, "UTF-8");
            Assert.assertTrue(decoded, decoded.endsWith("="));
            decodedLength = decoded.length();
            if (decoded.contains("+")) {
                plusCount++;
            }
        }
        // HMAC-SHA1 -> 20 bytes -> 28 base64 characters
        Assert.assertEquals(28, decodedLength);
        // Make sure the samples really cover '+', otherwise this case does not protect against a regression
        Assert.assertTrue("expected at least one base64 signature containing '+'", plusCount > 0);
    }

    private DescribeBinlogFilesRequest request() {
        DescribeBinlogFilesRequest request = new DescribeBinlogFilesRequest();
        request.setEndPoint("rds.aliyuncs.com");
        request.setAccessKeyId("testAccessKeyId");
        request.setAccessKeySecret("testAccessKeySecret");
        return request;
    }
}
