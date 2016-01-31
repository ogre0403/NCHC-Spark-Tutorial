package org.nchc.spark.java.sample;


import java.io.Serializable;

/**
 * This class represents an Apache access log line.
 * See http://httpd.apache.org/docs/2.2/logs.html for more details.
 */
public class ApacheAccessLog implements Serializable {

    private String ipAddress;
    private String clientIdentd;
    private String userID;
    private String dateTimeString;
    private String method;
    private String endpoint;
    private String protocol;
    private int responseCode;
    private long contentSize;

    public ApacheAccessLog(String ipAddress, String clientIdentd, String userID,
                            String dateTime, String method, String endpoint,
                            String protocol, String responseCode,
                            String contentSize) {
        this.ipAddress = ipAddress;
        this.clientIdentd = clientIdentd;
        this.userID = userID;
        this.dateTimeString = dateTime;
        this.method = method;
        this.endpoint = endpoint;
        this.protocol = protocol;
        this.responseCode = Integer.parseInt(responseCode);
        this.contentSize = Long.parseLong(contentSize);
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getClientIdentd() {
        return clientIdentd;
    }

    public String getUserID() {
        return userID;
    }

    public String getDateTimeString() {
        return dateTimeString;
    }

    public String getMethod() {
        return method;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getProtocol() {
        return protocol;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public long getContentSize() {
        return contentSize;
    }

    public double getContentSizeDouble(){
        return (double)contentSize;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public void setClientIdentd(String clientIdentd) {
        this.clientIdentd = clientIdentd;
    }

    public void setUserID(String userID) {
        this.userID = userID;
    }

    public void setDateTimeString(String dateTimeString) {
        this.dateTimeString = dateTimeString;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setResponseCode(int responseCode) {
        this.responseCode = responseCode;
    }

    public void setContentSize(long contentSize) {
        this.contentSize = contentSize;
    }

    @Override public String toString() {
        return String.format("%s %s %s [%s] \"%s %s %s\" %s %s",
                ipAddress, clientIdentd, userID, dateTimeString, method, endpoint,
                protocol, responseCode, contentSize);
    }
}