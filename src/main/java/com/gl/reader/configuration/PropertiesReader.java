package com.gl.reader.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

//@PropertySource("classpath:application.properties")
@Configuration
@PropertySources({
        @PropertySource(value = {"file:application.properties"}, ignoreResourceNotFound = true),
        @PropertySource(value = {"file:configuration.properties"}, ignoreResourceNotFound = true)
})

public class PropertiesReader {


    @Value("${appdbName}")
    public String appdbName;

    @Value("${edrappdbName}")
    public String edrappdbName;

    @Value("${auddbName}")
    public String auddbName;

    @Value("${COMMA-DELIMITER}")
    public String commaDelimiter;

    @Value("${SEMI-COLON-DELIMITER}")
    public String semiColonDelimiter;

    @Value("${NEW-LINE-SEPARATOR}")
    public String newLineSeprator;

    @Value("${FILE-HEADER}")
    public String fileHeader;

    @Value("${FILES-COUNT-PER-REPORT}")
    public Long filesCount;

    @Value("${serverName}")
    public String servername;

    @Value("${EXTENSION}")
    public String extension;

    @Value("${INPUTPATH}")
    public String inputLocation;

    @Value("${OUTPUTPATH}")
    public String outputPath;

    @Value("${ROW-COUNT-FOR-SPLIT}")
    public Integer rowCountForSplit;

    @Value("#{'${REPORT-TYPE}'.split(',')}")
    public Set<String> reportType;

    @Value("#{'${IMS-SOURCE}'.split(',')}")
    public List<String> imsSources;

    @Value("#{'${yyMMddSource}'.split(',')}")
    public List<String> yyMMddSource;

    @Value("#{'${ddMMyySource}'.split(',')}")
    public List<String> ddMMyySource;

    @Value("#{'${ddMMyyyySource}'.split(',')}")
    public List<String> ddMMyyyySource;

    @Value("${ATTRIBUTE-SEPARATOR}")
    public String attributeSeperator;

    @Value("${ERRORPATH}")
    public String errorPath;
    @Value("${PROCESSEDPATH}")
    public String  processedPath;
    @Value("${eirs.alert.url}")
    public String  alertUrl;
}
