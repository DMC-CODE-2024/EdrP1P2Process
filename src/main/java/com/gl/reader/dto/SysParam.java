package com.gl.reader.dto;

 import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.gl.reader.configuration.AlertService.raiseAlert;
import static com.gl.reader.service.ProcessController.appdbName;


public class SysParam {
    static Logger logger = LogManager.getLogger(SysParam.class);

    public static Map<String, String> imeiLengthValueCheck(Connection conn) {
        Map<String, String> ImeiCheckMap = new HashMap<String, String>();
        String sql = "select tag , value  from " + appdbName + ".sys_param where tag in  " +
                " ('EDR_IMEI_LENGTH_CHECK' ,'EDR_IMEI_LENGTH_VALUE','EDR_NULL_IMEI_CHECK','EDR_NULL_IMEI_REPLACE_PATTERN'   , 'EDR_ALPHANUMERIC_IMEI_CHECK')";
        logger.info("Fetching details " + sql);
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql);) {
            while (rs.next()) {
                ImeiCheckMap.put(rs.getString("tag"), rs.getString("value"));
            }
        } catch (Exception e) {
            logger.error("Not able to access details from sys_param " + e);
            raiseAlert("alert006", e.toString());
            System.exit(0);
        }
        return ImeiCheckMap;
    }


    public static List<String> getFilePatternByOperatorSource(Connection conn, String operator, String sourceName) {
        String sql = "select  value  from " + appdbName + ".sys_param where tag =  '" + operator.toUpperCase() + "_" + sourceName.toUpperCase() + "_FILE_PATTERN'   ";
        logger.info("Fetching details for FILE_PATTERN " + sql);
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql);) {
            String response = "null";
            while (rs.next()) {
                response = rs.getString("value");
            }
            logger.info("Fetching response  " + response);
            return Arrays.asList(response.split(","));
        } catch (Exception e) {
            logger.error("Not able to access details from sys_param " + e);
            raiseAlert("alert006", e.toString());
            System.exit(0);
        }
        return null;
    }


}
