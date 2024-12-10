package com.gl.reader.dto;

import com.gl.reader.constants.Alerts;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static com.gl.reader.service.ProcessController.*;

public class FilePreProcessing {
    static Logger logger = LogManager.getLogger(FilePreProcessing.class);

    public static void insertReportv2(String fileType, String fileName, Long totalRecords, Long totalErrorRecords,
                                      Long totalDuplicateRecords, Long totalOutputRecords, String startTime, String endTime, Float timeTaken,
                                      Float tps, String operatorName, String sourceName, long volume, String tag, Integer FileCount,
                                      Integer headCount, String servername, Long totalBlackListedError) {
        try (Statement stmt = conn.createStatement();) {
            endTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());
            if (fileType.equalsIgnoreCase("O")) {
                headCount = headCount + 1;
            }
            String dateFunc = defaultStringtoDate(procesStart_timeStamp);
            String sql = "Insert into " + edrappdbName
                    + ".cdr_file_pre_processing_detail(FILE_TYPE,TOTAL_RECORD,TOTAL_ERROR_RECORD,TOTAL_DUPLICATE_RECORD,TOTAL_OUTPUT_RECORD,FILE_NAME,START_TIME,END_TIME,TIME_TAKEN,TPS,OPERATOR_NAME,SOURCE_NAME,VOLUME,TAG,FILE_COUNT , HEAD_COUNT ,servername, total_blacklist_record )"
                    + "values(   '" + fileType + "'," + totalRecords + "," + totalErrorRecords + ","
                    + totalDuplicateRecords + "," + totalOutputRecords + ",'" + fileName + "'," + defaultStringtoDate(startTime) + ","
                    + defaultStringtoDate(endTime) + "," + timeTaken + "," + tps + ",'" + operatorName + "','" + sourceName + "'," + volume
                    + ",'" + tag + "'," + FileCount + "  ," + headCount + " , '" + servername + "' , '" + totalBlackListedError + "'        )";
            logger.info("Inserting for FileTypes {} , SQL [{}]   :: ",fileType, sql);
            stmt.executeUpdate(sql);
        } catch (Exception e) {
            Alert.raiseAlert(Alerts.ALERT_006, Map.of("<e>", "not able to insert in file_pre_processing_detail " + e.toString() + ". in   ", "<process_name>", "EDR_pre_processor"), 0);
        }
    }


    public static String defaultStringtoDate(String date1) {
        if (conn.toString().contains("oracle")) {
            return "to_timestamp('" + date1 + "','YYYY-MM-DD HH24:MI:SS')";
        } else {
            return "'" + date1 + "'";
        }
    }




}
