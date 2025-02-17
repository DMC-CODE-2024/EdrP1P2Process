package com.gl.reader.service.impl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import static com.gl.reader.service.ProcessController.*;

@Component
public class FileReaderService {
    static Logger logger = LogManager.getLogger(FileReaderService.class);

    static List<String> pattern = new ArrayList<>();

    public static String getArrivalTimeFromFilePattern(String source, String fileName) {
        String date = "";

        if (!source.contains("all")) {
            pattern.addAll(file_patterns);
        }

        logger.debug("fileName" + fileName);
        for (String filePattern : pattern) {
            String[] attributes = filePattern.split("-", -1);
            if (fileName.contains(attributes[0])) {
                date = fileName.substring(fileName.indexOf(attributes[0]) + Integer.parseInt(attributes[1]),
                        fileName.indexOf(attributes[0]) + Integer.parseInt(attributes[1])
                                + Integer.parseInt(attributes[2]));
            }
        }
        logger.debug("date-" + date);
        String imei_arrivalTime = null;
        try {
            String dateType = "yyyyMMdd";
            if (propertiesReader.ddMMyyyySource.contains(source)) {
                dateType = "ddMMyyyy";
            } else if (propertiesReader.yyMMddSource.contains(source)) {
                dateType = "yyMMdd";
            } else if (propertiesReader.ddMMyySource.contains(source)) {
                dateType = "ddMMddyy";
            }
            imei_arrivalTime = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat(dateType).parse(date));
        } catch (Exception e) {
            logger.error(fileName + " Unable to parse Date ,Defined Pattern" + date + ", Error " + e);
        }
        logger.debug("File arrival date  " + imei_arrivalTime);
        return imei_arrivalTime;
    }


    public static void moveFileToError(String fileName) throws IOException {
        Path pathFile = Paths.get(errorPath + "/" + operatorName + "/" + sourceName + "/" );
        if (!Files.exists(pathFile)) {
            Files.createDirectories(pathFile);
            logger.info("Directory created");
        }
        // rename file
        if (Files.exists(Paths.get(errorPath + "/" + operatorName + "/" + sourceName + "/" + fileName))) {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            File sourceFile = new File(errorPath + "/" + operatorName + "/" + sourceName + "/" + fileName);
            String newName = fileName + "-" + sdf.format(timestamp);
            File destFile = new File(errorPath + "/" + operatorName + "/" + sourceName + "/" + newName);
            if (sourceFile.renameTo(destFile)) {
                logger.info("File renamed successfully");
            } else {
                logger.info("Failed to rename file");
            }
        }
        // move file
        Path temp = null;
        try {
            temp = Files.move(Paths.get(inputLocation + "/" + operatorName + "/" + sourceName + "/" + fileName),
                    Paths.get(errorPath + "/" + operatorName + "/" + sourceName + "/" + fileName) , StandardCopyOption.REPLACE_EXISTING );
        } catch (Exception e) {
            logger.warn(" File   " + fileName + " Not able to move ");
        }
        if (temp != null) {
            logger.info("File moved in Error Folder successfully");
        } else {
            logger.warn("Failed to move the file in Error Folder" + fileName);
        }
    }

}


