package com.gl.reader.service.impl;


import com.gl.reader.constants.Alerts;
import com.gl.reader.dto.Alert;
import com.gl.reader.model.Book;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Month;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.gl.reader.service.ProcessController.BookHashMap;
import static com.gl.reader.service.ProcessController.propertiesReader;


public class CsvCreater {
    static LocalDate currentdate = LocalDate.now();
    static Integer day = currentdate.getDayOfMonth();
    static Month month = currentdate.getMonth();
    static Integer year = currentdate.getYear();
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");

    static Logger logger = LogManager.getLogger(CsvCreater.class);

    public static void makeErrorCsv(String outputLocation, String sourceName, String folderName, String fileName, Set<Book> errorFile) {
        FileWriter fileWriter = null;
        String errorPathTillCurrent = outputLocation + "/" + sourceName + "/" + folderName + "/error/" ;  //+ year + "/" + month + "/" + day + "/"

        try {
            if (errorFile.isEmpty()) {
                logger.info("No record found,So no need to create Error File!!!" + errorFile.size());
                return ;
            }

            // rename file
            createAndRenameFileIfExists(errorPathTillCurrent, fileName);
            if (!errorFile.isEmpty()) {// optimise to not create folder
                fileWriter = new FileWriter(errorPathTillCurrent + fileName);
                fileWriter.append(propertiesReader.fileHeader);
                fileWriter.append(propertiesReader.newLineSeprator);
            }
            for (Book csvf : errorFile) {
                fileWriter.append(String.valueOf(csvf.getIMEI()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getIMSI()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getMSISDN()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(csvf.getTimeStamp()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getProtocol()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getSourceName()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getFileName()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getEventTime()));

                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(sourceName);

                fileWriter.append(propertiesReader.newLineSeprator);
                fileWriter.flush();
            }
            if (fileWriter != null) {
                fileWriter.flush();
                fileWriter.close();
            }
            logger.info("CSV file was created successfully for Error File!!!");

        } catch (Exception e) {
            logger.info("Error in CsvFileWriter for Error File!!!" + e);
            Alert.raiseAlert(Alerts.ALERT_006, Map.of("<e>", e.toString() + " Not able to crete error CSV  ", "<process_name>", "EDR_pre_processor"), 0);
        }
    }


    public static void makeBlacklistErrorCsv(String outputLocation, String sourceName, String folderName, String fileName, Set<Book> errorBlacklistFile) {
        FileWriter fileWriter = null;
        fileName= "BlackListed_"+fileName;
        String errorPathTillCurrent = outputLocation + "/" + sourceName + "/" + folderName + "/error/"  ;

        try {
            // rename file
            createAndRenameFileIfExists(errorPathTillCurrent, fileName);
            if (!errorBlacklistFile.isEmpty()) {// optimise to not create folder
                fileWriter = new FileWriter(errorPathTillCurrent + fileName);
                fileWriter.append(propertiesReader.fileHeader);
                fileWriter.append(propertiesReader.newLineSeprator);
            }
            for (Book csvf : errorBlacklistFile) {
                fileWriter.append(String.valueOf(csvf.getIMEI()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getIMSI()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getMSISDN()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(csvf.getTimeStamp()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getProtocol()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getSourceName()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getFileName()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(String.valueOf(csvf.getEventTime()));
                fileWriter.append(propertiesReader.commaDelimiter);
                fileWriter.append(sourceName);

                fileWriter.append(propertiesReader.newLineSeprator);
                fileWriter.flush();
            }
            if (fileWriter != null) {
                fileWriter.flush();
                fileWriter.close();
            }
            logger.info(" Blacklist CSV file was created successfully File!!!");

        } catch (Exception e) {
            logger.info("Error in CsvFileWriter for errorBlacklistFile File!!!" + e);
            Alert.raiseAlert(Alerts.ALERT_006, Map.of("<e>", e.toString() + " Not able to crete Error BlacklistFile  CSV  ", "<process_name>", "EDR_pre_processor"), 0);
        }
    }






    public static void createAndRenameFileIfExists(String pathTillCurrent, String fileName) throws IOException {
        Path pathDay = Paths.get(pathTillCurrent + "/");
        if (!Files.exists(pathDay)) {
            Files.createDirectories(pathDay);
            logger.info("Directory created for error");
        }
        if (Files.exists(Paths.get(pathTillCurrent + "/" + fileName))) {
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            File sourceFile = new File(pathTillCurrent + "/" + fileName);
            String newName = fileName + "-" + sdf.format(timestamp);
            File destFile = new File(pathTillCurrent + "/" + newName);
            if (sourceFile.renameTo(destFile)) {
                logger.info("File renamed successfully");
            } else {
                logger.info("Failed to rename file");
            }
        }
    }


    public static void makeCsv(String outputLocation, String sourceName, String folderName, String fileName, int returnCount) {
        FileWriter fileWriter = null;
        int i = 1;
        try {
            createAndRenameFileIfExists(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/", fileName);

            if (returnCount == 0) {
                logger.debug("inside non split block");
                fileWriter = new FileWriter(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                fileWriter.append(propertiesReader.fileHeader);
                fileWriter.append(propertiesReader.newLineSeprator);
                for (HashMap.Entry<String, HashMap<String, Book>> csvf : BookHashMap.entrySet()) {
                    // String levelOne = csvf.getKey();
                    for (HashMap.Entry<String, Book> csvf3 : csvf.getValue().entrySet()) {
                        // String levelTwo = csvf2.getKey();
                        fileWriter.append(String.valueOf(csvf3.getValue().getIMEI()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getIMSI()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getMSISDN()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(csvf3.getValue().getTimeStamp()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getProtocol()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getSourceName()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getFileName()));
                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(String.valueOf(csvf3.getValue().getEventTime()));

                        fileWriter.append(propertiesReader.commaDelimiter);
                        fileWriter.append(sourceName);

                        fileWriter.append(propertiesReader.newLineSeprator);
                        fileWriter.flush();
                    }
                }
            } else {
                logger.debug("inside split block");
                int count = 0;
                fileWriter = new FileWriter(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                fileWriter.append(propertiesReader.fileHeader);
                fileWriter.append(propertiesReader.newLineSeprator);

                for (HashMap.Entry<String, HashMap<String, Book>> csvf : BookHashMap.entrySet()) {// String levelOne = csvf.getKey();
                    for (HashMap.Entry<String, Book> csvf3 : csvf.getValue().entrySet()) {// String levelTwo = csvf2.getKey();
                        if (count < returnCount) {
                            // logger.info("count less than return count: " + count);
                            fileWriter.append(String.valueOf(csvf3.getValue().getIMEI()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getIMSI()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getMSISDN()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(csvf3.getValue().getTimeStamp()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getProtocol()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getSourceName()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getFileName()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getEventTime()));

                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(sourceName);

                            fileWriter.append(propertiesReader.newLineSeprator);
                            count++;
                            fileWriter.flush();
                        } else {
                            // logger.info("count greater than split count: " + count);
                            if (Files.exists(Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName))) {
                                File sourceFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                                String newName = fileName + "_00" + i;
                                i++;
                                File destFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + newName);
                                if (sourceFile.renameTo(destFile)) {
                                    logger.info("File split successfully: " + newName);
                                } else {
                                    logger.info("Failed to split file");
                                }
                            }
                            count = 0;
                            fileWriter = new FileWriter(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                            fileWriter.append(propertiesReader.fileHeader);
                            fileWriter.append(propertiesReader.newLineSeprator);
                            fileWriter.append(String.valueOf(csvf3.getValue().getIMEI()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getIMSI()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getMSISDN()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(csvf3.getValue().getTimeStamp()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getProtocol()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getSourceName()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getFileName()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(String.valueOf(csvf3.getValue().getEventTime()));
                            fileWriter.append(propertiesReader.commaDelimiter);
                            fileWriter.append(sourceName);
                            fileWriter.append(propertiesReader.newLineSeprator);
                            count++;
                            fileWriter.flush();
                        }
                    }
                }
                if (Files.exists(Paths.get(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName))) {
                    File sourceFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + fileName);
                    String newName = fileName + "_00" + i++;
                    File destFile = new File(outputLocation + "/" + sourceName + "/" + folderName + "/" + "output/" + newName);
                    if (sourceFile.renameTo(destFile)) {
                        logger.info("File split successfully: " + newName);
                    } else {
                        logger.info("Failed to split file");
                    }
                }
            }
            logger.info("CSV file was created successfully !!!");
        } catch (Exception e) {
            logger.error("Error in CsvFileWriter !!!");
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String exceptionDetails = sw.toString();
            Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
            placeholderMapForAlert.put("<e>", e.toString());
            placeholderMapForAlert.put("<process_name>", "EDR_pre_processor");
            Alert.raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
            logger.error("Alert [ALERT_006] is raised. So, doing nothing." + e);
        } finally {
            try {
                fileWriter.flush();
                fileWriter.close();
            } catch (IOException e) {
                logger.info("Error while flushing/closing fileWriter !!!");
                Map<String, String> placeholderMapForAlert = new HashMap<String, String>();
                placeholderMapForAlert.put("<e>", e.toString());
                placeholderMapForAlert.put("<process_name>", "EDR_pre_processor");
                Alert.raiseAlert(Alerts.ALERT_006, placeholderMapForAlert, 0);
                logger.error("Alert [ALERT_006] is raised. So, doing nothing.");
            }
        }
    }


}