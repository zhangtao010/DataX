package com.alibaba.datax.plugin.writer.dbfwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.*;


//TODO writeProxy
public class DbfWriter extends Writer {
    private static final Logger LOG = LoggerFactory.getLogger(Job.class);

    public static class Job extends Writer.Job {
        private Configuration writerSliceConfig = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.validateParameter();
        }

        private void validateParameter() {
            this.writerSliceConfig
                    .getNecessaryValue(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                            DbfWriterErrorCode.REQUIRED_VALUE);
            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH, DbfWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException
                            .asDataXException(
                                    DbfWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                            path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException
                                .asDataXException(
                                        DbfWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("您指定的文件路径 : [%s] 创建失败.",
                                                path));
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        DbfWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }
        }
        // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
        @Override
        public void prepare() {
            String path = this.writerSliceConfig.getString(Key.PATH);
            String fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode truncate, 开始清理 [%s] 下 [%s] 同名文件!",
                        path, fileName));
                File dir = new File(path);
                // warn:需要判断文件是否存在，不存在时，不能删除
                try {
                    if (dir.exists()) {
                        String filePath=path+File.separator+fileName;
                        File dbfFile=new File(filePath);
                        if(dbfFile.exists()){
                            FileUtils.forceDelete(dbfFile);
                        }
                    }
                } catch (NullPointerException npe) {
                    throw DataXException
                            .asDataXException(
                                    DbfWriterErrorCode.Write_FILE_ERROR,
                                    String.format("您配置的目录清空时出现空指针异常 : [%s]",
                                            path), npe);
                } catch (IllegalArgumentException iae) {
                    throw DataXException.asDataXException(
                            DbfWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您配置的目录参数异常 : [%s]", path));
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            DbfWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 : [%s]", path));
                } catch (IOException e) {
                    throw DataXException.asDataXException(
                            DbfWriterErrorCode.Write_FILE_ERROR,
                            String.format("无法清空目录 : [%s]", path), e);
                }
            } else if ("append".equals(writeMode)) {
                LOG.info(String
                        .format("由于您配置了writeMode append, 写入前不做清理工作, [%s] 目录下写入相应文件名前缀  [%s] 的文件",
                                path, fileName));
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info(String.format(
                        "由于您配置了writeMode nonConflict, 开始检查 [%s] 下面的内容", path));
                // warn: check two times about exists, mkdirs
                File dir = new File(path);
                try {
                    if (dir.exists()) {
                        if (dir.isFile()) {
                            throw DataXException
                                    .asDataXException(
                                            DbfWriterErrorCode.ILLEGAL_VALUE,
                                            String.format(
                                                    "您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.",
                                                    path));
                        }
                        // fileName is not null
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        if (filesWithFileNamePrefix.length > 0) {
                            List<String> allFiles = new ArrayList<String>();
                            for (File eachFile : filesWithFileNamePrefix) {
                                allFiles.add(eachFile.getName());
                            }
                            LOG.error(String.format("冲突文件列表为: [%s]",
                                    StringUtils.join(allFiles, ",")));
                            throw DataXException
                                    .asDataXException(
                                            DbfWriterErrorCode.ILLEGAL_VALUE,
                                            String.format(
                                                    "您配置的path: [%s] 目录不为空, 下面存在其他文件或文件夹.",
                                                    path));
                        }
                    } else {
                        boolean createdOk = dir.mkdirs();
                        if (!createdOk) {
                            throw DataXException
                                    .asDataXException(
                                            DbfWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                            String.format(
                                                    "您指定的文件路径 : [%s] 创建失败.",
                                                    path));
                        }
                    }
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            DbfWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("您没有权限查看目录 : [%s]", path));
                }
            } else {
                throw DataXException
                        .asDataXException(
                                DbfWriterErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "仅支持 truncate, append, nonConflict 三种模式, 不支持您配置的 writeMode 模式 : [%s]",
                                        writeMode));
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String filePrefix = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);

            Set<String> allFiles = new HashSet<String>();
            String path = null;
            try {
                path = this.writerSliceConfig.getString(Key.PATH);
                File dir = new File(path);
                allFiles.addAll(Arrays.asList(dir.list()));
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        DbfWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限查看目录 : [%s]", path));
            }

            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name

                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();

                String fullFileName = filePrefix;
                allFiles.add(fullFileName);

                splitedTaskConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                                fullFileName);

                LOG.info(String.format("splited write file name:[%s]",
                        fullFileName));

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            return writerSplitConfigs;
        }

        // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
        @Override
        public void post() {}

        @Override
        public void destroy() {}

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;

        private String fileName;

        private DataxBaseDBFWriter dbfWriter=null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            this.path = this.writerSliceConfig.getString(Key.PATH);
            this.fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
        }



        //TODO 改用连接池，确保每次获取的连接都是可用的（注意：连接可能需要每次都初始化其 session）
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            String fileFullPath = this.buildFilePath();
            LOG.info(String.format("write to file : [%s]", fileFullPath));
            try {
                DbfUtils.createDbf(fileFullPath, lineReceiver);
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        DbfWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件  : [%s]", this.fileName));
            }catch (Exception ex){
                LOG.error(ex.toString(),ex);
                throw DataXException.asDataXException(
                        DbfWriterErrorCode.WRITE_DBF_ERROR,
                        ex.toString());
            }
            LOG.info("end do write");
        }

        private String buildFilePath() {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
                case IOUtils.DIR_SEPARATOR_UNIX:
                    isEndWithSeparator = this.path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR));
                    break;
                case IOUtils.DIR_SEPARATOR_WINDOWS:
                    isEndWithSeparator = this.path.endsWith(String
                            .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                    break;
                default:
                    break;
            }
            if (!isEndWithSeparator) {
                this.path = this.path + IOUtils.DIR_SEPARATOR;
            }
            return String.format("%s%s", this.path, this.fileName);
        }

        @Override
        public void post() {}

        @Override
        public void destroy() {}

    }


}
