package com.alibaba.datax.hook;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.FileUtil;
import cn.hutool.core.io.IoUtil;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.spi.ErrorCode;
import com.alibaba.datax.common.spi.Hook;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 用于合并文件的hook
 * @author dingyao
 * @date 2020/5/9 17:54
 **/
public class MergeHook implements Hook {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergeHook.class);

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void invoke(Configuration jobConf, Map<String, Number> msg) {
        LOGGER.info("start to do mergeHook ...");
        long start = System.currentTimeMillis();
        String fileName0 = jobConf.getString("job.content[0].writer.parameter.fileName");
        String fileName = fileName0.split("__")[0];
        LOGGER.info("fileName = {}", fileName);
        String targetPath = jobConf.getString("job.content[0].writer.parameter.path");
        LOGGER.info("targetPath = {}", targetPath);
        String sourcePath = jobConf.getString("job.content[0].writer.parameter.tempPath");
        sourcePath = sourcePath == null ? targetPath : sourcePath;
        LOGGER.info("sourcePath = {}", sourcePath);
        List<String> headers = jobConf.getList("job.content[0].writer.parameter.header", String.class);
        LOGGER.info("headers = {}", headers);
        File file = new File(sourcePath);
        File[] files = file.listFiles(f -> f.isFile() && f.getName().startsWith(fileName));
        if (files != null && files.length > 0) {
            LOGGER.info("start to merge, total file : {}", files.length);
            String newFileName = fileName + "_" + DateUtil.format(DateUtil.date(), DatePattern.PURE_DATETIME_FORMAT);
            File target = new File(targetPath, newFileName);
            try {
                target.createNewFile();
            } catch (IOException e) {
                LOGGER.warn("创建 {} 文件失败", target.getName());
                throw DataXException.asDataXException(MergeFileErrorCode.createFileError(), String.format("无法创建文件 [%s]", target.getAbsolutePath()), e);
            }
            String principalName = jobConf.getString("job.content[0].writer.parameter.principalName");
            if (principalName != null) {
                LOGGER.info("开始修改 {} 文件的所属用户至 : {}", fileName, principalName);
                setFilePrincipalName(target, principalName);
            }
            try {
                for (int i = 0; i < files.length; i++) {
                    File source = files[i];
                    boolean skipHeader = CollectionUtil.isNotEmpty(headers) && i > 0;
                    mergeFile(source, target, skipHeader);
                }
            }catch (Exception e) {
                throw DataXException.asDataXException(MergeFileErrorCode.writeFileError(), "合并文件失败", e);
            }finally {
                // 删除临时文件
                Stream.of(files).forEach(f -> {
                    boolean delete = f.delete();
                    LOGGER.info("delete temp file [ {} ] : {}", f.getName(), delete);
                });
            }

        }
        LOGGER.info("end to do mergeHook,total elapsed time is {}s", (System.currentTimeMillis() - start) / 1000);
    }

    private void mergeFile(File source, File target, boolean skipHeader) {
        List<String> lines = FileUtil.readLines(source, Charset.defaultCharset());
        if (skipHeader) {
            String header = lines.remove(0);
            LOGGER.info("skip the [ {} ] header : {}", source.getName(), header);
        }
        if (!FileUtil.exist(target)) {
            throw DataXException.asDataXException(MergeFileErrorCode.fileNotExistError(), String.format("目标文件 [%s] 已被删除,写入失败", target.getName()));
        }
        FileUtil.appendLines(lines, target, Charset.defaultCharset());
    }

    private void setFilePrincipalName(File file, String name) {
        if (name != null && name.length() > 0) {
            Path path = file.toPath();
            try {
                UserPrincipal gluster = path.getFileSystem().getUserPrincipalLookupService().lookupPrincipalByName(name);
                Files.setOwner(path, gluster);
            } catch (IOException e) {
                throw DataXException.asDataXException(
                        MergeFileErrorCode.setFilePrincipalNameError(),
                        String.format("无法修改创建的文件 [%s] 的所属用户 [%s]", file.getName(), name), e);
            }
        }
    }
}
