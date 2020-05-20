package com.alibaba.datax.hook;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author dingyao
 * @date 2020/5/13 13:53
 **/
public class MergeFileErrorCode implements ErrorCode {

    private final String code;

    private final String description;

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    private MergeFileErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    public static MergeFileErrorCode createFileError() {
        return new MergeFileErrorCode("MergeFileError_00", "您配置的文件在创建的时候出现IO异常");
    }

    public static MergeFileErrorCode writeFileError() {
        return new MergeFileErrorCode("MergeFileError_01", "您配置的文件在写入的时候出现IO异常");
    }

    public static MergeFileErrorCode fileNotExistError() {
        return new MergeFileErrorCode("MergeFileError_02", "您配置的文件在写入的时候被人为删除");
    }

    public static MergeFileErrorCode setFilePrincipalNameError() {
        return new MergeFileErrorCode("MergeFileError_02", "您配置的文件在修改所属用户组时出现IO异常");
    }
}
