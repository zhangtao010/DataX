package com.alibaba.datax.plugin.writer.dbfwriter;
import com.alibaba.datax.common.spi.ErrorCode;


/**
 * @author zhangtao01
 * @Title: ${file_name}
 * @Package ${package_name}
 * @Description: ${todo}
 * @date 2020/10/29 11:14
 */
public enum DbfWriterErrorCode implements ErrorCode {
    CONFIG_INVALID_EXCEPTION("DbfWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("DbfWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("DbfWriter-02", "您填写的参数值不合法."),
    Write_FILE_ERROR("DbfWriter-03", "您配置的目标文件在写入时异常."),
    Write_FILE_IO_ERROR("DbfWriter-04", "您配置的文件在写入时出现IO异常."),
    SECURITY_NOT_ENOUGH("DbfWriter-05", "您缺少权限执行相应的文件写入操作."),
    WRITE_DBF_FIELD_LEN("DbfWriter-06", "DBF字段超长."),
    WRITE_DBF_ERROR("DbfWriter-99", "写入DBF异常.");

    private final String code;
    private final String description;

    private DbfWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}