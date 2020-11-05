package com.alibaba.datax.plugin.writer.dbfwriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.linuxense.javadbf.DBFDataType;

import java.sql.Types;

/**
 * @author zhangtao01
 * @date 2020/11/220:11
 */
public class DatabaseFieldTypeConvert {
    public static DBFDataType getDatabase2DbfType(String fieldLabel,int fieldType){
        switch (fieldType) {

            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return DBFDataType.CHARACTER;

            case Types.CLOB:
            case Types.NCLOB:
                return DBFDataType.CHARACTER;

            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.INTEGER:
            case Types.BIGINT:
                return DBFDataType.NUMERIC;

            case Types.NUMERIC:
            case Types.DECIMAL:
                return DBFDataType.NUMERIC;

            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return DBFDataType.NUMERIC;

            case Types.TIME:
                return DBFDataType.DATE;

            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                return DBFDataType.DATE;

            case Types.TIMESTAMP:
                return DBFDataType.CHARACTER;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return DBFDataType.CHARACTER;

            // warn: bit(1) -> Types.BIT 可使用BoolColumn
            // warn: bit(>1) -> Types.VARBINARY 可使用BytesColumn
            case Types.BOOLEAN:
            case Types.BIT:
                return DBFDataType.LOGICAL;

            case Types.NULL:
                return DBFDataType.CHARACTER;

            default:
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.UNSUPPORTED_TYPE,
                                String.format(
                                        "您的配置文件中的列配置信息有误. 因为DataX 不支持数据库读取这种字段类型.  字段名称:[%s] 字段类型:[%d].. 请尝试使用数据库函数将其转换datax支持的类型 或者不同步该字段 .",
                                        fieldLabel,
                                        fieldType));
        }
    }
}
