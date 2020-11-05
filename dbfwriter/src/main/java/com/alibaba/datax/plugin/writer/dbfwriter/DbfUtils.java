package com.alibaba.datax.plugin.writer.dbfwriter;

import com.alibaba.datax.common.base.DataXResultMetaData;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.google.common.collect.Lists;
import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.util.List;

/**
 * @author zhangtao01
 * @Title: 生成DBF帮助类
 * @date 2020/10/29 13:58
 */
public class DbfUtils {
    private static Logger LOG=LoggerFactory.getLogger(DbfUtils.class);

    private static DBFField[] getDbfFieldList(DataXResultMetaData metaData){
        if(metaData==null){
            throw DataXException.asDataXException(
                    DbfWriterErrorCode.WRITE_DBF_ERROR,"DBF字段名超长，字段信息为空!");
        }
        try {
            DBFField[] fields = new DBFField[metaData.getColumnCount()];
            for (int i = 0; i <metaData.getColumnCount() ; i++) {//数据库是从1开始的
                int dbTypeIndex=metaData.getColumnType(i);
                String dbfLabel=metaData.getColumnLabel(i);
                int dbfLength=metaData.getPrecision(i);
                int dbfDecimal=metaData.getScale(i);

                DBFDataType dbfType=DatabaseFieldTypeConvert.getDatabase2DbfType(dbfLabel,dbTypeIndex);
                if(dbfLabel.length()>10){
                    throw DataXException.asDataXException(
                            DbfWriterErrorCode.WRITE_DBF_FIELD_LEN,
                            String.format("DBF字段名超长，不能超出10个字符，当前字段名：%s",dbfLabel));
                }

                if(dbfType==DBFDataType.CHARACTER){
                    if(dbfLength==-1 || dbfLength>254){
                        dbfLength=254;
                    }
                }else if(dbfType==DBFDataType.NUMERIC){
                    if(dbfLength==0 || dbfLength>32){
                        dbfLength=32;
                    }
                    if(dbfDecimal<0){
                        dbfDecimal=0;
                    }
                }

                DBFField field = new DBFField(dbfLabel,dbfType,dbfLength,dbfDecimal);
                //DBFField field = new DBFField(dbfLabel,dbfType,dbfLength);
                fields[i]=field;

            }
            return fields;
        }catch (Exception ex){
            LOG.error(ex.toString(),ex);
        }
        return null;
    }
    public static void createDbf(String dbfPath, RecordReceiver lineReceiver){
        DataXResultMetaData dataXResultMetaData = null;
        File file=new File(dbfPath);
        DataxBaseDBFWriter dbfWriter = new DataxBaseDBFWriter(file, Charset.forName("GBK"));
        Record record;
        while ((record = lineReceiver.getFromReader()) != null) {
            if(!dbfWriter.isSetHeader()){
                dataXResultMetaData=lineReceiver.getDataXResultMetaData();
                DBFField[] fields=getDbfFieldList(dataXResultMetaData);
                dbfWriter.setFields(fields);
            }
            Column[] rowData = new Column[record.getColumnNumber()];
            for (int i = 0; i <record.getColumnNumber() ; i++) {
                rowData[i]=record.getColumn(i);
            }
            dbfWriter.addRecord(rowData);
        }
        if(dataXResultMetaData==null){
            DBFField[] fields=getDbfFieldList(lineReceiver.getDataXResultMetaData());
            dbfWriter.setFields(fields);
        }
        dbfWriter.close();
    }
}
