package com.alibaba.datax.plugin.writer.dbfwriter;

import com.alibaba.datax.common.element.Column;
import com.linuxense.javadbf.DBFDataType;
import com.linuxense.javadbf.DBFException;
import com.linuxense.javadbf.DBFField;
import com.linuxense.javadbf.DBFWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;

/**
 * @author zhangtao01
 * @Description: 实现DBF类型转换
 * @date 2020/11/2 10:01
 */
public class DataxBaseDBFWriter extends DBFWriter {
    private static Logger LOG= LoggerFactory.getLogger(DbfUtils.class);

    private DBFField[] fields=null;

    public DataxBaseDBFWriter(File dbfFile, Charset charset) {
        super(dbfFile,charset);
    }

    @Override
    public void setFields(DBFField[] fields) {
        this.fields=fields;
        super.setFields(fields);
    }

    public boolean isSetHeader(){
        return fields!=null;
    }

    public DBFField getField(int fieldIndex){
        if(fields == null){
            LOG.error("字段没初始化!");
            return null;
        }
        if(fields.length>fieldIndex){
            return fields[fieldIndex];
        }else{
            LOG.error("超出字段索引");
            return null;
        }
    }

    private Object getRecordAsValue(Column column,int index){
        DBFDataType type = getField(index).getType();
        if (type != null) {
            switch(type) {
                case CHARACTER:
                    return column.asString();
                case LOGICAL:
                    return column.asBoolean();
                case DATE:
                    return column.asDate();
                case NUMERIC:
                case FLOATING_POINT:
                    return column.asDouble();
                default:
                    throw new DBFException("Dbf 转换异常!");
            }
        }
        throw new DBFException("Dbf 请检查dbf表头是否写入!");
    }

    public void addRecord(Column[] columns) {
        if(fields == null || fields.length==0){
            throw new DBFException("Dbf 请检查dbf表头是否写入!");
        }
        Object[] recordRow=new Object[fields.length];
        for (int i = 0; i <fields.length ; i++) {
            recordRow[i]=getRecordAsValue(columns[i],i);
        }
        super.addRecord(recordRow);
    }
}
