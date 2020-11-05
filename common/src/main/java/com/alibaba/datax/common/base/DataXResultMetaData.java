package com.alibaba.datax.common.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSetMetaData;
import java.util.LinkedList;

/**
 * @author zhangtao01
 * @Title: 缓存数据库信息
 * @date 2020/10/30 13:38
 */
public class DataXResultMetaData {
    private LinkedList<DataXResultMetaDataEntry> entries;
    private int columnCount;

    static Logger LOG= LoggerFactory.getLogger(DataXResultMetaData.class);

    public DataXResultMetaData(ResultSetMetaData metaData){
        entries=new LinkedList<>();
        try {
            this.columnCount=metaData.getColumnCount();
            for (int i = 1; i <=this.columnCount ; i++) {
                entries.add(new DataXResultMetaDataEntry(
                        metaData.getColumnLabel(i),
                        metaData.getColumnTypeName(i),
                        metaData.getScale(i),
                        metaData.getPrecision(i),
                        metaData.getColumnType(i)
                ));
            }
        }catch (Exception ex){
            LOG.error(ex.toString(),ex);
        }
    }

    public int getColumnType(int column) {
        return entries.get(column).getColumnType();
    }

    public String getColumnTypeName(int column) {
        return entries.get(column).getColumnTypeName();
    }

    public String getColumnLabel(int column)  {
        return entries.get(column).getColumnLabel();
    }

    public int getScale(int column)  {
        return entries.get(column).getScale();
    }

    public int getPrecision(int column)  {
        return entries.get(column).getPrecision();
    }

    public int getColumnCount()  {
        return columnCount;
    }
}

class DataXResultMetaDataEntry {


    private int columnType;
    private String columnTypeName;
    private String columnLabel;
    private int scale;
    private int precision;

    public DataXResultMetaDataEntry(String columnLabel,String columnTypeName,int scale,int precision,int columnType){
        this.columnLabel=columnLabel;
        this.columnTypeName=columnTypeName;
        this.scale=scale;
        this.precision=precision;
        this.columnType=columnType;
    }
    public int getColumnType() {
        return columnType;
    }

    public String getColumnTypeName() {
        return columnTypeName;
    }


    public String getColumnLabel() {
        return columnLabel;
    }


    public int getScale() {
        return scale;
    }


    public int getPrecision() {
        return precision;
    }

}


