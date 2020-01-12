package com.desi.data.bean;

public interface Exportable {

    public String toCSVLine();

    public String toSQL(final String dataSet, final String table);

}
