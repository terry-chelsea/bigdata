package com.terry.netease.calcite.test;

import java.util.List;

import org.apache.calcite.linq4j.Enumerator;

public class MemoryEnumerator<E>  implements Enumerator<E> {
    private List<List<String>> data = null;
    private int currentIndex = -1;
    private RowConverter<E> rowConvert;
    
    public MemoryEnumerator(int[] fields, List<List<String>> data) {
        this.data = data;
        rowConvert = (RowConverter<E>) new ArrayRowConverter(fields);
    }
    
    abstract static class RowConverter<E>{
        abstract E convertRow(List<String> rows);
    }
    
    static class ArrayRowConverter extends RowConverter<Object[]> {
        private int[] fields;
        
        public ArrayRowConverter(int[] fields) {
            this.fields = fields;
        }
        
        @Override
        Object[] convertRow(List<String> rows) {
            Object[] objects = new Object[fields.length];
            int i = 0 ; 
            for(int field : this.fields) {
                objects[i ++] = rows.get(field);
            }
            return objects;
        }
    }

	public void close() {
		// TODO Auto-generated method stub
		
	}
	
	public E current() {
        List<String> line = data.get(currentIndex);
        return rowConvert.convertRow(line);
	}

	public boolean moveNext() {
        return ++ currentIndex < data.size();
	}

	public void reset() {
		// TODO Auto-generated method stub
		
	}
}
