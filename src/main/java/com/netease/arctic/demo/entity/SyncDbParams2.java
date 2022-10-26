package com.netease.arctic.demo.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SyncDbParams2 implements Serializable {
    Schema schema;
    OutputTag<RowData> tag;
    String db;
    String table;
    ObjectPath path;
    List<String> primaryKeys;
}
