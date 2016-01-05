package org.apache.kylin.jdbc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.kylin.client.Kylin;
import org.apache.kylin.client.meta.ColumnMeta;
import org.apache.kylin.client.meta.CreateCubeMeta;
import org.apache.kylin.client.meta.CreateDimensionMeta;
import org.apache.kylin.client.meta.CreateDimensionMeta.DimensionType;
import org.apache.kylin.client.meta.CreateMeasureMeta;
import org.apache.kylin.client.meta.CreateMeasureMeta.MeasureType;
import org.apache.kylin.client.meta.CubeMeasureMeta;
import org.apache.kylin.client.meta.CubeMeta;
import org.apache.kylin.client.meta.CubeModelMeta;
import org.apache.kylin.client.meta.LookupTableMeta;
import org.apache.kylin.client.meta.ProjectMeta;
import org.apache.kylin.client.meta.TableMeta;

public class TestCreateCube {
	private static String PROJECT_NAME = "test-default-hive-client";
	private static String NEW_PROJECT_HIVE_NAME = null;
	private static String FACT_TABLE_DATABASE = "cube";
	private static String FACT_TABLE_NAME = "fact";
	private static String NEW_CUBE_NAME = "new_test_cube";
	private static String CUBE_DESCRIPTION = "test cube by kylin client";
	private static String CUBE_FILTER = "cost > 0";
	private static List<String> NOTIFY = Arrays.asList("hzfengyu@corp.netease.com", "fengyuatad@126.com");
	private static String PARTITION_KEY = "cube.fact.dt";
	private static long PARTITION_START  = 0l;
	
	private static int RETENT_RANGE = 30;
	private static String CUBE_SIZE = "SMALL";
	
	static List<LookupTableMeta> lookupTables = Arrays.asList(new LookupTableMeta(new TableMeta("cube","customer"), "inner", 
			Arrays.asList(new ColumnMeta("fname", "string"), new ColumnMeta("name", "string")), 
			Arrays.asList(new ColumnMeta("fname", "string"), new ColumnMeta("lname", "string"))), 
			
			new LookupTableMeta(new TableMeta("cube","time_by_day"), "left", 
					Arrays.asList(new ColumnMeta("dtk", "date")), Arrays.asList(new ColumnMeta("dt", "date")))
	);
	
	static List<CreateDimensionMeta> dimensions = Arrays.asList(
			new CreateDimensionMeta(DimensionType.NORMAL, "cube.fact", "type", Arrays.asList("type")),
			new CreateDimensionMeta(DimensionType.NORMAL, "cube.fact", "cmd", Arrays.asList("cmd")) ,
			new CreateDimensionMeta(DimensionType.NORMAL, "cube.customer", "age", Arrays.asList("age")),
			new CreateDimensionMeta(DimensionType.DERIVED, "cube.customer", "derived", Arrays.asList("gender", "home")), 
			new CreateDimensionMeta(DimensionType.HIERARCHY, "cube.time_by_day", "time", Arrays.asList("the_month", "the_day")), 
			new CreateDimensionMeta(DimensionType.DERIVED, "cube.time_by_day", "year", Arrays.asList("the_year"))
			);
	
	static List<List<String>> groups = Arrays.asList(Arrays.asList("type", "cmd", "gender", "home"), 
			Arrays.asList("age", "the_month", "the_day", "the_year", "type", "cmd"));
	
	static List<String> mandatory = Arrays.asList("type", "age");
			
	static List<CreateMeasureMeta> measures = Arrays.asList(
			new CreateMeasureMeta("_COUNT_", MeasureType.constant, "1", "integer", Arrays.asList("count")),
			new CreateMeasureMeta("COST", MeasureType.column, "cost", "integer", Arrays.asList("sum", "max", "min"))
			);

	public static void main(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("./cmd ip port");
			return ;
		}
		
		Kylin kylin = new Kylin(args[0], Integer.valueOf(args[1]), "ADMIN", "KYLIN");
		
		ProjectMeta project = kylin.getProjectByName(PROJECT_NAME);
		if(project == null) {
			String description = "Create hive for test cube create...";
			project = new ProjectMeta(PROJECT_NAME, description, NEW_PROJECT_HIVE_NAME);
			kylin.createProject(project);
		}
		
		System.out.println("Get project " + project);	
		
		TableMeta factTable = new TableMeta(FACT_TABLE_DATABASE, FACT_TABLE_NAME);
		List<String> loaded = new LinkedList<String>();
		loaded.add(factTable.getFullTableName());
		for(LookupTableMeta table : lookupTables) {
			loaded.add(table.getTable().getFullTableName());
		}
		
		kylin.loadTable(loaded, project.getProjectName());
		
		CubeModelMeta model = new CubeModelMeta(NEW_CUBE_NAME, factTable, lookupTables, CUBE_FILTER, null);
		
		CreateCubeMeta cubeMeta = new CreateCubeMeta(NEW_CUBE_NAME, model, NOTIFY, CUBE_DESCRIPTION, dimensions, measures, 
				PARTITION_KEY, PARTITION_START, CUBE_SIZE, RETENT_RANGE, groups, mandatory, 1);
		
		kylin.createCube(cubeMeta, project.getProjectName());
		
		System.out.println("Create cube success ....");
	}
}
