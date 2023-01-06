package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import static com.jie.bigdata.realtime.utils.Constant.*;
import static com.jie.bigdata.realtime.utils.SomeSql.*;

public class OdsMesTotal {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(intervalCheckpoint, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(checkpointTimeout);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBetweenCheckpoints);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts,delayInterval));
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(100);
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                checkpointAddress
        );
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        //TODO 3. 利用FlinkCDC读取MES数据 将所有所需表都同步过来
        tableEnv.executeSql(sourceSql1);
        tableEnv.executeSql(sourceSql2);
        tableEnv.executeSql(sourceSql3);
        tableEnv.executeSql(sourceSql4);
        tableEnv.executeSql(sourceSql5);
        tableEnv.executeSql(sourceSql6);
        tableEnv.executeSql(sourceSql7);
        tableEnv.executeSql(sourceSql8);
        tableEnv.executeSql(sourceSql9);
        tableEnv.executeSql(sourceSql10);
        tableEnv.executeSql(sourceSql11);
        tableEnv.executeSql(sourceSql12);
        tableEnv.executeSql(sourceSql13);
        tableEnv.executeSql(sourceSql14);
        tableEnv.executeSql(sourceSql15);
        tableEnv.executeSql(sourceSql16);
        tableEnv.executeSql(sourceSql17);
        tableEnv.executeSql(sourceSql18);
        tableEnv.executeSql(sourceSql19);
        tableEnv.executeSql(sourceSql20);
        tableEnv.executeSql(sourceSql21);
        tableEnv.executeSql(sourceSql22);
        tableEnv.executeSql(sourceSql23);
        tableEnv.executeSql(sourceSql24);
        tableEnv.executeSql(sourceSql25);
        tableEnv.executeSql(sourceSql26);
        tableEnv.executeSql(sourceSql27);
        tableEnv.executeSql(sourceSql28);
        tableEnv.executeSql(sourceSql29);
        tableEnv.executeSql(sourceSql30);
        tableEnv.executeSql(sourceSql31);

        //TODO 4. 读取doris的表
        tableEnv.executeSql(destinationSql1);
        tableEnv.executeSql(destinationSql2);
        tableEnv.executeSql(destinationSql3);
        tableEnv.executeSql(destinationSql4);
        tableEnv.executeSql(destinationSql5);
        tableEnv.executeSql(destinationSql6);
        tableEnv.executeSql(destinationSql7);
        tableEnv.executeSql(destinationSql8);
        tableEnv.executeSql(destinationSql9);
        tableEnv.executeSql(destinationSql10);
        tableEnv.executeSql(destinationSql11);
        tableEnv.executeSql(destinationSql12);
        tableEnv.executeSql(destinationSql13);
        tableEnv.executeSql(destinationSql14);
        tableEnv.executeSql(destinationSql15);
        tableEnv.executeSql(destinationSql16);
        tableEnv.executeSql(destinationSql17);
        tableEnv.executeSql(destinationSql18);
        tableEnv.executeSql(destinationSql19);
        tableEnv.executeSql(destinationSql20);
        tableEnv.executeSql(destinationSql21);
        tableEnv.executeSql(destinationSql22);
        tableEnv.executeSql(destinationSql23);
        tableEnv.executeSql(destinationSql24);
        tableEnv.executeSql(destinationSql25);
        tableEnv.executeSql(destinationSql26);
        //TODO 5. 插入数据
        //创建语句集
        StatementSet insertSet = tableEnv.createStatementSet();
        // 增加insert语句
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_management_employee_user SELECT\n" +
                "  cast(USER_ID as string) USER_ID\n" +
                ", USER_NAME\n" +
                ", HFWK_PASSWORD\n" +
                ", USER_PASSWORD\n" +
                ", PASSWORD_TIMESTAMP\n" +
                ", cast(PASSWORD_EXPIRATION as string) PASSWORD_EXPIRATION\n" +
                ", cast(LOGIN_EXPIRATION as string) LOGIN_EXPIRATION\n" +
                ", cast(ACTIVE_EXPIRATION as string) ACTIVE_EXPIRATION\n" +
                ", LAST_LOGON_TIMESTAMP\n" +
                ", START_TIMESTAMP\n" +
                ", END_TIMESTAMP\n" +
                ", DESCRIPTION\n" +
                ", cast(CID as string) CID\n" +
                ", CREATION_TIMESTAMP\n" +
                ", cast(CREATED_BY as string) CREATE_BY\n" +
                ", cast(LAST_UPTIMESTAMPD_BY as string) LAST_UPTIMESTAMPD_BY\n" +
                ", LAST_UPTIMESTAMP_TIMESTAMP\n" +
                ", cast(LAST_UPTIMESTAMP_LOGIN as string) LAST_UPTIMESTAMP_LOGIN\n" +
                ", ATTRIBUTE_CATEGORY\n" +
                ", cast(PASSWORD_ERROR_TIMES as string) PASSWORD_ERROR_TIMES\n" +
                ", PASSWORD_RESET_FLAG\n" +
                ", cast(AUTO_START_MENU_ID as STRING) AUTO_START_MENU_ID\n" +
                ", cast(LOGIN_COUNT as string) LOGIN_COUNT\n" +
                ", DEPARTMENT" +
                ",CURRENT_TIMESTAMP from ods_mes_management_employee_user");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_operation_sequence select " +
                "   KID,\n" +
                "  CREATION_DATE,\n" +
                " cast(CREATED_BY as string) CREATED_BY,\n" +
                " cast(LAST_UPDATED_BY as string) LAST_UPDATED_BY,\n" +
                " LAST_UPDATE_DATE,\n" +
                " cast(LAST_UPDATE_LOGIN as string) LAST_UPDATE_LOGIN,\n" +
                " MAKE_ORDER_ID,\n" +
                " OPERATION_SEQ_NUM,\n" +
                " STANDARD_OP_ID,\n" +
                " ENABLE_FLAG,\n" +
                " cast(OPERATION_TIME1 as string) OPERATION_TIME1,\n" +
                " cast(OPERATION_TIME2 as string) OPERATION_TIME2,\n" +
                " cast(OPERATION_TIME3 as string) OPERATION_TIME3,\n" +
                " cast(OPERATION_TIME4 as string) OPERATION_TIME4,\n" +
                " cast(OPERATION_TIME5 as string) OPERATION_TIME5,\n" +
                " cast(OPERATION_TIME6 as string) OPERATION_TIME6,\n" +
                " KEY_OP_FLAG,\n" +
                " MOVE_TYPE,\n" +
                " INSPECT_TYPE,\n" +
                " CHARGE_TYPE,\n" +
                " cast(CID as string) CID,\n" +
                " cast(PLANT as string) PLANT,\n" +
                " SEQUENCE,\n" +
                " WORK_CENTER,\n" +
                " cast(EXECUTE_PLANT as string) EXECUTE_PLANT,\n" +
                " STANDARD_TXT,\n" +
                " OPERATION_DES,\n" +
                " OPERATION_LONG_TXT,\n" +
                " OUTSOURCE_FLAG,\n" +
                " cast(OPERATION_QTY as string) OPERATION_QTY,\n" +
                " LABOR_TIME,\n" +
                " LABOR_UOM,\n" +
                " MACHINE_TIME,\n" +
                " MACHINE_UOM, " +
                " CURRENT_TIMESTAMP FROM ods_mes_production_operation_sequence");

        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_calendar select " +
                "CALENDAR_ID,\n" +
                "CREATED_BY,\n" +
                "CREATION_DATE,\n" +
                "LAST_UPDATED_BY,\n" +
                "LAST_UPDATE_DATE,\n" +
                "LAST_UPDATE_LOGIN,\n" +
                "CALENDAR_TYPE,\n" +
                "DESCRIPTION,\n" +
                "PROD_LINE_ID,\n" +
                "ENABLE_FLAG,\n" +
                "PLANT_ID,\n" +
                "CALENDAR_CODE,\n" +
                "CID," +
                "CURRENT_TIMESTAMP from ods_mes_production_prodline_calendar");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_calendar_shift select" +
                " CALENDAR_SHIFT_ID\n" +
                ",CREATED_BY\n" +
                ",CREATION_DATE\n" +
                ",LAST_UPDATED_BY\n" +
                ",LAST_UPDATE_DATE\n" +
                ",LAST_UPDATE_LOGIN\n" +
                ",CALENDAR_ID\n" +
                ",CALENDAR_DAY\n" +
                ",SHIFT_CODE\n" +
                ",ENABLE_FLAG\n" +
                ",SHIFT_START_TIME\n" +
                ",SHIFT_END_TIME\n" +
                ",BREAK_TIME\n" +
                ",ACTIVITY\n" +
                ",REPLENISH_CAPACITY\n" +
                ",AVAILABLE_TIME\n" +
                ",AVAILABLE_CAPACITY\n" +
                ",REMARK\n" +
                ",CID" +
                ",CURRENT_TIMESTAMP from ods_mes_production_prodline_calendar_shift");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_component select " +
                " cast(KID as STRING) KID\n" +
                ", CREATED_DATE\n" +
                ", cast(CREATED_BY as STRING) CREATED_BY\n" +
                ", cast(LAST_UPDATE_BY as STRING) LAST_UPDATE_BY\n" +
                ", LAST_UPDATE_DATE\n" +
                ", cast(LAST_UPDATE_LOGIN as STRING) LAST_UPDATE_LOGIN\n" +
                ", cast(EO_ID as STRING) EO_ID\n" +
                ", cast(COMPONET_PLANT_ID as STRING) COMPONET_PLANT_ID\n" +
                ", cast(COMPONET_ITEM_ID as STRING) COMPONET_ITEM_ID\n" +
                ", UOM\n" +
                ", cast(BOM_USAGE as STRING) BOM_USAGE\n" +
                ", cast(DEMAND_QTY as STRING) DEMAND_QTY\n" +
                ", cast(ISSUED_QTY as STRING) ISSUED_QTY\n" +
                ", cast(MTL_SCRAPED_QTY as STRING) MTL_SCRAPED_QTY\n" +
                ", cast(OP_SCRAPED_QTY as STRING) OP_SCRAPED_QTY\n" +
                ", SUPPLY_TYPE\n" +
                ", WAREHOUSE_CODE\n" +
                ", LOCATOR_CODE\n" +
                ", cast(CID as STRING) CID\n" +
                ", RECOIL_FLAG , CURRENT_TIMESTAMP from ods_mes_production_prodline_component");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_executive_order select " +
                "  EO_ID\n" +
                ", CREATED_DATE\n" +
                ", CREATED_BY\n" +
                ", LAST_UPDATE_BY\n" +
                ", LAST_UPDATE_DATE\n" +
                ", LAST_UPDATE_LOGIN\n" +
                ", EXECUTIVE_ORDER_NUM\n" +
                ", WORK_ORDER_ID\n" +
                ", STATUS\n" +
                ", WORKCELL_ID\n" +
                ", PRIORITY\n" +
                ", START_TIME\n" +
                ", END_TIME\n" +
                ", QUANTITY\n" +
                ", UOM\n" +
                ", EO_TYPE\n" +
                ", KEY_OP_FLAG\n" +
                ", MOVE_TYPE\n" +
                ", INSPECT_TYPE\n" +
                ", CHARGE_TYPE\n" +
                ", COMPLETED_QTY\n" +
                ", IN_STATION_QTY\n" +
                ", OUT_STATION_QTY\n" +
                ", SCRAPED_QTY\n" +
                ", CID\n" +
                ", VALIDATE_FLAG\n" +
                ", MAKE_ORDER_ID\n" +
                ", OPERATION_SEQ_NUM\n" +
                ", DUTY_SCRAPED_QTY\n" +
                ", ITEM_SCRAPED_QTY\n" +
                ", REPAIR_QTY\n" +
                ", REPAIR_FLAG\n" +
                ", COMPLETE_USER_ID\n" +
                ", DELIVERY_TICKET\n" +
                ", WIP_BARCODE_QTY\n" +
                ", WIP_BARCODE_SCRAPED_QTY\n" +
                ", REMARKS, CURRENT_TIMESTAMP FROM ods_mes_production_prodline_executive_order");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_group select " +
                " PROD_LINE_GROUP_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", SCHEDULE_REGION_ID" +
                ", PROD_LINE_GROUP_CODE" +
                ", DESCRIPTIONS" +
                ", ORDER_BY_CODE" +
                ", PLAN_START_TIME" +
                ", ENABLE_FLAG" +
                ", PROCESS_SEQUENCE" +
                ", PERIODIC_TIME" +
                ", BASIC_ALGORITHM" +
                ", EXTENDED_ALGORITHM" +
                ", FIX_TIME_FENCE" +
                ", FORWARD_PLANNING_TIME_FENCE" +
                ", PROD_LINE_RULE" +
                ", RELEASE_TIME_FENCE" +
                ", PLANNING_PHASE_TIME" +
                ", PLANNING_BASE" +
                ", DELAY_TIME_FENCE" +
                ", FROZEN_TIME_FENCE" +
                ", ORDER_TIME_FENCE" +
                ", RELEASE_CONCURRENT_RULE" +
                ", PLAN_COLLABORATIVE_RULE" +
                ", CID" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_group");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_line select " +
                "  PROD_LINE_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", SCHEDULE_REGION_ID" +
                ", PLANT_ID" +
                ", PROD_LINE_GROUP_ID" +
                ", PROD_LINE_CODE" +
                ", PROD_LINE_ALIAS" +
                ", PROD_LINE_TYPE" +
                ", DESCRIPTIONS" +
                ", RATE_TYPE" +
                ", RATE" +
                ", ACTIVITY" +
                ", PLANNER" +
                ", BUSINESS_AREA" +
                ", ORDER_BY_CODE" +
                ", ENABLE_FLAG" +
                ", ISSUED_WAREHOUSE_CODE" +
                ", ISSUED_LOCATOR_CODE" +
                ", FIX_TIME_FENCE" +
                ", SUPPLIER_ID" +
                ", SUPPLIER_SITE_ID" +
                ", FORWARD_PLANNING_TIME_FENCE" +
                ", MO_REFRESH_FLAG" +
                ", COMPLETION_WAREHOUSE_CODE" +
                ", COMPLETION_LOCATOR_CODE" +
                ", FROZEN_TIME_FENCE" +
                ", ORDER_TIME_FENCE" +
                ", RELEASE_TIME_FENCE" +
                ", INVENTORY_WAREHOUSE_CODE" +
                ", INVENTORY_LOCATOR_CODE" +
                ", CID" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_line");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_line_item select " +
                "  PROD_LINE_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", RELATION_ID" +
                ", PLANT_ID" +
                ", ITEM_ID" +
                ", PRODUCTION_VERSION" +
                ", RATE_TYPE" +
                ", RATE" +
                ", ACTIVITY" +
                ", PRIORITY" +
                ", PROCESS_BATCH" +
                ", STANDARD_RATE_TYPE" +
                ", STANDARD_RATE" +
                ", AUTO_ASSIGN_FLAG" +
                ", PRE_PROCESSING_LEAD_TIME" +
                ", PROCESSING_LEAD_TIME" +
                ", POST_PROCESSING_LEAD_TIME" +
                ", SAFETY_LEAD_TIME" +
                ", ISSUE_WAREHOUSE_CODE" +
                ", ISSUE_LOCATOR_CODE" +
                ", COMPLETE_WAREHOUSE_CODE" +
                ", COMPLETE_LOCATOR_CODE" +
                ", INVENTORY_WAREHOUSE_CODE" +
                ", INVENTORY_LOCATOR_CODE" +
                ", CID" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_line_item");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_make_order select " +
                "  MAKE_ORDER_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", CID" +
                ", CONCAT('000',MAKE_ORDER_NUM) as MAKE_ORDER_NUM" +
                ", PLANT_ID" +
                ", SCHEDULE_REGION_ID" +
                ", ITEM_ID" +
                ", MO_QTY" +
                ", SUPPLY_QTY" +
                ", PLAN_QTY" +
                ", DEMAND_DATE" +
                ", MAKE_ORDER_TYPE" +
                ", PRODUCTION_VERSION" +
                ", PRIORITY" +
                ", MAKE_ORDER_STATUS" +
                ", MO_LAST_STATUS" +
                ", ERP_JOB_TYPE_ID" +
                ", DEMAND_ORDER_ID" +
                ", USER_DEMAND_DATE" +
                ", PROD_LINE_ID" +
                ", PROD_LINE_FIX_FLAG" +
                ", SCHEDULE_RELEASE_TIME" +
                ", EARLIEST_START_TIME" +
                ", START_TIME" +
                ", FPS_TIME" +
                ", FPC_TIME" +
                ", LPS_TIME" +
                ", LPC_TIME" +
                ", FULFILL_TIME" +
                ", TOP_MO_ID" +
                ", PARENT_MO_ID" +
                ", SOURCE_MO_ID" +
                ", REFERENCE_MO_ID" +
                ", MO_REFERENCE_TYPE" +
                ", MO_CAPACITY" +
                ", RATE" +
                ", RATE_TYPE" +
                ", DIE_PLANNING_ID" +
                ", PLAN_TYPE" +
                ", PLANNING_FLAG" +
                ", EXPLORED_FLAG" +
                ", DERIVED_FLAG" +
                ", MO_WARNNING_FLAG" +
                ", ENDING_PROCESS_FLAG" +
                ", BOM_LEVEL" +
                ", EXCEED_LEAD_TIME" +
                ", PRE_PROCESSING_LEAD_TIME" +
                ", PROCESSING_LEAD_TIME" +
                ", POST_PROCESSING_LEAD_TIME" +
                ", SAFETY_LEAD_TIME" +
                ", SWITCH_TIME" +
                ", SWITCH_TIME_USED" +
                ", RELEASE_TIME_FENCE" +
                ", ORDER_TIME_FENCE" +
                ", RELEASED_DATE" +
                ", RELEASED_BY" +
                ", CLOSED_DATE" +
                ", CLOSED_BY" +
                ", SPECIAL_COLOR" +
                ", PLANNING_REMARK" +
                ", REMARK" +
                ", ATTRIBUTE1" +
                ", ATTRIBUTE2" +
                ", ATTRIBUTE3" +
                ", ATTRIBUTE4" +
                ", ATTRIBUTE5" +
                ", ATTRIBUTE6" +
                ", ATTRIBUTE7" +
                ", ATTRIBUTE8" +
                ", ATTRIBUTE9" +
                ", ATTRIBUTE10" +
                ", ATTRIBUTE11" +
                ", ATTRIBUTE12" +
                ", ATTRIBUTE13" +
                ", ATTRIBUTE14" +
                ", ATTRIBUTE15" +
                ", SO_NO" +
                ", SO_ITEM" +
                ", ENABLE_FLAG" +
                ", HEAT_SPLIT_FLAG" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_make_order");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_move_record select " +
                "  cast(KID as STRING) KID" +
                ", cast(CREATED_BY as STRING) CREATED_BY" +
                ", CREATION_DATE" +
                ", cast(LAST_UPDATE_BY as STRING) LAST_UPDATE_BY" +
                ", LAST_UPDATE_DATE" +
                ", cast(LAST_UPDATE_LOGIN as STRING) LAST_UPDATE_LOGIN" +
                ", cast(PLANT_ID as STRING) PLANT_ID" +
                ", BARCODE_NUMBER" +
                ", cast(WORKCELL_ID as STRING) WORKCELL_ID" +
                ", cast(EO_ID as STRING) EO_ID" +
                ", IN_STATION_DATE" +
                ", cast(IN_STATION_QTY as STRING) IN_STATION_QTY" +
                ", cast(IN_STATION_USER_ID as STRING) IN_STATION_USER_ID" +
                ", OUT_STATION_DATE" +
                ", cast(OUT_STATION_QTY as STRING) OUT_STATION_QTY" +
                ", cast(OUT_STATION_USER_ID as STRING) OUT_STATION_USER_ID" +
                ", REMARK" +
                ", ATTRIBUTE1" +
                ", ATTRIBUTE2" +
                ", ATTRIBUTE3" +
                ", ATTRIBUTE4" +
                ", ATTRIBUTE5" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_move_record");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_operation_sequence select " +
                "  cast(KID as STRING) KID" +
                ", CREATED_DATE" +
                ", cast(CREATED_BY as STRING) CREATED_BY" +
                ", cast(LAST_UPDATE_BY as STRING) LAST_UPDATE_BY" +
                ", LAST_UPDATE_DATE" +
                ", cast(LAST_UPDATE_LOGIN as STRING) LAST_UPDATE_LOGIN" +
                ", cast(EO_ID as STRING) EO_ID" +
                ", cast(OPERATION_SEQ_NUM as STRING) OPERATION_SEQ_NUM" +
                ", FIRST_OPS_FLAG" +
                ", LAST_OPS_FLAG" +
                ", cast(CID as STRING) CID" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_operation_sequence");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_prodline_wkcg_rel select " +
                "  cast(REL_ID as STRING) REL_ID" +
                ", CREATED_DATE" +
                ", cast(CREATED_BY as STRING) CREATED_BY" +
                ", cast(LAST_UPDATE_BY as STRING) LAST_UPDATE_BY" +
                ", LAST_UPDATE_DATE" +
                ", cast(LAST_UPDATE_LOGIN as STRING) LAST_UPDATE_LOGIN" +
                ", cast(PROD_LINE_ID as STRING) PROD_LINE_ID" +
                ", cast(WORKCELL_ID as STRING) WORKCELL_ID" +
                ", cast(PRIORITY as STRING) PRIORITY" +
                ", ENABLE_FLAG" +
                ", cast(CID as STRING) CID" +
                ", CURRENT_TIMESTAMP from ods_mes_production_prodline_wkcg_rel");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_standard_operations select " +
                "b.`STANDARD_OP_ID`," +
                "b.`CREATION_DATE`," +
                "cast(b.`CREATED_BY` as string)," +
                "cast(b.`LAST_UPDATED_BY` as string)," +
                "b.`LAST_UPDATE_DATE`," +
                "cast(b.`LAST_UPDATE_LOGIN` as string)," +
                "cast(b.`PLANT_ID` as string)," +
                "b.`CODE`," +
                "b.`KEY_OP_FLAG`," +
                "b.`MOVE_TYPE`," +
                "b.`INSPECT_TYPE`," +
                "b.`CHARGE_TYPE`," +
                "b.`ENABLE_FLAG`," +
                "cast(b.`PROCESS_TIME` as string)," +
                "cast(b.`STANDARD_WORK_TIME` as string)," +
                "b.`OPERATION_DOCUMENT`," +
                "b.`PROCESS_PROGRAM`," +
                "cast(b.`CID` as string)," +
                "t.`DESCRIPTION`," +
                "b.`SELF_TEST_FLAG`," +
                "b.`FIRST_TEST_FLAG`," +
                "b.`HEAT_TREATMENT_FLAG`," +
                "b.`HEAT_TREATMENT_OS_FLAG` " +
                ", CURRENT_TIMESTAMP FROM ods_mes_production_standard_operations1 b," +
                "ods_mes_production_standard_operations2 t " +
                "WHERE b.STANDARD_OP_ID = t.STANDARD_OP_ID " +
                "AND t.`LANGUAGE` = 'ZHS'");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_production_wip_barcode select " +
                "  KID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", WIP_BARCODE" +
                ", EO_ID" +
                ", PLANT_ID" +
                ", ITEM_ID" +
                ", QTY" +
                ", STATUS" +
                ", PACKING_LIST" +
                ", PACKING_LIST_CREATED_BY" +
                ", PACKING_LIST_CREATED_DATE" +
                ", PACKING_LIST_PRINTED_BY" +
                ", PACKING_LIST_PRINTED_DATE" +
                ", PARENT_WIP_BARCODE" +
                ", INSPECT_STATUS" +
                ", FURNACE_ID" +
                ", OUTSOURCING_STATUS" +
                ", REWORK_MARK" +
                ", LOCATOR_CODE" +
                ", CARRIER_CODE" +
                ", DISMANTLING" +
                ", CID" +
                ", ATTRIBUTE1" +
                ", ATTRIBUTE2" +
                ", ATTRIBUTE3" +
                ", ATTRIBUTE4" +
                ", ATTRIBUTE5" +
                ", CURRENT_TIMESTAMP from ods_mes_production_wip_barcode");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_supplychain_material_factory select " +
                "  PLANT_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", SCHEDULE_REGION_ID" +
                ", PLANT_CODE" +
                ", DESCRIPTIONS" +
                ", ENABLE_FLAG" +
                ", MAIN_PLANT_FLAG" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_material_factory");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_supplychain_material_item select " +
                "B.ITEM_ID," +
                "B.CREATED_BY," +
                "B.CREATION_DATE," +
                "B.LAST_UPDATED_BY," +
                "B.LAST_UPDATE_DATE," +
                "B.PLANT_ID," +
                "B.ITEM_CODE," +
                "B.PRIMARY_UOM," +
                "B.DESIGN_CODE," +
                "B.PLAN_CODE," +
                "B.ITEM_IDENTIFY_CODE," +
                "B.ITEM_TYPE," +
                "B.MAKE_BUY_CODE," +
                "B.SUPPLY_TYPE," +
                "B.KEY_COMPONENT_FLAG," +
                "B.SCHEDULE_FLAG," +
                "B.MAKE_TO_ORDER_FLAG," +
                "B.PROD_LINE_RULE," +
                "B.ITEM_SIZE," +
                "B.UNIT_WEIGHT," +
                "B.ENABLE_FLAG," +
                "B.PRE_PROCESSING_LEAD_TIME," +
                "B.PROCESSING_LEAD_TIME," +
                "B.POST_PROCESSING_LEAD_TIME," +
                "B.SAFETY_LEAD_TIME," +
                "B.EXCEED_LEAD_TIME," +
                "B.DEMAND_TIME_FENCE," +
                "B.RELEASE_TIME_FENCE," +
                "B.ORDER_TIME_FENCE," +
                "B.DEMAND_MERGE_TIME_FENCE," +
                "B.SUPPLY_MERGE_TIME_FENCE," +
                "B.SAFETY_STOCK_METHOD," +
                "B.SAFETY_STOCK_PERIOD," +
                "B.SAFETY_STOCK_VALUE," +
                "B.MAX_STOCK_QTY," +
                "B.MIN_STOCK_QTY," +
                "B.PRODUCT_CAPACITY_TIME_FENCE," +
                "B.PRODUCT_CAPACITY," +
                "B.ASSEMBLY_SHRINKAGE," +
                "B.ECONOMIC_LOT_SIZE," +
                "B.ECONOMIC_SPLIT_PARAMETER," +
                "B.MIN_ORDER_QTY," +
                "B.FIXED_LOT_MULTIPLE," +
                "B.PACK_QTY," +
                "B.SEQUENCE_LOT_CONTROL," +
                "B.ISSUE_CONTROL_TYPE," +
                "B.ISSUE_CONTROL_QTY," +
                "B.COMPLETE_CONTROL_TYPE," +
                "B.COMPLETE_CONTROL_QTY," +
                "B.EXPIRE_CONTROL_FLAG," +
                "B.EXPIRE_DAYS," +
                "B.RELEASE_CONCURRENT_RULE," +
                "B.CID," +
                "T.DESCRIPTIONS," +
                "T.`LANGUAGE`," +
                "'1' SCHEDULE_REGION_ID," +
                "IG.ITEM_GROUP_CODE            ITEM_GROUP," +
                "B.`MODULE`," +
                "B.RECOIL_SAFE_QTY," +
                "B.RECOIL_CALL_QTY," +
                "B.MO_SPLIT_FLAG," +
                "B.RECOIL_FLAG," +
                "B.LOT_FLAG," +
                "B.DES_LONG," +
                "B.ELECT_MACHINE_BRAND," +
                "B.ELECT_MACHINE_V," +
                "B.BRAKE_B_V," +
                "B.INDEP_FAN_V," +
                "B.PROTECT_LEVEL," +
                "B.ELECT_MACHINE_FREQUENCY," +
                "B.ELECT_MACHINE_REMARK1," +
                "B.ELECT_MACHINE_REMARK2," +
                "B.WEIGHT," +
                "B.CASE_MODEL," +
                "B.ELECT_MACHINE_MODEL," +
                "B.INSTALL," +
                "B.SPEED_RATIO," +
                "B.LENGTHS," +
                "B.WIDE," +
                "B.HIGH," +
                "CURRENT_TIMESTAMP " +
                "FROM ods_mes_supplychain_material_item1 B," +
                "ods_mes_supplychain_material_item2 T," +
                "ods_mes_supplychain_material_item3 HP," +
                "ods_mes_supplychain_material_item4 IG " +
                "WHERE B.PLANT_ID = T.PLANT_ID " +
                "AND B.ITEM_ID = T.ITEM_ID " +
                "AND HP.PLANT_ID = B.PLANT_ID " +
                "AND T.`LANGUAGE` = 'ZHS' " +
                "AND B.ITEM_GROUP_ID = IG.ITEM_GROUP_ID");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_supplychain_material_item_b select " +
                "  ITEM_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", PLANT_ID" +
                ", ITEM_CODE" +
                ", PRIMARY_UOM" +
                ", DESIGN_CODE" +
                ", PLAN_CODE" +
                ", ITEM_IDENTIFY_CODE" +
                ", ITEM_TYPE" +
                ", MAKE_BUY_CODE" +
                ", SUPPLY_TYPE" +
                ", KEY_COMPONENT_FLAG" +
                ", SCHEDULE_FLAG" +
                ", MAKE_TO_ORDER_FLAG" +
                ", PROD_LINE_RULE" +
                ", ITEM_SIZE" +
                ", UNIT_WEIGHT" +
                ", ENABLE_FLAG" +
                ", PRE_PROCESSING_LEAD_TIME" +
                ", PROCESSING_LEAD_TIME" +
                ", POST_PROCESSING_LEAD_TIME" +
                ", SAFETY_LEAD_TIME" +
                ", EXCEED_LEAD_TIME" +
                ", DEMAND_TIME_FENCE" +
                ", RELEASE_TIME_FENCE" +
                ", ORDER_TIME_FENCE" +
                ", DEMAND_MERGE_TIME_FENCE" +
                ", SUPPLY_MERGE_TIME_FENCE" +
                ", SAFETY_STOCK_METHOD" +
                ", SAFETY_STOCK_PERIOD" +
                ", SAFETY_STOCK_VALUE" +
                ", MAX_STOCK_QTY" +
                ", MIN_STOCK_QTY" +
                ", PRODUCT_CAPACITY_TIME_FENCE" +
                ", PRODUCT_CAPACITY" +
                ", ASSEMBLY_SHRINKAGE" +
                ", ECONOMIC_LOT_SIZE" +
                ", ECONOMIC_SPLIT_PARAMETER" +
                ", MIN_ORDER_QTY" +
                ", FIXED_LOT_MULTIPLE" +
                ", PACK_QTY" +
                ", SEQUENCE_LOT_CONTROL" +
                ", ISSUE_CONTROL_TYPE" +
                ", ISSUE_CONTROL_QTY" +
                ", COMPLETE_CONTROL_TYPE" +
                ", COMPLETE_CONTROL_QTY" +
                ", EXPIRE_CONTROL_FLAG" +
                ", EXPIRE_DAYS" +
                ", RELEASE_CONCURRENT_RULE" +
                ", CID" +
                ", LAST_UPDATE_LOGIN" +
                ", ITEM_GROUP_ID" +
                ", RECOIL_SAFE_QTY" +
                ", RECOIL_CALL_QTY" +
                ", MO_SPLIT_FLAG" +
                ", RECOIL_FLAG" +
                ", LOT_FLAG" +
                ", REVIEW_FLAG" +
                ", DES_LONG" +
                ", SOURCE_ITEM_ID" +
                ", ELECT_MACHINE_BRAND" +
                ", ELECT_MACHINE_V" +
                ", BRAKE_B_V" +
                ", INDEP_FAN_V" +
                ", PROTECT_LEVEL" +
                ", ELECT_MACHINE_FREQUENCY" +
                ", ELECT_MACHINE_REMARK1" +
                ", ELECT_MACHINE_REMARK2" +
                ", WEIGHT" +
                ", `MODULE`" +
                ", CASE_MODEL" +
                ", ELECT_MACHINE_MODEL" +
                ", INSTALL" +
                ", SPEED_RATIO" +
                ", LENGTHS" +
                ", WIDE" +
                ", HIGH" +
                ", ATTRIBUTE1" +
                ", ATTRIBUTE2" +
                ", ATTRIBUTE3" +
                ", ATTRIBUTE4" +
                ", ATTRIBUTE5" +
                ", ATTRIBUTE6" +
                ", ATTRIBUTE7" +
                ", ATTRIBUTE8" +
                ", ATTRIBUTE9" +
                ", ATTRIBUTE10" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_material_item_b");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_supplychain_material_locator select " +
                "  KID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", PLANT_ID" +
                ", LOCATOR_CODE" +
                ", WAREHOUSE_CODE" +
                ", DESCRIPTION" +
                ", ENABLE_FLAG" +
                ", CID" +
                ", LOCATOR_TYPE" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_material_locator");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_supplychain_material_requirement select " +
                "  cast(KID as STRING) KID" +
                ", CREATED_DATE" +
                ", cast(CREATED_BY as STRING) CREATED_BY" +
                ", cast(LAST_UPDATE_BY as STRING) LAST_UPDATE_BY" +
                ", LAST_UPDATE_DATE" +
                ", cast(LAST_UPDATE_LOGIN as STRING) LAST_UPDATE_LOGIN" +
                ", cast(EO_ID as STRING) EO_ID" +
                ", cast(COMPONET_PLANT_ID as STRING) COMPONET_PLANT_ID" +
                ", cast(COMPONET_ITEM_ID as STRING) COMPONET_ITEM_ID" +
                ", UOM" +
                ", cast(BOM_USAGE as STRING) BOM_USAGE" +
                ", cast(DEMAND_QTY as STRING) DEMAND_QTY" +
                ", cast(ISSUED_QTY as STRING) ISSUED_QTY" +
                ", cast(MTL_SCRAPED_QTY as STRING) MTL_SCRAPED_QTY" +
                ", cast(OP_SCRAPED_QTY as STRING) OP_SCRAPED_QTY" +
                ", SUPPLY_TYPE" +
                ", WAREHOUSE_CODE" +
                ", LOCATOR_CODE" +
                ", cast(CID as STRING) CID" +
                ", RECOIL_FLAG" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_material_requirement");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_supplychain_material_warehouse select " +
                "  KID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", PLANT_ID" +
                ", WAREHOUSE_CODE" +
                ", DESCRIPTIONS" +
                ", ENABLE_FLAG" +
                ", WAREHOUSE_SHORT_CODE" +
                ", WAREHOUSE_TYPE" +
                ", WMS_FLAG" +
                ", ONHAND_FLAG" +
                ", LOCATOR_FLAG" +
                ", PLAN_FLAG" +
                ", NEGATIVE_FLAG" +
                ", CID" +
                ", THING_LABEL_FLAG" +
                ", WORKSHOP_ID" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_material_warehouse");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_supplychain_material_wip_barcode select " +
                "  KID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", WIP_BARCODE" +
                ", EO_ID" +
                ", PLANT_ID" +
                ", ITEM_ID" +
                ", QTY" +
                ", STATUS" +
                ", PACKING_LIST" +
                ", PACKING_LIST_CREATED_BY" +
                ", PACKING_LIST_CREATED_DATE" +
                ", PACKING_LIST_PRINTED_BY" +
                ", PACKING_LIST_PRINTED_DATE" +
                ", PARENT_WIP_BARCODE" +
                ", INSPECT_STATUS" +
                ", FURNACE_ID" +
                ", OUTSOURCING_STATUS" +
                ", REWORK_MARK" +
                ", LOCATOR_CODE" +
                ", CARRIER_CODE" +
                ", DISMANTLING" +
                ", CID" +
                ", ATTRIBUTE1" +
                ", ATTRIBUTE2" +
                ", ATTRIBUTE3" +
                ", ATTRIBUTE4" +
                ", ATTRIBUTE5" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_material_wip_barcode");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_supplychain_plan_region select " +
                "  SCHEDULE_REGION_ID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATED_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", SCHEDULE_REGION_CODE" +
                ", DESCRIPTIONS" +
                ", ENABLE_FLAG" +
                ", PLAN_START_TIME" +
                ", PERIODIC_TIME" +
                ", DEMAND_TIME_FENCE" +
                ", FIX_TIME_FENCE" +
                ", FROZEN_TIME_FENCE" +
                ", FORWARD_PLANNING_TIME_FENCE" +
                ", RELEASE_TIME_FENCE" +
                ", ORDER_TIME_FENCE" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_plan_region");
        insertSet.addInsertSql("INSERT INTO doris_ods_mes_supplychain_workcell select " +
                "  WORKCELL_ID" +
                ", CREATED_DATE" +
                ", CREATED_BY" +
                ", LAST_UPDATE_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", WORKCELL_CODE" +
                ", DESCRIPTION" +
                ", WORKCELL_TYPE" +
                ", MERGE_FLAG" +
                ", GPS_INFO" +
                ", ENABLE_FLAG" +
                ", CID" +
                ", WORKSHOP_ID" +
                ", CURRENT_TIMESTAMP from ods_mes_supplychain_workcell");
        insertSet.execute();
    }
}
