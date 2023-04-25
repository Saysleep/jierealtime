package com.jie.bigdata.realtime.app.ods;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import static com.jie.bigdata.realtime.utils.Constant.*;

public class OdsMesTotal {
    public static void main(String[] args) throws Exception {
        // TODO 1. 环境准备
        //一些配置
        Configuration config = new Configuration();
        config.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        // set low-level key-value options
        configuration.setString("table.exec.mini-batch.enabled", "true"); // enable mini-batch optimization
        configuration.setString("table.exec.mini-batch.allow-latency", "120 s"); // use 180 seconds to buffer input records
        configuration.setString("table.exec.mini-batch.size", "5000"); // the maximum number of records can be buffered by each aggregate operator task
        // TODO 2. 状态后端设置
        //激活检查点（设置为精准一次）
        env.enableCheckpointing(intervalCheckpoint, CheckpointingMode.EXACTLY_ONCE);
        /*设置检查点属性*/
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

        //TODO 3. 读取hiveCatalog
        // Create a HiveCatalog
        String name            = "myhive";
        String defaultDatabase = "realtime_database";
        String hiveConfDir     = "/opt/module/hive/conf";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);
        // 使用注册的catalog
        tableEnv.useCatalog("myhive");

        //TODO 4. 插入数据
        //创建语句集
        StatementSet insertSet = tableEnv.createStatementSet();
        /* 增加insert语句 */

        //测试
        //insertSet.addInsertSql("INSERT INTO doris_tbl_test SELECT ID,NAME,PID FROM tbl_test");
        //制造订单表
/*        insertSet.addInsertSql("INSERT INTO\n" +
                "    ods_mes_production_prodline_make_order\n" +
                "select\n" +
                "    MAKE_ORDER_ID,\n" +
                "    CREATED_BY,\n" +
                "    CREATION_DATE,\n" +
                "    LAST_UPDATED_BY,\n" +
                "    LAST_UPDATE_DATE,\n" +
                "    LAST_UPDATE_LOGIN,\n" +
                "    CID,\n" +
                "    CONCAT('000', MAKE_ORDER_NUM) as MAKE_ORDER_NUM,\n" +
                "    PLANT_ID,\n" +
                "    SCHEDULE_REGION_ID,\n" +
                "    ITEM_ID,\n" +
                "    MO_QTY,\n" +
                "    SUPPLY_QTY,\n" +
                "    PLAN_QTY,\n" +
                "    DEMAND_DATE,\n" +
                "    MAKE_ORDER_TYPE,\n" +
                "    PRODUCTION_VERSION,\n" +
                "    PRIORITY,\n" +
                "    MAKE_ORDER_STATUS,\n" +
                "    MO_LAST_STATUS,\n" +
                "    ERP_JOB_TYPE_ID,\n" +
                "    DEMAND_ORDER_ID,\n" +
                "    USER_DEMAND_DATE,\n" +
                "    PROD_LINE_ID,\n" +
                "    PROD_LINE_FIX_FLAG,\n" +
                "    SCHEDULE_RELEASE_TIME,\n" +
                "    EARLIEST_START_TIME,\n" +
                "    START_TIME,\n" +
                "    FPS_TIME,\n" +
                "    FPC_TIME,\n" +
                "    LPS_TIME,\n" +
                "    LPC_TIME,\n" +
                "    FULFILL_TIME,\n" +
                "    TOP_MO_ID,\n" +
                "    PARENT_MO_ID,\n" +
                "    SOURCE_MO_ID,\n" +
                "    REFERENCE_MO_ID,\n" +
                "    MO_REFERENCE_TYPE,\n" +
                "    MO_CAPACITY,\n" +
                "    RATE,\n" +
                "    RATE_TYPE,\n" +
                "    DIE_PLANNING_ID,\n" +
                "    PLAN_TYPE,\n" +
                "    PLANNING_FLAG,\n" +
                "    EXPLORED_FLAG,\n" +
                "    DERIVED_FLAG,\n" +
                "    MO_WARNNING_FLAG,\n" +
                "    ENDING_PROCESS_FLAG,\n" +
                "    BOM_LEVEL,\n" +
                "    EXCEED_LEAD_TIME,\n" +
                "    PRE_PROCESSING_LEAD_TIME,\n" +
                "    PROCESSING_LEAD_TIME,\n" +
                "    POST_PROCESSING_LEAD_TIME,\n" +
                "    SAFETY_LEAD_TIME,\n" +
                "    SWITCH_TIME,\n" +
                "    SWITCH_TIME_USED,\n" +
                "    RELEASE_TIME_FENCE,\n" +
                "    ORDER_TIME_FENCE,\n" +
                "    RELEASED_DATE,\n" +
                "    RELEASED_BY,\n" +
                "    CLOSED_DATE,\n" +
                "    CLOSED_BY,\n" +
                "    SPECIAL_COLOR,\n" +
                "    PLANNING_REMARK,\n" +
                "    REMARK,\n" +
                "    ATTRIBUTE1,\n" +
                "    ATTRIBUTE2,\n" +
                "    ATTRIBUTE3,\n" +
                "    ATTRIBUTE4,\n" +
                "    ATTRIBUTE5,\n" +
                "    ATTRIBUTE6,\n" +
                "    ATTRIBUTE7,\n" +
                "    ATTRIBUTE8,\n" +
                "    ATTRIBUTE9,\n" +
                "    ATTRIBUTE10,\n" +
                "    ATTRIBUTE11,\n" +
                "    ATTRIBUTE12,\n" +
                "    ATTRIBUTE13,\n" +
                "    ATTRIBUTE14,\n" +
                "    ATTRIBUTE15,\n" +
                "    SO_NO,\n" +
                "    SO_ITEM,\n" +
                "    ENABLE_FLAG,\n" +
                "    HEAT_SPLIT_FLAG,\n" +
                "    CURRENT_TIMESTAMP\n" +
                "from\n" +
                "    hps_make_order");
        //制造订单工艺路线
        insertSet.addInsertSql("INSERT INTO\n" +
                "    ods_mes_production_operation_sequence\n" +
                "select\n" +
                "    KID,\n" +
                "    CREATION_DATE,\n" +
                "    CREATED_BY,\n" +
                "    LAST_UPDATED_BY,\n" +
                "    LAST_UPDATE_DATE,\n" +
                "    LAST_UPDATE_LOGIN,\n" +
                "    MAKE_ORDER_ID,\n" +
                "    OPERATION_SEQ_NUM,\n" +
                "    STANDARD_OP_ID,\n" +
                "    ENABLE_FLAG,\n" +
                "    OPERATION_TIME1,\n" +
                "    OPERATION_TIME2,\n" +
                "    OPERATION_TIME3,\n" +
                "    OPERATION_TIME4,\n" +
                "    OPERATION_TIME5,\n" +
                "    OPERATION_TIME6,\n" +
                "    KEY_OP_FLAG,\n" +
                "    MOVE_TYPE,\n" +
                "    INSPECT_TYPE,\n" +
                "    CHARGE_TYPE,\n" +
                "    CID,\n" +
                "    PLANT,\n" +
                "    SEQUENCE,\n" +
                "    WORK_CENTER,\n" +
                "    EXECUTE_PLANT,\n" +
                "    STANDARD_TXT,\n" +
                "    OPERATION_DES,\n" +
                "    OPERATION_LONG_TXT,\n" +
                "    OUTSOURCE_FLAG,\n" +
                "    OPERATION_QTY,\n" +
                "    LABOR_TIME,\n" +
                "    LABOR_UOM,\n" +
                "    MACHINE_TIME,\n" +
                "    MACHINE_UOM,\n" +
                "    CURRENT_TIMESTAMP\n" +
                "FROM\n" +
                "    hps_mo_operation_sequence");
        //在制条码表
        insertSet.addInsertSql("INSERT INTO ods_mes_production_wip_barcode select " +
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
                ", CURRENT_TIMESTAMP from hme_jie_wip_barcode");
        //进出站明细
        insertSet.addInsertSql("INSERT INTO ods_mes_production_prodline_move_record select " +
                "  KID" +
                ", CREATED_BY" +
                ", CREATION_DATE" +
                ", LAST_UPDATE_BY" +
                ", LAST_UPDATE_DATE" +
                ", LAST_UPDATE_LOGIN" +
                ", PLANT_ID" +
                ", BARCODE_NUMBER" +
                ", WORKCELL_ID" +
                ", EO_ID" +
                ", IN_STATION_DATE" +
                ", IN_STATION_QTY" +
                ", IN_STATION_USER_ID" +
                ", OUT_STATION_DATE" +
                ", OUT_STATION_QTY" +
                ", OUT_STATION_USER_ID" +
                ", REMARK" +
                ", ATTRIBUTE1" +
                ", ATTRIBUTE2" +
                ", ATTRIBUTE3" +
                ", ATTRIBUTE4" +
                ", ATTRIBUTE5" +
                ", CURRENT_TIMESTAMP " +
                " from hme_jie_move_record");

        //物料
        insertSet.addInsertSql("INSERT INTO\n" +
                "    ods_mes_supplychain_material_item\n" +
                "select\n" +
                "    B.ITEM_ID,\n" +
                "    B.CREATED_BY,\n" +
                "    B.CREATION_DATE,\n" +
                "    B.LAST_UPDATED_BY,\n" +
                "    B.LAST_UPDATE_DATE,\n" +
                "    B.PLANT_ID,\n" +
                "    B.ITEM_CODE,\n" +
                "    B.PRIMARY_UOM,\n" +
                "    B.DESIGN_CODE,\n" +
                "    B.PLAN_CODE,\n" +
                "    B.ITEM_IDENTIFY_CODE,\n" +
                "    B.ITEM_TYPE,\n" +
                "    B.MAKE_BUY_CODE,\n" +
                "    B.SUPPLY_TYPE,\n" +
                "    B.KEY_COMPONENT_FLAG,\n" +
                "    B.SCHEDULE_FLAG,\n" +
                "    B.MAKE_TO_ORDER_FLAG,\n" +
                "    B.PROD_LINE_RULE,\n" +
                "    B.ITEM_SIZE,\n" +
                "    B.UNIT_WEIGHT,\n" +
                "    B.ENABLE_FLAG,\n" +
                "    B.PRE_PROCESSING_LEAD_TIME,\n" +
                "    B.PROCESSING_LEAD_TIME,\n" +
                "    B.POST_PROCESSING_LEAD_TIME,\n" +
                "    B.SAFETY_LEAD_TIME,\n" +
                "    B.EXCEED_LEAD_TIME,\n" +
                "    B.DEMAND_TIME_FENCE,\n" +
                "    B.RELEASE_TIME_FENCE,\n" +
                "    B.ORDER_TIME_FENCE,\n" +
                "    B.DEMAND_MERGE_TIME_FENCE,\n" +
                "    B.SUPPLY_MERGE_TIME_FENCE,\n" +
                "    B.SAFETY_STOCK_METHOD,\n" +
                "    B.SAFETY_STOCK_PERIOD,\n" +
                "    B.SAFETY_STOCK_VALUE,\n" +
                "    B.MAX_STOCK_QTY,\n" +
                "    B.MIN_STOCK_QTY,\n" +
                "    B.PRODUCT_CAPACITY_TIME_FENCE,\n" +
                "    B.PRODUCT_CAPACITY,\n" +
                "    B.ASSEMBLY_SHRINKAGE,\n" +
                "    B.ECONOMIC_LOT_SIZE,\n" +
                "    B.ECONOMIC_SPLIT_PARAMETER,\n" +
                "    B.MIN_ORDER_QTY,\n" +
                "    B.FIXED_LOT_MULTIPLE,\n" +
                "    B.PACK_QTY,\n" +
                "    B.SEQUENCE_LOT_CONTROL,\n" +
                "    B.ISSUE_CONTROL_TYPE,\n" +
                "    B.ISSUE_CONTROL_QTY,\n" +
                "    B.COMPLETE_CONTROL_TYPE,\n" +
                "    B.COMPLETE_CONTROL_QTY,\n" +
                "    B.EXPIRE_CONTROL_FLAG,\n" +
                "    B.EXPIRE_DAYS,\n" +
                "    B.RELEASE_CONCURRENT_RULE,\n" +
                "    B.CID,\n" +
                "    T.DESCRIPTIONS,\n" +
                "    T.`LANGUAGE`,\n" +
                "    '1' SCHEDULE_REGION_ID,\n" +
                "    'null' ITEM_GROUP,\n" +
                "    B.`MODULE`,\n" +
                "    B.RECOIL_SAFE_QTY,\n" +
                "    B.RECOIL_CALL_QTY,\n" +
                "    B.MO_SPLIT_FLAG,\n" +
                "    B.RECOIL_FLAG,\n" +
                "    B.LOT_FLAG,\n" +
                "    B.DES_LONG,\n" +
                "    B.ELECT_MACHINE_BRAND,\n" +
                "    B.ELECT_MACHINE_V,\n" +
                "    B.BRAKE_B_V,\n" +
                "    B.INDEP_FAN_V,\n" +
                "    B.PROTECT_LEVEL,\n" +
                "    B.ELECT_MACHINE_FREQUENCY,\n" +
                "    B.ELECT_MACHINE_REMARK1,\n" +
                "    B.ELECT_MACHINE_REMARK2,\n" +
                "    B.WEIGHT,\n" +
                "    B.CASE_MODEL,\n" +
                "    B.ELECT_MACHINE_MODEL,\n" +
                "    B.INSTALL,\n" +
                "    B.SPEED_RATIO,\n" +
                "    B.LENGTHS,\n" +
                "    B.WIDE,\n" +
                "    B.HIGH,\n" +
                "    CURRENT_TIMESTAMP\n" +
                "FROM\n" +
                "    hcm_item_b B\n" +
                "LEFT JOIN\n  " +
                "hcm_item_tl T ON \n" +
                "    B.PLANT_ID = T.PLANT_ID\n" +
                "    AND B.ITEM_ID = T.ITEM_ID\n" +
                "    AND T.`LANGUAGE` = 'ZHS'");
*/
        insertSet.addInsertSql("INSERT INTO\n" +
                "    ods_mes_production_entry_exit_details\n" +
                "select\n" +
                "    MO.MAKE_ORDER_NUM,\n" +
                "    --工单号\n" +
                "    MR.BARCODE_NUMBER,\n" +
                "    --条码号\n" +
                "    EO.OPERATION_SEQ_NUM,\n" +
                "    --工序号\n" +
                "    CASE\n" +
                "        MRD.OUT_STATION_TYPE\n" +
                "        WHEN 'ITEM_SCRAP' THEN '料废出站'\n" +
                "        WHEN 'QUALIFIED' THEN '合格出站'\n" +
                "        WHEN 'RES_SCRAP' THEN '责废出站'\n" +
                "        ELSE ''\n" +
                "    END AS MEANING,\n" +
                "    --出站类型\n" +
                "    IFNULL(MRD.OUT_STATION_DATE, MR.OUT_STATION_DATE) OUT_STATION_DATE,\n" +
                "    --出站时间\n" +
                "    WB.QTY,\n" +
                "    --条形码数量\n" +
                "    EO.EXECUTIVE_ORDER_NUM,\n" +
                "    --EO号\n" +
                "    MR.CREATION_DATE,\n" +
                "    --EO创建时间\n" +
                "    EO.SCRAPED_QTY,\n" +
                "    --累计报废数量\n" +
                "    HI.ITEM_CODE,\n" +
                "    --物料编码\n" +
                "    HI.DESCRIPTIONS ITEMDESC,\n" +
                "    --物料描述\n" +
                "    HI.ITEM_GROUP,\n" +
                "    --物料组\n" +
                "    HI.PRIMARY_UOM,\n" +
                "    --单位\n" +
                "    MR.IN_STATION_QTY,\n" +
                "    --进站数量\n" +
                "    MR.IN_STATION_DATE,\n" +
                "    --进站时间\n" +
                "    hui.user_name IUSER_NAME,\n" +
                "    --进站账号\n" +
                "    hui.description IDESC,\n" +
                "    --进站人\n" +
                "    IFNULL(MRD.OUT_STATION_QTY, MR.OUT_STATION_QTY) OUT_STATION_QTY,\n" +
                "    --出站数量\n" +
                "    hui.user_name OUSER_NAME,\n" +
                "    --出站账号\n" +
                "    hui.description ODESC,\n" +
                "    --出站人\n" +
                "    HJF.FURNACE_CODE,\n" +
                "    --炉号\n" +
                "    WB.INSPECT_STATUS,\n" +
                "    --质检状态\n" +
                "    MO.MO_QTY,\n" +
                "    --工单数量\n" +
                "    MO.RELEASED_DATE,\n" +
                "    --下达时间\n" +
                "    hso.description OPERATION_SEQ_NUM_DESCRITION,\n" +
                "    --工序名称\n" +
                "    MOS.KEY_OP_FLAG,\n" +
                "    --是否关键工序\n" +
                "    HW.workcell_code,\n" +
                "    --工作中心\n" +
                "    HW.description,\n" +
                "    --工作中心描述\n" +
                "    HLS.LOOKUP_CODE,\n" +
                "    --生产线线组编码\n" +
                "    HLS.MEANING AS LOOKUP_DESC,\n" +
                "    --生产线线组描述\n" +
                "    HLS.ADDITION_CODE,\n" +
                "    --车间代码\n" +
                "    HLS.DESCRIPTION ADDITION_DESC --车间描述\n" +
                "from\n" +
                "    hps_make_order MO\n" +
                "left join    \n" +
                "    hme_executive_order EO on EO.MAKE_ORDER_ID = MO.MAKE_ORDER_ID \n" +
                "left join\n" +
                "    hme_jie_move_record MR on MR.EO_ID = EO.EO_ID\n" +
                "left join\n" +
                "    hme_jie_move_record_del MRD on MR.KID = MRD.MOVE_RECORD_KID\n" +
                "left join \n" +
                "    hcm_item HI on MO.PLANT_ID = HI.PLANT_ID AND MO.ITEM_ID = HI.ITEM_ID\n" +
                "left join\n" +
                "    hme_jie_wip_barcode WB on WB.WIP_BARCODE = MR.BARCODE_NUMBER\n" +
                "left join\n" +
                "    hps_mo_operation_sequence MOS on EO.OPERATION_SEQ_NUM = MOS.OPERATION_SEQ_NUM and MOS.MAKE_ORDER_ID = MO.MAKE_ORDER_ID\n" +
                "left join    \n" +
                "    hfwk_lookup_values HLS on HLS.LOOKUP_TYPE_ID = 13147\n" +
                "left join    \n" +
                "    data_jie.ods_mes_management_employee_user hui on  cast(MR.IN_STATION_USER_ID as string) = hui.user_id\n" +
                "    AND cast(MR.OUT_STATION_USER_ID as string) = hui.user_id    \n" +
                "    AND cast(MRD.OUT_STATION_USER_ID as string) = hui.user_id\n" +
                "left join\n" +
                "    hme_jie_furnace HJF on WB.FURNACE_ID = HJF.FURNACE_ID\n" +
                "left join\n" +
                "    data_jie.ods_mes_production_standard_operations hso on MOS.STANDARD_OP_ID = hso.standard_op_id\n" +
                "left join\n" +
                "    data_jie.ods_mes_supplychain_workcell HW on cast(MR.WORKCELL_ID as double) = HW.workcell_id\n" +
                "left join\n" +
                "    data_jie.ods_mes_production_prodline_line HPL on cast(MO.PROD_LINE_ID as double) = HPL.prod_line_id\n" +
                "left join    \n" +
                "    data_jie.ods_mes_production_prodline_group HPLG on HPLG.prod_line_group_code = cast(HLS.LOOKUP_CODE as string) and HPL.prod_line_group_id = HPLG.prod_line_group_id");
        insertSet.execute();

    }
}
