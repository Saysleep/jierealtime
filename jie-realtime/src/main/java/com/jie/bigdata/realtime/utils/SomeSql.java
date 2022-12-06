package com.jie.bigdata.realtime.utils;

public class SomeSql {
    public static String sourceSql0 = "" +
            "CREATE TABLE tbl_test (" +
            "ID INT," +
            "NAME STRING," +
            "PID INT) " + DDLUtil.getOracleDDL("TBL_TEST");

    public static String sourceSql1 = "" +
            "CREATE TABLE ods_mes_management_employee_user ( " +
            "USER_ID INT," +
            "USER_NAME STRING," +
            "HFWK_PASSWORD STRING," +
            "USER_PASSWORD STRING," +
            "PASSWORD_TIMESTAMP TIMESTAMP," +
            "PASSWORD_EXPIRATION INT," +
            "LOGIN_EXPIRATION INT," +
            "ACTIVE_EXPIRATION INT," +
            "LAST_LOGON_TIMESTAMP TIMESTAMP," +
            "START_TIMESTAMP TIMESTAMP," +
            "END_TIMESTAMP TIMESTAMP," +
            "DESCRIPTION STRING," +
            "CID INT," +
            "CREATION_TIMESTAMP TIMESTAMP," +
            "CREATED_BY INT," +
            "LAST_UPTIMESTAMPD_BY INT," +
            "LAST_UPTIMESTAMP_TIMESTAMP TIMESTAMP," +
            "LAST_UPTIMESTAMP_LOGIN INT," +
            "ATTRIBUTE_CATEGORY STRING," +
            "ATTRIBUTE1 STRING," +
            "ATTRIBUTE2 STRING," +
            "ATTRIBUTE3 STRING," +
            "ATTRIBUTE4 STRING," +
            "ATTRIBUTE5 STRING," +
            "ATTRIBUTE6 STRING," +
            "ATTRIBUTE7 STRING," +
            "ATTRIBUTE8 STRING," +
            "ATTRIBUTE9 STRING," +
            "ATTRIBUTE10 STRING," +
            "ATTRIBUTE11 STRING," +
            "ATTRIBUTE12 STRING," +
            "ATTRIBUTE13 STRING," +
            "ATTRIBUTE14 STRING," +
            "ATTRIBUTE15 STRING," +
            "PASSWORD_ERROR_TIMES INT," +
            "PASSWORD_RESET_FLAG STRING," +
            "AUTO_START_MENU_ID INT," +
            "LOGIN_COUNT INT," +
            "DEPARTMENT STRING" +
            ")" + DDLUtil.getOracleDDL("HFWK_USERS") ;
    //HPS_MO_OPERATION_SEQUENCE
    public static String sourceSql2 = "CREATE TABLE ods_mes_production_operation_sequence ( " +
            "        CREATION_DATE      TIMESTAMP,\n" +
            "        CREATED_BY         INT,\n" +
            "        LAST_UPDATED_BY    INT,\n" +
            "        LAST_UPDATE_DATE   TIMESTAMP,\n" +
            "        LAST_UPDATE_LOGIN  INT,\n" +
            "        KID                INT,\n" +
            "        MAKE_ORDER_ID      INT,\n" +
            "        OPERATION_SEQ_NUM  INT,\n" +
            "        STANDARD_OP_ID     INT,\n" +
            "        ENABLE_FLAG        STRING,\n" +
            "        OPERATION_TIME1    INT,\n" +
            "        OPERATION_TIME2    INT,\n" +
            "        OPERATION_TIME3    INT,\n" +
            "        OPERATION_TIME4    INT,\n" +
            "        OPERATION_TIME5    INT,\n" +
            "        OPERATION_TIME6    INT,\n" +
            "        KEY_OP_FLAG        STRING,\n" +
            "        MOVE_TYPE          STRING,\n" +
            "        INSPECT_TYPE       STRING,\n" +
            "        CHARGE_TYPE        STRING,\n" +
            "        CID                INT,\n" +
            "        PLANT              INT,\n" +
            "        SEQUENCE           STRING,\n" +
            "        WORK_CENTER        STRING,\n" +
            "        EXECUTE_PLANT      INT,\n" +
            "        STANDARD_TXT       STRING,\n" +
            "        OPERATION_DES      STRING,\n" +
            "        OPERATION_LONG_TXT STRING,\n" +
            "        OUTSOURCE_FLAG     STRING,\n" +
            "        OPERATION_QTY      INT,\n" +
            "        LABOR_TIME         STRING,\n" +
            "        LABOR_UOM          STRING,\n" +
            "        MACHINE_TIME       STRING,\n" +
            "        MACHINE_UOM        STRING" +
            ")" + DDLUtil.getOracleDDL("HPS_MO_OPERATION_SEQUENCE");
    //HCM_CALENDAR
    public static String sourceSql3 = "" +
            "CREATE TABLE ods_mes_production_prodline_calendar ( " +
            "CREATED_BY INT ," +
            "CREATION_DATE TIMESTAMP ," +
            "LAST_UPDATED_BY INT ," +
            "LAST_UPDATE_DATE TIMESTAMP ," +
            "LAST_UPDATE_LOGIN INT ," +
            "CALENDAR_ID INT ," +
            "CALENDAR_TYPE STRING ," +
            "DESCRIPTION STRING ," +
            "PROD_LINE_ID INT ," +
            "ENABLE_FLAG STRING ," +
            "PLANT_ID INT ," +
            "CALENDAR_CODE STRING ," +
            "CID INT" +
            ")" + DDLUtil.getOracleDDL("HCM_CALENDAR");
    //HCM_CALENDAR_SHIFT
    public static String sourceSql4 = "" +
            "CREATE TABLE ods_mes_production_prodline_calendar_shift ( " +
            "CREATED_BY         INT," +
            "CREATION_DATE      TIMESTAMP," +
            "LAST_UPDATED_BY    INT," +
            "LAST_UPDATE_DATE   TIMESTAMP," +
            "LAST_UPDATE_LOGIN  INT," +
            "CALENDAR_SHIFT_ID  INT," +
            "CALENDAR_ID        INT," +
            "CALENDAR_DAY       TIMESTAMP," +
            "SHIFT_CODE         STRING," +
            "ENABLE_FLAG        STRING," +
            "SHIFT_START_TIME   TIMESTAMP," +
            "SHIFT_END_TIME     TIMESTAMP," +
            "BREAK_TIME         INT," +
            "ACTIVITY           INT," +
            "REPLENISH_CAPACITY INT," +
            "AVAILABLE_TIME     INT," +
            "AVAILABLE_CAPACITY INT," +
            "REMARK             STRING," +
            "CID                INT" +
            ")" + DDLUtil.getOracleDDL("HCM_CALENDAR_SHIFT");
    //HME_EO_REQUIREMENT
    public static String sourceSql5 = "" +
            "CREATE TABLE ods_mes_production_prodline_component ( " +
            "CREATED_DATE      TIMESTAMP," +
            "CREATED_BY        INT," +
            "LAST_UPDATE_BY    INT," +
            "LAST_UPDATE_DATE  TIMESTAMP," +
            "LAST_UPDATE_LOGIN INT," +
            "KID               INT," +
            "EO_ID             INT," +
            "COMPONET_PLANT_ID INT," +
            "COMPONET_ITEM_ID  INT," +
            "UOM               STRING," +
            "BOM_USAGE         INT," +
            "DEMAND_QTY        INT," +
            "ISSUED_QTY        INT," +
            "MTL_SCRAPED_QTY   INT," +
            "OP_SCRAPED_QTY    INT," +
            "SUPPLY_TYPE       STRING," +
            "WAREHOUSE_CODE    STRING," +
            "LOCATOR_CODE      STRING," +
            "CID               INT," +
            "RECOIL_FLAG       STRING" +
            ")" + DDLUtil.getOracleDDL("HME_EO_REQUIREMENT");
    //HME_EXECUTIVE_ORDER
    public static String sourceSql6 = "" +
            "CREATE TABLE ods_mes_production_prodline_executive_order (  " +
            "CREATED_DATE            TIMESTAMP," +
            "CREATED_BY              INT," +
            "LAST_UPDATE_BY          INT," +
            "LAST_UPDATE_DATE        TIMESTAMP," +
            "LAST_UPDATE_LOGIN       INT," +
            "EO_ID                   INT," +
            "EXECUTIVE_ORDER_NUM     STRING," +
            "WORK_ORDER_ID           INT," +
            "STATUS                  STRING," +
            "WORKCELL_ID             INT," +
            "PRIORITY                INT," +
            "START_TIME              TIMESTAMP," +
            "END_TIME                TIMESTAMP," +
            "QUANTITY                INT," +
            "UOM                     STRING," +
            "EO_TYPE                 STRING," +
            "KEY_OP_FLAG             STRING," +
            "MOVE_TYPE               STRING," +
            "INSPECT_TYPE            STRING," +
            "CHARGE_TYPE             STRING," +
            "COMPLETED_QTY           INT," +
            "IN_STATION_QTY          INT," +
            "OUT_STATION_QTY         INT," +
            "SCRAPED_QTY             INT," +
            "CID                     INT," +
            "VALIDATE_FLAG           STRING," +
            "MAKE_ORDER_ID           INT," +
            "OPERATION_SEQ_NUM       INT," +
            "DUTY_SCRAPED_QTY        INT," +
            "ITEM_SCRAPED_QTY        INT," +
            "REPAIR_QTY              INT," +
            "REPAIR_FLAG             STRING," +
            "COMPLETE_USER_ID        INT," +
            "DELIVERY_TICKET         STRING," +
            "WIP_BARCODE_QTY         INT," +
            "WIP_BARCODE_SCRAPED_QTY INT," +
            "REMARKS                 STRING" +
            ")" + DDLUtil.getOracleDDL("HME_EXECUTIVE_ORDER");
    //HCM_PRODUCTION_LINE_GROUP
    public static String sourceSql7 = "" +
            "CREATE TABLE ods_mes_production_prodline_group ( " +
            "CREATED_BY                  INT," +
            "CREATION_DATE               TIMESTAMP," +
            "LAST_UPDATED_BY             INT," +
            "LAST_UPDATE_DATE            TIMESTAMP," +
            "LAST_UPDATE_LOGIN           INT," +
            "SCHEDULE_REGION_ID          INT," +
            "PROD_LINE_GROUP_ID          INT," +
            "PROD_LINE_GROUP_CODE        STRING," +
            "DESCRIPTIONS                STRING," +
            "ORDER_BY_CODE               STRING," +
            "PLAN_START_TIME             TIMESTAMP," +
            "ENABLE_FLAG                 STRING," +
            "PROCESS_SEQUENCE            STRING," +
            "PERIODIC_TIME               STRING," +
            "BASIC_ALGORITHM             STRING," +
            "EXTENDED_ALGORITHM          STRING," +
            "FIX_TIME_FENCE              INT," +
            "FORWARD_PLANNING_TIME_FENCE INT," +
            "PROD_LINE_RULE              STRING," +
            "RELEASE_TIME_FENCE          INT," +
            "PLANNING_PHASE_TIME         STRING," +
            "PLANNING_BASE               STRING," +
            "DELAY_TIME_FENCE            INT," +
            "FROZEN_TIME_FENCE           INT," +
            "ORDER_TIME_FENCE            INT," +
            "RELEASE_CONCURRENT_RULE     STRING," +
            "PLAN_COLLABORATIVE_RULE     STRING," +
            "CID                         INT" +
            ")" + DDLUtil.getOracleDDL("HCM_PRODUCTION_LINE_GROUP");
    //HCM_PRODUCTION_LINE
    public static String sourceSql8 = "" +
            "CREATE TABLE ods_mes_production_prodline_line ( " +
            "CREATED_BY                  INT," +
            "CREATION_DATE               TIMESTAMP," +
            "LAST_UPDATED_BY             INT," +
            "LAST_UPDATE_DATE            TIMESTAMP," +
            "LAST_UPDATE_LOGIN           INT," +
            "SCHEDULE_REGION_ID          INT," +
            "PLANT_ID                    INT," +
            "PROD_LINE_GROUP_ID          INT," +
            "PROD_LINE_ID                INT," +
            "PROD_LINE_CODE              STRING," +
            "PROD_LINE_ALIAS             STRING," +
            "PROD_LINE_TYPE              STRING," +
            "DESCRIPTIONS                STRING," +
            "RATE_TYPE                   STRING," +
            "RATE                        INT," +
            "ACTIVITY                    INT," +
            "PLANNER                     INT," +
            "BUSINESS_AREA               STRING," +
            "ORDER_BY_CODE               STRING," +
            "ENABLE_FLAG                 STRING," +
            "ISSUED_WAREHOUSE_CODE       STRING," +
            "ISSUED_LOCATOR_CODE         STRING," +
            "FIX_TIME_FENCE              INT," +
            "SUPPLIER_ID                 INT," +
            "SUPPLIER_SITE_ID            INT," +
            "FORWARD_PLANNING_TIME_FENCE INT," +
            "MO_REFRESH_FLAG             STRING," +
            "COMPLETION_WAREHOUSE_CODE   STRING," +
            "COMPLETION_LOCATOR_CODE     STRING," +
            "FROZEN_TIME_FENCE           INT," +
            "ORDER_TIME_FENCE            INT," +
            "RELEASE_TIME_FENCE          INT," +
            "INVENTORY_WAREHOUSE_CODE    STRING," +
            "INVENTORY_LOCATOR_CODE      STRING," +
            "CID                         INT" +
            ")" + DDLUtil.getOracleDDL("HCM_PRODUCTION_LINE");
    //HCM_PROD_LINE_ITEM
    public static String sourceSql9 = "" +
            "CREATE TABLE ods_mes_production_prodline_line_item ( " +
            "CREATED_BY                INT," +
            "CREATION_DATE             TIMESTAMP," +
            "LAST_UPDATED_BY           INT," +
            "LAST_UPDATE_DATE          TIMESTAMP," +
            "LAST_UPDATE_LOGIN         INT," +
            "RELATION_ID               INT," +
            "PLANT_ID                  INT," +
            "ITEM_ID                   INT," +
            "PROD_LINE_ID              INT," +
            "PRODUCTION_VERSION        STRING," +
            "RATE_TYPE                 STRING," +
            "RATE                      INT," +
            "ACTIVITY                  INT," +
            "PRIORITY                  INT," +
            "PROCESS_BATCH             INT," +
            "STANDARD_RATE_TYPE        STRING," +
            "STANDARD_RATE             INT," +
            "AUTO_ASSIGN_FLAG          STRING," +
            "PRE_PROCESSING_LEAD_TIME  INT," +
            "PROCESSING_LEAD_TIME      INT," +
            "POST_PROCESSING_LEAD_TIME INT," +
            "SAFETY_LEAD_TIME          INT," +
            "ISSUE_WAREHOUSE_CODE      STRING," +
            "ISSUE_LOCATOR_CODE        STRING," +
            "COMPLETE_WAREHOUSE_CODE   STRING," +
            "COMPLETE_LOCATOR_CODE     STRING," +
            "INVENTORY_WAREHOUSE_CODE  STRING," +
            "INVENTORY_LOCATOR_CODE    STRING," +
            "CID                       INT" +
            ")" + DDLUtil.getOracleDDL("HCM_PROD_LINE_ITEM");
    //HPS_MAKE_ORDER
    public static String sourceSql10 = "" +
            "CREATE TABLE ods_mes_production_prodline_make_order ( " +
            "CREATED_BY                INT," +
            "CREATION_DATE             TIMESTAMP," +
            "LAST_UPDATED_BY           INT," +
            "LAST_UPDATE_DATE          TIMESTAMP," +
            "LAST_UPDATE_LOGIN         INT," +
            "CID                       INT," +
            "MAKE_ORDER_ID             INT," +
            "MAKE_ORDER_NUM            STRING," +
            "PLANT_ID                  INT," +
            "SCHEDULE_REGION_ID        INT," +
            "ITEM_ID                   INT," +
            "MO_QTY                    INT," +
            "SUPPLY_QTY                INT," +
            "PLAN_QTY                  INT," +
            "DEMAND_DATE               TIMESTAMP," +
            "MAKE_ORDER_TYPE           STRING," +
            "PRODUCTION_VERSION        STRING," +
            "PRIORITY                  INT," +
            "MAKE_ORDER_STATUS         STRING," +
            "MO_LAST_STATUS            STRING," +
            "ERP_JOB_TYPE_ID           INT," +
            "DEMAND_ORDER_ID           INT," +
            "USER_DEMAND_DATE          TIMESTAMP," +
            "PROD_LINE_ID              INT," +
            "PROD_LINE_FIX_FLAG        STRING," +
            "SCHEDULE_RELEASE_TIME     TIMESTAMP," +
            "EARLIEST_START_TIME       TIMESTAMP," +
            "START_TIME                TIMESTAMP," +
            "FPS_TIME                  TIMESTAMP," +
            "FPC_TIME                  TIMESTAMP," +
            "LPS_TIME                  TIMESTAMP," +
            "LPC_TIME                  TIMESTAMP," +
            "FULFILL_TIME              TIMESTAMP," +
            "TOP_MO_ID                 INT," +
            "PARENT_MO_ID              INT," +
            "SOURCE_MO_ID              INT," +
            "REFERENCE_MO_ID           INT," +
            "MO_REFERENCE_TYPE         STRING," +
            "MO_CAPACITY               INT," +
            "RATE                      INT," +
            "RATE_TYPE                 STRING," +
            "DIE_PLANNING_ID           INT," +
            "PLAN_TYPE                 STRING," +
            "PLANNING_FLAG             STRING," +
            "EXPLORED_FLAG             STRING," +
            "DERIVED_FLAG              STRING," +
            "MO_WARNNING_FLAG          STRING," +
            "ENDING_PROCESS_FLAG       STRING," +
            "BOM_LEVEL                 STRING," +
            "EXCEED_LEAD_TIME          INT," +
            "PRE_PROCESSING_LEAD_TIME  INT," +
            "PROCESSING_LEAD_TIME      INT," +
            "POST_PROCESSING_LEAD_TIME INT," +
            "SAFETY_LEAD_TIME          INT," +
            "SWITCH_TIME               INT," +
            "SWITCH_TIME_USED          INT," +
            "RELEASE_TIME_FENCE        INT," +
            "ORDER_TIME_FENCE          INT," +
            "RELEASED_DATE             TIMESTAMP," +
            "RELEASED_BY               INT," +
            "CLOSED_DATE               TIMESTAMP," +
            "CLOSED_BY                 INT," +
            "SPECIAL_COLOR             STRING," +
            "PLANNING_REMARK           STRING," +
            "REMARK                    STRING," +
            "ATTRIBUTE1                STRING," +
            "ATTRIBUTE2                STRING," +
            "ATTRIBUTE3                STRING," +
            "ATTRIBUTE4                STRING," +
            "ATTRIBUTE5                STRING," +
            "ATTRIBUTE6                STRING," +
            "ATTRIBUTE7                STRING," +
            "ATTRIBUTE8                STRING," +
            "ATTRIBUTE9                STRING," +
            "ATTRIBUTE10               STRING," +
            "ATTRIBUTE11               STRING," +
            "ATTRIBUTE12               STRING," +
            "ATTRIBUTE13               STRING," +
            "ATTRIBUTE14               STRING," +
            "ATTRIBUTE15               STRING," +
            "SO_NO                     STRING," +
            "SO_ITEM                   STRING," +
            "ENABLE_FLAG               STRING," +
            "HEAT_SPLIT_FLAG           STRING" +
            ")" + DDLUtil.getOracleDDL("HPS_MAKE_ORDER");
    //HME_JIE_MOVE_RECORD
    public static String sourceSql11 = "" +
            "CREATE TABLE ods_mes_production_prodline_move_record ( " +
            "CREATED_BY          INT," +
            "CREATION_DATE       TIMESTAMP," +
            "LAST_UPDATE_BY      INT," +
            "LAST_UPDATE_DATE    TIMESTAMP," +
            "LAST_UPDATE_LOGIN   INT," +
            "KID                 INT," +
            "PLANT_ID            INT," +
            "BARCODE_NUMBER      STRING," +
            "WORKCELL_ID         INT," +
            "EO_ID               INT," +
            "IN_STATION_DATE     TIMESTAMP," +
            "IN_STATION_QTY      INT," +
            "IN_STATION_USER_ID  INT," +
            "OUT_STATION_DATE    TIMESTAMP," +
            "OUT_STATION_QTY     INT," +
            "OUT_STATION_USER_ID INT," +
            "REMARK              STRING," +
            "ATTRIBUTE1          STRING," +
            "ATTRIBUTE2          STRING," +
            "ATTRIBUTE3          STRING," +
            "ATTRIBUTE4          STRING," +
            "ATTRIBUTE5          STRING" +
            ")" + DDLUtil.getOracleDDL("HME_JIE_MOVE_RECORD");
    //HME_EO_OPERATION_SEQUENCE
    public static String sourceSql12 = "" +
            "CREATE TABLE ods_mes_production_prodline_operation_sequence ( " +
            "CREATED_DATE      TIMESTAMP," +
            "CREATED_BY        INT," +
            "LAST_UPDATE_BY    INT," +
            "LAST_UPDATE_DATE  TIMESTAMP," +
            "LAST_UPDATE_LOGIN INT," +
            "KID               INT," +
            "EO_ID             INT," +
            "OPERATION_SEQ_NUM INT," +
            "FIRST_OPS_FLAG    STRING," +
            "LAST_OPS_FLAG     STRING," +
            "CID               INT" +
            ")" + DDLUtil.getOracleDDL("HME_EO_OPERATION_SEQUENCE");
    //HCM_PROD_LINE_WKCG_REL
    public static String sourceSql13 = "" +
            "CREATE TABLE ods_mes_production_prodline_wkcg_rel ( " +
            "CREATED_DATE      TIMESTAMP," +
            "CREATED_BY        INT," +
            "LAST_UPDATE_BY    INT," +
            "LAST_UPDATE_DATE  TIMESTAMP," +
            "LAST_UPDATE_LOGIN INT," +
            "REL_ID            INT," +
            "PROD_LINE_ID      INT," +
            "WORKCELL_ID       INT," +
            "PRIORITY          INT," +
            "ENABLE_FLAG       STRING," +
            "CID               INT" +
            ")" + DDLUtil.getOracleDDL("HCM_PROD_LINE_WKCG_REL");
    //HCM_STANDARD_OP_B & HCM_STANDARD_OP_TL
    public static String sourceSql14 = "" +
            "CREATE TABLE ods_mes_production_standard_operations1 ( " +
            "CREATION_DATE          TIMESTAMP," +
            "CREATED_BY             INT," +
            "LAST_UPDATED_BY        INT," +
            "LAST_UPDATE_DATE       TIMESTAMP," +
            "LAST_UPDATE_LOGIN      INT," +
            "STANDARD_OP_ID         INT," +
            "PLANT_ID               INT," +
            "CODE                   STRING," +
            "KEY_OP_FLAG            STRING," +
            "MOVE_TYPE              STRING," +
            "INSPECT_TYPE           STRING," +
            "CHARGE_TYPE            STRING," +
            "ENABLE_FLAG            STRING," +
            "PROCESS_TIME           INT," +
            "STANDARD_WORK_TIME     INT," +
            "OPERATION_DOCUMENT     STRING," +
            "PROCESS_PROGRAM        STRING," +
            "CID                    INT," +
            "SELF_TEST_FLAG         STRING," +
            "FIRST_TEST_FLAG        STRING," +
            "HEAT_TREATMENT_FLAG    STRING," +
            "HEAT_TREATMENT_OS_FLAG STRING," +
            "ATTRIBUTE1             STRING," +
            "ATTRIBUTE2             STRING," +
            "ATTRIBUTE3             STRING," +
            "ATTRIBUTE4             STRING," +
            "ATTRIBUTE5             STRING" +
            ")" + DDLUtil.getOracleDDL("HCM_STANDARD_OP_B");
    public static String sourceSql15 = ""+
            "CREATE TABLE ods_mes_production_standard_operations2 ( " +
            "CREATION_DATE     timestamp," +
            "CREATED_BY        int," +
            "LAST_UPDATED_BY   int," +
            "LAST_UPDATE_DATE  timestamp," +
            "LAST_UPDATE_LOGIN int," +
            "STANDARD_OP_ID    int," +
            "DESCRIPTION       string," +
            "`LANGUAGE`          string," +
            "SOURCE_LANGUAGE   string" +
            ")" + DDLUtil.getOracleDDL("HCM_STANDARD_OP_TL");
    //HME_JIE_WIP_BARCODE
    public static String sourceSql16 = "" +
            "CREATE TABLE ods_mes_production_wip_barcode ( " +
            "CREATED_BY                INT," +
            "CREATION_DATE             TIMESTAMP," +
            "LAST_UPDATED_BY           INT," +
            "LAST_UPDATE_DATE          TIMESTAMP," +
            "LAST_UPDATE_LOGIN         INT," +
            "KID                       INT," +
            "WIP_BARCODE               STRING," +
            "EO_ID                     INT," +
            "PLANT_ID                  INT," +
            "ITEM_ID                   INT," +
            "QTY                       INT," +
            "STATUS                    STRING," +
            "PACKING_LIST              STRING," +
            "PACKING_LIST_CREATED_BY   INT," +
            "PACKING_LIST_CREATED_DATE TIMESTAMP," +
            "PACKING_LIST_PRINTED_BY   INT," +
            "PACKING_LIST_PRINTED_DATE TIMESTAMP," +
            "PARENT_WIP_BARCODE        STRING," +
            "INSPECT_STATUS            STRING," +
            "FURNACE_ID                INT," +
            "OUTSOURCING_STATUS        STRING," +
            "REWORK_MARK               STRING," +
            "LOCATOR_CODE              STRING," +
            "CARRIER_CODE              STRING," +
            "DISMANTLING               STRING," +
            "CID                       INT," +
            "ATTRIBUTE1                STRING," +
            "ATTRIBUTE2                STRING," +
            "ATTRIBUTE3                STRING," +
            "ATTRIBUTE4                STRING," +
            "ATTRIBUTE5                STRING" +
            ")" + DDLUtil.getOracleDDL("HME_JIE_WIP_BARCODE");
    //HCM_PLANT
    public static String sourceSql17 = "" +
            "CREATE TABLE ods_mes_supplychain_material_factory ( " +
            "CREATED_BY         int," +
            "CREATION_DATE      timestamp," +
            "LAST_UPDATED_BY    int," +
            "LAST_UPDATE_DATE   timestamp," +
            "LAST_UPDATE_LOGIN  int," +
            "SCHEDULE_REGION_ID int," +
            "PLANT_ID           int," +
            "PLANT_CODE         string," +
            "DESCRIPTIONS       string," +
            "ENABLE_FLAG        string," +
            "MAIN_PLANT_FLAG    string" +
            ")" + DDLUtil.getOracleDDL("HCM_PLANT");
    //HCM_ITEM_B & HCM_ITEM_TL & HCM_PLANT & HCM_JIE_ITEM_GROUP
    public static String sourceSql18 = "" +
            "CREATE TABLE ods_mes_supplychain_material_item1 ( " +
            "CREATED_BY                  INT," +
            "CREATION_DATE               TIMESTAMP," +
            "LAST_UPDATED_BY             INT," +
            "LAST_UPDATE_DATE            TIMESTAMP," +
            "ITEM_ID                     INT," +
            "PLANT_ID                    INT," +
            "ITEM_CODE                   STRING," +
            "PRIMARY_UOM                 STRING," +
            "DESIGN_CODE                 STRING," +
            "PLAN_CODE                   STRING," +
            "ITEM_IDENTIFY_CODE          STRING," +
            "ITEM_TYPE                   STRING," +
            "MAKE_BUY_CODE               STRING," +
            "SUPPLY_TYPE                 STRING," +
            "KEY_COMPONENT_FLAG          STRING," +
            "SCHEDULE_FLAG               STRING," +
            "MAKE_TO_ORDER_FLAG          STRING," +
            "PROD_LINE_RULE              STRING," +
            "ITEM_SIZE                   STRING," +
            "UNIT_WEIGHT                 INT," +
            "ENABLE_FLAG                 STRING," +
            "PRE_PROCESSING_LEAD_TIME    INT," +
            "PROCESSING_LEAD_TIME        INT," +
            "POST_PROCESSING_LEAD_TIME   INT," +
            "SAFETY_LEAD_TIME            INT," +
            "EXCEED_LEAD_TIME            INT," +
            "DEMAND_TIME_FENCE           INT," +
            "RELEASE_TIME_FENCE          INT," +
            "ORDER_TIME_FENCE            INT," +
            "DEMAND_MERGE_TIME_FENCE     STRING," +
            "SUPPLY_MERGE_TIME_FENCE     STRING," +
            "SAFETY_STOCK_METHOD         STRING," +
            "SAFETY_STOCK_PERIOD         INT," +
            "SAFETY_STOCK_VALUE          INT," +
            "MAX_STOCK_QTY               INT," +
            "MIN_STOCK_QTY               INT," +
            "PRODUCT_CAPACITY_TIME_FENCE STRING," +
            "PRODUCT_CAPACITY            INT," +
            "ASSEMBLY_SHRINKAGE          INT," +
            "ECONOMIC_LOT_SIZE           INT," +
            "ECONOMIC_SPLIT_PARAMETER    INT," +
            "MIN_ORDER_QTY               INT," +
            "FIXED_LOT_MULTIPLE          INT," +
            "PACK_QTY                    INT," +
            "SEQUENCE_LOT_CONTROL        STRING," +
            "ISSUE_CONTROL_TYPE          STRING," +
            "ISSUE_CONTROL_QTY           INT," +
            "COMPLETE_CONTROL_TYPE       STRING," +
            "COMPLETE_CONTROL_QTY        INT," +
            "EXPIRE_CONTROL_FLAG         STRING," +
            "EXPIRE_DAYS                 INT," +
            "RELEASE_CONCURRENT_RULE     STRING," +
            "CID                         INT," +
            "LAST_UPDATE_LOGIN           INT," +
            "ITEM_GROUP_ID               INT," +
            "RECOIL_SAFE_QTY             INT," +
            "RECOIL_CALL_QTY             INT," +
            "MO_SPLIT_FLAG               STRING," +
            "RECOIL_FLAG                 STRING," +
            "LOT_FLAG                    STRING," +
            "REVIEW_FLAG                 STRING," +
            "DES_LONG                    STRING," +
            "SOURCE_ITEM_ID              INT," +
            "ELECT_MACHINE_BRAND         STRING," +
            "ELECT_MACHINE_V             STRING," +
            "BRAKE_B_V                   STRING," +
            "INDEP_FAN_V                 STRING," +
            "PROTECT_LEVEL               STRING," +
            "ELECT_MACHINE_FREQUENCY     STRING," +
            "ELECT_MACHINE_REMARK1       STRING," +
            "ELECT_MACHINE_REMARK2       STRING," +
            "WEIGHT                      STRING," +
            "`MODULE`                      STRING," +
            "CASE_MODEL                  STRING," +
            "ELECT_MACHINE_MODEL         STRING," +
            "INSTALL                     STRING," +
            "SPEED_RATIO                 STRING," +
            "LENGTHS                     STRING," +
            "WIDE                        STRING," +
            "HIGH                        STRING," +
            "ATTRIBUTE1                  STRING," +
            "ATTRIBUTE2                  STRING," +
            "ATTRIBUTE3                  STRING," +
            "ATTRIBUTE4                  STRING," +
            "ATTRIBUTE5                  STRING," +
            "ATTRIBUTE6                  STRING," +
            "ATTRIBUTE7                  STRING," +
            "ATTRIBUTE8                  STRING," +
            "ATTRIBUTE9                  STRING," +
            "ATTRIBUTE10                 STRING" +
            ")" + DDLUtil.getOracleDDL("HCM_ITEM_B");
    public static String sourceSql19 = "" +
            "CREATE TABLE ods_mes_supplychain_material_item2 ( " +
            "CREATED_BY        INT," +
            "CREATION_DATE     TIMESTAMP," +
            "LAST_UPDATED_BY   INT," +
            "LAST_UPDATE_DATE  TIMESTAMP," +
            "LAST_UPDATE_LOGIN INT," +
            "`LANGUAGE`         STRING," +
            "SOURCE_LANGUAGE   STRING," +
            "PLANT_ID          INT," +
            "ITEM_ID           INT," +
            "DESCRIPTIONS      STRING" +
            ")" + DDLUtil.getOracleDDL("HCM_ITEM_TL");
    public static String sourceSql20 = "" +
            "CREATE TABLE ods_mes_supplychain_material_item3 ( " +
            "CREATED_BY         INT," +
            "CREATION_DATE      TIMESTAMP," +
            "LAST_UPDATED_BY    INT," +
            "LAST_UPDATE_DATE   TIMESTAMP," +
            "LAST_UPDATE_LOGIN  INT," +
            "SCHEDULE_REGION_ID INT," +
            "PLANT_ID           INT," +
            "PLANT_CODE         STRING," +
            "DESCRIPTIONS       STRING," +
            "ENABLE_FLAG        STRING," +
            "MAIN_PLANT_FLAG    STRING" +
            ")" + DDLUtil.getOracleDDL("HCM_PLANT");
    public static String sourceSql21 = "" +
            "CREATE TABLE ods_mes_supplychain_material_item4 ( " +
            "CREATED_BY        INT," +
            "CREATION_DATE     TIMESTAMP," +
            "LAST_UPDATED_BY   INT," +
            "LAST_UPDATE_DATE  TIMESTAMP," +
            "LAST_UPDATE_LOGIN INT," +
            "ITEM_GROUP_ID     INT," +
            "PLANT_ID          INT," +
            "ITEM_GROUP_CODE   STRING," +
            "DESCRIPTIONS      STRING," +
            "SPLIT_NUM         INT," +
            "ENABLE_FLAG       STRING," +
            "CID               INT," +
            "ATTRIBUTE1        STRING," +
            "ATTRIBUTE2        STRING," +
            "ATTRIBUTE3        STRING," +
            "ATTRIBUTE4        STRING," +
            "ATTRIBUTE5        STRING," +
            "ATTRIBUTE6        STRING," +
            "ATTRIBUTE7        STRING," +
            "ATTRIBUTE8        STRING," +
            "ATTRIBUTE9        STRING," +
            "ATTRIBUTE10       STRING" +
            ")" + DDLUtil.getOracleDDL("HCM_JIE_ITEM_GROUP");
    //HCM_ITEM_B
    public static String sourceSql22 = "" +
            "CREATE TABLE ods_mes_supplychain_material_item_b ( " +
            "CREATED_BY                  INT," +
            "CREATION_DATE               TIMESTAMP," +
            "LAST_UPDATED_BY             INT," +
            "LAST_UPDATE_DATE            TIMESTAMP," +
            "ITEM_ID                     INT," +
            "PLANT_ID                    INT," +
            "ITEM_CODE                   STRING," +
            "PRIMARY_UOM                 STRING," +
            "DESIGN_CODE                 STRING," +
            "PLAN_CODE                   STRING," +
            "ITEM_IDENTIFY_CODE          STRING," +
            "ITEM_TYPE                   STRING," +
            "MAKE_BUY_CODE               STRING," +
            "SUPPLY_TYPE                 STRING," +
            "KEY_COMPONENT_FLAG          STRING," +
            "SCHEDULE_FLAG               STRING," +
            "MAKE_TO_ORDER_FLAG          STRING," +
            "PROD_LINE_RULE              STRING," +
            "ITEM_SIZE                   STRING," +
            "UNIT_WEIGHT                 INT," +
            "ENABLE_FLAG                 STRING," +
            "PRE_PROCESSING_LEAD_TIME    INT," +
            "PROCESSING_LEAD_TIME        INT," +
            "POST_PROCESSING_LEAD_TIME   INT," +
            "SAFETY_LEAD_TIME            INT," +
            "EXCEED_LEAD_TIME            INT," +
            "DEMAND_TIME_FENCE           INT," +
            "RELEASE_TIME_FENCE          INT," +
            "ORDER_TIME_FENCE            INT," +
            "DEMAND_MERGE_TIME_FENCE     STRING," +
            "SUPPLY_MERGE_TIME_FENCE     STRING," +
            "SAFETY_STOCK_METHOD         STRING," +
            "SAFETY_STOCK_PERIOD         INT," +
            "SAFETY_STOCK_VALUE          INT," +
            "MAX_STOCK_QTY               INT," +
            "MIN_STOCK_QTY               INT," +
            "PRODUCT_CAPACITY_TIME_FENCE STRING," +
            "PRODUCT_CAPACITY            INT," +
            "ASSEMBLY_SHRINKAGE          INT," +
            "ECONOMIC_LOT_SIZE           INT," +
            "ECONOMIC_SPLIT_PARAMETER    INT," +
            "MIN_ORDER_QTY               INT," +
            "FIXED_LOT_MULTIPLE          INT," +
            "PACK_QTY                    INT," +
            "SEQUENCE_LOT_CONTROL        STRING," +
            "ISSUE_CONTROL_TYPE          STRING," +
            "ISSUE_CONTROL_QTY           INT," +
            "COMPLETE_CONTROL_TYPE       STRING," +
            "COMPLETE_CONTROL_QTY        INT," +
            "EXPIRE_CONTROL_FLAG         STRING," +
            "EXPIRE_DAYS                 INT," +
            "RELEASE_CONCURRENT_RULE     STRING," +
            "CID                         INT," +
            "LAST_UPDATE_LOGIN           INT," +
            "ITEM_GROUP_ID               INT," +
            "RECOIL_SAFE_QTY             INT," +
            "RECOIL_CALL_QTY             INT," +
            "MO_SPLIT_FLAG               STRING," +
            "RECOIL_FLAG                 STRING," +
            "LOT_FLAG                    STRING," +
            "REVIEW_FLAG                 STRING," +
            "DES_LONG                    STRING," +
            "SOURCE_ITEM_ID              INT," +
            "ELECT_MACHINE_BRAND         STRING," +
            "ELECT_MACHINE_V             STRING," +
            "BRAKE_B_V                   STRING," +
            "INDEP_FAN_V                 STRING," +
            "PROTECT_LEVEL               STRING," +
            "ELECT_MACHINE_FREQUENCY     STRING," +
            "ELECT_MACHINE_REMARK1       STRING," +
            "ELECT_MACHINE_REMARK2       STRING," +
            "WEIGHT                      STRING," +
            "`MODULE`                      STRING," +
            "CASE_MODEL                  STRING," +
            "ELECT_MACHINE_MODEL         STRING," +
            "INSTALL                     STRING," +
            "SPEED_RATIO                 STRING," +
            "LENGTHS                     STRING," +
            "WIDE                        STRING," +
            "HIGH                        STRING," +
            "ATTRIBUTE1                  STRING," +
            "ATTRIBUTE2                  STRING," +
            "ATTRIBUTE3                  STRING," +
            "ATTRIBUTE4                  STRING," +
            "ATTRIBUTE5                  STRING," +
            "ATTRIBUTE6                  STRING," +
            "ATTRIBUTE7                  STRING," +
            "ATTRIBUTE8                  STRING," +
            "ATTRIBUTE9                  STRING," +
            "ATTRIBUTE10                 STRING" +
            ")" + DDLUtil.getOracleDDL("HCM_ITEM_B");
    //HCM_LOCATOR
    public static String sourceSql23 = "" +
            "CREATE TABLE ods_mes_supplychain_material_locator ( " +
            "CREATED_BY        INT," +
            "CREATION_DATE     TIMESTAMP," +
            "LAST_UPDATED_BY   INT," +
            "LAST_UPDATE_DATE  TIMESTAMP," +
            "LAST_UPDATE_LOGIN INT," +
            "KID               INT," +
            "PLANT_ID          INT," +
            "LOCATOR_CODE      STRING," +
            "WAREHOUSE_CODE    STRING," +
            "DESCRIPTION       STRING," +
            "ENABLE_FLAG       STRING," +
            "CID               INT," +
            "LOCATOR_TYPE      STRING" +
            ")" + DDLUtil.getOracleDDL("HCM_LOCATOR");
    //HME_EO_REQUIREMENT
    public static String sourceSql24 = "" +
            "CREATE TABLE ods_mes_supplychain_material_requirement ( " +
            "CREATED_DATE      TIMESTAMP," +
            "CREATED_BY        INT," +
            "LAST_UPDATE_BY    INT," +
            "LAST_UPDATE_DATE  TIMESTAMP," +
            "LAST_UPDATE_LOGIN INT," +
            "KID               INT," +
            "EO_ID             INT," +
            "COMPONET_PLANT_ID INT," +
            "COMPONET_ITEM_ID  INT," +
            "UOM               STRING," +
            "BOM_USAGE         INT," +
            "DEMAND_QTY        INT," +
            "ISSUED_QTY        INT," +
            "MTL_SCRAPED_QTY   INT," +
            "OP_SCRAPED_QTY    INT," +
            "SUPPLY_TYPE       STRING," +
            "WAREHOUSE_CODE    STRING," +
            "LOCATOR_CODE      STRING," +
            "CID               INT," +
            "RECOIL_FLAG       STRING" +
            ")" + DDLUtil.getOracleDDL("HME_EO_REQUIREMENT");
    //HCM_WAREHOUSE
    public static String sourceSql25 = "" +
            "CREATE TABLE ods_mes_supplychain_material_warehouse ( " +
            "CREATED_BY           INT," +
            "CREATION_DATE        TIMESTAMP," +
            "LAST_UPDATED_BY      INT," +
            "LAST_UPDATE_DATE     TIMESTAMP," +
            "LAST_UPDATE_LOGIN    INT," +
            "KID                  INT," +
            "PLANT_ID             INT," +
            "WAREHOUSE_CODE       STRING," +
            "DESCRIPTIONS         STRING," +
            "ENABLE_FLAG          STRING," +
            "WAREHOUSE_SHORT_CODE STRING," +
            "WAREHOUSE_TYPE       STRING," +
            "WMS_FLAG             STRING," +
            "ONHAND_FLAG          STRING," +
            "LOCATOR_FLAG         STRING," +
            "PLAN_FLAG            STRING," +
            "NEGATIVE_FLAG        STRING," +
            "CID                  INT," +
            "THING_LABEL_FLAG     STRING," +
            "WORKSHOP_ID          INT" +
            ")" + DDLUtil.getOracleDDL("HCM_WAREHOUSE");
    //HME_JIE_WIP_BARCODE
    public static String sourceSql26 = "" +
            "CREATE TABLE ods_mes_supplychain_material_wip_barcode ( " +
            "CREATED_BY                INT," +
            "CREATION_DATE             TIMESTAMP," +
            "LAST_UPDATED_BY           INT," +
            "LAST_UPDATE_DATE          TIMESTAMP," +
            "LAST_UPDATE_LOGIN         INT," +
            "KID                       INT," +
            "WIP_BARCODE               STRING," +
            "EO_ID                     INT," +
            "PLANT_ID                  INT," +
            "ITEM_ID                   INT," +
            "QTY                       INT," +
            "STATUS                    STRING," +
            "PACKING_LIST              STRING," +
            "PACKING_LIST_CREATED_BY   INT," +
            "PACKING_LIST_CREATED_DATE TIMESTAMP," +
            "PACKING_LIST_PRINTED_BY   INT," +
            "PACKING_LIST_PRINTED_DATE TIMESTAMP," +
            "PARENT_WIP_BARCODE        STRING," +
            "INSPECT_STATUS            STRING," +
            "FURNACE_ID                INT," +
            "OUTSOURCING_STATUS        STRING," +
            "REWORK_MARK               STRING," +
            "LOCATOR_CODE              STRING," +
            "CARRIER_CODE              STRING," +
            "DISMANTLING               STRING," +
            "CID                       INT," +
            "ATTRIBUTE1                STRING," +
            "ATTRIBUTE2                STRING," +
            "ATTRIBUTE3                STRING," +
            "ATTRIBUTE4                STRING," +
            "ATTRIBUTE5                STRING" +
            ")" + DDLUtil.getOracleDDL("HME_JIE_WIP_BARCODE");
    //HCM_SCHEDULE_REGION
    public static String sourceSql27 = "" +
            "CREATE TABLE ods_mes_supplychain_plan_region ( " +
            "CREATED_BY                  INT," +
            "CREATION_DATE               TIMESTAMP," +
            "LAST_UPDATED_BY             INT," +
            "LAST_UPDATE_DATE            TIMESTAMP," +
            "LAST_UPDATE_LOGIN           INT," +
            "SCHEDULE_REGION_ID          INT," +
            "SCHEDULE_REGION_CODE        STRING," +
            "DESCRIPTIONS                STRING," +
            "ENABLE_FLAG                 STRING," +
            "PLAN_START_TIME             TIMESTAMP," +
            "PERIODIC_TIME               STRING," +
            "DEMAND_TIME_FENCE           INT," +
            "FIX_TIME_FENCE              INT," +
            "FROZEN_TIME_FENCE           INT," +
            "FORWARD_PLANNING_TIME_FENCE INT," +
            "RELEASE_TIME_FENCE          INT," +
            "ORDER_TIME_FENCE            INT" +
            ")" + DDLUtil.getOracleDDL("HCM_SCHEDULE_REGION");
    //HCM_WORKCELL
    public static String sourceSql28 = "" +
            "CREATE TABLE ods_mes_supplychain_workcell ( " +
            "CREATED_DATE      TIMESTAMP," +
            "CREATED_BY        INT," +
            "LAST_UPDATE_BY    INT," +
            "LAST_UPDATE_DATE  TIMESTAMP," +
            "LAST_UPDATE_LOGIN INT," +
            "WORKCELL_ID       INT," +
            "WORKCELL_CODE     STRING," +
            "DESCRIPTION       STRING," +
            "WORKCELL_TYPE     STRING," +
            "MERGE_FLAG        STRING," +
            "GPS_INFO          STRING," +
            "ENABLE_FLAG       STRING," +
            "CID               INT," +
            "WORKSHOP_ID       INT" +
            ")" + DDLUtil.getOracleDDL("HCM_WORKCELL");
    public static String sourceSql29 = "" +
            "CREATE TABLE ods_mes_production_prodline_version1 ( " +
            "CREATED_BY            INT," +
            "CREATION_DATE         TIMESTAMP," +
            "LAST_UPDATED_BY       INT," +
            "LAST_UPDATE_DATE      TIMESTAMP," +
            "LAST_UPDATE_LOGIN     INT," +
            "PRODUCTION_VERSION_ID INT," +
            "PLANT_ID              INT," +
            "ITEM_ID               INT," +
            "PRODUCTION_VERSION    STRING," +
            "BOM_ID                INT," +
            "ROUTING_ID            INT," +
            "START_DATE            TIMESTAMP," +
            "END_DATE              TIMESTAMP," +
            "CID                   INT" +
            ")" + DDLUtil.getOracleDDL("HCM_PRODUCTION_VERSION_B");
    public static String sourceSql30 = "" +
            "CREATE TABLE ods_mes_production_prodline_version2 ( " +
            "CREATED_BY            INT," +
            "CREATION_DATE         TIMESTAMP," +
            "LAST_UPDATED_BY       INT," +
            "LAST_UPDATE_DATE      TIMESTAMP," +
            "LAST_UPDATE_LOGIN     INT," +
            "PRODUCTION_VERSION_ID INT," +
            "DESCRIPTIONS          STRING," +
            "`LANGUAGE`             STRING," +
            "SOURCE_LANGUAGE       STRING" +
            ")" + DDLUtil.getOracleDDL("HCM_PRODUCTION_VERSION_TL");
    public static String sourceSql31 = "" +
            "CREATE TABLE ods_mes_production_prodline_version_b ( " +
            "CREATED_BY            INT," +
            "CREATION_DATE         TIMESTAMP," +
            "LAST_UPDATED_BY       INT," +
            "LAST_UPDATE_DATE      TIMESTAMP," +
            "LAST_UPDATE_LOGIN     INT," +
            "PRODUCTION_VERSION_ID INT," +
            "PLANT_ID              INT," +
            "ITEM_ID               INT," +
            "PRODUCTION_VERSION    STRING," +
            "BOM_ID                INT," +
            "ROUTING_ID            INT," +
            "START_DATE            TIMESTAMP," +
            "END_DATE              TIMESTAMP," +
            "CID                   INT" +
            ")" + DDLUtil.getOracleDDL("HCM_PRODUCTION_VERSION_B");

    public static String destinationSql0 = "CREATE TABLE doris_tbl_test (\n" +
            "  ID INT,\n" +
            "  NAME STRING,\n" +
            "  PID INT\n" +
            ")" + DDLUtil.getDorisDDL("tbl_test");
    //ods_mes_management_employee_user
    public static String destinationSql1 = "CREATE TABLE doris_ods_mes_management_employee_user (\n" +
            " USER_ID string " +
            ", USER_NAME string" +
            ", HFWK_PASSWORD string" +
            ", USER_PASSWORD string" +
            ", PASSWORD_DATE TIMESTAMP" +
            ", PASSWORD_EXPIRATION string"+
            ", LOGIN_EXPIRATION string"+
            ", ACTIVE_EXPIRATION string"+
            ", LAST_LOGON_DATE TIMESTAMP"+
            ", START_DATE TIMESTAMP"+
            ", END_DATE TIMESTAMP"+
            ", DESCRIPTION string"+
            ", CID string"+
            ", CREATION_DATE TIMESTAMP"+
            ", CREATED_BY string"+
            ", LAST_UPDATED_BY string"+
            ", LAST_UPDATE_DATE TIMESTAMP"+
            ", LAST_UPDATE_LOGIN string"+
            ", ATTRIBUTE_CATEGORY String"+
            ", PASSWORD_ERROR_TIMES string"+
            ", PASSWORD_RESET_FLAG string"+
            ", AUTO_START_MENU_ID string"+
            ", LOGIN_COUNT string"+
            ", DEPARTMENT string"+
            ", UPDATE_DATETIME TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_management_employee_user");
    //ods_mes_production_operation_sequence
    public static String destinationSql2 = "CREATE TABLE doris_ods_mes_production_operation_sequence(\n" +
            " KID int,\n" +
            "CREATION_DATE timestamp,\n" +
            "CREATED_BY string,\n" +
            "LAST_UPDATED_BY string,\n" +
            "LAST_UPDATE_DATE timestamp,\n" +
            "LAST_UPDATE_LOGIN string,\n" +
            "MAKE_ORDER_ID int,\n" +
            "OPERATION_SEQ_NUM int,\n" +
            "STANDARD_OP_ID int,\n" +
            "ENABLE_FLAG string,\n" +
            "OPERATION_TIME1 string,\n" +
            "OPERATION_TIME2 string,\n" +
            "OPERATION_TIME3 string,\n" +
            "OPERATION_TIME4 string,\n" +
            "OPERATION_TIME5 string,\n" +
            "OPERATION_TIME6 string,\n" +
            "KEY_OP_FLAG string,\n" +
            "MOVE_TYPE string,\n" +
            "INSPECT_TYPE string,\n" +
            "CHARGE_TYPE string,\n" +
            "CID string,\n" +
            "PLANT string,\n" +
            "SEQUENCE string,\n" +
            "WORK_CENTER string,\n" +
            "EXECUTE_PLANT string,\n" +
            "STANDARD_TXT string,\n" +
            "OPERATION_DES string,\n" +
            "OPERATION_LONG_TXT string,\n" +
            "OUTSOURCE_FLAG string,\n" +
            "OPERATION_QTY string,\n" +
            "LABOR_TIME string,\n" +
            "LABOR_UOM string,\n" +
            "MACHINE_TIME string,\n" +
            "MACHINE_UOM string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_operation_sequence");
    //
    public static String destinationSql3 = "CREATE TABLE doris_ods_mes_production_prodline_calendar (\n" +
            "calendar_id decimal(27,3)," +
            "created_by double," +
            "creation_date TIMESTAMP," +
            "last_updated_by double," +
            "last_update_date TIMESTAMP," +
            "last_update_login double," +
            "calendar_type string," +
            "description string," +
            "prod_line_id double," +
            "enable_flag string," +
            "plant_id double," +
            "calendar_code string," +
            "cid double," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_calendar");
    //
    public static String destinationSql4 = "CREATE TABLE doris_ods_mes_production_prodline_calendar_shift (\n" +
            "calendar_shift_id decimal(27,3)," +
            "created_by int," +
            "creation_date timestamp," +
            "last_updated_by int," +
            "last_update_date timestamp," +
            "last_update_login int," +
            "calendar_id int," +
            "calendar_day timestamp," +
            "shift_code string," +
            "enable_flag string," +
            "shift_start_time timestamp," +
            "shift_end_time timestamp," +
            "break_time int," +
            "activity int," +
            "replenish_capacity int," +
            "available_time int," +
            "available_capacity int," +
            "remark string," +
            "cid int," +
            "update_datetime timestamp" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_calendar_shift");
    //
    public static String destinationSql5 = "CREATE TABLE doris_ods_mes_production_prodline_component (\n" +
            "KID STRING,\n" +
            "CREATED_DATE timestamp,\n" +
            "CREATED_BY STRING,\n" +
            "LAST_UPDATE_BY STRING,\n" +
            "LAST_UPDATE_DATE timestamp,\n" +
            "LAST_UPDATE_LOGIN STRING,\n" +
            "EO_ID STRING,\n" +
            "COMPONET_PLANT_ID STRING,\n" +
            "COMPONET_ITEM_ID STRING,\n" +
            "UOM STRING,\n" +
            "BOM_USAGE STRING,\n" +
            "DEMAND_QTY STRING,\n" +
            "ISSUED_QTY STRING,\n" +
            "MTL_SCRAPED_QTY STRING,\n" +
            "OP_SCRAPED_QTY STRING,\n" +
            "SUPPLY_TYPE STRING,\n" +
            "WAREHOUSE_CODE STRING,\n" +
            "LOCATOR_CODE STRING,\n" +
            "CID STRING,\n" +
            "RECOIL_FLAG STRING,\n" +
            "update_datetime timestamp" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_component");
    //
    public static String destinationSql6 = "CREATE TABLE doris_ods_mes_production_prodline_executive_order (\n" +
            "EO_ID decimal," +
            "CREATED_DATE timestamp," +
            "CREATED_BY decimal," +
            "LAST_UPDATE_BY decimal," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN decimal," +
            "EXECUTIVE_ORDER_NUM string," +
            "WORK_ORDER_ID decimal," +
            "STATUS string," +
            "WORKCELL_ID decimal," +
            "PRIORITY decimal," +
            "START_TIME timestamp," +
            "END_TIME timestamp," +
            "QUANTITY decimal," +
            "UOM string," +
            "EO_TYPE string," +
            "KEY_OP_FLAG string," +
            "MOVE_TYPE string," +
            "INSPECT_TYPE string," +
            "CHARGE_TYPE string," +
            "COMPLETED_QTY decimal," +
            "IN_STATION_QTY decimal," +
            "OUT_STATION_QTY decimal," +
            "SCRAPED_QTY decimal," +
            "CID decimal," +
            "VALIDATE_FLAG string," +
            "MAKE_ORDER_ID decimal," +
            "OPERATION_SEQ_NUM decimal," +
            "DUTY_SCRAPED_QTY decimal," +
            "ITEM_SCRAPED_QTY decimal," +
            "REPAIR_QTY decimal," +
            "REPAIR_FLAG string," +
            "COMPLETE_USER_ID decimal," +
            "DELIVERY_TICKET string," +
            "WIP_BARCODE_QTY decimal," +
            "WIP_BARCODE_SCRAPED_QTY decimal," +
            "REMARKS string," +
            "update_datetime timestamp" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_executive_order");
    //
    public static String destinationSql7 = "CREATE TABLE doris_ods_mes_production_prodline_group (\n" +
            "PROD_LINE_GROUP_ID int," +
            "CREATED_BY int," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY int," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN int," +
            "SCHEDULE_REGION_ID int," +
            "PROD_LINE_GROUP_CODE string," +
            "DESCRIPTIONS string," +
            "ORDER_BY_CODE string," +
            "PLAN_START_TIME timestamp," +
            "ENABLE_FLAG string," +
            "PROCESS_SEQUENCE string," +
            "PERIODIC_TIME string," +
            "BASIC_ALGORITHM string," +
            "EXTENDED_ALGORITHM string," +
            "FIX_TIME_FENCE double," +
            "FORWARD_PLANNING_TIME_FENCE double," +
            "PROD_LINE_RULE string," +
            "RELEASE_TIME_FENCE double," +
            "PLANNING_PHASE_TIME string," +
            "PLANNING_BASE string," +
            "DELAY_TIME_FENCE double," +
            "FROZEN_TIME_FENCE double," +
            "ORDER_TIME_FENCE double," +
            "RELEASE_CONCURRENT_RULE string," +
            "PLAN_COLLABORATIVE_RULE string," +
            "CID int," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_group");
    //
    public static String destinationSql8 = "CREATE TABLE doris_ods_mes_production_prodline_line (\n" +
            "prod_line_id decimal," +
            "created_by decimal," +
            "creation_date timestamp," +
            "last_updated_by decimal," +
            "last_update_date timestamp," +
            "last_update_login decimal," +
            "schedule_region_id decimal," +
            "plant_id decimal," +
            "prod_line_group_id decimal," +
            "prod_line_code string," +
            "prod_line_alias string," +
            "prod_line_type string," +
            "descriptions string," +
            "rate_type string," +
            "rate decimal," +
            "activity decimal," +
            "planner decimal," +
            "business_area string," +
            "order_by_code string," +
            "enable_flag string," +
            "issued_warehouse_code string," +
            "issued_locator_code string," +
            "fix_time_fence decimal," +
            "supplier_id decimal," +
            "supplier_site_id decimal," +
            "forward_planning_time_fence decimal," +
            "mo_refresh_flag string," +
            "completion_warehouse_code string," +
            "completion_locator_code string," +
            "frozen_time_fence decimal," +
            "order_time_fence decimal," +
            "release_time_fence decimal," +
            "inventory_warehouse_code string," +
            "inventory_locator_code string," +
            "cid decimal," +
            "update_datetime timestamp" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_line");
    //
    public static String destinationSql9 = "CREATE TABLE doris_ods_mes_production_prodline_line_item (\n" +
            "prod_line_id decimal," +
            "created_by decimal," +
            "creation_date timestamp," +
            "last_updated_by decimal," +
            "last_update_date timestamp," +
            "last_update_login decimal," +
            "relation_id decimal," +
            "plant_id decimal," +
            "material_id decimal," +
            "production_version string," +
            "rate_type string," +
            "rate decimal," +
            "activity decimal," +
            "priority decimal," +
            "process_batch decimal," +
            "standard_rate_type string," +
            "standard_rate decimal," +
            "auto_assign_flag string," +
            "pre_processing_lead_time decimal," +
            "processing_lead_time decimal," +
            "post_processing_lead_time decimal," +
            "safety_lead_time decimal," +
            "issue_warehouse_code string," +
            "issue_locator_code string," +
            "complete_warehouse_code string," +
            "complete_locator_code string," +
            "inventory_warehouse_code string," +
            "inventory_locator_code string," +
            "cid decimal," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_line_item");
    //
    public static String destinationSql10 = "CREATE TABLE doris_ods_mes_production_prodline_make_order (\n" +
            "MAKE_ORDER_ID decimal," +
            "CREATED_BY decimal," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY decimal," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN decimal," +
            "CID decimal," +
            "manufacture_order_id string," +
            "PLANT_ID decimal," +
            "SCHEDULE_REGION_ID decimal," +
            "material_id decimal," +
            "MO_QTY decimal," +
            "SUPPLY_QTY decimal," +
            "PLAN_QTY decimal," +
            "DEMAND_DATE timestamp," +
            "MAKE_ORDER_TYPE string," +
            "PRODUCTION_VERSION string," +
            "PRIORITY decimal," +
            "MAKE_ORDER_STATUS string," +
            "MO_LAST_STATUS string," +
            "ERP_JOB_TYPE_ID decimal," +
            "DEMAND_ORDER_ID decimal," +
            "USER_DEMAND_DATE timestamp," +
            "PROD_LINE_ID decimal," +
            "PROD_LINE_FIX_FLAG string," +
            "SCHEDULE_RELEASE_TIME timestamp," +
            "EARLIEST_START_TIME timestamp," +
            "START_TIME timestamp," +
            "FPS_TIME timestamp," +
            "FPC_TIME timestamp," +
            "LPS_TIME timestamp," +
            "LPC_TIME timestamp," +
            "FULFILL_TIME timestamp," +
            "TOP_MO_ID decimal," +
            "PARENT_MO_ID decimal," +
            "SOURCE_MO_ID decimal," +
            "REFERENCE_MO_ID decimal," +
            "MO_REFERENCE_TYPE string," +
            "MO_CAPACITY decimal," +
            "RATE decimal," +
            "RATE_TYPE string," +
            "DIE_PLANNING_ID decimal," +
            "PLAN_TYPE string," +
            "PLANNING_FLAG string," +
            "EXPLORED_FLAG string," +
            "DERIVED_FLAG string," +
            "MO_WARNNING_FLAG string," +
            "ENDING_PROCESS_FLAG string," +
            "BOM_LEVEL string," +
            "EXCEED_LEAD_TIME decimal," +
            "PRE_PROCESSING_LEAD_TIME decimal," +
            "PROCESSING_LEAD_TIME decimal," +
            "POST_PROCESSING_LEAD_TIME decimal," +
            "SAFETY_LEAD_TIME decimal," +
            "SWITCH_TIME decimal," +
            "SWITCH_TIME_USED decimal," +
            "RELEASE_TIME_FENCE decimal," +
            "ORDER_TIME_FENCE decimal," +
            "RELEASED_DATE timestamp," +
            "RELEASED_BY decimal," +
            "CLOSED_DATE timestamp," +
            "CLOSED_BY decimal," +
            "SPECIAL_COLOR string," +
            "PLANNING_REMARK string," +
            "REMARK string," +
            "ATTRIBUTE1 string," +
            "ATTRIBUTE2 string," +
            "ATTRIBUTE3 string," +
            "ATTRIBUTE4 string," +
            "ATTRIBUTE5 string," +
            "ATTRIBUTE6 string," +
            "ATTRIBUTE7 string," +
            "ATTRIBUTE8 string," +
            "ATTRIBUTE9 string," +
            "ATTRIBUTE10 string," +
            "ATTRIBUTE11 string," +
            "ATTRIBUTE12 string," +
            "ATTRIBUTE13 string," +
            "ATTRIBUTE14 string," +
            "ATTRIBUTE15 string," +
            "sale_order_id string," +
            "sale_row_id string," +
            "ENABLE_FLAG string," +
            "HEAT_SPLIT_FLAG string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_make_order");
    //
    public static String destinationSql11 = "CREATE TABLE doris_ods_mes_production_prodline_move_record (\n" +
            "KID string," +
            "CREATED_BY string," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATE_BY string," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN string," +
            "PLANT_ID string," +
            "BARCODE_NUMBER string," +
            "WORKCELL_ID string," +
            "EO_ID string," +
            "IN_STATION_DATE timestamp," +
            "IN_STATION_QTY string," +
            "IN_STATION_USER_ID string," +
            "OUT_STATION_DATE timestamp," +
            "OUT_STATION_QTY string," +
            "OUT_STATION_USER_ID string," +
            "REMARK string," +
            "ATTRIBUTE1 string," +
            "ATTRIBUTE2 string," +
            "ATTRIBUTE3 string," +
            "ATTRIBUTE4 string," +
            "ATTRIBUTE5 string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_move_record");
    //
    public static String destinationSql12 = "CREATE TABLE doris_ods_mes_production_prodline_operation_sequence (\n" +
            "KID STRING," +
            "CREATED_DATE timestamp," +
            "CREATED_BY STRING," +
            "LAST_UPDATE_BY STRING," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN STRING," +
            "EO_ID STRING," +
            "OPERATION_SEQ_NUM STRING," +
            "FIRST_OPS_FLAG STRING," +
            "LAST_OPS_FLAG STRING," +
            "CID string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_operation_sequence");
    //
    public static String destinationSql13 = "CREATE TABLE doris_ods_mes_production_prodline_wkcg_rel (\n" +
            "REL_ID string," +
            "CREATED_DATE timestamp," +
            "CREATED_BY string," +
            "LAST_UPDATE_BY string," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN string," +
            "PROD_LINE_ID string," +
            "WORKCELL_ID string," +
            "PRIORITY string," +
            "ENABLE_FLAG string," +
            "CID string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_wkcg_rel");
    //
    public static String destinationSql14 = "CREATE TABLE doris_ods_mes_production_standard_operations (\n" +
            "STANDARD_OP_ID int," +
            "CREATION_DATE timestamp," +
            "CREATED_BY string," +
            "LAST_UPDATED_BY string," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN string," +
            "PLANT_ID string," +
            "CODE string," +
            "KEY_OP_FLAG string," +
            "MOVE_TYPE string," +
            "INSPECT_TYPE string," +
            "CHARGE_TYPE string," +
            "ENABLE_FLAG string," +
            "PROCESS_TIME string," +
            "STANDARD_WORK_TIME string," +
            "OPERATION_DOCUMENT string," +
            "PROCESS_PROGRAM string," +
            "CID string," +
            "DESCRIPTION string," +
            "SELF_TEST_FLAG string," +
            "FIRST_TEST_FLAG string," +
            "HEAT_TREATMENT_FLAG string," +
            "HEAT_TREATMENT_OS_FLAG string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_standard_operations");
    //
    public static String destinationSql15 = "CREATE TABLE doris_ods_mes_production_wip_barcode (\n" +
            "KID decimal," +
            "CREATED_BY decimal," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY decimal," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN decimal," +
            "WIP_BARCODE string," +
            "EO_ID decimal," +
            "PLANT_ID decimal," +
            "material_id decimal," +
            "QTY decimal," +
            "STATUS string," +
            "PACKING_LIST string," +
            "PACKING_LIST_CREATED_BY decimal," +
            "PACKING_LIST_CREATED_DATE timestamp," +
            "PACKING_LIST_PRINTED_BY decimal," +
            "PACKING_LIST_PRINTED_DATE timestamp," +
            "PARENT_WIP_BARCODE string," +
            "INSPECT_STATUS string," +
            "FURNACE_ID decimal," +
            "OUTSOURCING_STATUS string," +
            "REWORK_MARK string," +
            "LOCATOR_CODE string," +
            "CARRIER_CODE string," +
            "DISMANTLING string," +
            "CID decimal," +
            "ATTRIBUTE1 string," +
            "ATTRIBUTE2 string," +
            "ATTRIBUTE3 string," +
            "ATTRIBUTE4 string," +
            "ATTRIBUTE5 string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_wip_barcode");
    //
    public static String destinationSql16 = "CREATE TABLE doris_ods_mes_supplychain_material_factory (\n" +
            "PLANT_ID int," +
            "CREATED_BY int," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY int," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN int," +
            "SCHEDULE_REGION_ID int," +
            "PLANT_CODE string," +
            "DESCRIPTIONS string," +
            "ENABLE_FLAG string," +
            "MAIN_PLANT_FLAG string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_supplychain_material_factory");
    //
    public static String destinationSql17 = "CREATE TABLE doris_ods_mes_supplychain_material_item (\n" +
            "MATERIAL_ID decimal," +
            "CREATED_BY decimal," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY decimal," +
            "LAST_UPDATE_DATE timestamp," +
            "PLANT_ID decimal," +
            "ITEM_CODE string," +
            "PRIMARY_UOM string," +
            "DESIGN_CODE string," +
            "PLAN_CODE string," +
            "ITEM_IDENTIFY_CODE string," +
            "ITEM_TYPE string," +
            "MAKE_BUY_CODE string," +
            "SUPPLY_TYPE string," +
            "KEY_COMPONENT_FLAG string," +
            "SCHEDULE_FLAG string," +
            "MAKE_TO_ORDER_FLAG string," +
            "PROD_LINE_RULE string," +
            "ITEM_SIZE string," +
            "UNIT_WEIGHT decimal," +
            "ENABLE_FLAG string," +
            "PRE_PROCESSING_LEAD_TIME decimal," +
            "PROCESSING_LEAD_TIME decimal," +
            "POST_PROCESSING_LEAD_TIME decimal," +
            "SAFETY_LEAD_TIME decimal," +
            "EXCEED_LEAD_TIME decimal," +
            "DEMAND_TIME_FENCE decimal," +
            "RELEASE_TIME_FENCE decimal," +
            "ORDER_TIME_FENCE decimal," +
            "DEMAND_MERGE_TIME_FENCE string," +
            "SUPPLY_MERGE_TIME_FENCE string," +
            "SAFETY_STOCK_METHOD string," +
            "SAFETY_STOCK_PERIOD decimal," +
            "SAFETY_STOCK_VALUE decimal," +
            "MAX_STOCK_QTY decimal," +
            "MIN_STOCK_QTY decimal," +
            "PRODUCT_CAPACITY_TIME_FENCE string," +
            "PRODUCT_CAPACITY decimal," +
            "ASSEMBLY_SHRINKAGE decimal," +
            "ECONOMIC_LOT_SIZE decimal," +
            "ECONOMIC_SPLIT_PARAMETER decimal," +
            "MIN_ORDER_QTY decimal," +
            "FIXED_LOT_MULTIPLE decimal," +
            "PACK_QTY decimal," +
            "SEQUENCE_LOT_CONTROL string," +
            "ISSUE_CONTROL_TYPE string," +
            "ISSUE_CONTROL_QTY decimal," +
            "COMPLETE_CONTROL_TYPE string," +
            "COMPLETE_CONTROL_QTY decimal," +
            "EXPIRE_CONTROL_FLAG string," +
            "EXPIRE_DAYS decimal," +
            "RELEASE_CONCURRENT_RULE string," +
            "CID decimal," +
            "DESCRIPTIONS string," +
            "`LANGUAGE` string," +
            "SCHEDULE_REGION_ID string," +
            "ITEM_GROUP string," +
            "`MODULE` string," +
            "RECOIL_SAFE_QTY decimal," +
            "RECOIL_CALL_QTY decimal," +
            "MO_SPLIT_FLAG string," +
            "RECOIL_FLAG string," +
            "LOT_FLAG string," +
            "DES_LONG string," +
            "ELECT_MACHINE_BRAND string," +
            "ELECT_MACHINE_V string," +
            "BRAKE_B_V string," +
            "INDEP_FAN_V string," +
            "PROTECT_LEVEL string," +
            "ELECT_MACHINE_FREQUENCY string," +
            "ELECT_MACHINE_REMARK1 string," +
            "ELECT_MACHINE_REMARK2 string," +
            "WEIGHT string," +
            "CASE_MODEL string," +
            "ELECT_MACHINE_MODEL string," +
            "INSTALL string," +
            "SPEED_RATIO string," +
            "LENGTHS string," +
            "WIDE string," +
            "HIGH string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_supplychain_material_item");
    //
    public static String destinationSql18 = "CREATE TABLE doris_ods_mes_supplychain_material_item_b (\n" +
            "material_id decimal," +
            "created_by decimal," +
            "creation_date timestamp," +
            "last_updated_by decimal," +
            "last_update_date timestamp," +
            "plant_id decimal," +
            "material_id_2 string," +
            "primary_uom string," +
            "design_code string," +
            "plan_code string," +
            "item_identify_code string," +
            "item_type string," +
            "make_buy_code string," +
            "supply_type string," +
            "key_component_flag string," +
            "schedule_flag string," +
            "make_to_order_flag string," +
            "prod_line_rule string," +
            "item_size string," +
            "unit_weight decimal," +
            "enable_flag string," +
            "pre_processing_lead_time decimal," +
            "processing_lead_time decimal," +
            "post_processing_lead_time decimal," +
            "safety_lead_time decimal," +
            "exceed_lead_time decimal," +
            "demand_time_fence decimal," +
            "release_time_fence decimal," +
            "order_time_fence decimal," +
            "demand_merge_time_fence string," +
            "supply_merge_time_fence string," +
            "safety_stock_method string," +
            "safety_stock_period decimal," +
            "safety_stock_value decimal," +
            "max_stock_qty decimal," +
            "min_stock_qty decimal," +
            "product_capacity_time_fence string," +
            "product_capacity decimal," +
            "assembly_shrinkage decimal," +
            "economic_lot_size decimal," +
            "economic_split_parameter decimal," +
            "min_order_qty decimal," +
            "fixed_lot_multiple decimal," +
            "pack_qty decimal," +
            "sequence_lot_control string," +
            "issue_control_type string," +
            "issue_control_qty decimal," +
            "complete_control_type string," +
            "complete_control_qty decimal," +
            "expire_control_flag string," +
            "expire_days decimal," +
            "release_concurrent_rule string," +
            "cid decimal," +
            "last_update_login decimal," +
            "item_group_id decimal," +
            "recoil_safe_qty decimal," +
            "recoil_call_qty decimal," +
            "mo_split_flag string," +
            "recoil_flag string," +
            "lot_flag string," +
            "review_flag string," +
            "des_long string," +
            "source_item_id decimal," +
            "elect_machine_brand string," +
            "elect_machine_v string," +
            "brake_b_v string," +
            "indep_fan_v string," +
            "protect_level string," +
            "elect_machine_frequency string," +
            "elect_machine_remark1 string," +
            "elect_machine_remark2 string," +
            "weight string," +
            "`module` string," +
            "case_model string," +
            "elect_machine_model string," +
            "install string," +
            "speed_ratio string," +
            "lengths string," +
            "wide string," +
            "high string," +
            "attribute1 string," +
            "attribute2 string," +
            "attribute3 string," +
            "attribute4 string," +
            "attribute5 string," +
            "attribute6 string," +
            "attribute7 string," +
            "attribute8 string," +
            "attribute9 string," +
            "attribute10 string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_supplychain_material_item_b");
    //
    public static String destinationSql19 = "CREATE TABLE doris_ods_mes_supplychain_material_locator (\n" +
            "KID int," +
            "CREATED_BY int," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY int," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN int," +
            "PLANT_ID int," +
            "LOCATOR_CODE string," +
            "WAREHOUSE_CODE string," +
            "DESCRIPTION string," +
            "ENABLE_FLAG string," +
            "CID int," +
            "LOCATOR_TYPE string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_supplychain_material_locator");
    //
    public static String destinationSql20 = "CREATE TABLE doris_ods_mes_supplychain_material_requirement (\n" +
            "KID STRING," +
            "CREATED_DATE timestamp," +
            "CREATED_BY STRING," +
            "LAST_UPDATE_BY STRING," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN STRING," +
            "EO_ID STRING," +
            "COMPONET_PLANT_ID STRING," +
            "COMPONET_ITEM_ID STRING," +
            "UOM STRING," +
            "BOM_USAGE STRING," +
            "DEMAND_QTY STRING," +
            "ISSUED_QTY STRING," +
            "MTL_SCRAPED_QTY STRING," +
            "OP_SCRAPED_QTY STRING," +
            "SUPPLY_TYPE STRING," +
            "WAREHOUSE_CODE STRING," +
            "LOCATOR_CODE STRING," +
            "CID STRING," +
            "RECOIL_FLAG STRING," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_supplychain_material_requirement");
    //
    public static String destinationSql21 = "CREATE TABLE doris_ods_mes_supplychain_material_warehouse (\n" +
            "KID int," +
            "CREATED_BY int," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY int," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN int," +
            "PLANT_ID int," +
            "WAREHOUSE_CODE string," +
            "DESCRIPTIONS string," +
            "ENABLE_FLAG string," +
            "WAREHOUSE_SHORT_CODE string," +
            "WAREHOUSE_TYPE string," +
            "WMS_FLAG string," +
            "ONHAND_FLAG string," +
            "LOCATOR_FLAG string," +
            "PLAN_FLAG string," +
            "NEGATIVE_FLAG string," +
            "CID int," +
            "THING_LABEL_FLAG string," +
            "WORKSHOP_ID int," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_supplychain_material_warehouse");
    //
    public static String destinationSql22 = "CREATE TABLE doris_ods_mes_supplychain_material_wip_barcode (\n" +
            "KID decimal," +
            "CREATED_BY decimal," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY decimal," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN decimal," +
            "WIP_BARCODE string," +
            "EO_ID decimal," +
            "PLANT_ID decimal," +
            "material_id decimal," +
            "QTY decimal," +
            "STATUS string," +
            "PACKING_LIST string," +
            "PACKING_LIST_CREATED_BY decimal," +
            "PACKING_LIST_CREATED_DATE timestamp," +
            "PACKING_LIST_PRINTED_BY decimal," +
            "PACKING_LIST_PRINTED_DATE timestamp," +
            "PARENT_WIP_BARCODE string," +
            "INSPECT_STATUS string," +
            "FURNACE_ID decimal," +
            "OUTSOURCING_STATUS string," +
            "REWORK_MARK string," +
            "LOCATOR_CODE string," +
            "CARRIER_CODE string," +
            "DISMANTLING string," +
            "CID decimal," +
            "ATTRIBUTE1 string," +
            "ATTRIBUTE2 string," +
            "ATTRIBUTE3 string," +
            "ATTRIBUTE4 string," +
            "ATTRIBUTE5 string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_supplychain_material_wip_barcode");
    //
    public static String destinationSql23 = "CREATE TABLE doris_ods_mes_supplychain_plan_region (\n" +
            "SCHEDULE_REGION_ID int," +
            "CREATED_BY int," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY int," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN int," +
            "SCHEDULE_REGION_CODE string," +
            "DESCRIPTIONS string," +
            "ENABLE_FLAG string," +
            "PLAN_START_TIME timestamp," +
            "PERIODIC_TIME string," +
            "DEMAND_TIME_FENCE decimal," +
            "FIX_TIME_FENCE decimal," +
            "FROZEN_TIME_FENCE decimal," +
            "FORWARD_PLANNING_TIME_FENCE decimal," +
            "RELEASE_TIME_FENCE decimal," +
            "ORDER_TIME_FENCE decimal," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_supplychain_plan_region");
    //
    public static String destinationSql24 = "CREATE TABLE doris_ods_mes_supplychain_workcell (\n" +
            "WORKCELL_ID decimal," +
            "CREATED_DATE TIMESTAMP," +
            "CREATED_BY decimal," +
            "LAST_UPDATE_BY decimal," +
            "LAST_UPDATE_DATE TIMESTAMP," +
            "LAST_UPDATE_LOGIN decimal," +
            "WORKCELL_CODE STRING," +
            "DESCRIPTION STRING," +
            "WORKCELL_TYPE STRING," +
            "MERGE_FLAG STRING," +
            "GPS_INFO STRING," +
            "ENABLE_FLAG STRING," +
            "CID decimal," +
            "WORKSHOP_ID decimal," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_supplychain_workcell");
    public static String destinationSql25 = "CREATE TABLE doris_ods_mes_production_prodline_version (\n" +
            "PRODUCTION_VERSION_ID decimal," +
            "CREATED_BY decimal," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY decimal," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN decimal," +
            "PLANT_ID decimal," +
            "material_id decimal," +
            "PRODUCTION_VERSION string," +
            "BOM_ID decimal," +
            "ROUTING_ID decimal," +
            "START_DATE timestamp," +
            "END_DATE timestamp," +
            "DESCRIPTIONS string," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_version");
    public static String destinationSql26 = "CREATE TABLE doris_ods_mes_production_prodline_version_b (\n" +
            "PRODUCTION_VERSION_ID decimal," +
            "CREATED_BY decimal," +
            "CREATION_DATE timestamp," +
            "LAST_UPDATED_BY decimal," +
            "LAST_UPDATE_DATE timestamp," +
            "LAST_UPDATE_LOGIN decimal," +
            "PLANT_ID decimal," +
            "material_id decimal," +
            "PRODUCTION_VERSION STRING," +
            "BOM_ID decimal," +
            "ROUTING_ID decimal," +
            "START_DATE timestamp," +
            "END_DATE timestamp," +
            "CID decimal," +
            "update_datetime TIMESTAMP" +
            "    ) \n" + DDLUtil.getDorisDDL("ods_mes_production_prodline_version_b");
}
