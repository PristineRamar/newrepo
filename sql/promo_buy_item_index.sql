--drop existing index
 DROP INDEX "PRESTO_OM_AZ_DEV"."PM_PROMO_BUY_ITEM_UK1"
--create new index
  CREATE UNIQUE INDEX "PRESTO_OM_AZ_DEV"."PM_PROMO_BUY_ITEM_UK1" ON "PRESTO_OM_AZ_DEV"."PM_PROMO_BUY_ITEM"
  (
    "PROMO_DEFINITION_ID", "ITEM_CODE","CONSTRAINT"
  )
  PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS STORAGE
  (
    INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT
  )
  TABLESPACE "AZ_OM"
  
 