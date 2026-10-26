CREATE OR REPLACE TABLE `dp-mlops17-ohuaman-pf-proc.ds_procesamiento.table_output` (
  MSSubClass FLOAT64,
  MSZoning FLOAT64,
  LotArea FLOAT64,
  LotShape FLOAT64,
  LandContour FLOAT64,
  LotConfig FLOAT64,
  Neighborhood FLOAT64,
  OverallQual FLOAT64,
  OverallCond FLOAT64,
  YearRemodAdd FLOAT64,
  RoofStyle FLOAT64,
  Exterior1st FLOAT64,
  ExterQual FLOAT64,
  Foundation FLOAT64,
  BsmtQual FLOAT64,
  BsmtExposure FLOAT64,
  BsmtFinType1 FLOAT64,
  HeatingQC FLOAT64,
  CentralAir FLOAT64,
  stFlrSF FLOAT64,
  ndFlrSF FLOAT64,
  GrLivArea FLOAT64,
  BsmtFullBath FLOAT64,
  FullBath FLOAT64,
  HalfBath FLOAT64,
  KitchenQual FLOAT64,
  TotRmsAbvGrd FLOAT64,
  Functional FLOAT64,
  Fireplaces FLOAT64,
  FireplaceQu FLOAT64,
  GarageFinish FLOAT64,
  GarageCars FLOAT64,
  PavedDrive FLOAT64,
  WoodDeckSF FLOAT64,
  ScreenPorch FLOAT64,
  SaleCondition FLOAT64
)
OPTIONS(description="Tabla temporal del pipeline: universo preparado para la prediccion");

CREATE OR REPLACE PROCEDURE `dp-mlops17-ohuaman-pf-proc.ds_procesamiento.prc_preparar_universo`(input_overall_qual FLOAT64)
BEGIN

  TRUNCATE TABLE `dp-mlops17-ohuaman-pf-proc.ds_procesamiento.table_output`;

  INSERT INTO `dp-mlops17-ohuaman-pf-proc.ds_procesamiento.table_output`
  SELECT
    MSSubClass,
    MSZoning,
    LotArea,
    LotShape,
    LandContour,
    LotConfig,
    Neighborhood,
    OverallQual,
    OverallCond,
    YearRemodAdd,
    RoofStyle,
    Exterior1st,
    ExterQual,
    Foundation,
    BsmtQual,
    BsmtExposure,
    BsmtFinType1,
    HeatingQC,
    CentralAir,
    stFlrSF,
    ndFlrSF,
    GrLivArea,
    BsmtFullBath,
    FullBath,
    HalfBath,
    KitchenQual,
    TotRmsAbvGrd,
    Functional,
    Fireplaces,
    FireplaceQu,
    GarageFinish,
    GarageCars,
    PavedDrive,
    WoodDeckSF,
    ScreenPorch,
    SaleCondition
  FROM `dp-mlops17-ohuaman-pf-fuente.hist_table.input_data`
  WHERE OverallQual >= input_overall_qual;

END;
