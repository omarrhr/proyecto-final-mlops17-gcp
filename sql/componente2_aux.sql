CREATE OR REPLACE TABLE `dp-mlops17-ohuaman-pf-prd.ds_produccion.ba_aux_modelo_universo`
(
  -- Universo puntuado: las 36 variables que entran al modelo
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
  SaleCondition FLOAT64,

  -- Resultado del modelo
  prediction INT64,
  prediction_log FLOAT64,
  sale_price_usd FLOAT64,

  -- Parametro con el que se definio el universo de esta corrida
  input_overall_qual FLOAT64,

  -- Control, igual que la tabla de predicciones
  creation_user STRING,
  process_date DATE,
  load_date DATETIME
)
PARTITION BY process_date
OPTIONS(
  partition_expiration_days = 365,
  description = "Foto del universo puntuado en cada ejecucion del pipeline, con el resultado del modelo. Sirve para auditoria, monitoreo de drift y reproceso."
);
