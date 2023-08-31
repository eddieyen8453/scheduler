# scheduler
It's a scheduler base on airflow and work with flask  

# 情境
透過模型計算客戶預期的消費間距再比對大量且即時的客戶消費資料，若客戶沒有在預期時間內進行刷卡消費則給予關懷與提醒，但未於預測時間內刷卡的情形，沒有交易資料去驅動決策平台發送簡訊。
## 目標
因上述情境需求，要有一個可以接收大量交易紀錄與預測消費間距的服務(Transaction trigger)，該服務將會接到大量的客戶交易紀錄與批次名單的客戶預測消費間距，並且透過定時排程系統計算出沒有進行消費的目標客戶名單，並將這些目標客戶存入資料庫，提供訊息系統發送促刷簡訊給客戶。

# 架構
可以三個部分來看:
Scheduler是透過Airflow實作的定時trigger排程器，共有三項子任務排程執行
API是透過Flask框架實作的服務，其中有四隻API提供服務使用
Stroage則是由redis與sql組成，大量且即時的客戶消費資料透過符合向性的Redis資料庫儲存，其餘的資料由sql資料庫儲存

# 實作
在airflow目錄下，以 docker compose up 啟用build多個image與run多個container

# 補充
## airflow 目錄下結構

├─config

├─dags

│  └─__pycache__

├─flask_api

├─logs

└─plugins

## volumes
docker-compose.yaml中volumes相關路徑改為本機路徑
