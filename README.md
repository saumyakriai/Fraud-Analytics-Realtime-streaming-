# Fraud-Analytics-Realtime-streaming-

Fraud Analytics

1) Create Producer : Generate a data (fraud/non-fraud) in a real time. We will send this real time data to Kafka topics.
2) From this kafka we are going to create a consumer inside a databricks.
Then we will write some logic to find out the dataset we are consuming is a fraud / non-fraud.
If we find fraud data, we will store it in a mongoDB,
Once the data is available in a mongoDB, we will create a trigger which is going to send a mail for fraudlant detection.
