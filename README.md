# Fraud-Analytics-Realtime-streaming-

Fraud Analytics

1) Create Producer : Generate a data (fraud/non-fraud) in a real time. We will send this real time data to Kafka topics.
   
2) From this kafka we are going to create a consumer inside a databricks.
Then we will write some logic to find out the dataset we are consuming if is a fraud / non-fraud.

If we find fraud data, we will store it in a one mongoDB collection,
Once the fraud data is available in a one mongoDB collection, we will create a trigger which is going to send a mail for fraudlant detection.

First we created a producer which is going to produce/generate a data in real time in a batch.

Producer : It is a system which produce/generate a data in real time.

