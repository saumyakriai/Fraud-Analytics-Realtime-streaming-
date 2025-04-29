Fraud Analytics 

1) Create Producer : Generate a data (fraud/non-fraud) in a real time. We will send this real time data to Kafka topics.
2) From this kafka we are going to create a consumer inside a databricks.
Then we will write some logic to find out the dataset we are consuming is a fraud / non-fraud.
If we find fraud data, we will store it in a mongoDB,
Once the fraud data is available in a one mongoDB collection, we will create a trigger which is going to send a mail for fraudlant detection.

First we created a producer which is going to produce/generate a data in real time in a batch.
Producer : It is a system which produce/generate a data in real time.

First : Pushing the data into kafka cluster. 
We used faker to generate synthetic data.
created a producer variable to connect with kafka with real time and created a function to generate a synthetic data to sending to kafka clusters.
keep on sending the data to kafla topics

We have marked fraud transaction where the amount > 5000 and fraud score is > 0.8.

One more collection for non- fradulant data as collection non_fraud.

Once we have data into mongodb, we need to send the mail and set trigger.

**Fraud Condition:**
1)If transaction amount is usually high. ex amount > 50000;
if amount > 50000:
  fraud_reason.append(f"blacklisted merchant{merchant}")
2)check if merchanct is high risk location.
if merchant in blacklisted_merchants:
	fraud_reason.append(f"suspicious location: {location}")
3)check if merchant is blacklisted.
if merchant in blacklisted_merchants:
	fraud_reason.append(f"suspicious location: {location}")
4)Detect rapid transaction (multiple transaction within 10 seconds)
if user_id in user_transactions:
	for prev_timestamp,prev_amount,prev_location in user_transactions[user_id]:
		if timestamp - prev_timestamps < 10:
		fraud_reason.append("multiple rapid transaction detected.)

5)check for unusual location change(user shoud not be at two different location in short time)
if user_id in user_transaction and len(user_transaction[user_id])>0:
	last_location = user_transaction[user_id][-1][2]
	coord1 = get_coordinates(last_location)
	coord2 = get_cordinates(location)

if coord1 and coord2:
	distance_km = geodesic(coord1, coord2).km
if distance_km > 1000:
	fraud_reason.append(f"unusaul location change: {last_location} ({distance})

Length of user transaction

