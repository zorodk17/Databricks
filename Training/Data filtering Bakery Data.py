# Databricks notebook source
from pyspark.sql import Row
import datetime
 
bakery={
	"id": "0001",
	"type": "donut",
	"name": "Cake",
	"ppu": 0.55,
	"batters":
		{
			"batter":
				[
					{ "id": "1001", "type": "Regular" },
					{ "id": "1002", "type": "Chocolate" },
					{ "id": "1003", "type": "Blueberry" },
					{ "id": "1004", "type": "Devil's Food" }
				]
		},
	"topping":
		[
			{ "id": "5001", "type": "None" },
			{ "id": "5002", "type": "Glazed" },
			{ "id": "5005", "type": "Sugar" },
			{ "id": "5007", "type": "Powdered Sugar" },
			{ "id": "5006", "type": "Chocolate with Sprinkles" },
			{ "id": "5003", "type": "Chocolate" },
			{ "id": "5004", "type": "Maple" }
		]
}

# COMMAND ----------

def convert_ids(data):
    if isinstance(data, list):
        return [{**item, "id": int(item["id"])} for item in data]
    elif isinstance(data, dict):
        return {**data, "id": int(data["id"])} if "id" in data else data
    return data

# COMMAND ----------

bakery_row = Row(
    id=int(bakery["id"]),
    type=bakery["type"],
    name=bakery["name"],
    ppu=bakery["ppu"]
)
df_bakery = spark.createDataFrame([bakery_row])
df_bakery.display()

# COMMAND ----------

#batters_data = bakery['batters']['batter']
#df_batters = spark.createDataFrame(batters_data)
bakery_row = convert_ids(bakery)
bakery_df = spark.createDataFrame([bakery_row])
df_bakery.display()

# COMMAND ----------

batters_data = convert_ids(bakery['batters']['batter'])
df_batters = spark.createDataFrame(batters_data)
df_batters.display()

# COMMAND ----------

#toppings_df = spark.createDataFrame(bakery['topping'])
toppings_data = convert_ids(bakery['topping'])
df_toppings = spark.createDataFrame(toppings_data)
df_toppings.display()
