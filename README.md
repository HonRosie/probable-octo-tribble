# Customer Events Counts

## Description
This system returns the number of events sent by a specified customer within a specified timerange, bucketed by hour.

Event data comes in via csv and is processed and stored in a sqlite datbase when the app is initialized. Since the goal of the system is to return only the number of events, the raw event logs are not stored. Instead, we store the count of events per minute per customer to allow for fast retrieval of count information. Events are stored at the minute granularity instead of hour to allow more flexible bucketing in the future should the need arise.

There were a few different options on how to bucket the events per hour. The current implementation returns buckets clamped to the hour, but will only include events within the passed in timestamps. (ie. start => 2:15pm  end => 6pm  Bucket 2-3pm will only include events between 2:15pm-3pm). I went with this strategy since it preserves the user's intent if they give an intra-hour start/end (events are between those specified times), but keeps the buckets consistent with the canonical concept of an hour.

I chose to store event counts in a sqlite database as that seemed like a good, low complexity tradeoff for resilience. If the service were to fail, we wouldn't need to reprocess all the events we've seen up until that point. This allows restarts to be near instant with no extra operational overhead. Using sqlite also allows this service to run on low-end hardware with very little memory, while still scaling up to large numbers of events/customers.

### Assumptions
* System always receives events via csv
* Storing the raw event data is handled elsewhere. The data model for this is designed for enabling fast return of hourly event counts for a specific customer.
* We need to be robust to arbitrary event orderings


## Getting Started

### Dependencies

* pip
* python3
* flask
* pytz
* sqlite3
* dateutil

### Installing

1. Clone this project locally
2. `cd` into the relevant directory
3. `python3 -m venv venv`
4. `. venv/bin/activate`
5. `pip install -r requirements.txt`


### Executing program
1. `python main.py file_path [--server] [--customer-id] [--start] [--end]`
    1. `file_path` Required. is the file path for the events csv
    2. `server` Optional. Start an http service.
    3. `customer_id` Required if not `--server`. If not using the webserver, user must provide the uuid of the customer they want event counts for
    4. `start`/`end` Required if not `--server`. If not using the webserver, user must provide RFC3339 timestamps for the `start` and `end` they want event counts for 

### Example executions
```bash
python main.py 'events.csv' --customer_id 'b4f9279a0196e40632e947dd1a88e857' --start '2021-03-01 00:15:00+00' --end '2021-03-01 04:30:00+00'
```

```bash
python main.py 'events.csv' --server
curl "http://127.0.0.1:8000/hourly?customer_id=b4f9279a0196e40632e947dd1a88e857&start=2021-03-01%2000:15:00%2b00&end=2021-03-01%2004:00:00%2b00"
```

## Future work
* When displaying info to the user, it'd be nice to indicate if the start/end buckets contain truncated information.
* Authentication to confirm callers have access to this data
* Parameter verification in both the route and standard function. At the moment, neither verifies that the passed in parameters are in the expected format
* Data verification on events.csv. There are several rows in the provided csv which appear to have non-standard transaction ids (some are dates and some appear to be partial uuids). While reading in the csv, there should probably be some validation to verify the data is as expected 
* If csv files are very large, it might be useful to have some sort of graceful restart logic so all work isn't lost if processing the csv fails for some reason
* Tests
* The design is ammenable to receiving events from other sources if the need arises.
* For prototype purposes, the CSV is loaded from scratch everytime. We'd want to check if we've seen this CSV or if there have been changes, ideally only looking at new data if possible.