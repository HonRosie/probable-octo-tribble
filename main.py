import argparse
import csv
import json
import pytz
import sqlite3  
import sys

from collections import defaultdict
from datetime import datetime
from dateutil.parser import parse
from flask import Flask, g, request


app = Flask(__name__)

##############################
# Database functions
##############################
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect('database.db')
    db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def init_db():
    with app.app_context():
        db = get_db()
        db.execute("pragma journal_mode=wal")
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()


##############################
# Data Utilities
##############################
def rfc_to_utc_dt(timestamp):
    """
    Convert RFC3339 timestamp to utc datetime
    NOTE: Using strptime by default instead of dateutil.parse since
    lightweight testing had parse taking around ~3x as long
    """
    # Some of the dates have just 00 instead of 0000 for the timezone offset, 
    # we need to ensure tz_info has len == 4 so strptime can recognize it 
    tz_info_idx = timestamp.index('+')
    tz_info_len = len(timestamp) - (tz_info_idx + 1)
    if tz_info_len != 4:
        timestamp = timestamp + "0" * (4 - tz_info_len)
    try:
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f%z')
    except ValueError:
        dt = parse(timestamp)

    return dt.astimezone(pytz.utc)


##############################
# Process events
##############################
def batch_commit(event_count):
    """
    Batch upsert rows in event_count

    :param event_count: dict[(customer_id, minute), count]
    """
    with app.app_context():
        conn = get_db()
        cur = conn.cursor()

        # transform event_count into a list for executemany()
        data = []
        for (customer_id, minute), count in event_count.items():
            data.append((customer_id, minute, count))

        # since events are not ordered by ts, a row for (customer, minute) may already
        # exist. In that case, update the count instead of inserting a new row
        insert = """
            INSERT INTO events_aggregation(customer_id, minute, event_count)
                VALUES(?,?,?)
                ON CONFLICT(customer_id, minute) DO UPDATE SET event_count=event_count+EXCLUDED.event_count;
            """
        cur.executemany(insert, data)
        conn.commit()


def process_csv(file_path):
    """ Read in events.csv and write to event_aggregation table """
    with open(file_path) as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=['customer_id', 'event_type', 'txn_id', 'timestamp'])
        event_count = defaultdict(lambda: 0)
        for row in reader:
            # Commit after processing 1_000 unique (customer, minute)
            # NOTE: The number of rows processed before a commit can be significantly increased
            # without causing any memory issues. Artificially decreased to 1_000 since the
            # given events.csv does not contain that many unique (customer_id, minute) tuples
            if len(event_count.keys()) >= 1_000:
                batch_commit(event_count)

                # Reset event_count and row_idx for next batch
                event_count.clear()

            # Update event_count
            customer_id = row['customer_id']
            timestamp = row['timestamp']
            utc_dt = rfc_to_utc_dt(timestamp)
            rounded_dt = utc_dt.replace(second=0, microsecond=0)
            event_count[(customer_id, rounded_dt)] += 1

        batch_commit(event_count)


##############################
# Initialize app
##############################
def init_app(file_path):
    """ Initialize application """
    init_db()
    process_csv(file_path)


##############################
# Query Events
##############################
def hourly_event_count(customer_id, start, end):
    """
    Returns the number of events sent per hour for a specific customer between the start and end
    timestamps. If the start and end times do not start on the hour, those buckets will only
    include events within the passed in timestamps.
    ie. start => 2:15pm  end => 6pm  Bucket 2-3pm will only include events between 2:15pm-3pm

    :param customer_id: uuid. Customer to return event counts for
    :param start: RFC 3339 str. Start counting events from this timestamp
    :param end: RFC 3339 str. Stop counting events at this timestamp
    """
    start_dt = rfc_to_utc_dt(start)
    end_dt = rfc_to_utc_dt(end)

    event_counts = defaultdict(lambda:0)
    with app.app_context():
        conn = get_db()
        cur = conn.cursor()

        query = (
            "SELECT strftime ('%Y-%m-%d %H', minute) hour, sum(event_count) count "
            "FROM events_aggregation "
            "WHERE minute >= ? "
            "AND minute < ? "
            "AND customer_id = ? "
            "GROUP BY strftime ('%Y-%m-%d %H', minute);"
        )

        hour_events = cur.execute(query, [str(start_dt), str(end_dt), customer_id]).fetchall()
        for hour in hour_events:
            # Converting back to stringified RFC 3339 so as to be consistent with the passed in
            # start/end times. If matching the passed in format isn't important, I'd probably
            # choose to return these as epoch times to make them easier to work with for consumers
            utc_dt = pytz.utc.localize(parse(hour['hour']))
            event_counts[str(utc_dt)] = hour['count']

    return event_counts


##############################
# Routes
##############################

# GET /hourly?customer_id=[...]&start=2021-03-01%2000:30:00%2b00:00&end=2021-03-01%2002:00:00%2b00:00
@app.route('/hourly', methods = ['GET'])
def hourly_event_count_route():
    args = request.args
    customer_id = args.get('customer_id')
    start = args.get('start') # RFC 3339 str
    end = args.get('end')  # RFC 3339 str

    event_counts = hourly_event_count(customer_id, start, end)

    return json.dumps(event_counts)


##############################
# Main
##############################
if __name__ == "__main__":
    parser = argparse.ArgumentParser("Event count by hour")
    parser.add_argument("file_path", help="File path for events csv", type=str)
    parser.add_argument("--server", help="Whether or not to spin up server", nargs="?", const=True, type=bool)
    parser.add_argument("--customer_id", help="Customer uuid to get events for", type=str)
    parser.add_argument("--start", help="RFC3339 timestamp. Start of the timestamp range", type=str)
    parser.add_argument("--end", help="RFC3339 timestamp. End of the timestamp range", type=str)

    args = parser.parse_args()
    init_app(args.file_path)
    if args.server:
        app.run(port=8000)
    else:
        customer_id = args.customer_id
        start = args.start
        end = args.end
        if not args.customer_id or not args.start or not args.end:
            print(f'Missing one of required args: customer_id, start or end')
            sys.exit()
        else:
            print(dict(hourly_event_count(customer_id, start, end)))
