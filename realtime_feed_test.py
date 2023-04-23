from constants import *
import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import pandas as pd
from datetime import datetime

url = SUBWAY_FEEDS['A']
res = requests.get(url, headers={"x-api-key": API_KEY})
feed = gtfs_realtime_pb2.FeedMessage()
feed.ParseFromString(res.content)

# for entity in feed.entity:
#     if entity.HasField('trip_update'):
#         print(entity.trip_update)
#         print("-----------\n")

message_array_all = MessageToDict(feed).get('entity')
message_array = list(filter(lambda x: 'tripUpdate' in x, message_array_all))

df = pd.read_csv('Data/google_transit/stops.txt')
stop_lookup_dict = df.set_index('stop_id')['stop_name'].to_dict()


# Parsing logic
for message in message_array:
    trip_update = message.get('tripUpdate')
    trip_date = trip_update.get('trip').get('startDate')
    trip_id = trip_update.get('trip').get('tripId')

    print('Trip date: {}, Trip Id: {}\n'.format(trip_date, trip_id))

    stop_time_updates = trip_update.get('stopTimeUpdate')
    for stop_time_update in stop_time_updates:
        stop_id = stop_time_update.get('stopId')
        stop_name = stop_lookup_dict.get(stop_id)
        print('Stop Location: {} '.format(stop_name))

        if 'arrival' in stop_time_update:
            arrival_time = stop_time_update.get('arrival').get('time')
            arrival_time_readable = datetime.fromtimestamp(int(arrival_time))
            print('Arrival time: {}'.format(arrival_time_readable))

        if 'departure' in stop_time_update:
            departure_time = stop_time_update.get('departure').get('time')
            departure_time_readable = datetime.fromtimestamp(int(departure_time))
            print('Departure time: {}'.format(departure_time_readable))

    print("---------------------\n")


print('hello')
