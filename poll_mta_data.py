from constants import *
import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import pandas as pd
from datetime import datetime
from functools import reduce
import time
import glob
import os


def poll_trip_updates():
    data_update_time = datetime.now()

    def parse_data(url):
        res = requests.get(url, headers={"x-api-key": API_KEY})
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(res.content)
        message_array = MessageToDict(feed).get('entity')
        return list(filter(lambda x: 'tripUpdate' in x, message_array))

    trip_updates = [parse_data(url) for url in URLS_ALL_LINES]
    trip_updates = reduce(lambda x, y: x + y, trip_updates, [])

    return trip_updates, data_update_time


def parse_trip_update(trip_updates, update_time):
    message_data = []
    for message in trip_updates:
        trip_update = message.get('tripUpdate')
        trip_date = trip_update.get('trip').get('startDate')
        trip_id = trip_update.get('trip').get('tripId')
        line = trip_update.get('trip').get('routeId')

        stop_time_updates = trip_update.get('stopTimeUpdate')
        if stop_time_updates is None:
            continue
        for stop_time_update in stop_time_updates:
            stop_id = stop_time_update.get('stopId')
            stop_name = stop_lookup_dict.get(stop_id)

            stop_update_info_all = {'trip_id': trip_id,
                                    'trip_date': trip_date,
                                    'line': line,
                                    'stop_id': stop_id,
                                    'stop_name': stop_name,
                                    'update_time': update_time}

            if 'arrival' in stop_time_update:
                arrival_time = stop_time_update.get('arrival').get('time')
                arrival_time_readable = datetime.fromtimestamp(int(arrival_time))
                stop_update_info_all['arrival_time'] = arrival_time_readable
            else:
                stop_update_info_all['arrival_time'] = None

            if 'departure' in stop_time_update:
                departure_time = stop_time_update.get('departure').get('time')
                departure_time_readable = datetime.fromtimestamp(int(departure_time))
                stop_update_info_all['departure_time'] = departure_time_readable
            else:
                stop_update_info_all['departure_time'] = None

            message_data.append(stop_update_info_all)

    return message_data


def poll_and_update():
    raw_trip_updates, data_update_time = poll_trip_updates()
    message_data = parse_trip_update(raw_trip_updates, data_update_time)
    date = data_update_time.date()
    time = data_update_time.time()
    pd.DataFrame(data=message_data).to_csv(
        'Data/train_timing_data/{}/train_data_{}.csv'.format(date, time), index=False)


if __name__ == '__main__':

    stop_id_to_name = pd.read_csv('Data/google_transit/stops.txt')
    stop_lookup_dict = stop_id_to_name.set_index('stop_id')['stop_name'].to_dict()

    poll_and_update()
