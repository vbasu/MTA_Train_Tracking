from constants import *
import requests
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import pandas as pd
from datetime import datetime, timedelta
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
            stop_name = STOP_LOOKUP_DICT.get(stop_id)

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


def get_single_snap(station=None):
    df = pd.DataFrame(columns=['trip_id',
                               'line',
                               'stop_id',
                               'stop_name',
                               'arrival_time',
                               'departure_time',
                               'trip_date',
                               'update_time'
                               ])

    raw_trip_updates, data_update_time = poll_trip_updates()
    message_data = parse_trip_update(raw_trip_updates, data_update_time)
    df = pd.concat([df, pd.DataFrame(data=message_data)], ignore_index=True)

    current_time = datetime.now()
    cutoff_time = current_time + timedelta(0, 600)
    if station is None:
        return df
    df = df.query("stop_name==@station and arrival_time>@current_time and arrival_time<@cutoff_time")
    return df


def get_all_stations():
    df = get_single_snap(station=None)
    return list(df['stop_name'].unique())


def display_df(df: pd.DataFrame):
    # df = df[['line', 'departure_time']].groupby('line').agg('min')

    df = df[['line', 'departure_time']].set_index('line')

    # styled_df = df.style \
    #     .set_table_styles([
    #     {'selector': 'table',
    #      'props': [('border-collapse', 'collapse')]},
    #     {'selector': 'th, td',
    #      'props': [('border', '1px solid black'),
    #                ('padding', '8px')]}]) \
    #     .set_properties(**{'text-align': 'center'}) \
    #     .set_caption('Sample DataFrame')
    # return styled_df.render()
    return df.to_html()


if __name__ == '__main__':

    # poll_and_update()
    # df = pd.DataFrame(columns=['trip_id',
    #                            'line',
    #                            'stop_id',
    #                            'stop_name',
    #                            'arrival_time',
    #                            'departure_time',
    #                            'trip_date',
    #                            'update_time'
    #                            ])
    #
    # raw_trip_updates, data_update_time = poll_trip_updates()
    # message_data = parse_trip_update(raw_trip_updates, data_update_time)
    # df = pd.concat([df, pd.DataFrame(data=message_data)], ignore_index=True)

    test_df = get_single_snap()
    test_df = test_df[['line', 'stop_name', 'departure_time']]
    #test_df['departure_time'] = pd.to_datetime(test_df['departure_time'], format='%H:%M:%s')
    test = list(get_all_stations())
    print('hello')

