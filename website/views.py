from flask import Blueprint, render_template, request, jsonify
from poll_mta_data import get_single_snap, display_df, get_all_stations

views = Blueprint('views', __name__)


@views.route('/')
def home():
    return render_template("home.html")


@views.route('/train_data', methods=['GET'])
def get_train_data():
    station = request.args.get('station')
    return display_df(get_single_snap(station))
    # return 'This is from my local Flask!'


@views.route('/station_data', methods=['GET'])
def get_station_data():
    return jsonify(data=get_all_stations())
