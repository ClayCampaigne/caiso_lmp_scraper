import pandas as pd
import requests
import zipfile
from datetime import datetime
from pytz import timezone
from io import BytesIO
import xmltodict
import time
import os
from argparse import ArgumentParser
# from collections import OrderedDict as odict


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--node', type=str, help = "a CAISO node name")
    parser.add_argument('-m', '--market', type = str, help = "string: RT5, RT15, or DA")
    parser.add_argument('-s', '--startdate', type = str, help = "a string parsable by pandas as a datetime")
    parser.add_argument('-e', '--enddate', type = str, help = "a string parsable by pandas as a datetime")
    parser.add_argument('-p', '--store_path', type = str, default = os.path.dirname(__file__),
                        help="a string representing the directory in which we will create the resulting data file")
    parser.add_argument('--tz_in', type = str, default = 'US/Pacific', help='the timezone of your input args')
    parser.add_argument('--tz_query', type = str, default = 'US/Pacific',
                        help = 'the timezone of your desired query params')
    # by default save the results next to the current file
    args = parser.parse_args()

    assert args.market in ('RT5', 'RT15', 'DA')
    # process / validate datetime arguments
    startdate_pd = pd.to_datetime(args.startdate).tz_localize(args.tz_in)#.tz_convert('UTC')
    enddate_pd = pd.to_datetime(args.enddate).tz_localize(args.tz_in)#.tz_convert('UTC')
    args.startdate = datetime(year=startdate_pd.year, month=startdate_pd.month, day=startdate_pd.day)
    args.enddate = datetime(year=enddate_pd.year, month=enddate_pd.month, day=enddate_pd.day)
    return args


def format_time(dtime, tz_in='US/Pacific', tz_out='US/Pacific'):
    """format a datetime.datetime (in as tz-naive, implicitly tz_in, out as tz_out) for a CAISO OASIS API query"""
    # Sometimes it seems that local time works, and other times UTC works. Could be Descartes' evil genius messing w me again
    # perhaps because for some markets we are restricted to be within a single day
    dtime = timezone(tz_in).localize(dtime)
    if tz_out != tz_in: # convert to desired query timezone if for some reason you care for them to differ
        dtime = dtime.astimezone(timezone(tz_out))
    return dtime.strftime("%Y%m%dT%H:%M%z")


def get_query_params(node='SLAP_PGEB-APND',
                     market='RT5',
                     startdate=datetime(2019, 1, 1),
                     enddate=datetime(2019, 1, 15),
                     tz_in='US/Pacific',
                     tz_out='US/Pacific'):
    """Create a dictionary of query parameters for single query based on arguments. market = RT5, RT15, or DA"""
    assert market in ('RT5', 'RT15', 'DA')
    params = {'node': node,
              'version': 1,
              'startdatetime': format_time(startdate, tz_in=tz_in, tz_out=tz_out),
              'enddatetime': format_time(enddate, tz_in=tz_in, tz_out=tz_out),
              'resultformat': 6}  # 6 is CSV

    if (datetime.now() - startdate) > pd.Timedelta(days=39 * 30.3):
        # CAISO data retention policy as of late 2017 -- no bueno
        print("Watch out! CAISO does not retain data over 39 months old, and your startdate seems to be older.")

    if market == 'DA':  # these querynames only work with these market_run_ids:
        params['queryname'] = 'PRC_LMP'
        params['market_run_id'] = 'DAM'
    elif market == 'RT5':
        params['queryname'] = 'PRC_INTVL_LMP'
        params['market_run_id'] = 'RTM'
        if enddate - startdate > pd.Timedelta(days=1):
            print(
                "Watch out! real-time market queries may be (?) restricted to a single 24 hour period, and yours is not!")
    elif market == 'RT15':
        params['queryname'] = 'PRC_RTPD_LMP'
        params['market_run_id'] = 'RTPD'
        if enddate - startdate > pd.Timedelta(days=15):
            print("Watch out! real-time market queries may be (?) restricted by length, and yours kinda long!")

    return params


def scrape_singlezip(params):
    """Make a single API URL requests"""
    r = requests.get('http://oasis.caiso.com/oasisapi/SingleZip', params=params)
    try:
        zf = zipfile.ZipFile(BytesIO(r.content))
    except Exception as e:
        print(f"Could not load zipfile for following query: exception is {e}")
        for item_ in params.items():  # print out the query details
            print(f"{item_[0]}: {item_[1]}")
        return r
    try:
        return pd.read_csv(BytesIO(zf.read(zf.namelist()[0])), parse_dates=[0, 1, 2])
    except Exception as e:
        print(f"Could not parse zipfile as CSV: (exception is {e})")
        xml_dict = xmltodict.parse(BytesIO(zf.read(zf.namelist()[0])))
        error_msg = xml_dict['m:OASISReport']['m:MessagePayload']['m:RTO']['m:ERROR']['m:ERR_DESC']
        print(f"Could not parse as CSV. Error message is '{error_msg}'")
        for item_ in params.items():  # print out the query details
            print(f"{item_[0]}: {item_[1]}")
        return None  # this will allow an append process to continue if necessary


def scrape_daterange(node='SLAP_PGEB-APND',  # 'SLAP_PGEB-APND', 'PGEB-APND'
                     startdate=datetime(2017, 1, 1),
                     enddate=datetime(2017, 12, 31),
                     market='RT5',
                     tz_in='US/Pacific',
                     tz_query='UTC',
                     store_path=None,
                     cache_continuously=True):
    """
    Breaks up a daterange into appropriate chunks and gets them.
    cache_continuously=True is less efficient but will will always save the sorted result after each query
    """
    assert market in ("RT5", "RT15", "DA")
    chunk_period = {'RT5': 1, 'RT15': 15, 'DA': 30}[market]  # different markets have different allowable query sizes
    chunk_starts = pd.date_range(start=startdate, end=enddate, freq=f'{chunk_period}D')
    print(f"chunk_starts = {chunk_starts}")
    success_srs = pd.Series(index=chunk_starts, data=False)  # a series of indicators for successful retreieval
    result_freq = {'RT5': 5, 'RT15': 15, 'DA': 60}[market]  # will use this for validating results
    result_srs = pd.Series()
    results_dict = {}  #

    i = 0
    while not success_srs.all():
        if not success_srs[i]:
            # we do not have the data for this range
            # print(f"i, i+1 = {i}, {i + 1}")
            # print(f"chunk_starts[i]={chunk_starts[i]}")
            ts = datetime(chunk_starts[i].year, chunk_starts[i].month, chunk_starts[i].day)
            if enddate - ts > pd.Timedelta(days=chunk_period):
                # we are not at the last startdate
                te = datetime(chunk_starts[i + 1].year, chunk_starts[i + 1].month, chunk_starts[i + 1].day)
            else:
                te = enddate
            print(f"Querying (startdate, enddate) = {ts, te}")
            params = get_query_params(node=node,  # 'SLAP_PGEB-APND', 'PGEB-APND'
                                      startdate=ts,
                                      enddate=te,
                                      market=market,
                                      tz_in=tz_in,
                                      tz_out=tz_query)
            df = scrape_singlezip(params)
            pricecol = {'DA': 'MW', 'RT5': 'MW', 'RT15': 'PRC'}[market]
            try:
                df2 = df.set_index('INTERVALSTARTTIME_GMT', drop=True)[['LMP_TYPE', pricecol]].sort_index()
                # get the series
                srs = df2[df2['LMP_TYPE'] == 'LMP'][pricecol]
                results_dict[chunk_starts[i]] = srs
                assert not srs.isna().any()
                success_srs[ts] = True
            except Exception as e:
                print(f'Failed for startdate {ts} with exception {e}')
                # success_srs[ts] = False (remains False) in this case
        if i < len(chunk_starts) - 1:
            i += 1
        else:  # start over to collect missing data
            i = 0
        time.sleep(5)  # don't want the OASIS API to lock us out
        if cache_continuously or success_srs.all():
            try:
                result_srs = pd.concat(results_dict.values()).sort_index()
                result_srs.to_csv(f'./LMP_{node}_{market}_{startdate.date()}_{enddate.date()}.csv', header=True)
            except Exception as e:
                print("could not concatenate results, presumably because there are none")
                print(f"exception is: {e}")
        # TODO: add a validation step with an expected DatetimeIndex of freq = result_freq
    return result_srs


def main(args):
    node = args.node
    startdate = args.startdate
    enddate = args.enddate
    market = args.market
    tz_in = args.tz_in
    tz_query = args.tz_query
    store_path = args.store_path

    result = scrape_daterange(node=node,  # 'SLAP_PGEB-APND',
                              startdate=startdate,  # datetime(2017, 1, 1),
                              enddate=enddate,  # datetime(2017, 1, 16),
                              market=market,  # 'RT15',
                              tz_in=tz_in,
                              tz_query=tz_query,
                              store_path=store_path)


if __name__ == '__main__':
    # example command line params:
    # --node "DLAP_SCE-APND" --startdate "2017-03-29" --enddate "2019-10-20" --market "RT5"
    args = parse_args()
    main(args)
    