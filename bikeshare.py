#!/usr/bin/python3
import argparse
import collections
import json
import numpy as np
import os
import plotly
import plotly.plotly as py
import pymysql
import pytz

from datetime import timezone
from dateutil import tz
from plotly.graph_objs import Layout,Scatter
from urllib.request import urlopen

'''
import pymysql
RDS_URL = "bmorebikeshare.cyi1fe7cdnil.us-east-1.rds.amazonaws.com"
db = pymysql.connect(RDS_URL, "root", "abcd1234", "bmorebikeshare")
cursor = db.cursor()
cursor.execute("")
'''

RDS_URL = "bmorebikeshare.cyi1fe7cdnil.us-east-1.rds.amazonaws.com"

BikeData = collections.namedtuple("bike_data", ("max_extra_bikes, stocking_full,"
            "name, primary_locked_cycle_count, stocking_low, "
            "total_locked_cycle_count, free_dockes, free_spaces"))

class BikeShare:
    def __init__(self, station_url, table_name):
        self.station_url = "{}/stations/stations".format(station_url)
        self.db = pymysql.connect(RDS_URL, "root", "abcd1234", "bmorebikeshare")
        self.cursor = self.db.cursor()
        self.table_name = self.db.escape_string(table_name)
        self.working_dir = os.path.join('/', 'var', 'www', 'html', self.table_name)
        self.totals = {'max_extra_bikes':0,
                       'stocking_full':0,
                       'primary_locked_cycle_count':0, 
                       'stocking_low':0,
                       'total_locked_cycle_count':0,
                       'free_dockes':0,
                       'free_spaces':0
                       }
        if not os.path.exists(self.working_dir):
            os.makedirs(self.working_dir)


    def _run_sql(self, command, *args):
        try:
            print(command, *args)
            self.cursor.execute(command, *args)
            self.db.commit()
        except pymysql.err.ProgrammingError as ex:
            self.db.rollback
            raise

    def _utc_to_local(self, utc_dt):
        utc = tz.gettz('UTC')
        return utc_dt.replace(tzinfo=utc).astimezone(tz.gettz('America/New_York')).replace(tzinfo=utc)

    def _make_filename_safe(self, filename):
        return "".join([c for c in filename if c.isalpha() or c.isdigit() or c==' ']).rstrip()

    def create_table(self):
        print("Entering create_table")
        self._run_sql(("CREATE TABLE {} ("
                            "eventdatetime DATETIME,"
                            "max_extra_bikes INT, "
                            "stocking_full INT, "
                            "name VARCHAR(60), "
                            "primary_locked_cycle_count INT, "
                            "stocking_low INT, "
                            "total_locked_cycle_count INT, "
                            "free_dockes INT, "
                            "free_spaces INT )".format(self.table_name)))

    def scrape_data(self):
        request = urlopen(self.station_url)
        data = request.read()
        encoding = request.info().get_content_charset('utf-8')

        bike_tuples = [BikeData(x['max_extra_bikes'],
                                x['stocking_full'],
                                x['name'],
                                x['primary_locked_cycle_count'], 
                                x['stocking_low'],
                                x['total_locked_cycle_count'],
                                x['free_dockes'],
                                x['free_spaces']) for x in json.loads(data.decode(encoding))]

        return bike_tuples

    def save_data(self, bikedatatuple):

        print("Entering save_data")
        self._run_sql(("INSERT INTO {}(eventdatetime, max_extra_bikes, stocking_full, name, "
                       "primary_locked_cycle_count, stocking_low, total_locked_cycle_count, "
                       "free_dockes, free_spaces) VALUES (NOW(), %s, %s, %s, %s, %s, %s, %s, %s)"
                       "".format(self.table_name)),
                            (bikedatatuple.max_extra_bikes, 
                            bikedatatuple.stocking_full, 
                            bikedatatuple.name, 
                            bikedatatuple.primary_locked_cycle_count, 
                            bikedatatuple.stocking_low, 
                            bikedatatuple.total_locked_cycle_count, 
                            bikedatatuple.free_dockes, 
                            bikedatatuple.free_spaces,)
                            )

    def add_to_total(self, bikedatatuple):
        for x in bikedatatuple._fields:
            if type(bikedatatuple.__getattribute__(x)) == type(int()):
                self.totals[x]+=bikedatatuple.__getattribute__(x)

    def save_total(self):
        self.save_data(BikeData(**self.totals, name='Totals'))

    def populate_current_values(self):
        for dock in self.scrape_data():
            self.add_to_total(dock)
            self.save_data(dock)
        self.save_total()

    def generate_graphs(self):
        self._run_sql("SELECT DISTINCT name FROM {} ORDER BY name ASC".format(self.table_name))

        locations = self.cursor.fetchall()
            
        for location in locations:
            print("Generating {}".format(location[0]))
            self._run_sql(("SELECT eventdatetime,total_locked_cycle_count FROM {} WHERE "
                           "name=%s".format(self.table_name)), location[0])
            results = self.cursor.fetchall()

            # Create chart 
            data = Scatter(x=[self._utc_to_local(x[0]) for x in results],
                                                  y=[x[1] for x in results])
            filename = self._make_filename_safe(location[0])
            plotly.offline.plot({"data": [data], 
                                 "layout": Layout(title=location[0],
                                                  font=dict(family='Courier New, monospace', 
                                                  size=18, color='rgb(0,0,0)'))
                                },
                                filename=os.path.join(self.working_dir, 
                                                    "{}.html".format(filename)))
            if args.generate_images:
                # the free license only allows 100 images per day
                try:
                    py.image.save_as([data], filename=os.path.join(self.working_dir, 
                                                            "{}.png".format(filename)), format='png')
                except plotly.exceptions.PlotlyRequestError as ex:
                    print(ex)

        side_menu = ""
        graph_images = ""
        with open(os.path.join(self.working_dir, 'index.html'), 'w') as dashboard:
            for location in locations:
                location = self._make_filename_safe(location[0])
                if location == 'Totals':
                    continue

                side_menu += "  <li><a href=\"#{0}\">{0}</a></li>\n".format(location)
                graph_images += "  <a name=\"{0}\"><h1>{0}</h1></a>\n  <p><a href=\"{0}.html\"><img src=\"{0}.png\"></a></p>\n".format(location)
            dashboard.write(
"""<!DOCTYPE html>
<html>
<head>
<style> 
.flex-container {{
    display: -webkit-flex;
    display: flex;  
    -webkit-flex-flow: row wrap;
    flex-flow: row wrap;
    text-align: center;
}}

.flex-container > * {{
    padding: 15px;
    -webkit-flex: 1 100%;
    flex: 1 100%;
}}

.article {{
    text-align: left;
}}

header {{background: black;color:white;}}
footer {{background: #aaa;color:white;}}
.nav {{background:#eee;}}

.nav ul {{
    list-style-type: none;
    padding: 0;
}}
.nav ul a {{
    text-decoration: none;
}}

@media all and (min-width: 768px) {{
    .nav {{text-align:left;-webkit-flex: 1 auto;flex:1 auto;-webkit-order:1;order:1;}}
    .article {{-webkit-flex:5 0px;flex:5 0px;-webkit-order:2;order:2;}}
    footer {{-webkit-order:3;order:3;}}
}}
</style>
</head>
<body>

<div class="flex-container">
<header>
  <h1>{}.bike.share</h1>
</header>

<nav class="nav">
<ul>
  <li><a href="#Totals">Totals</a></li>
  {}
</ul>
</nav>

<article class="article">
  <h1>Totals</h1>
  <p><a href="Totals.html"><img src="Totals.png"></a></p>
  {}
</article>

<footer>Coded by Brian Seel (brian dot seel at gmail)</footer>
</div>

</body>
</html>""".format(self.table_name[10:], side_menu, graph_images))


parser = argparse.ArgumentParser()
parser.add_argument("--table",
                    dest="table",
                    required=True, 
                    help="Creates the database table")
parser.add_argument("--create-table",
                    dest="create_table",
                    action="store_true", 
                    help="Creates the database table")
parser.add_argument("--generate-graphs",
                    dest="generate_graphs",
                    action="store_true", 
                    help="Generates the graphs based on the data from the database")
parser.add_argument("--generate-images",
                    dest="generate_images",
                    action="store_true", 
                    help="Generates the thumbnails as well")
parser.add_argument("--scrape-data",
                    dest="scrape_data",
                    action="store_true", 
                    help=("Scrapes the data from the bike share site. This is the default if no "
                          "options are given"))
parser.add_argument("--base-url",
                    dest="base_url",
                    default="http://www.bmorebikeshare.com",
                    help="Base url to query. Form of http://www.url.com")
args = parser.parse_args()

bikeshare = BikeShare(args.base_url, args.table)
if args.create_table:
    bikeshare.create_table()
if args.scrape_data or not (args.create_table or args.generate_graphs):
    bikeshare.populate_current_values()
if args.generate_graphs or args.generate_images:
    bikeshare.generate_graphs()