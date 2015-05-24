# query for data
'''
SELECT date, lat, lon, cnt from (
  SELECT date, lat, lon, COUNT(*) as cnt
  FROM (
    SELECT REGEXP_REPLACE(STRING(date), '(....)(..)(..)(..)(..)(..)', r'\1-\2-\3 \4:\5:\6') date,
           REGEXP_REPLACE(
              REGEXP_EXTRACT(
                SPLIT(V2Locations, ';'), 
                r'^[2-5]#.*?#.*?#.*?#.*?#(.*?#.*?)#'),
              '^(.*?)#(.*?)$', '\\1') AS lat,
           REGEXP_REPLACE(
              REGEXP_EXTRACT(
                SPLIT(V2Locations, ';'), 
                r'^[2-5]#.*?#.*?#.*?#.*?#(.*?#.*?)#'),
                '^(.*?)#(.*?)$', '\\2') AS lon,
    FROM [gdelt-bq:gdeltv2.gkg@-86400000-]
  )
  WHERE lat is not null
  GROUP BY date, lat, lon
  ORDER BY date DESC
)
WHERE cnt >= 3
limit 10000;
'''
import sys
import numpy
import datetime as dt
from collections import defaultdict

import matplotlib
# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap

def main():
    data = numpy.genfromtxt("data.csv", dtype=None, delimiter=',', names=True) 
#lat - data[x][1]
#lon - data[x][2]
    print(data)
    
    # Note that we're drawing on a regular matplotlib figure, so we set the 
    # figure size just like we would any other.
    plt.figure(figsize=(28,18))
  
    # Create the Basemap
    event_map = Basemap(projection='merc', 
                        resolution='l', area_thresh=1000.0, # Low resolution
                        lat_0 = 55.0, lon_0=60.0, # Map center 
                        llcrnrlon=-179, llcrnrlat=-72, # Lower left corner
                        urcrnrlon=179, urcrnrlat=78) # Upper right corner
                        
    # Draw important features
    event_map.drawcoastlines() 
    event_map.drawcountries()
    event_map.fillcontinents(color='0.8') # Light gray
    event_map.drawmapboundary()
    
    # Draw the points on the map:
    for i in range(len(data)-8000):
        x, y = event_map(data[i][2], data[i][1]) # Convert lat, long to y,x
        marker_size = data[i][3]/4
        event_map.plot(x,y, 'ro', markersize=marker_size, alpha=0.3)
    
    plt.savefig('map.png', bbox_inches='tight')

if __name__ == '__main__':
    main()