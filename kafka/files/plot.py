import pandas as pd
import matplotlib.pyplot as plt
import json
import os

months = ['January', 'February', 'March']
data = {month: {} for month in months}

for i in range(4):
    filename = f'/files/partition-{i}.json'
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            json_data = json.load(file)
            for month in months:
                if month in json_data:
                    for year, details in json_data[month].items():
                        if not data[month].get('year') or int(year) > int(data[month]['year']):
                            data[month]['year'] = year
                            data[month]['avg'] = details['avg']

month_series = pd.Series({f"{month}-{data[month].get('year', 'N/A')}": data[month].get('avg', 0) for month in months})

fig, ax = plt.subplots()
month_series.plot.bar(ax=ax, color='skyblue')
ax.set_title('Month Averages')
ax.set_ylabel('Avg. Max Temperature')
ax.set_xlabel('Month and Year')
plt.tight_layout()
plt.savefig("/files/month.svg")
