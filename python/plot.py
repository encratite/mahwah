from datetime import datetime
import json
import sys

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter

def render_equity_curve():
	input = sys.stdin.read()
	decoded = json.loads(input)
	date_format = "%Y-%m-%d"
	dates = [datetime.strptime(t, date_format) for t in decoded["dates"]]
	returns = decoded["returns"]
	DATE = "date"
	RETURNS = "returns"
	df = pd.DataFrame({
		DATE: dates,
		RETURNS: returns,
	})
	plt.figure(figsize=(12, 8))
	sns.lineplot(data=df, x=DATE, y=RETURNS)
	plt.title("Equity Curve")
	plt.xlabel("Date")
	plt.ylabel("Return")
	ax = plt.gca()
	formatter = PercentFormatter(xmax=1, decimals=0)
	ax.yaxis.set_major_formatter(formatter)
	plt.tight_layout()
	plt.show()

def format_percentage(value):
	if value == 0:
		return "0%"
	else:
		return f"{value:+.0f}%"

render_equity_curve()